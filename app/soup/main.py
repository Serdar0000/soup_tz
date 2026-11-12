import hmac
import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import asyncpg
from fastapi import Depends, FastAPI, HTTPException, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

from . import auth, ingest, stream
from .auth import Principal, read_principal, write_principal
from .db import create_pool

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"),
                    format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("soup")

ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "")
TEST_HOOKS = os.environ.get("SOUP_TEST_HOOKS") == "1"
STATIC = Path(__file__).parent / "static"
NOT_FOUND = "run not found"
# The database is unreachable or restarting: the request was NOT applied (the transaction
# never committed), so tell the client to retry. A 500 is "don't retry" for many SDKs,
# which would turn a DB blip into silently dropped batches on the client side.
DB_UNAVAILABLE = (OSError, asyncpg.OperatorInterventionError, asyncpg.PostgresConnectionError,
                  asyncpg.ConnectionDoesNotExistError)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await create_pool()
    app.state.hub = stream.Hub()
    await app.state.hub.start()
    if TEST_HOOKS:
        log.warning("SOUP_TEST_HOOKS=1: test-only endpoints are enabled")
    yield
    await app.state.hub.stop()
    await app.state.pool.close()


app = FastAPI(lifespan=lifespan, title="soup tracker")


# ---------------------------------------------------------------- middleware / errors

@app.middleware("http")
async def access_log_and_headers(request: Request, call_next):
    t0 = time.perf_counter()
    rid = uuid.uuid4().hex[:12]
    try:
        resp = await call_next(request)
    except DB_UNAVAILABLE as e:
        log.warning("rid=%s database unavailable: %s", rid, type(e).__name__)
        resp = JSONResponse({"detail": "service unavailable, retry", "request_id": rid},
                            status_code=503, headers={"Retry-After": "1"})
    except Exception:
        # never leak stack traces or SQL to the client
        log.exception("rid=%s unhandled error", rid)
        resp = JSONResponse({"detail": "internal error", "request_id": rid}, status_code=500)
    resp.headers["Cache-Control"] = "no-store"
    resp.headers["Vary"] = "Authorization, Cookie"
    resp.headers["X-Request-Id"] = rid
    p = getattr(request.state, "principal", None)
    # path only: no query string, no headers, no body. Keys only by prefix.
    log.info("rid=%s %s %s %d %.1fms key=%s", rid, request.method, request.url.path,
             resp.status_code, (time.perf_counter() - t0) * 1000, p.prefix if p else "-")
    return resp


@app.exception_handler(RequestValidationError)
async def validation_error(_request: Request, exc: RequestValidationError):
    # echo locations/messages, not the submitted values
    errs = [{"loc": e["loc"], "msg": e["msg"]} for e in exc.errors()]
    return JSONResponse({"detail": errs}, status_code=422)


def parse_run_id(run_id: str) -> uuid.UUID:
    # malformed ids get the same 404 as foreign/non-existent ones
    try:
        return uuid.UUID(run_id)
    except ValueError:
        raise HTTPException(404, NOT_FOUND) from None


# ------------------------------------------------------------------- run cache
# (project_id, run_id) -> run row. Keyed by project so A's hit can never answer B.
_RUN_CACHE: dict[tuple[uuid.UUID, uuid.UUID], tuple[float, dict]] = {}
RUN_CACHE_TTL = 30.0


async def get_run(request: Request, p: Principal, run_id: uuid.UUID) -> dict:
    k = (p.project_id, run_id)
    hit = _RUN_CACHE.get(k)
    if hit and hit[0] > time.monotonic():
        return hit[1]
    row = await request.app.state.pool.fetchrow(
        "SELECT id, name, created_at FROM runs WHERE id = $1 AND project_id = $2", run_id, p.project_id,
    )
    if row is None:
        raise HTTPException(404, NOT_FOUND)
    run = {"id": str(row["id"]), "name": row["name"], "created_at": row["created_at"].isoformat()}
    _RUN_CACHE[k] = (time.monotonic() + RUN_CACHE_TTL, run)
    return run


# ------------------------------------------------------------------- admin / keys

class NewProject(BaseModel):
    name: str = Field(min_length=1, max_length=200)


@app.post("/admin/projects", status_code=201)
async def create_project(body: NewProject, request: Request):
    tok = request.headers.get("x-admin-token", "")
    if not ADMIN_TOKEN or not hmac.compare_digest(tok, ADMIN_TOKEN):
        raise HTTPException(401, "bad admin token")
    pid = uuid.uuid4()
    async with request.app.state.pool.acquire() as conn, conn.transaction():
        await conn.execute("INSERT INTO projects (id, name) VALUES ($1, $2)", pid, body.name)
        key = await auth.create_key(conn, pid)
    return {"project_id": str(pid), "api_key": key}


class Rotate(BaseModel):
    grace_seconds: int = Field(default=0, ge=0, le=3600)


@app.post("/keys/rotate")
async def rotate_key(request: Request, body: Rotate | None = None, p: Principal = Depends(write_principal)):
    grace = body.grace_seconds if body else 0
    async with request.app.state.pool.acquire() as conn, conn.transaction():
        await conn.execute(
            "UPDATE api_keys SET revoked_at = now() + make_interval(secs => $2) WHERE id = $1",
            p.key_id, grace,
        )
        key = await auth.create_key(conn, p.project_id)
    log.info("key %s rotated (grace=%ss)", p.prefix, grace)
    return {"api_key": key, "old_key_valid_for_seconds": grace}


class Login(BaseModel):
    api_key: str


@app.post("/session")
async def login(body: Login, request: Request, response: Response):
    p = await auth.lookup(request.app.state.pool, body.api_key)
    if p is None:
        raise HTTPException(401, "invalid api key")
    response.set_cookie(auth.COOKIE_NAME, body.api_key, httponly=True, samesite="strict",
                        secure=os.environ.get("COOKIE_SECURE") == "1", max_age=86400)
    return {"project_id": str(p.project_id)}


@app.delete("/session")
async def logout(response: Response):
    response.delete_cookie(auth.COOKIE_NAME)
    return {"ok": True}


# ------------------------------------------------------------------- runs

class NewRun(BaseModel):
    name: str = Field(min_length=1, max_length=200)


@app.post("/runs", status_code=201)
async def create_run(body: NewRun, request: Request, p: Principal = Depends(write_principal)):
    rid = uuid.uuid4()
    row = await request.app.state.pool.fetchrow(
        "INSERT INTO runs (id, project_id, name) VALUES ($1, $2, $3) RETURNING created_at",
        rid, p.project_id, body.name,
    )
    return {"id": str(rid), "name": body.name, "created_at": row["created_at"].isoformat()}


@app.get("/runs")
async def list_runs(request: Request, p: Principal = Depends(read_principal)):
    rows = await request.app.state.pool.fetch(
        "SELECT id, name, created_at, last_seq FROM runs WHERE project_id = $1 ORDER BY created_at DESC LIMIT 500",
        p.project_id,
    )
    return {"runs": [{"id": str(r["id"]), "name": r["name"], "created_at": r["created_at"].isoformat(),
                      "points": r["last_seq"]} for r in rows]}


@app.get("/runs/{run_id}")
async def read_run(run_id: str, request: Request, p: Principal = Depends(read_principal)):
    return await get_run(request, p, parse_run_id(run_id))


@app.get("/runs/{run_id}/metrics")
async def read_metrics(run_id: str, request: Request, p: Principal = Depends(read_principal)):
    rid = parse_run_id(run_id)
    async with request.app.state.pool.acquire() as conn, conn.transaction(isolation="repeatable_read"):
        last_seq = await conn.fetchval("SELECT last_seq FROM runs WHERE id = $1 AND project_id = $2", rid, p.project_id)
        if last_seq is None:
            raise HTTPException(404, NOT_FOUND)
        rows = await conn.fetch("SELECT seq, key, step, value, ts FROM metrics WHERE run_id = $1 ORDER BY seq", rid)
    return {"last_seq": last_seq, "points": [
        {"seq": r["seq"], "key": r["key"], "step": r["step"], "value": r["value"],
         "ts": r["ts"].isoformat() if r["ts"] else None} for r in rows]}


@app.post("/runs/{run_id}/metrics")
async def post_metrics(run_id: str, request: Request, p: Principal = Depends(write_principal)):
    rid = parse_run_id(run_id)
    # parse body only after auth + id check, so unauthenticated callers learn nothing
    raw = await request.body()
    if len(raw) > 2_000_000:
        raise HTTPException(413, "batch too large")
    try:
        batch = ingest.Batch.model_validate_json(raw)
    except ValueError as e:
        errs = [{"loc": x["loc"], "msg": x["msg"]} for x in getattr(e, "errors", lambda: [])()]
        return JSONResponse({"detail": errs or "invalid body"}, status_code=422)
    if len(batch.points) > ingest.MAX_BATCH:
        raise HTTPException(413, f"max {ingest.MAX_BATCH} points per batch")
    fault = request.headers.get("x-test-fault") if TEST_HOOKS else None
    async with request.app.state.pool.acquire() as conn:
        try:
            res = await ingest.ingest(conn, p.project_id, rid, batch.points, fault=fault)
        except ingest.RunNotFound:
            raise HTTPException(404, NOT_FOUND) from None
    return res.__dict__


@app.get("/runs/{run_id}/stream")
async def run_stream(run_id: str, request: Request, after: int = 0, p: Principal = Depends(read_principal)):
    rid = parse_run_id(run_id)
    await get_run(request, p, rid)  # 404 before any stream bytes
    # EventSource reconnects send Last-Event-ID; it wins over the ?after of the original URL
    lei = request.headers.get("last-event-id")
    if lei and lei.isdigit():
        after = int(lei)
    gen = stream.event_stream(request.app.state.pool, request.app.state.hub, p, rid, max(after, 0),
                              request.is_disconnected)
    return StreamingResponse(gen, media_type="text/event-stream",
                             headers={"X-Accel-Buffering": "no"})


# ------------------------------------------------------------------- UI

@app.get("/", include_in_schema=False)
async def ui_index():
    return FileResponse(STATIC / "index.html")


@app.get("/r/{run_id}", include_in_schema=False)
async def ui_run(run_id: str):
    return FileResponse(STATIC / "run.html")


@app.get("/healthz", include_in_schema=False)
async def healthz(request: Request):
    await request.app.state.pool.fetchval("SELECT 1")
    return {"ok": True, "subscribers": request.app.state.hub.subscriber_count(),
            "listener_connects": request.app.state.hub.listener_connects}


# ------------------------------------------------------------------- test hooks

if TEST_HOOKS:
    @app.post("/_test/drop-streams", include_in_schema=False)
    async def drop_streams(request: Request):
        # via NOTIFY, so every replica drops its streams, not just the one serving this request
        await request.app.state.pool.execute("SELECT pg_notify('soup_control', 'drop-streams')")
        return {"ok": True}
