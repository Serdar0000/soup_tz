"""API keys: generation, hashing, lookup. Raw keys are never stored or logged."""
import hashlib
import secrets
import uuid
from dataclasses import dataclass

import asyncpg
from fastapi import HTTPException, Request

COOKIE_NAME = "soup_key"


@dataclass(frozen=True)
class Principal:
    project_id: uuid.UUID
    key_id: uuid.UUID
    prefix: str  # safe to log


def new_key() -> str:
    return "soup_" + secrets.token_urlsafe(32)


def hash_key(raw: str) -> bytes:
    return hashlib.sha256(raw.encode()).digest()


def key_prefix(raw: str) -> str:
    return raw[:9]  # "soup_" + 4 chars


async def create_key(conn: asyncpg.Connection, project_id: uuid.UUID) -> str:
    raw = new_key()
    await conn.execute(
        "INSERT INTO api_keys (id, project_id, key_hash, prefix) VALUES ($1, $2, $3, $4)",
        uuid.uuid4(), project_id, hash_key(raw), key_prefix(raw),
    )
    return raw


# No cache on purpose: rotation/revocation must take effect on the very next request.
async def lookup(pool: asyncpg.Pool, raw: str) -> Principal | None:
    row = await pool.fetchrow(
        """SELECT id, project_id, prefix FROM api_keys
           WHERE key_hash = $1 AND (revoked_at IS NULL OR revoked_at > now())""",
        hash_key(raw),
    )
    if row is None:
        return None
    return Principal(project_id=row["project_id"], key_id=row["id"], prefix=row["prefix"])


async def key_still_valid(pool: asyncpg.Pool, key_id: uuid.UUID) -> bool:
    return bool(await pool.fetchval(
        "SELECT 1 FROM api_keys WHERE id = $1 AND (revoked_at IS NULL OR revoked_at > now())", key_id,
    ))


def _bearer(request: Request) -> str | None:
    h = request.headers.get("authorization", "")
    if h.lower().startswith("bearer "):
        return h[7:].strip() or None
    return None


async def _resolve(request: Request, allow_cookie: bool) -> Principal:
    raw = _bearer(request)
    if raw is None and allow_cookie:
        raw = request.cookies.get(COOKIE_NAME)
    if not raw:
        raise HTTPException(401, "missing api key")
    p = await lookup(request.app.state.pool, raw)
    if p is None:
        raise HTTPException(401, "invalid api key")
    request.state.principal = p
    return p


async def read_principal(request: Request) -> Principal:
    """Reads: header or HttpOnly cookie (the browser's EventSource cannot send headers)."""
    return await _resolve(request, allow_cookie=True)


async def write_principal(request: Request) -> Principal:
    """Writes: header only, so a cookie can never be used for CSRF writes."""
    return await _resolve(request, allow_cookie=False)
