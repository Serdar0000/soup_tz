"""Metric ingestion: idempotent, all-or-nothing, gap-free per-run seq."""
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime

import asyncpg
from pydantic import BaseModel, Field

log = logging.getLogger("soup.ingest")

MAX_BATCH = 1000


class Point(BaseModel):
    key: str = Field(min_length=1, max_length=200)
    step: int = Field(ge=0, le=2**62)
    # NaN/Inf would be stored fine by Postgres but break JSON.parse in the browser.
    value: float = Field(allow_inf_nan=False)
    ts: datetime | None = None


class Batch(BaseModel):
    points: list[Point] = Field(min_length=1)


@dataclass
class IngestResult:
    accepted: int
    duplicates: int
    conflicts: int
    last_seq: int


class RunNotFound(Exception):
    pass


class InjectedFault(Exception):
    pass


async def ingest(conn: asyncpg.Connection, project_id: uuid.UUID, run_id: uuid.UUID,
                 points: list[Point], fault: str | None = None) -> IngestResult:
    async with conn.transaction():
        # Row lock on the run serializes writers of THIS run, so seq commit order == seq
        # order and readers that advance a cursor can never skip an uncommitted seq.
        last_seq = await conn.fetchval(
            "SELECT last_seq FROM runs WHERE id = $1 AND project_id = $2 FOR UPDATE",
            run_id, project_id,
        )
        if last_seq is None:
            raise RunNotFound()

        unique: dict[tuple[str, int], Point] = {}
        duplicates = conflicts = 0
        for p in points:
            k = (p.key, p.step)
            if k in unique:
                duplicates += 1
                conflicts += unique[k].value != p.value
            else:
                unique[k] = p

        cand = list(unique.values())
        existing = await conn.fetch(
            """SELECT m.key, m.step, m.value FROM metrics m
               JOIN unnest($2::text[], $3::bigint[]) AS c(key, step) USING (key, step)
               WHERE m.run_id = $1""",
            run_id, [p.key for p in cand], [p.step for p in cand],
        )
        stored = {(r["key"], r["step"]): r["value"] for r in existing}
        new = []
        for p in cand:
            old = stored.get((p.key, p.step))
            if old is None:
                new.append(p)
            else:
                duplicates += 1
                conflicts += old != p.value

        if new:
            seqs = list(range(last_seq + 1, last_seq + 1 + len(new)))
            await conn.execute(
                """INSERT INTO metrics (run_id, seq, key, step, value, ts)
                   SELECT $1, * FROM unnest($2::bigint[], $3::text[], $4::bigint[],
                                            $5::float8[], $6::timestamptz[])""",
                run_id, seqs, [p.key for p in new], [p.step for p in new],
                [p.value for p in new], [p.ts for p in new],
            )
            if fault == "after_insert":
                raise InjectedFault("injected fault after insert, before commit")
            last_seq = seqs[-1]
            await conn.execute("UPDATE runs SET last_seq = $2 WHERE id = $1", run_id, last_seq)
            # NOTIFY is transactional: delivered only if this commits.
            await conn.execute("SELECT pg_notify('run_events', $1)", str(run_id))

    if conflicts:
        log.warning("run=%s conflicting re-sends=%d (first write kept)", run_id, conflicts)
    return IngestResult(accepted=len(new), duplicates=duplicates, conflicts=conflicts, last_seq=last_seq)
