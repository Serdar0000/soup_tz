import os
from pathlib import Path

import asyncpg

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://soup:soup@localhost:5432/soup")
SCHEMA = (Path(__file__).parent / "schema.sql").read_text()


async def create_pool() -> asyncpg.Pool:
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=int(os.environ.get("DB_POOL_SIZE", "20")))
    async with pool.acquire() as conn:
        # advisory lock so several app replicas can start at once
        await conn.execute("SELECT pg_advisory_lock(42)")
        try:
            await conn.execute(SCHEMA)
        finally:
            await conn.execute("SELECT pg_advisory_unlock(42)")
    return pool
