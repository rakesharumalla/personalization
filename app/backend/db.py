"""Async Lakebase (Postgres) pool with OAuth token refresh.

In Databricks Apps the platform injects PGHOST/PGUSER/PGDATABASE for the
``postgres`` app resource; locally they are derived from the Lakebase endpoint.
Every new physical connection gets a fresh-enough OAuth token (cached ~45 min).
"""
from __future__ import annotations

import os
import time

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from . import config

_token: dict = {"value": None, "exp": 0.0}
pool: AsyncConnectionPool | None = None


def _get_token() -> str:
    if not _token["value"] or time.time() > _token["exp"]:
        cred = config.ws().postgres.generate_database_credential(endpoint=config.LAKEBASE_ENDPOINT)
        _token.update(value=cred.token, exp=time.time() + 45 * 60)
    return _token["value"]


class _TokenConnection(psycopg.AsyncConnection):
    @classmethod
    async def connect(cls, conninfo: str = "", **kwargs):
        kwargs["password"] = _get_token()
        return await super().connect(conninfo, **kwargs)


def _conn_kwargs() -> dict:
    host = os.getenv("PGHOST")
    user = os.getenv("PGUSER")
    if not host:
        host = config.ws().postgres.get_endpoint(config.LAKEBASE_ENDPOINT).status.hosts.host
    if not user:
        user = config.ws().current_user.me().user_name
    return {"host": host, "port": int(os.getenv("PGPORT", "5432")), "dbname": os.getenv("PGDATABASE", "databricks_postgres"),
            "user": user, "sslmode": "require", "row_factory": dict_row, "autocommit": True,
            "application_name": "pers-personalization-app"}


MIGRATIONS = [
    f"CREATE SCHEMA IF NOT EXISTS {config.APP_SCHEMA}",
    f"""CREATE TABLE IF NOT EXISTS {config.APP_SCHEMA}.session_state (
        id BIGSERIAL PRIMARY KEY,
        session_id TEXT NOT NULL,
        user_id TEXT NOT NULL,
        event_type TEXT NOT NULL,
        product_id TEXT,
        ts TIMESTAMPTZ NOT NULL DEFAULT now())""",
    f"CREATE INDEX IF NOT EXISTS session_state_sess_idx ON {config.APP_SCHEMA}.session_state (session_id, ts DESC)",
    f"""CREATE TABLE IF NOT EXISTS {config.APP_SCHEMA}.recommendation_log (
        log_id BIGSERIAL PRIMARY KEY,
        request_id TEXT NOT NULL,
        ts TIMESTAMPTZ NOT NULL DEFAULT now(),
        user_id TEXT NOT NULL,
        session_id TEXT,
        event TEXT NOT NULL,            -- impression | click | add_to_cart | purchase
        product_id TEXT,
        position INT,
        model_variant TEXT,
        source TEXT,
        latency_ms DOUBLE PRECISION,
        served_by TEXT,                 -- model_serving | precomputed_fallback | popularity
        app_user TEXT)""",
    f"CREATE INDEX IF NOT EXISTS reclog_ts_idx ON {config.APP_SCHEMA}.recommendation_log (ts DESC)",
    f"""CREATE TABLE IF NOT EXISTS {config.APP_SCHEMA}.explanation_cache (
        user_id TEXT NOT NULL,
        product_id TEXT NOT NULL,
        explanation TEXT NOT NULL,
        model TEXT,
        created_ts TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (user_id, product_id))""",
]


async def open_pool():
    global pool
    pool = AsyncConnectionPool(conninfo="", kwargs=_conn_kwargs(), connection_class=_TokenConnection,
                               min_size=2, max_size=10, max_lifetime=40 * 60, open=False)
    await pool.open(wait=True, timeout=60)
    async with pool.connection() as conn:
        for stmt in MIGRATIONS:
            try:
                await conn.execute(stmt)
            except psycopg.errors.InsufficientPrivilege as e:
                # local dev as a non-owner: objects already exist, owned by the app's service principal
                print("migration skipped (not owner):", str(e).splitlines()[0])


async def close_pool():
    if pool:
        await pool.close()


async def fetch(sql: str, params=None) -> list[dict]:
    async with pool.connection() as conn:
        cur = await conn.execute(sql, params)
        return await cur.fetchall() if cur.description else []


async def fetchone(sql: str, params=None) -> dict | None:
    rows = await fetch(sql, params)
    return rows[0] if rows else None


async def execute(sql: str, params=None):
    async with pool.connection() as conn:
        await conn.execute(sql, params)


async def executemany(sql: str, rows: list):
    if not rows:
        return
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.executemany(sql, rows)
