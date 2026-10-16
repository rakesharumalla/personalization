"""Databricks SQL (statement execution API) with a small TTL cache for console KPIs."""
from __future__ import annotations

import asyncio
import time

from databricks.sdk.service.sql import Disposition, Format, StatementState

from . import config

_cache: dict[str, tuple[float, list[dict]]] = {}


def _run(statement: str) -> list[dict]:
    w = config.ws()
    r = w.statement_execution.execute_statement(statement=statement, warehouse_id=config.WAREHOUSE_ID,
                                                wait_timeout="50s", disposition=Disposition.INLINE,
                                                format=Format.JSON_ARRAY)
    while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(1)
        r = w.statement_execution.get_statement(r.statement_id)
    if r.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(r.status.error.message if r.status.error else str(r.status.state))
    cols = [c.name for c in r.manifest.schema.columns]
    types = [c.type_name.value if c.type_name else "STRING" for c in r.manifest.schema.columns]
    rows = []
    for raw in (r.result.data_array or []) if r.result else []:
        row = {}
        for c, t, v in zip(cols, types, raw):
            if v is not None and t in ("INT", "LONG", "SHORT", "BYTE"):
                v = int(v)
            elif v is not None and t in ("DOUBLE", "FLOAT", "DECIMAL"):
                v = float(v)
            elif v is not None and t == "BOOLEAN":
                v = v == "true"
            row[c] = v
        rows.append(row)
    return rows


async def query(statement: str, ttl: int = 120) -> list[dict]:
    hit = _cache.get(statement)
    if hit and time.time() - hit[0] < ttl:
        return hit[1]
    rows = await asyncio.to_thread(_run, statement)
    _cache[statement] = (time.time(), rows)
    return rows
