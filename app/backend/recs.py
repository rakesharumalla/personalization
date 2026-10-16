"""Recommendation hot path.

    Lakebase: user_features_online + session_state (last 5 clicks)   ~ few ms
    Model Serving: two-stage recommender (champion | challenger)       ~ tens of ms
    Lakebase: product_catalog enrichment                               ~ few ms
    fallback -> Lakebase precomputed_recs (user, then segment) if serving is slow/unavailable
"""
from __future__ import annotations

import asyncio
import json
import time
import uuid

import httpx

from . import config, db

S, A = config.SERVING_SCHEMA, config.APP_SCHEMA
_http: httpx.AsyncClient | None = None


def http() -> httpx.AsyncClient:
    global _http
    if _http is None:
        _http = httpx.AsyncClient(timeout=config.MODEL_TIMEOUT_S,
                                  limits=httpx.Limits(max_keepalive_connections=20, max_connections=50))
    return _http


def _jsonable(row: dict | None) -> dict:
    if not row:
        return {}
    out = {}
    for k, v in row.items():
        if hasattr(v, "isoformat"):
            v = v.isoformat()
        elif isinstance(v, (list, tuple)):
            v = list(v)
        elif v is not None and not isinstance(v, (str, int, float, bool)):
            v = float(v)
        out[k] = v
    return out


async def user_features(user_id: str) -> dict:
    return _jsonable(await db.fetchone(f"SELECT * FROM {S}.user_features_online WHERE user_id = %s", (user_id,)))


async def session_items(session_id: str, n: int = 5) -> list[str]:
    rows = await db.fetch(
        f"""SELECT product_id FROM {A}.session_state
            WHERE session_id = %s AND product_id IS NOT NULL ORDER BY ts DESC, id DESC LIMIT 20""", (session_id,))
    return list(dict.fromkeys(r["product_id"] for r in rows))[:n]


async def catalog(pids: list[str]) -> dict[str, dict]:
    if not pids:
        return {}
    rows = await db.fetch(
        f"""SELECT product_id, title, description, category, brand, price, image_url, price_tier, tags
            FROM {S}.product_catalog WHERE product_id = ANY(%s)""", (pids,))
    return {r["product_id"]: _jsonable(r) for r in rows}


async def call_model(user_id: str, items: list[str], uf: dict, k: int, variant: str) -> dict:
    w = config.ws()
    headers = await asyncio.to_thread(w.config.authenticate)
    url = f"{w.config.host}/serving-endpoints/{config.RECOMMENDER_ENDPOINT}"
    url += "/invocations" if variant == "champion" else f"/served-models/{variant}/invocations"
    body = {"dataframe_records": [{"user_id": user_id, "session_items": json.dumps(items),
                                   "user_features": json.dumps(uf, default=str), "k": k}]}
    r = await http().post(url, json=body, headers=headers)
    r.raise_for_status()
    pred = r.json()["predictions"][0]
    if isinstance(pred, dict) and "predictions" in pred:  # named pandas Series -> {"predictions": "<json>"}
        pred = pred["predictions"]
    return json.loads(pred) if isinstance(pred, str) else pred


async def precomputed(user_id: str, uf: dict) -> tuple[list[dict], str]:
    keys = [user_id, f"segment:{uf.get('acquisition_channel')}" if uf.get("acquisition_channel") else None,
            "segment:global"]
    for key in [k for k in keys if k]:
        row = await db.fetchone(f"SELECT recs_json FROM {S}.precomputed_recs WHERE user_id = %s", (key,))
        if row:
            return json.loads(row["recs_json"]), key
    return [], "none"


async def recommend(user_id: str, session_id: str, k: int = 10, variant: str = "champion",
                    personalized: bool = True, app_user: str | None = None) -> dict:
    t0 = time.perf_counter()
    timings = {}
    uf, items = await asyncio.gather(user_features(user_id), session_items(session_id))
    timings["feature_lookup_ms"] = round((time.perf_counter() - t0) * 1000, 1)
    served_by, model_variant, error = None, variant, None
    recs: list[dict] = []
    t1 = time.perf_counter()
    if personalized:
        try:
            out = await call_model(user_id, items, uf, k + len(items), variant)
            recs, model_variant, served_by = out["recommendations"], out.get("model_variant", variant), "model_serving"
        except Exception as e:  # noqa: BLE001 — timeouts, 5xx, scale-from-zero
            error = f"{type(e).__name__}: {str(e)[:160]}"
    if not recs:
        recs, key = await precomputed(user_id if personalized else "segment:global", uf if personalized else {})
        served_by = ("precomputed_fallback" if personalized else "popularity") + f" ({key})"
        model_variant = "popularity" if not personalized or key.startswith("segment") else "champion (batch)"
    timings["model_ms"] = round((time.perf_counter() - t1) * 1000, 1)
    recs = [r for r in recs if r["product_id"] not in set(items)][:k]
    t2 = time.perf_counter()
    cat = await catalog([r["product_id"] for r in recs])
    timings["catalog_ms"] = round((time.perf_counter() - t2) * 1000, 1)
    timings["total_ms"] = round((time.perf_counter() - t0) * 1000, 1)
    request_id = uuid.uuid4().hex
    enriched = [{**r, **cat.get(r["product_id"], {}), "position": i} for i, r in enumerate(recs)]
    asyncio.create_task(_log_impressions(request_id, user_id, session_id, enriched, model_variant, served_by,
                                         timings["total_ms"], app_user))
    return {"request_id": request_id, "user_id": user_id, "session_items": items, "served_by": served_by,
            "model_variant": model_variant, "timings": timings, "error": error,
            "user_features": uf, "recommendations": enriched}


async def _log_impressions(request_id, user_id, session_id, recs, variant, served_by, latency, app_user):
    try:
        await db.executemany(
            f"""INSERT INTO {A}.recommendation_log
                (request_id, user_id, session_id, event, product_id, position, model_variant, source,
                 latency_ms, served_by, app_user)
                VALUES (%s, %s, %s, 'impression', %s, %s, %s, %s, %s, %s, %s)""",
            [(request_id, user_id, session_id, r["product_id"], r["position"], variant, r.get("source"), latency,
              served_by, app_user) for r in recs])
    except Exception as e:  # noqa: BLE001
        print("impression log failed:", e)


async def record_event(user_id: str, session_id: str, event_type: str, product_id: str | None,
                       request_id: str | None, position: int | None, model_variant: str | None,
                       app_user: str | None):
    await db.execute(f"INSERT INTO {A}.session_state (session_id, user_id, event_type, product_id) "
                     "VALUES (%s, %s, %s, %s)", (session_id, user_id, event_type, product_id))
    if request_id:
        ev = {"view": "click"}.get(event_type, event_type)
        await db.execute(
            f"""INSERT INTO {A}.recommendation_log (request_id, user_id, session_id, event, product_id, position,
                model_variant, app_user) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            (request_id, user_id, session_id, ev, product_id, position, model_variant, app_user))
