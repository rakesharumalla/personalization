"""FastAPI backend for the Real-Time Personalization app.

Routes (all under /api): recs, events, explain, shoppers, products, similar, kpis/*, genie, agent (SSE),
about, health, img. The built React bundle is served from ./static.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from . import agent, config, db, explain, genie, recs, sql

HERE = Path(__file__).parent
STATIC = HERE / "static"
CONTENT = HERE / "content"
G = f"{config.CATALOG}.gold"
S = config.SERVING_SCHEMA


@asynccontextmanager
async def lifespan(_: FastAPI):
    await db.open_pool()
    yield
    await db.close_pool()


app = FastAPI(title="Real-Time Personalization", lifespan=lifespan)


def app_user(req: Request) -> str | None:
    return req.headers.get("X-Forwarded-Email") or req.headers.get("X-Forwarded-Preferred-Username")


# ----------------------------------------------------------------------------- storefront

@app.get("/api/shoppers")
async def shoppers(limit: int = 4):
    rows = await db.fetch(f"""
        SELECT * FROM (
          SELECT user_id, segment, country, loyalty_tier, top_category, top_brand, n_orders_90d, n_sessions_30d,
                 row_number() OVER (PARTITION BY segment ORDER BY n_sessions_30d DESC, user_id) AS rn
          FROM {S}.user_features_online WHERE has_history) t
        WHERE rn <= %s ORDER BY segment, rn""", (limit,))
    return [recs._jsonable(r) for r in rows]


@app.get("/api/shoppers/{user_id}")
async def shopper(user_id: str):
    return await recs.user_features(user_id) or {"user_id": user_id, "segment": "new", "has_history": False}


@app.post("/api/shoppers/new")
async def new_shopper():
    return {"user_id": f"NEW-{uuid.uuid4().hex[:8].upper()}", "segment": "new", "has_history": False}


@app.get("/api/categories")
async def categories():
    rows = await db.fetch(f"SELECT category, count(*) AS n FROM {S}.product_catalog GROUP BY 1 ORDER BY 1")
    return [recs._jsonable(r) for r in rows]


@app.get("/api/products")
async def products(category: str | None = None, q: str | None = None, limit: int = 24, offset: int = 0):
    where, params = ["TRUE"], []
    if category:
        where.append("category = %s")
        params.append(category)
    if q:
        where.append("(title ILIKE %s OR description ILIKE %s)")
        params += [f"%{q}%", f"%{q}%"]
    rows = await db.fetch(f"""SELECT product_id, title, category, brand, price, image_url, price_tier, popularity_score
                              FROM {S}.product_catalog WHERE {' AND '.join(where)}
                              ORDER BY popularity_score DESC, product_id LIMIT %s OFFSET %s""",
                          (*params, limit, offset))
    return [recs._jsonable(r) for r in rows]


@app.get("/api/products/{product_id}")
async def product(product_id: str):
    p = (await recs.catalog([product_id])).get(product_id)
    if not p:
        raise HTTPException(404, "unknown product")
    return p


@app.get("/api/similar/{product_id}")
async def similar(product_id: str, k: int = 6):
    """'Similar to what you just viewed' — Vector Search over description embeddings."""
    p = (await recs.catalog([product_id])).get(product_id)
    if not p:
        raise HTTPException(404, "unknown product")
    w = config.ws()
    res = await asyncio.to_thread(
        w.vector_search_indexes.query_index, index_name=config.VS_INDEX,
        columns=["product_id", "title", "category", "brand", "price", "image_url"],
        query_text=f"{p['title']}. {p.get('description') or ''}", num_results=k + 1)
    cols = [c.name for c in res.manifest.columns]
    rows = [dict(zip(cols, r)) for r in (res.result.data_array or [])]
    return [r for r in rows if r["product_id"] != product_id][:k]


class RecsRequest(BaseModel):
    user_id: str
    session_id: str
    k: int = 10
    variant: str = "champion"
    compare: bool = False


@app.post("/api/recs")
async def get_recs(body: RecsRequest, request: Request):
    who = app_user(request)
    if body.variant not in ("champion", "challenger"):
        raise HTTPException(400, "variant must be champion or challenger")
    personal = recs.recommend(body.user_id, body.session_id, body.k, body.variant, True, who)
    if not body.compare:
        return {"personalized": await personal}
    p, base = await asyncio.gather(personal, recs.recommend(body.user_id, body.session_id, body.k, body.variant,
                                                            False, who))
    return {"personalized": p, "baseline": base}


class EventRequest(BaseModel):
    user_id: str
    session_id: str
    event_type: str
    product_id: str | None = None
    request_id: str | None = None
    position: int | None = None
    model_variant: str | None = None


@app.post("/api/events")
async def events(body: EventRequest, request: Request):
    if body.event_type not in ("view", "add_to_cart", "purchase", "search"):
        raise HTTPException(400, "bad event_type")
    await recs.record_event(body.user_id, body.session_id, body.event_type, body.product_id, body.request_id,
                            body.position, body.model_variant, app_user(request))
    return {"ok": True}


@app.get("/api/session/{session_id}")
async def session(session_id: str):
    rows = await db.fetch(f"""SELECT s.event_type, s.product_id, s.ts, c.title, c.category
                              FROM {config.APP_SCHEMA}.session_state s
                              LEFT JOIN {S}.product_catalog c USING (product_id)
                              WHERE s.session_id = %s ORDER BY s.ts DESC, s.id DESC LIMIT 20""", (session_id,))
    return [recs._jsonable(r) for r in rows]


class ExplainRequest(BaseModel):
    user_id: str
    product: dict
    user_features: dict = {}
    session_titles: list[str] = []


@app.post("/api/explain")
async def explain_one(body: ExplainRequest):
    try:
        return await explain.explain(body.user_id, body.product, body.user_features, body.session_titles)
    except Exception as e:  # noqa: BLE001
        return {"product_id": body.product.get("product_id"), "explanation": None, "error": str(e)[:200]}


# ----------------------------------------------------------------------------- business console

KPI_SQL = {
    "summary": f"""
        SELECT model_variant, sum(sessions) AS sessions, sum(impressions) AS impressions, sum(clicks) AS clicks,
               sum(clicks) / sum(impressions) AS ctr, sum(converted_sessions) / sum(sessions) AS conversion,
               sum(revenue) / greatest(sum(converted_sessions), 1) AS aov, sum(revenue) AS revenue,
               sum(CASE WHEN is_cold_start THEN converted_sessions END) / sum(CASE WHEN is_cold_start THEN sessions END) AS cold_conversion,
               sum(CASE WHEN is_cold_start THEN clicks END) / sum(CASE WHEN is_cold_start THEN impressions END) AS cold_ctr
        FROM {G}.experiment_results GROUP BY model_variant ORDER BY model_variant""",
    "daily": f"""
        SELECT event_date, model_variant, sum(clicks) / sum(impressions) AS ctr,
               sum(converted_sessions) / sum(sessions) AS conversion, sum(revenue) AS revenue
        FROM {G}.experiment_results GROUP BY ALL ORDER BY event_date, model_variant""",
    "segments": f"""
        SELECT segment, CASE WHEN model_variant = 'popularity' THEN 'baseline' ELSE 'personalized' END AS arm,
               sum(sessions) AS sessions, sum(clicks) / sum(impressions) AS ctr,
               sum(converted_sessions) / sum(sessions) AS conversion
        FROM {G}.experiment_results GROUP BY ALL ORDER BY segment, arm""",
    "categories": f"""
        SELECT category, CASE WHEN model_variant = 'popularity' THEN 'baseline' ELSE 'personalized' END AS arm,
               count(*) AS impressions, avg(CAST(clicked AS INT)) AS ctr
        FROM {G}.recommendation_outcomes GROUP BY ALL ORDER BY category, arm""",
    "countries": f"""
        SELECT country, model_variant, sum(clicks) / sum(impressions) AS ctr,
               sum(converted_sessions) / sum(sessions) AS conversion
        FROM {G}.experiment_results GROUP BY ALL ORDER BY country, model_variant""",
    "funnel": f"""
        SELECT session_date, sum(sessions) AS sessions, sum(converted_sessions) AS converted_sessions,
               sum(converted_sessions) / sum(sessions) AS conversion_rate, sum(revenue) AS revenue,
               sum(views) AS views, sum(add_to_carts) AS add_to_carts
        FROM {G}.session_kpis_daily WHERE session_date >= current_date() - INTERVAL 28 DAYS
        GROUP BY ALL ORDER BY session_date""",
    "offline_eval": f"SELECT * FROM {config.CATALOG}.ml.offline_evaluation ORDER BY model_variant",
    "data_quality": f"SELECT * FROM {G}.pipeline_health ORDER BY snapshot_ts DESC LIMIT 1",
}


@app.get("/api/kpis/{name}")
async def kpis(name: str):
    if name == "live":
        return await live_stats()
    if name not in KPI_SQL:
        raise HTTPException(404, "unknown kpi")
    try:
        return await sql.query(KPI_SQL[name])
    except Exception as e:  # noqa: BLE001
        return JSONResponse({"error": str(e)[:300]}, status_code=502)


async def live_stats():
    rows = await db.fetch(f"""
        SELECT served_by_kind, count(*) FILTER (WHERE event = 'impression') AS impressions,
               count(*) FILTER (WHERE event = 'click') AS clicks,
               count(*) FILTER (WHERE event = 'purchase') AS purchases,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY latency_ms) FILTER (WHERE event = 'impression') AS p50_ms,
               percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms) FILTER (WHERE event = 'impression') AS p95_ms,
               count(DISTINCT request_id) AS requests
        FROM (SELECT *, split_part(coalesce(served_by, 'event'), ' ', 1) AS served_by_kind
              FROM {config.APP_SCHEMA}.recommendation_log WHERE ts >= now() - interval '7 days') t
        GROUP BY 1 ORDER BY 1""")
    return [recs._jsonable(r) for r in rows]


@app.get("/api/pipeline")
async def pipeline():
    def _get():
        w = config.ws()
        out = []
        for p in w.pipelines.list_pipelines(filter=f"name LIKE '%{config.PIPELINE_NAME}%'"):
            out.append({"pipeline_id": p.pipeline_id, "name": p.name, "state": p.state.value if p.state else None,
                        "latest_updates": [{"state": u.state.value if u.state else None, "creation_time": u.creation_time}
                                           for u in (p.latest_updates or [])[:3]]})
        return out
    try:
        return await asyncio.to_thread(_get)
    except Exception as e:  # noqa: BLE001
        return {"error": str(e)[:200]}


# ----------------------------------------------------------------------------- ask the data

class GenieRequest(BaseModel):
    question: str
    conversation_id: str | None = None


@app.post("/api/genie")
async def ask_genie(body: GenieRequest):
    try:
        return await genie.ask(body.question, body.conversation_id)
    except Exception as e:  # noqa: BLE001
        return JSONResponse({"error": str(e)[:400]}, status_code=502)


class AgentRequest(BaseModel):
    question: str
    history: list[dict] = []


@app.post("/api/agent")
async def ask_agent(body: AgentRequest):
    async def stream():
        try:
            async for ev in agent.run(body.question, body.history):
                yield f"data: {json.dumps(ev, default=str)}\n\n"
        except Exception as e:  # noqa: BLE001
            yield f"data: {json.dumps({'type': 'error', 'content': str(e)[:400]})}\n\n"
        yield "data: {\"type\": \"done\"}\n\n"
    return StreamingResponse(stream(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# ----------------------------------------------------------------------------- about / health / assets

@app.get("/api/about")
async def about():
    def read(name):
        p = CONTENT / name
        return p.read_text() if p.exists() else ""
    w = config.ws()
    host = w.config.host
    return {"about": read("about.md"), "decisions": read("decisions.md"), "spec": read("spec.md"),
            "links": {
                "workspace": host,
                "catalog": f"{host}/explore/data/{config.CATALOG}",
                "lineage": f"{host}/explore/data/{config.CATALOG}/gold/user_features?activeTab=lineage",
                "model": f"{host}/explore/data/models/{config.CATALOG}/ml/recommender",
                "serving": f"{host}/ml/endpoints/{config.RECOMMENDER_ENDPOINT}",
                "genie": f"{host}/genie/rooms/{genie.space_id() or ''}",
                "lakebase": f"{host}/lakebase/projects/{config.LAKEBASE_PROJECT}",
                "vector_index": f"{host}/explore/data/{config.CATALOG}/gold/product_index",
            }}


@app.get("/api/health")
async def health():
    out = {"catalog": config.CATALOG}
    try:
        await db.fetchone("SELECT 1 AS ok")
        out["lakebase"] = "ok"
    except Exception as e:  # noqa: BLE001
        out["lakebase"] = f"error: {e}"[:200]
    try:
        ep = await asyncio.to_thread(config.ws().serving_endpoints.get, config.RECOMMENDER_ENDPOINT)
        out["model_serving"] = ep.state.ready.value if ep.state and ep.state.ready else "unknown"
    except Exception as e:  # noqa: BLE001
        out["model_serving"] = f"error: {e}"[:200]
    try:
        out["genie_space_id"] = await asyncio.to_thread(genie.space_id)
    except Exception as e:  # noqa: BLE001
        out["genie_space_id"] = f"error: {e}"[:200]
    return out


CATEGORY_STYLE = {
    "electronics": ("#1e3a8a", "#60a5fa", "🎧"), "home_kitchen": ("#7c2d12", "#fdba74", "🍳"),
    "fashion_women": ("#831843", "#f9a8d4", "👗"), "fashion_men": ("#1f2937", "#9ca3af", "👔"),
    "beauty": ("#701a75", "#f0abfc", "💄"), "sports_outdoors": ("#14532d", "#86efac", "🏃"),
    "toys_games": ("#713f12", "#fde047", "🧸"), "books": ("#3b0764", "#c4b5fd", "📚"),
    "grocery_gourmet": ("#365314", "#bef264", "☕"), "pets": ("#78350f", "#fcd34d", "🐾"),
    "health_wellness": ("#134e4a", "#5eead4", "💊"), "garden": ("#166534", "#4ade80", "🌱"),
}


_img_cat: dict[str, str] = {}


@app.get("/api/img/{product_id}.svg")
async def product_image(product_id: str, category: str | None = None):
    category = category or _img_cat.get(product_id)
    if not category:
        p = (await recs.catalog([product_id])).get(product_id) or {}
        category = _img_cat[product_id] = p.get("category", "electronics")
    a, b, emoji = CATEGORY_STYLE.get(category, ("#334155", "#94a3b8", "🛍️"))
    h = int(hashlib.md5(product_id.encode()).hexdigest()[:6], 16)
    angle, x, y = h % 360, 30 + h % 40, 30 + (h >> 8) % 40
    svg = f"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 400 400">
<defs><linearGradient id="g" gradientTransform="rotate({angle} .5 .5)"><stop offset="0" stop-color="{a}"/>
<stop offset="1" stop-color="{b}"/></linearGradient></defs><rect width="400" height="400" fill="url(#g)"/>
<circle cx="{x * 4}" cy="{y * 4}" r="120" fill="#ffffff" fill-opacity="0.12"/>
<text x="200" y="235" font-size="140" text-anchor="middle">{emoji}</text></svg>"""
    return Response(svg, media_type="image/svg+xml", headers={"Cache-Control": "public, max-age=86400"})


if (STATIC / "assets").exists():
    app.mount("/assets", StaticFiles(directory=STATIC / "assets"), name="assets")


@app.get("/{path:path}")
async def spa(path: str):
    f = STATIC / path
    if path and f.is_file():
        return FileResponse(f)
    return FileResponse(STATIC / "index.html")
