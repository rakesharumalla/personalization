"""Merchandiser copilot: tool-calling agent over Foundation Model APIs.

Tools
    ask_genie          quantitative questions -> Genie Conversation API (Insights space)
    simulate_campaign  UC table function gold.simulate_campaign(category, segment, discount)
    lookup_product     Vector Search over the catalog ("find products like X")

``run`` is an async generator of events so the API can stream them over SSE:
    {"type": "tool_call", ...} {"type": "tool_result", ...} {"type": "answer", ...}
"""
from __future__ import annotations

import asyncio
import json
import time

from . import config, genie, llm, sql

SYSTEM = f"""You are the Merchandiser Copilot for Acme Retail's personalization program (Acme Retail runs an online marketplace).
You help merchandising managers understand recommendation performance and plan campaigns.
Rules:
- For any number about performance (CTR, conversion, lift, revenue, segments, categories) call ask_genie. Never invent numbers.
- To estimate a campaign (category x segment x discount), call simulate_campaign. Segments: new, champion, buyer,
  engaged_browser, casual_browser, at_risk, or all. Categories: electronics, home_kitchen, fashion_women, fashion_men,
  beauty, sports_outdoors, toys_games, books, grocery_gourmet, pets, health_wellness, garden, or all.
- To find products ("like X", "for runners", "gift ideas"), call lookup_product.
- For dollar value, margin, ROI or payback ("what is this worth?", "make the case to the CFO"), call business_case.
  Acme Retail defaults unless the user gives their own: 5,000,000 monthly sessions, 0.35 gross margin,
  0.5 realization of the simulated lift, 750000 annual run cost. Always state the assumptions you used.
- Metric owners at Acme Retail: conversion and revenue per session -> VP of E-commerce; AOV and margin ->
  VP of E-commerce with the CFO; recommendation CTR and categories -> VP of Merchandising; cold-start
  conversion and paid-traffic efficiency -> CMO; run cost, ROI and payback -> CFO.
- Be concise: short paragraphs or bullets, cite the numbers you got from tools, and end with one recommended action.
Catalog: {config.CATALOG}."""

TOOLS = [
    {"type": "function", "function": {
        "name": "ask_genie",
        "description": "Ask the Personalization Insights Genie space a quantitative question in plain English. "
                       "Returns the SQL it ran and the result table.",
        "parameters": {"type": "object", "properties": {"question": {"type": "string"}}, "required": ["question"]}}},
    {"type": "function", "function": {
        "name": "simulate_campaign",
        "description": "Estimate reach, expected conversion and incremental orders/revenue for a personalized "
                       "campaign targeting a product category and user segment with a discount.",
        "parameters": {"type": "object", "properties": {
            "category": {"type": "string"}, "segment": {"type": "string"},
            "discount_pct": {"type": "number", "description": "e.g. 10 for 10%"}},
            "required": ["category", "segment", "discount_pct"]}}},
    {"type": "function", "function": {
        "name": "business_case",
        "description": "Annual incremental revenue, gross profit, net benefit, ROI and payback of personalization, "
                       "from the measured revenue-per-session lift and the customer's own volume and margin.",
        "parameters": {"type": "object", "properties": {
            "monthly_sessions": {"type": "number", "default": 5000000},
            "gross_margin": {"type": "number", "description": "fraction, e.g. 0.35", "default": 0.35},
            "realization": {"type": "number", "description": "share of simulated lift expected, e.g. 0.5", "default": 0.5},
            "annual_run_cost": {"type": "number", "default": 750000}}}}},
    {"type": "function", "function": {
        "name": "lookup_product",
        "description": "Semantic search over the product catalog (Vector Search on title/description).",
        "parameters": {"type": "object", "properties": {
            "query": {"type": "string"}, "k": {"type": "integer", "default": 5}}, "required": ["query"]}}},
]


def _q(s: str) -> str:
    return str(s).replace("'", "''")[:80]


async def _tool(name: str, args: dict) -> dict:
    if name == "ask_genie":
        r = await genie.ask(args["question"])
        return {"text": r.get("text"), "sql": r.get("sql"), "columns": r.get("columns"),
                "rows": (r.get("rows") or [])[:25]}
    if name == "simulate_campaign":
        rows = await sql.query(
            f"SELECT * FROM {config.CATALOG}.gold.simulate_campaign('{_q(args['category'])}', "
            f"'{_q(args['segment'])}', {float(args['discount_pct'])})", ttl=30)
        return {"rows": rows}
    if name == "business_case":
        a = {"monthly_sessions": 5_000_000, "gross_margin": 0.35, "realization": 0.5, "annual_run_cost": 750_000,
             **{k: v for k, v in args.items() if v is not None}}
        rows = await sql.query(
            f"SELECT * FROM {config.CATALOG}.gold.business_case({float(a['monthly_sessions'])}, "
            f"{float(a['gross_margin'])}, {float(a['realization'])}, {float(a['annual_run_cost'])})", ttl=30)
        return {"assumptions": a, "rows": rows}
    if name == "lookup_product":
        k = int(args.get("k") or 5)
        w = config.ws()
        res = await asyncio.to_thread(
            w.vector_search_indexes.query_index, index_name=config.VS_INDEX,
            columns=["product_id", "title", "category", "brand", "price"], query_text=args["query"], num_results=k)
        cols = [c.name for c in res.manifest.columns]
        return {"products": [dict(zip(cols, r)) for r in (res.result.data_array or [])]}
    raise ValueError(f"unknown tool {name}")


def _summ(result: dict) -> str:
    s = json.dumps(result, default=str)
    return s if len(s) < 6000 else s[:6000] + "...(truncated)"


async def run(question: str, history: list[dict] | None = None, max_steps: int = 6):
    messages = [{"role": "system", "content": SYSTEM}, *(history or []), {"role": "user", "content": question}]
    t0 = time.perf_counter()
    for step in range(max_steps):
        msg = await llm.chat(config.AGENT_LLM, messages, tools=TOOLS, max_tokens=1200)
        calls = msg.get("tool_calls") or []
        if not calls:
            yield {"type": "answer", "content": llm.text_of(msg), "steps": step,
                   "elapsed_ms": round((time.perf_counter() - t0) * 1000)}
            return
        messages.append({"role": "assistant", "content": llm.text_of(msg) or None, "tool_calls": calls})
        for c in calls:
            fn = c["function"]["name"]
            try:
                args = json.loads(c["function"].get("arguments") or "{}")
            except json.JSONDecodeError:
                args = {}
            yield {"type": "tool_call", "id": c["id"], "name": fn, "arguments": args}
            ts = time.perf_counter()
            try:
                result = await _tool(fn, args)
                ok = True
            except Exception as e:  # noqa: BLE001
                result, ok = {"error": f"{type(e).__name__}: {str(e)[:300]}"}, False
            yield {"type": "tool_result", "id": c["id"], "name": fn, "ok": ok, "result": result,
                   "elapsed_ms": round((time.perf_counter() - ts) * 1000)}
            messages.append({"role": "tool", "tool_call_id": c["id"], "content": _summ(result)})
    yield {"type": "answer", "content": "I hit my step limit — try a narrower question.", "steps": max_steps}
