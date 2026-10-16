"""'Why this?' explanations: one grounded sentence per (user, product), cached in Lakebase.

The prompt only contains structured facts (feature payload + recommender signals),
and the model is told to use nothing else, so explanations stay grounded.
"""
from __future__ import annotations

import json

from . import config, db, llm

A = config.APP_SCHEMA

SYSTEM = (
    "You write the one-line 'Why this?' note under a product recommendation in an online store. "
    "Use ONLY the facts in the JSON payload. Never invent facts, prices, reviews or discounts. "
    "Address the shopper as 'you'. Max 22 words. No preamble, no quotes."
)


def _facts(uf: dict, product: dict, signals: dict, session_titles: list[str]) -> dict:
    aff = uf.get("cat_affinity_json")
    aff = json.loads(aff) if isinstance(aff, str) else (aff or {})
    return {
        "product": {k: product.get(k) for k in ("title", "category", "brand", "price", "price_tier")},
        "recommendation_source": product.get("source"),
        "signals": signals,
        "shopper": {
            "is_new_shopper": not uf or not uf.get("has_history"),
            "top_category": uf.get("top_category"), "favourite_brand": uf.get("top_brand"),
            "share_of_activity_in_this_category": round(float(aff.get(product.get("category"), 0)), 2),
            "loyalty_tier": uf.get("loyalty_tier"), "orders_last_90d": uf.get("n_orders_90d"),
        },
        "viewed_this_session": session_titles[:3],
    }


async def explain(user_id: str, product: dict, uf: dict, session_titles: list[str]) -> dict:
    pid = product["product_id"]
    hit = await db.fetchone(f"SELECT explanation, model FROM {A}.explanation_cache WHERE user_id = %s AND product_id = %s",
                            (user_id, pid))
    if hit:
        return {"product_id": pid, "explanation": hit["explanation"], "cached": True, "model": hit["model"]}
    facts = _facts(uf, product, product.get("signals") or {}, session_titles)
    msg = await llm.chat(config.EXPLAIN_LLM, [
        {"role": "system", "content": SYSTEM},
        {"role": "user", "content": json.dumps(facts, default=str)}], max_tokens=80, temperature=0.3)
    text = llm.text_of(msg).strip().strip('"')
    await db.execute(
        f"""INSERT INTO {A}.explanation_cache (user_id, product_id, explanation, model) VALUES (%s, %s, %s, %s)
            ON CONFLICT (user_id, product_id) DO UPDATE SET explanation = EXCLUDED.explanation""",
        (user_id, pid, text, config.EXPLAIN_LLM))
    return {"product_id": pid, "explanation": text, "cached": False, "model": config.EXPLAIN_LLM, "facts": facts}
