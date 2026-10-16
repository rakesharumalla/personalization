"""Create or update the 'Personalization Insights' Genie space over gold tables."""
import argparse
import hashlib
import json

from databricks.sdk import WorkspaceClient

ap = argparse.ArgumentParser()
ap.add_argument("--catalog", default="fevm_catalog_ra_test")
ap.add_argument("--warehouse_id", required=True)
ap.add_argument("--title", default="Personalization Insights")
ap.add_argument("--root", default=None)
args, _ = ap.parse_known_args()
C = args.catalog
G = f"{C}.gold"


def hid(s: str) -> str:
    return hashlib.md5(s.encode()).hexdigest()


SAMPLE_QUESTIONS = [
    "Which product categories have the highest recommendation CTR this week?",
    "How do cold-start users convert vs. returning users?",
    "Show me the lift of the challenger model over champion by country",
    "What is the conversion lift of personalized recommendations over the popularity baseline by segment in the last 7 days?",
    "What were daily sessions, conversion rate and revenue over the last 14 days?",
    "What is personalization worth per year at 5 million sessions a month?",
]

TRUSTED_SQL = [
    ("What is personalization worth per year at 5 million sessions a month?", f"""
SELECT * FROM {G}.business_case(5000000, 0.35, 0.5, 750000)"""),
    ("What is the revenue per session lift of personalization?", f"""
SELECT model_variant, sum(revenue) / sum(sessions) AS revenue_per_session,
       try_divide(sum(revenue) / sum(sessions),
                  (SELECT sum(revenue) / sum(sessions) FROM {G}.experiment_results WHERE model_variant = 'popularity')) - 1 AS lift_vs_baseline
FROM {G}.experiment_results GROUP BY model_variant ORDER BY revenue_per_session DESC"""),
    ("What is the conversion lift by segment in the last 7 days?", f"""
SELECT segment,
       sum(CASE WHEN model_variant <> 'popularity' THEN converted_sessions END) / sum(CASE WHEN model_variant <> 'popularity' THEN sessions END) AS personalized_conversion,
       sum(CASE WHEN model_variant = 'popularity' THEN converted_sessions END) / sum(CASE WHEN model_variant = 'popularity' THEN sessions END) AS baseline_conversion,
       try_divide(personalized_conversion, baseline_conversion) - 1 AS conversion_lift
FROM {G}.experiment_results
WHERE event_date >= current_date() - INTERVAL 7 DAYS
GROUP BY segment ORDER BY conversion_lift DESC"""),
    ("Show me the lift of the challenger model over champion by country", f"""
SELECT country,
       sum(CASE WHEN model_variant = 'challenger' THEN clicks END) / sum(CASE WHEN model_variant = 'challenger' THEN impressions END) AS challenger_ctr,
       sum(CASE WHEN model_variant = 'champion' THEN clicks END) / sum(CASE WHEN model_variant = 'champion' THEN impressions END) AS champion_ctr,
       try_divide(challenger_ctr, champion_ctr) - 1 AS ctr_lift,
       try_divide(sum(CASE WHEN model_variant = 'challenger' THEN converted_sessions END) / sum(CASE WHEN model_variant = 'challenger' THEN sessions END),
                  sum(CASE WHEN model_variant = 'champion' THEN converted_sessions END) / sum(CASE WHEN model_variant = 'champion' THEN sessions END)) - 1 AS conversion_lift
FROM {G}.experiment_results
GROUP BY country ORDER BY ctr_lift DESC"""),
    ("How do cold-start users convert vs. returning users?", f"""
SELECT CASE WHEN is_cold_start THEN 'cold-start' ELSE 'returning' END AS user_type,
       CASE WHEN model_variant = 'popularity' THEN 'baseline (popularity)' ELSE 'personalized' END AS experience,
       sum(sessions) AS sessions, sum(converted_sessions) / sum(sessions) AS conversion_rate,
       sum(clicks) / sum(impressions) AS ctr
FROM {G}.experiment_results
GROUP BY ALL ORDER BY user_type, experience"""),
    ("Which product categories have the highest recommendation CTR this week?", f"""
SELECT category, count(*) AS impressions, sum(CAST(clicked AS INT)) AS clicks,
       sum(CAST(clicked AS INT)) / count(*) AS ctr
FROM {G}.recommendation_outcomes
WHERE event_date >= current_date() - INTERVAL 7 DAYS
GROUP BY category ORDER BY ctr DESC"""),
]

INSTRUCTIONS = [
    "This space analyses Acme Retail's real-time personalization program (an online marketplace).",
    "Revenue per session = revenue / sessions over all sessions (cold start included); it combines conversion and AOV "
    "and is the figure used for dollar value. For annual value, revenue, gross profit, ROI or payback ALWAYS query the "
    "trusted table function (never compute it by hand, never blend champion and challenger) "
    f"{G}.business_case(monthly_sessions, gross_margin, realization, annual_run_cost); Acme Retail assumptions are "
    "5000000 sessions a month, 0.35 gross margin, 0.5 realization, 750000 annual run cost. State the assumptions.",
    "Metric owners at Acme Retail: conversion and revenue per session = VP of E-commerce; AOV and margin = VP of "
    "E-commerce with the CFO; recommendation CTR and category performance = VP of Merchandising; cold-start conversion "
    "= CMO; run cost, ROI and payback = CFO.",
    "Metric definitions: CTR = clicks / impressions. Conversion rate = converted_sessions / sessions. "
    "AOV = revenue / converted_sessions. Lift = (treatment metric / control metric) - 1, shown as a percentage.",
    "Experiment: model_variant 'popularity' is the control baseline (non-personalized). 'champion' and "
    "'challenger' are the personalized two-stage recommender models (treatment). 'Personalized' means "
    "model_variant IN ('champion','challenger') unless the user names a specific model.",
    "Always aggregate ratios as sum(numerator)/sum(denominator), never avg of a ratio column.",
    "Cold-start users are is_cold_start = true (signed up within the last 24 hours, no history). Returning = false.",
    "Synonyms: SKU = product = product_id; rec = recommendation; shoppers = users; variant/arm = model_variant.",
    "Default time window is the last 14 days when none is given; 'this week' means the last 7 days.",
    "For organic (non-recommendation) funnel KPIs use session_kpis_daily; for recommendation and experiment "
    "metrics use experiment_results (aggregated) or recommendation_outcomes (impression level).",
    "Segments in user_segments.segment: new, champion, buyer, engaged_browser, casual_browser, at_risk.",
]

space = {
    "version": 2,
    "config": {"sample_questions": sorted([{"id": hid("sq" + q), "question": [q]} for q in SAMPLE_QUESTIONS],
                                          key=lambda x: x["id"])},
    "data_sources": {"tables": sorted([
        {"identifier": f"{G}.experiment_results",
         "column_configs": sorted([
             {"column_name": "model_variant", "enable_format_assistance": True, "enable_entity_matching": True,
              "description": ["popularity = control baseline; champion/challenger = personalized models"],
              "synonyms": ["variant", "arm", "model"]},
             {"column_name": "segment", "enable_format_assistance": True, "enable_entity_matching": True},
             {"column_name": "country", "enable_format_assistance": True, "enable_entity_matching": True}],
             key=lambda c: c["column_name"])},
        {"identifier": f"{G}.recommendation_outcomes",
         "column_configs": sorted([
             {"column_name": "category", "enable_format_assistance": True, "enable_entity_matching": True},
             {"column_name": "product_id", "synonyms": ["SKU", "product"]},
             {"column_name": "model_variant", "enable_format_assistance": True, "enable_entity_matching": True}],
             key=lambda c: c["column_name"])},
        {"identifier": f"{G}.session_kpis_daily"},
        {"identifier": f"{G}.user_segments"},
        {"identifier": f"{G}.product_performance",
         "column_configs": [{"column_name": "product_id", "synonyms": ["SKU", "product"]}]},
    ], key=lambda t: t["identifier"])},
    "instructions": {
        "text_instructions": [{"id": hid("instructions"), "content": [s + "\n" for s in INSTRUCTIONS]}],
        "example_question_sqls": sorted([{"id": hid("ex" + q), "question": [q], "sql": [s.strip()]}
                                         for q, s in TRUSTED_SQL], key=lambda x: x["id"]),
        "join_specs": sorted([
            {"id": hid("join_ro_pp"), "left": {"identifier": f"{G}.recommendation_outcomes", "alias": "recommendation_outcomes"},
             "right": {"identifier": f"{G}.product_performance", "alias": "product_performance"},
             "sql": ["`recommendation_outcomes`.`product_id` = `product_performance`.`product_id`",
                     "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--"]},
            {"id": hid("join_ro_us"), "left": {"identifier": f"{G}.recommendation_outcomes", "alias": "recommendation_outcomes"},
             "right": {"identifier": f"{G}.user_segments", "alias": "user_segments"},
             "sql": ["`recommendation_outcomes`.`user_id` = `user_segments`.`user_id`",
                     "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--"]},
        ], key=lambda x: x["id"]),
        # trusted UC function: dollar value must come from one governed calculation
        "sql_functions": [{"id": hid("fn_business_case"), "identifier": f"{G}.business_case"}],
    },
}

w = WorkspaceClient()
payload = {"title": args.title, "warehouse_id": args.warehouse_id, "serialized_space": json.dumps(space),
           "description": "Natural-language analytics over personalization KPIs, A/B results and segments."}
existing = None
token = None
while True:
    r = w.api_client.do("GET", "/api/2.0/genie/spaces", query={"page_token": token} if token else None)
    for s in r.get("spaces", []):
        if s.get("title") == args.title:
            existing = s["space_id"]
    token = r.get("next_page_token")
    if existing or not token:
        break
if existing:
    w.api_client.do("PATCH", f"/api/2.0/genie/spaces/{existing}", body=payload)
    print("updated genie space", existing)
else:
    existing = w.api_client.do("POST", "/api/2.0/genie/spaces", body=payload)["space_id"]
    print("created genie space", existing)
