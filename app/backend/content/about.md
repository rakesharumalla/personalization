# Real-Time Personalization on Databricks

## 1. The business challenge

Digital-native marketplaces buy traffic and lose most of it in the first session. The default experience is
one-size-fits-all: the same homepage rails for a first-time visitor and a loyal repeat buyer.

- **Low conversion, high bounce** — generic rails don't match intent.
- **Cold start** — ~30% of sessions come from shoppers with no history; they get "trending" at best.
- **No explainability** — merchandisers can't see *why* a shopper got a product, or whether it worked.
- **Siloed stack** — personalization logic lives in app code, disconnected from the governed analytics the business trusts.

**Goal:** the right product for every shopper within ~100 ms of a page view, using what they *just* clicked, with
governance, explanations and a business feedback loop — on one platform.

## 2. How Databricks addresses it

One data journey; each stage consumes the previous stage's output.

| Stage | What runs here | Asset |
|---|---|---|
| **Lakeflow** | Auto Loader ingests streaming JSON (clickstream, orders, users CDC) into bronze; Declarative Pipelines build silver (expectations, dedupe, SCD2 users via AUTO CDC, quarantine) and gold KPI MVs | `pers-personalization-pipeline` |
| **Unity Catalog** | Schemas bronze/silver/gold/ml/app, PII column mask on `email`, regional row filter, tags, feature tables with PKs, model `ml.recommender@champion/@challenger`, lineage end-to-end | `fevm_catalog_ra_test` |
| **ML + GenAI** | Implicit-ALS + gte-large Vector Search candidates → LightGBM LambdaRank on Model Serving; Claude Haiku "why this?" explanations grounded in features; Claude Sonnet merchandiser copilot | `pers-recommender`, `gold.product_index` |
| **Lakebase** | Synced tables (`user_features_online` continuous, `product_catalog`, `precomputed_recs`) + native `session_state`, `recommendation_log`, `explanation_cache` | `pers-serving` |
| **Genie** | "Personalization Insights" space over gold with metric definitions, trusted SQL and sample questions | Genie space |
| **Databricks App** | React + FastAPI: storefront simulator, business console, Genie + copilot, this page | `pers-personalization` |

**Lineage proves the integration:** app table ← model ← gold ← silver ← bronze ← landing volume. Open the
lineage tab of `gold.user_features` or `ml.recommender` from the links above.

## 3. AI tools, workflow and trade-offs

See **Build log & trade-offs** — the tools used to build (Claude, Databricks Assistant, Genie, FMAPI), the
workflow, and every decision with its rationale (e.g. numpy ALS on serverless, app-side feature lookup from
Lakebase, in-app agent, precomputed fallback).

## 4. Outcomes & impact

Live numbers are on the **Business Console**, computed from `gold.experiment_results` — a simulated A/B test
where outcomes are drawn from the simulator's hidden shopper preferences (the model never sees them), so lift
is earned by ranking quality.

| Metric | Baseline (popularity) | Target (personalized) |
|---|---|---|
| Recommendation CTR | ~3% | ≥ 6% |
| Session → purchase conversion | ~2.0% | ≥ 2.6% |
| Cold-start conversion | ~0.8% | ≥ 1.4% |
| AOV | — | +5–8% |
| Rec latency p95 | n/a | < 100 ms (see the latency badge and live p95 in the console) |
| Time to answer an ad-hoc question | hours (analyst ticket) | seconds (Genie) |
