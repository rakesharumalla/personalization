# Acme Retail: real-time personalization on Databricks

**Customer:** Acme Retail, a digital-native online marketplace that buys most of its traffic through paid channels.
**Pilot sponsor:** VP of E-commerce. **Funding approver:** CFO.

## 1. The business challenge

Acme Retail buys traffic and loses most of it in the first session. The storefront is one-size-fits-all: the same
homepage rails for a first-time visitor and a loyal repeat buyer.

- **Low conversion, high bounce.** Generic rails don't match intent, so paid sessions end without a purchase.
- **Cold start.** About 30% of sessions come from shoppers with no history; they get "trending" at best.
- **No explainability.** Merchandisers can't see *why* a shopper got a product, or whether it worked.
- **Siloed stack.** Personalization logic lives in app code, disconnected from the governed analytics the business trusts.

### Who owns each outcome at Acme Retail

| Metric | Owner | Why it is on their scorecard |
|---|---|---|
| Session conversion, revenue per session | **VP of E-commerce** (pilot sponsor) | Online revenue growth target |
| Average order value, margin mix | **VP of E-commerce**, with the **CFO** | Revenue quality and contribution margin |
| Recommendation CTR, category performance | **VP of Merchandising** | Category growth and sell-through |
| Cold-start conversion | **CMO** | Return on paid acquisition: first-visit shoppers who buy |
| Run cost, ROI, payback | **CFO** (approves the pilot) | Return on investment and payback period |

**Goal:** the right product for every shopper within ~100 ms of a page view, using what they *just* clicked, with
governance, explanations and a business feedback loop, on one platform.

## 2. How Databricks addresses it

One data journey; each stage consumes the previous stage's output.

| Stage | What runs here | Asset |
|---|---|---|
| **Lakeflow** | Auto Loader ingests streaming JSON (clickstream, orders, users CDC) into bronze; Declarative Pipelines build silver (expectations, dedupe, SCD2 users via AUTO CDC, quarantine) and gold KPI MVs | `pers-personalization-pipeline` |
| **Unity Catalog** | Schemas bronze/silver/gold/ml/app, PII column mask on `email`, regional row filter, tags, feature tables with PKs, model `ml.recommender@champion/@challenger`, lineage end-to-end | `fevm_catalog_ra_test` |
| **ML + GenAI** | Implicit-ALS + gte-large Vector Search candidates → LightGBM LambdaRank on Model Serving; Claude Haiku "why this?" explanations grounded in features; Claude Sonnet merchandiser copilot | `pers-recommender`, `gold.product_index` |
| **Lakebase** | Synced tables (`user_features_online` continuous, `product_catalog`, `precomputed_recs`) + native `session_state`, `recommendation_log`, `explanation_cache` | `pers-serving` |
| **Genie** | "Personalization Insights" space over gold with metric definitions, metric owners, trusted SQL and sample questions | Genie space |
| **Databricks App** | React + FastAPI: storefront simulator, business console with the CFO business case, Genie + copilot, this page | `pers-personalization` |

**Lineage proves the integration:** app table ← model ← gold ← silver ← bronze ← landing volume. Open the
lineage tab of `gold.user_features` or `ml.recommender` from the links above.

## 3. AI tools, workflow and trade-offs

See **Build log & trade-offs**. It covers, part by part, what the AI coding agent (Claude Code) built, what the
solution architect decided, how each part was verified, and what went wrong and how it was fixed. It also lists
the AI that ships inside the product: Foundation Model APIs, Vector Search and Genie.

## 4. Outcomes & impact

Live numbers are on the **Business Console**, computed from `gold.experiment_results`: a simulated A/B test
where outcomes are drawn from the simulator's hidden shopper preferences (the model never sees them), so lift
is earned by ranking quality.

| Metric | Owner | Baseline (popularity) | Personalized | Target |
|---|---|---|---|---|
| Recommendation CTR | VP of Merchandising | 3.1% | 6.1% | ≥ 6% |
| Session conversion (returning) | VP of E-commerce | 1.93% | 2.84% to 3.30% | ≥ 2.6% |
| Cold-start conversion | CMO | 0.83% | 1.67% | ≥ 1.4% |
| AOV (returning) | VP of E-commerce + CFO | $57.92 | $56.47 to $60.34 | +5 to 8% (not met) |
| Revenue per session (all sessions) | VP of E-commerce | $0.95 | $1.36 to $1.65 | — |
| Rec latency | VP of E-commerce | n/a | ~112 ms p50 | < 100 ms p95 (not met) |
| Time to answer an ad-hoc question | VP of Merchandising | hours (analyst ticket) | seconds (Genie) | — |

### What it is worth to Acme Retail (for the CFO)

Revenue per session combines conversion and AOV, so it converts straight into dollars:

> annual value = (personalized − baseline revenue per session) × annual sessions × realization × gross margin − run cost

| Acme Retail assumption | Value | Note |
|---|---|---|
| Monthly sessions | 5,000,000 | Replace with Acme Retail actuals |
| Gross margin | 35% | Replace with Acme Retail actuals |
| Realization of simulated lift | 50% | Discount because the lift is simulated; the pilot measures the real number |
| Annual run cost | $750,000 | Platform plus a small team; size with the account team |

| Scenario (champion model) | Incremental revenue / yr | Gross profit / yr | Net benefit / yr | Payback |
|---|---|---|---|---|
| Low (25% realization) | $6.1M | $2.1M | $1.4M | 4.2 months |
| **Expected (50%)** | **$12.3M** | **$4.3M** | **$3.5M** | **2.1 months** |
| High (75%) | $18.4M | $6.4M | $5.7M | 1.4 months |

The challenger model would be worth $21.2M revenue and $7.4M gross profit a year at 50% realization. The UC
function `gold.business_case(...)` computes all of this, and the Business Console, Genie and the copilot all use it.
Cost to prove it: Databricks usage billed for this build so far is about $8 at list price (billing data through
01:30 UTC on Oct 16; later usage is not yet included).
