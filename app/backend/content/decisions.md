# Build Log & Trade-Off Log

Living document — rendered in the app's **About This Build → Build log & trade-offs** tab.

## Workflow

spec → DAB scaffold → data generator → SDP pipeline → UC setup → features & model → Lakebase sync → Genie space → app → experiment → About page.

Everything is deployed by `databricks bundle deploy` plus one job (`pers-setup`), then `databricks bundle run pers_app`.

## AI tools used in the build

| Tool | Where it helped |
|---|---|
| Claude (Claude Code) | Spec refinement, all code (generator, pipeline, ML, app), docs, simulator calibration loop |
| Databricks Assistant | In-notebook SQL / PySpark help while inspecting tables |
| Genie | Product feature (Insights space) and dev-time sanity checks of gold tables |
| Foundation Model APIs | `databricks-gte-large-en` embeddings, Claude Haiku "why this?" explanations, Claude Sonnet merchandiser copilot |

## Decisions

| Decision | Options | Choice & rationale |
|---|---|---|
| Data source | Synthetic vs public | **Synthetic** — enables streaming + PII governance demo; hidden ground-truth preferences let the A/B simulation *earn* its lift. |
| Pipeline mode | Continuous vs triggered | **Triggered by default** (`pipeline_continuous=false`, cost); flip the bundle variable for the demo window and run `pers-event-stream`. |
| Recommender | Popularity → ALS → two-stage | **Two-stage**: implicit ALS item-item CF + gte-large semantic neighbours → LightGBM LambdaRank. Credible, buildable in days. |
| ALS implementation | Spark MLlib vs numpy | **numpy/scipy implicit ALS** (Hu-Koren-Volinsky). MLlib ALS is not available on serverless compute, and at demo scale (≈20k users × 2k items) a driver-side solve takes seconds. Swap to MLlib on classic compute for larger catalogs. |
| Real-time signal | Batch features only vs session state | **Session state in Lakebase** (`app.session_state`): the last 5 clicks feed candidate generation and ranking features on the very next request. |
| Feature lookup | Automatic (online feature store in serving) vs app-side | **App-side lookup from Lakebase synced tables**, sent in the request payload. Same Lakebase rows, one fewer moving part, and the latency badge shows the lookup cost (~ms). |
| Feature tables | MVs vs Delta tables | **Delta tables with PRIMARY KEY + Change Data Feed**, upserted with MERGE + row hash, so only changed rows flow to Lakebase / Vector Search. |
| Lakebase UC catalog | Dedicated Lakebase catalog vs solution catalog | **Solution catalog** (`fevm_catalog_ra_test.serving.*`): the workspace user lacks CREATE CATALOG on the metastore. Postgres schema is `serving` either way. |
| Explanations | Template vs LLM | **LLM grounded in the feature payload**, cached per (user, product) in Lakebase `app.explanation_cache`; fetched asynchronously so recs latency is unaffected. |
| App framework | Streamlit vs Dash vs React | **React + FastAPI**: polished UX for exec demo; ~2 extra days of build. |
| App config | `app.yaml` vs bundle `config` | **Bundle `config`** (command + env) so catalog/endpoint names come from bundle variables per target. |
| Fallback | None vs precomputed recs | **Precomputed in Lakebase** (`serving.precomputed_recs`, per user + per acquisition-channel segment): resilience when serving is cold or slow (1.5 s timeout). |
| Agent | Genie only vs Genie + agent | **Genie + tool-calling agent** (ask_genie, simulate_campaign UC function, lookup_product Vector Search), streamed over SSE with visible tool calls. |
| Agent hosting | Agent Framework on Model Serving vs in-app | **In-app agent loop over FMAPI** for v1: no extra endpoint to keep warm, same tools/governance (the app SP's UC grants). Moving it to a `ResponsesAgent` on Model Serving is a packaging change (`genai/` follow-up). |
| recommendation_log → UC | Lakehouse Sync (Beta) vs job export | **Incremental job export** (`lakebase/export_reclog.py`, high-water mark on `log_id`) in the hourly `pers-refresh` job. |
| Groups | Create account groups vs best-effort grants | Account groups can't be created from a workspace; masks/filters use `is_account_group_member(...)` and group grants are best-effort. Create `pers_engineers`, `pers_analysts`, `pers_analysts_emea` at account level to activate them. |
| Product imagery | Stock photo URLs vs generated | **Generated SVG per product** (`/api/img/<id>.svg`) — no external dependency, deterministic. |
| Serving scale-to-zero | On vs off | **Off during the demo** (`serving_scale_to_zero=false`) per spec; flip for cost outside demo windows. |

## Simulator calibration

The click / purchase response model was tuned so that a popularity rail lands at ~3% CTR and ~2% session
conversion, and an oracle ranker reaches ~10% CTR. The trained model sits in between — that gap is the
honest, earned lift reported in the Business Console.
