# Build Log & Trade-Off Log

Living document — rendered in the app's **About This Build → Build log & trade-offs** tab.

## Workflow

spec → DAB scaffold → data generator → SDP pipeline → UC setup → features & model → Lakebase sync → Genie space → app → experiment → About page.

Everything is deployed by `databricks bundle deploy` plus one job (`pers-setup`), then `databricks bundle run pers_app`.

## How AI tools were used in the build

Two kinds of AI show up in this project, and they're worth keeping apart:

- **AI that built it.** Claude Code (an AI coding agent working in the terminal against the live workspace)
  wrote the spec refinements, every line of code, the bundle, the docs, the review notebook and the deck.
- **AI that ships in it.** Foundation Model APIs and Mosaic AI run inside the product: `databricks-gte-large-en`
  embeddings, Vector Search, Claude Haiku "why this?" explanations, the Claude Sonnet copilot, and Genie.

### Working pattern

1. **Human sets direction.** The spec (personas, metrics, targets, scope), the customer framing (Acme Retail),
   and every go/no-go call came from the solution architect. The agent proposed; the architect chose.
2. **Agent reads before it writes.** Before each layer, the agent loaded the current Databricks product
   guidance (Lakebase, Declarative Pipelines, bundles, Genie, Apps) and inspected the workspace itself
   (catalogs, warehouses, available model endpoints, CLI and SDK versions), rather than relying on memory.
3. **Small, verifiable steps.** Each layer was deployed and checked against real data before the next one
   was built: row counts, masked emails, offline NDCG, a live endpoint call, a Genie answer.
4. **Failures are fixed at the root and logged.** When something broke, the agent read the actual error,
   fixed the cause, and recorded the trade-off below. Nothing was papered over with retries or mocks.

### Part by part

| Part | What the AI did | What the human decided | How it was verified | What went wrong, and the fix |
|---|---|---|---|---|
| Spec and scope | Turned the draft spec into a buildable plan; mapped every section to a bundle resource | Personas, targets, Acme Retail framing, what to cut | Spec checklist against the deployed assets | Gaps between spec and workspace (no CREATE CATALOG) were raised, not hidden |
| Synthetic data | Wrote the persona simulator, session model and rec-response model | Personas and target baselines (3% CTR, 2% conversion) | Local calibration runs against the targets | First calibration gave 0.03% conversion; re-centred the utility function and re-tuned over 3 runs to land at 3.0% CTR / 2.1% conversion |
| Lakeflow pipeline | Wrote bronze/silver/gold, expectations, quarantine, SCD2 users with a column mask | Which rules drop vs. warn; masking policy | Pipeline run, row counts, `sh***@example.com` in silver | Backfill had no dirty rows, so the DQ panel read zero; added realistic noise to the stream generator (0.05% now dropped) |
| Governance | Wrote schemas, mask and row-filter functions, tags, least-privilege grants | Group model, what the app may read | Grants applied, masked reads checked | No CREATE CATALOG on the metastore; moved Lakebase synced tables into a `serving` schema |
| Features | Wrote PK + CDF feature tables with MERGE upserts | Segment definitions | Table counts, Lakebase sync | "Champion" segment had 11 users; the rule was too strict for the order volume, loosened to 1,001 |
| Recommender | Wrote implicit ALS, semantic candidates, LightGBM ranker, MLflow packaging | Two-stage design, champion vs. challenger features | Temporal holdout: NDCG@10 0.072 to 0.121 | Spark Connect's `toPandas()` attached unserializable metadata and crashed artifact saving; cleared it. MLlib ALS isn't on serverless; wrote a numpy ALS |
| Model Serving | Wrote endpoint deployment and the app client | Scale-to-zero off for the demo | Live calls from the app and notebook | The job's older SDK required an extra field; the endpoint nests predictions one level deeper than expected, so every live call had silently fallen back until the parser was fixed |
| Lakebase | Wrote synced tables, app schema migrations, OAuth token refresh | App-side feature lookup vs. in-serving lookup | Lookup ~5 ms; app wrote session state | Local dev hit "must be owner"; migrations now skip objects owned by the app's service principal |
| GenAI features | Wrote the grounded explanation prompt and the copilot tool loop | Haiku for speed, Sonnet for tool use; grounding rules | 20-question MLflow evaluation | Eval hung (event loop reused across threads); the grounding judge scored 10% because it couldn't see tool results. Fixed both, now 20/20 |
| Genie | Wrote the space: instructions, metric definitions, trusted SQL, owners | Metric definitions and default time window | Seeded questions answered with correct SQL | None, beyond adding the business-case function later |
| App | Wrote the FastAPI backend and React frontend | Four pages, what each persona sees | API smoke tests against the deployed app | MUI v9 had removed the layout props the first draft used (15 type errors); moved to `sx` |
| Business case | Wrote the `gold.business_case` UC function and the CFO panel | Acme Retail assumptions, 50% realization discount | Same math in SQL, app and deck | Added after review feedback that lifts weren't translated into dollars or tied to an owner |
| Security and release | Moved workspace IDs to a git-ignored `.env`, scanned for secrets, pushed to GitHub | Repo, account, public visibility | Secret-scan hooks; staged-file scan | GitHub rejected the private author email; used the account's no-reply address |

Tools not used: Databricks Assistant was not used in this build. Genie was used as a product feature and to
sanity-check gold tables during development.

### Where the AI needed the human

- **Business judgment.** Which metrics matter, who owns them, and how much of a simulated lift to bank (50%)
  are decisions, not code.
- **Honesty about results.** The agent reported the misses (AOV below target for the champion, latency above
  100 ms p95) instead of tuning the simulator until they disappeared.
- **Access.** GitHub accounts, workspace permissions and IP access lists needed the human's credentials and choices.

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
