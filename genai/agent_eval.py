"""Evaluate the merchandiser copilot on golden questions with MLflow GenAI.

Runs the exact agent code the app serves (app/backend/agent.py) as the job's
identity, traces every run, and scores:
  * expected_tool      — did the agent call the tool a merchandiser would expect?
  * grounded_numbers   — LLM judge: numbers in the answer come from tool output
  * RelevanceToQuery   — built-in judge
"""
import argparse
import asyncio
import json
import concurrent.futures
import os
import sys

ap = argparse.ArgumentParser()
ap.add_argument("--catalog", default="fevm_catalog_ra_test")
ap.add_argument("--root", default=None)
ap.add_argument("--warehouse_id", default=os.getenv("DATABRICKS_WAREHOUSE_ID", ""))
args, _ = ap.parse_known_args()
root = args.root or os.getcwd()
sys.path.insert(0, os.path.join(root, "app"))
os.environ.setdefault("PERS_CATALOG", args.catalog)
os.environ.setdefault("DATABRICKS_WAREHOUSE_ID", args.warehouse_id)
# the agent's async HTTP client is bound to one event loop -> evaluate sequentially, fresh client per question
os.environ.setdefault("MLFLOW_GENAI_EVAL_MAX_WORKERS", "1")

import mlflow  # noqa: E402
from mlflow.genai.scorers import Guidelines, RelevanceToQuery, scorer  # noqa: E402

from backend import agent, llm  # noqa: E402

GOLDEN = [
    ("Which product categories have the highest recommendation CTR this week?", "ask_genie"),
    ("How do cold-start users convert compared with returning users?", "ask_genie"),
    ("What is the lift of the challenger over the champion by country?", "ask_genie"),
    ("Which segment has the biggest conversion lift from personalization?", "ask_genie"),
    ("What was revenue from recommendation-attributed purchases in the last 7 days?", "ask_genie"),
    ("How many sessions did we have yesterday and what was the conversion rate?", "ask_genie"),
    ("Which country has the lowest recommendation CTR?", "ask_genie"),
    ("Is personalization working for at_risk users?", "ask_genie"),
    ("Estimate a 10% discount campaign on electronics for champion users.", "simulate_campaign"),
    ("What happens if we give new users 15% off beauty products?", "simulate_campaign"),
    ("Simulate a 5% campaign on books for all segments.", "simulate_campaign"),
    ("Should we run 20% off home_kitchen for buyers or for engaged_browser users?", "simulate_campaign"),
    ("What incremental revenue could a 10% pets campaign for at_risk users drive?", "simulate_campaign"),
    ("Find products similar to noise-cancelling headphones.", "lookup_product"),
    ("Find gift ideas for a runner.", "lookup_product"),
    ("What premium skincare products do we carry?", "lookup_product"),
    ("Show me cozy home items for winter.", "lookup_product"),
    ("Find dog toys and accessories.", "lookup_product"),
    ("Find coffee products and tell me how grocery_gourmet performs with personalization.", "lookup_product"),
    ("Recommend whether to promote the challenger model to champion.", "ask_genie"),
]


@mlflow.trace(name="merchandiser_copilot")
def predict(question: str) -> dict:
    async def go():
        llm._client = None
        tools, evidence, answer = [], [], ""
        async for ev in agent.run(question):
            if ev["type"] == "tool_call":
                tools.append(ev["name"])
            elif ev["type"] == "tool_result":
                # the judges only see inputs/outputs, so the evidence the answer must be grounded in goes here
                evidence.append({"tool": ev["name"], "ok": ev["ok"],
                                 "result": json.dumps(ev["result"], default=str)[:4000]})
            elif ev["type"] == "answer":
                answer = ev["content"]
        return {"answer": answer, "tools": tools, "tool_results": evidence}
    # job tasks run inside an ipykernel that already owns an event loop -> use a fresh loop in a worker thread
    with concurrent.futures.ThreadPoolExecutor(1) as ex:
        return ex.submit(asyncio.run, go()).result()


@scorer
def expected_tool(outputs, expectations) -> bool:
    return expectations["expected_tool"] in (outputs or {}).get("tools", [])


if __name__ == "__main__":
    from databricks.sdk import WorkspaceClient
    me = WorkspaceClient().current_user.me().user_name
    mlflow.set_experiment(f"/Users/{me}/personalization_copilot_eval")
    data = [{"inputs": {"question": q}, "expectations": {"expected_tool": t}} for q, t in GOLDEN]
    res = mlflow.genai.evaluate(
        data=data, predict_fn=predict,
        scorers=[expected_tool, RelevanceToQuery(),
                 Guidelines(name="grounded_numbers",
                            guidelines="Every metric, percentage, count or currency figure stated in outputs.answer must "
                                       "appear in, or be directly computable (rounding, ratios, differences) from, "
                                       "outputs.tool_results. Qualitative statements without numbers are acceptable. "
                                       "Fail only if the answer states a figure that is not supported by tool_results."),
                 Guidelines(name="actionable",
                            guidelines="The answer ends with a concrete recommended next action for a merchandiser.")])
    print(res.metrics)
