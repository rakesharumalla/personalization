"""Create/update the Model Serving endpoint for the recommender.

Serves @champion (100% traffic) and @challenger (0% traffic, callable by served
entity name for shadow/A-B calls) with AI Gateway inference tables in <catalog>.ml.
"""
import argparse
from datetime import timedelta

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (AiGatewayConfig, AiGatewayInferenceTableConfig, EndpointCoreConfigInput,
                                            Route, ServedEntityInput, TrafficConfig)

ap = argparse.ArgumentParser()
ap.add_argument("--catalog", default="fevm_catalog_ra_test")
ap.add_argument("--endpoint", default="pers-recommender")
ap.add_argument("--scale_to_zero", default="false")
ap.add_argument("--root", default=None)
args, _ = ap.parse_known_args()

w = WorkspaceClient()
model = f"{args.catalog}.ml.recommender"
s2z = args.scale_to_zero.lower() == "true"
versions = {a: w.model_versions.get_by_alias(model, a).version for a in ("champion", "challenger")}
entities = [ServedEntityInput(name=a, entity_name=model, entity_version=str(v), workload_size="Small",
                              scale_to_zero_enabled=s2z) for a, v in versions.items()]
traffic = TrafficConfig(routes=[Route(served_model_name="champion", traffic_percentage=100),
                                Route(served_model_name="challenger", traffic_percentage=0)])

existing = {e.name for e in w.serving_endpoints.list()}
if args.endpoint not in existing:
    print("creating endpoint", args.endpoint, versions)
    w.serving_endpoints.create_and_wait(
        name=args.endpoint,
        config=EndpointCoreConfigInput(name=args.endpoint, served_entities=entities, traffic_config=traffic),
        ai_gateway=AiGatewayConfig(inference_table_config=AiGatewayInferenceTableConfig(
            catalog_name=args.catalog, schema_name="ml", table_name_prefix="recommender", enabled=True)),
        description="Two-stage personalization recommender (champion/challenger)",
        timeout=timedelta(minutes=50))
else:
    cur = w.serving_endpoints.get(args.endpoint)
    cur_versions = {e.name: e.entity_version for e in (cur.config.served_entities or [])} if cur.config else {}
    if cur_versions == {k: str(v) for k, v in versions.items()}:
        print("endpoint already serving", versions)
    else:
        print("updating endpoint", cur_versions, "->", versions)
        w.serving_endpoints.update_config_and_wait(name=args.endpoint, served_entities=entities,
                                                   traffic_config=traffic,
                                                   timeout=timedelta(minutes=50))
print(w.serving_endpoints.get(args.endpoint).state)
