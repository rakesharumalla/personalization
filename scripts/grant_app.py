"""Grant the Databricks App's service principal least-privilege access to everything
it touches that is created by the setup job (and therefore can't be an app
resource at bundle-deploy time): UC schemas/tables, the recommender endpoint,
the Genie space, the Vector Search endpoint/index."""
import argparse

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import PermissionsChange, Privilege
from databricks.sdk.service.serving import ServingEndpointAccessControlRequest, ServingEndpointPermissionLevel

ap = argparse.ArgumentParser()
ap.add_argument("--catalog", default="fevm_catalog_ra_test")
ap.add_argument("--app", default="pers-personalization")
ap.add_argument("--serving_endpoint", default="pers-recommender")
ap.add_argument("--vs_endpoint", default="pers-vs")
ap.add_argument("--genie_title", default="Personalization Insights")
ap.add_argument("--root", default=None)
args, _ = ap.parse_known_args()
w = WorkspaceClient()
C = args.catalog
sp = w.apps.get(args.app).service_principal_client_id
print("app service principal", sp)


def uc(securable_type: str, name: str, privs):
    w.grants.update(securable_type=securable_type, full_name=name,
                    changes=[PermissionsChange(principal=sp, add=privs)])
    print("UC", securable_type, name, [p.value for p in privs])


uc("catalog", C, [Privilege.USE_CATALOG])
uc("schema", f"{C}.gold", [Privilege.USE_SCHEMA, Privilege.SELECT, Privilege.EXECUTE])
uc("schema", f"{C}.ml", [Privilege.USE_SCHEMA, Privilege.SELECT])
uc("schema", f"{C}.app", [Privilege.USE_SCHEMA, Privilege.SELECT, Privilege.MODIFY, Privilege.CREATE_TABLE])

ep = w.serving_endpoints.get(args.serving_endpoint)
w.serving_endpoints.update_permissions(ep.id, access_control_list=[ServingEndpointAccessControlRequest(
    service_principal_name=sp, permission_level=ServingEndpointPermissionLevel.CAN_QUERY)])
print("serving CAN_QUERY", args.serving_endpoint)

vs = w.vector_search_endpoints.get_endpoint(args.vs_endpoint)
try:
    w.api_client.do("PATCH", f"/api/2.0/permissions/vector-search-endpoints/{vs.id}", body={
        "access_control_list": [{"service_principal_name": sp, "permission_level": "CAN_USE"}]})
    print("vector search CAN_USE", args.vs_endpoint)
except Exception as e:  # noqa: BLE001
    print("vector search endpoint permission skipped:", str(e)[:200])

spaces = w.api_client.do("GET", "/api/2.0/genie/spaces").get("spaces", [])
for s in spaces:
    if s.get("title") == args.genie_title:
        w.api_client.do("PATCH", f"/api/2.0/permissions/genie/{s['space_id']}", body={
            "access_control_list": [{"service_principal_name": sp, "permission_level": "CAN_RUN"}]})
        print("genie CAN_RUN", s["space_id"])
