"""Mosaic AI Vector Search: delta-sync index over gold.product_features.search_text
with managed databricks-gte-large-en embeddings. Used by the app ("similar to what
you just viewed") and by the merchandiser copilot's lookup_product tool."""
import argparse
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.vectorsearch import (DeltaSyncVectorIndexSpecRequest, EmbeddingSourceColumn,
                                                 PipelineType, VectorIndexType)

ap = argparse.ArgumentParser()
ap.add_argument("--catalog", default="fevm_catalog_ra_test")
ap.add_argument("--endpoint", default="pers-vs")
ap.add_argument("--embedding_endpoint", default="databricks-gte-large-en")
ap.add_argument("--root", default=None)
args, _ = ap.parse_known_args()
w = WorkspaceClient()
index = f"{args.catalog}.gold.product_index"

for _ in range(60):
    ep = w.vector_search_endpoints.get_endpoint(args.endpoint)
    state = str(ep.endpoint_status.state) if ep.endpoint_status else "?"
    if "ONLINE" in state:
        break
    print("waiting for VS endpoint", state)
    time.sleep(30)

try:
    w.vector_search_indexes.get_index(index)
    print("index exists, syncing", index)
    w.vector_search_indexes.sync_index(index)
except NotFound:
    print("creating index", index)
    w.vector_search_indexes.create_index(
        name=index, endpoint_name=args.endpoint, primary_key="product_id",
        index_type=VectorIndexType.DELTA_SYNC,
        delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
            source_table=f"{args.catalog}.gold.product_features",
            pipeline_type=PipelineType.TRIGGERED,
            embedding_source_columns=[EmbeddingSourceColumn(
                name="search_text", embedding_model_endpoint_name=args.embedding_endpoint)],
            columns_to_sync=["product_id", "title", "category", "brand", "price", "price_tier", "image_url",
                             "search_text"]))
for _ in range(60):
    st = w.vector_search_indexes.get_index(index).status
    if st and st.ready:
        print("index ready", st.indexed_row_count)
        break
    print("index provisioning", st.message[:120] if st and st.message else "")
    time.sleep(30)
