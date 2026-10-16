"""Train, evaluate and register the two-stage recommender.

1. temporal split of silver.events (holdout = last ``--holdout_days`` days)
2. stage-1 artifacts: implicit-ALS item neighbours + gte-large semantic neighbours
3. point-in-time training queries (user state *before* the session, session prefix
   as context, the rest of the session as graded targets)
4. LightGBM LambdaRank: champion (base features) and challenger (+brand/price/loyalty)
5. offline NDCG@10 / Recall@50 vs. a popularity baseline, logged to MLflow
6. register both in UC as <catalog>.ml.recommender with @champion / @challenger
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import tempfile
from collections import OrderedDict, defaultdict

ap = argparse.ArgumentParser()
ap.add_argument("--catalog", default="fevm_catalog_ra_test")
ap.add_argument("--root", default=None)
ap.add_argument("--holdout_days", type=float, default=4)
ap.add_argument("--max_train_queries", type=int, default=20_000)
ap.add_argument("--embedding_endpoint", default="databricks-gte-large-en")
args, _ = ap.parse_known_args()
if args.root:
    sys.path.insert(0, args.root)

import lightgbm as lgb  # noqa: E402
import mlflow  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from mlflow.models import infer_signature  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

from ml.recommender import (BASE_FEATURES, CHALLENGER_FEATURES, Artifacts, RecommenderModel,  # noqa: E402
                            candidate_features, generate_candidates, implicit_als, popularity_recs,
                            recommend, top_k_cosine)

C = args.catalog
spark = SparkSession.builder.getOrCreate()
rng = np.random.default_rng(42)
GAIN = {"view": 1, "add_to_cart": 2, "purchase": 3}

# ----------------------------------------------------------------------------- load
ev = spark.sql(f"""
    SELECT e.user_id, e.session_id, e.event_type, e.product_id, e.ts
    FROM {C}.silver.events e WHERE e.product_id IS NOT NULL""").toPandas()
ev["ts"] = pd.to_datetime(ev["ts"], utc=True)
ev = ev.sort_values("ts").reset_index(drop=True)
products = spark.read.table(f"{C}.gold.product_features").toPandas().sort_values("product_id").reset_index(drop=True)
users = spark.sql(f"SELECT user_id, loyalty_tier FROM {C}.silver.users WHERE __END_AT IS NULL").toPandas()
tier = dict(zip(users.user_id, users.loyalty_tier))
cutoff = ev["ts"].max() - pd.Timedelta(days=args.holdout_days)
train_ev = ev[ev.ts < cutoff]
print(f"events={len(ev)} train={len(train_ev)} cutoff={cutoff}")

pid_to_idx = {p: i for i, p in enumerate(products.product_id)}
n_items = len(products)

# popularity / conversion from the training window only (no leakage)
cnt = train_ev.groupby(["product_id", "event_type"]).size().unstack(fill_value=0)
views = products.product_id.map(cnt.get("view", pd.Series(dtype=float))).fillna(0).to_numpy()
buys = products.product_id.map(cnt.get("purchase", pd.Series(dtype=float))).fillna(0).to_numpy()
products["popularity_score"] = np.log1p(views + 3 * buys)
products["conversion_rate"] = (buys + 0.1) / (views + 10)

# ----------------------------------------------------------------------------- stage 1: ALS
w = train_ev.event_type.map(GAIN).astype(float)
ui = train_ev.assign(w=w).groupby(["user_id", "product_id"])["w"].sum().reset_index()
u_codes, u_uniques = pd.factorize(ui.user_id)
i_codes = ui.product_id.map(pid_to_idx).to_numpy()
item_f = implicit_als(u_codes, i_codes, 10.0 * ui.w.to_numpy(), len(u_uniques), n_items, factors=32, iters=8)
cf_idx, cf_sim = top_k_cosine(item_f, 50)
print("ALS done", item_f.shape)

# ----------------------------------------------------------------------------- stage 1: semantic
emb_table = f"{C}.gold.product_embeddings"
emb = None
if spark.catalog.tableExists(emb_table):
    e = spark.read.table(emb_table).toPandas()
    if len(e) == n_items:
        emb = np.stack(e.set_index("product_id").loc[products.product_id, "embedding"].to_numpy())
if emb is None:
    from databricks.sdk import WorkspaceClient
    wc = WorkspaceClient()
    vecs = []
    texts = products.search_text.tolist()
    for s in range(0, len(texts), 100):
        r = wc.serving_endpoints.query(name=args.embedding_endpoint, input=texts[s:s + 100])
        vecs += [d.embedding for d in r.data]
    emb = np.array(vecs, dtype=np.float32)
    (spark.createDataFrame(pd.DataFrame({"product_id": products.product_id, "embedding": [v.tolist() for v in emb]}))
     .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(emb_table))
sem_idx, sem_sim = top_k_cosine(emb.astype(np.float32), 30)
print("semantic neighbours done", emb.shape)

prod_art = products[["product_id", "title", "category", "brand", "price", "popularity_score", "conversion_rate",
                     "price_tier"]].copy()
art = Artifacts(prod_art, cf_idx, cf_sim, sem_idx, sem_sim)

# ----------------------------------------------------------------------------- point-in-time queries
cat_of = dict(zip(products.product_id, products.category))
brand_of = dict(zip(products.product_id, products.brand))
price_of = dict(zip(products.product_id, products.price))


class UserState:
    __slots__ = ("items", "cat_w", "brand_w", "psum", "pcnt", "orders", "last_ts")

    def __init__(self):
        self.items, self.cat_w, self.brand_w = OrderedDict(), defaultdict(float), defaultdict(float)
        self.psum = self.pcnt = self.orders = 0
        self.last_ts = None

    def snapshot(self, uid, now) -> dict:
        tot = sum(self.cat_w.values()) or 1.0
        return {
            "user_id": uid, "last_items": list(reversed(self.items.keys()))[:20],
            "cat_affinity": {c: v / tot for c, v in self.cat_w.items()},
            "top_brand": max(self.brand_w, key=self.brand_w.get) if self.brand_w else None,
            "avg_price_viewed": self.psum / self.pcnt if self.pcnt else None,
            "n_orders_90d": self.orders, "loyalty_tier": tier.get(uid, "none"),
            "recency_days": (now - self.last_ts).total_seconds() / 86400 if self.last_ts is not None else None,
        }

    def update(self, rows):
        for r in rows:
            self.items.pop(r.product_id, None)
            self.items[r.product_id] = True
            g = GAIN[r.event_type]
            self.cat_w[cat_of[r.product_id]] += g
            self.brand_w[brand_of[r.product_id]] += g
            if r.event_type == "view":
                self.psum += price_of[r.product_id]
                self.pcnt += 1
        if any(r.event_type == "purchase" for r in rows):
            self.orders += 1
        while len(self.items) > 40:
            self.items.popitem(last=False)
        self.last_ts = rows[-1].ts


states = defaultdict(UserState)
queries = []  # (split, uf, session_items, {pid: gain})
for sid, g in ev.groupby("session_id", sort=False):
    rows = list(g.itertuples(index=False))
    uid = rows[0].user_id
    st = states[uid]
    uf = st.snapshot(uid, rows[0].ts)
    split = "train" if rows[0].ts < cutoff else "holdout"
    seq = [r for r in rows if r.event_type in GAIN]
    if len(seq) >= 2:
        k = int(rng.integers(1, len(seq)))
        ctx = list(dict.fromkeys(r.product_id for r in reversed(seq[:k])))
        tgt = {}
        for r in seq[k:]:
            if r.product_id not in ctx:
                tgt[r.product_id] = max(tgt.get(r.product_id, 0), GAIN[r.event_type])
        if tgt:
            queries.append((split, uf, ctx, tgt))
    if seq and uf["last_items"] and rng.random() < 0.25:  # "homepage" query: no session context yet
        tgt = {}
        for r in seq:
            tgt[r.product_id] = max(tgt.get(r.product_id, 0), GAIN[r.event_type])
        queries.append((split, uf, [], tgt))
    st.update(rows)

train_q = [q for q in queries if q[0] == "train"]
hold_q = [q for q in queries if q[0] == "holdout"]
rng.shuffle(train_q)
train_q = train_q[: args.max_train_queries]
print(f"queries train={len(train_q)} holdout={len(hold_q)}")


def build_matrix(qs):
    frames, groups = [], []
    for qi, (_, uf, ctx, tgt) in enumerate(qs):
        cand, cf, sem, popf = generate_candidates(art, uf, ctx)
        if len(cand) == 0:
            continue
        X = candidate_features(art, uf, ctx, cand, cf, sem, popf)
        X["label"] = [tgt.get(art.products.product_id.iat[i], 0) for i in cand]
        if X.label.sum() == 0:
            continue
        X["qid"] = qi
        frames.append(X)
        groups.append(len(X))
    return pd.concat(frames, ignore_index=True), groups


Xtr, gtr = build_matrix(train_q)
print("train matrix", Xtr.shape, "queries with a positive candidate:", len(gtr))


def train_ranker(features):
    m = lgb.LGBMRanker(objective="lambdarank", n_estimators=300, learning_rate=0.05, num_leaves=31,
                       min_child_samples=40, subsample=0.8, subsample_freq=1, colsample_bytree=0.9,
                       label_gain=[0, 1, 3, 7], verbose=-1)
    m.fit(Xtr[features].to_numpy(dtype=np.float32), Xtr.label.to_numpy(), group=gtr)
    return m.booster_


# ----------------------------------------------------------------------------- evaluation

def ndcg_at(ranked, tgt, k=10):
    dcg = sum((2 ** tgt.get(p, 0) - 1) / np.log2(i + 2) for i, p in enumerate(ranked[:k]))
    ideal = sorted(tgt.values(), reverse=True)[:k]
    idcg = sum((2 ** g - 1) / np.log2(i + 2) for i, g in enumerate(ideal))
    return dcg / idcg if idcg else 0.0


def evaluate(booster, features, qs, n=4000):
    sample = qs if len(qs) <= n else [qs[i] for i in rng.choice(len(qs), n, replace=False)]
    nd, rc, nd_cold, ncold = [], [], [], 0
    for _, uf, ctx, tgt in sample:
        if booster is None:
            ranked = [r["product_id"] for r in popularity_recs(art, 50, exclude=set(ctx))]
        else:
            ranked = [r["product_id"] for r in recommend(art, booster, features, uf, ctx, k=50)]
        v = ndcg_at(ranked, tgt)
        nd.append(v)
        rc.append(len(set(ranked[:50]) & set(tgt)) / len(tgt))
        if not uf["last_items"]:
            nd_cold.append(v)
    return {"ndcg_at_10": float(np.mean(nd)), "recall_at_50": float(np.mean(rc)),
            "ndcg_at_10_cold_start": float(np.mean(nd_cold)) if nd_cold else 0.0, "n_queries": len(sample)}


results = {"popularity": evaluate(None, None, hold_q)}
print("popularity", results["popularity"])

user = spark.sql("SELECT current_user()").first()[0]
mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(f"/Users/{user}/personalization_recommender")
model_name = f"{C}.ml.recommender"
client = mlflow.MlflowClient()
example = pd.DataFrame([{
    "user_id": "U000001", "session_items": json.dumps(list(products.product_id[:2])),
    "user_features": json.dumps({"cat_affinity_json": json.dumps({"electronics": 0.6}), "last_items": [],
                                 "n_orders_90d": 1, "loyalty_tier": "silver", "recency_days": 2.0,
                                 "avg_price_viewed": 80.0, "top_brand": None}), "k": 10}])

versions = {}
for variant, feats in (("champion", BASE_FEATURES), ("challenger", CHALLENGER_FEATURES)):
    booster = train_ranker(feats)
    metrics = evaluate(booster, feats, hold_q)
    results[variant] = metrics
    print(variant, metrics)
    d = tempfile.mkdtemp()
    bundle = os.path.join(d, "bundle")
    art.save(bundle)
    booster.save_model(os.path.join(bundle, "ranker.txt"))
    json.dump({"variant": variant, "features": feats, "cutoff": str(cutoff)}, open(f"{bundle}/meta.json", "w"))
    with mlflow.start_run(run_name=f"recommender-{variant}") as run:
        mlflow.log_params({"variant": variant, "n_features": len(feats), "als_factors": 32, "cf_neighbors": 50,
                           "semantic_neighbors": 30, "embedding_endpoint": args.embedding_endpoint,
                           "train_queries": len(gtr), "holdout_days": args.holdout_days})
        mlflow.log_metrics({f"{k}": v for k, v in metrics.items() if k != "n_queries"})
        mlflow.log_metrics({f"baseline_{k}": v for k, v in results["popularity"].items() if k != "n_queries"})
        imp = dict(zip(feats, booster.feature_importance("gain").round(1).tolist()))
        mlflow.log_dict(imp, "feature_importance.json")
        model = RecommenderModel()
        model.load_context(type("Ctx", (), {"artifacts": {"bundle": bundle}})())
        sig = infer_signature(example, model.predict(None, example))
        info = mlflow.pyfunc.log_model(
            name="recommender", python_model=RecommenderModel(), artifacts={"bundle": bundle},
            code_paths=[os.path.join(args.root or ".", "ml")], signature=sig, input_example=example,
            pip_requirements=["mlflow>=3.1", "lightgbm==4.6.0", "pandas>=2.0", "numpy>=1.26", "pyarrow>=14",
                              "scipy>=1.11"],
            registered_model_name=model_name)
        versions[variant] = info.registered_model_version
    shutil.rmtree(d, ignore_errors=True)

for alias, v in versions.items():
    client.set_registered_model_alias(model_name, alias, v)
client.update_registered_model(model_name, description=(
    "Two-stage recommender: implicit-ALS item-item CF + gte-large semantic candidates, LightGBM LambdaRank "
    f"ranker. Offline holdout: {json.dumps({k: round(v['ndcg_at_10'], 4) for k, v in results.items()})} NDCG@10."))

eval_rows = [{"model_variant": k, **{m: float(x) for m, x in v.items()}} for k, v in results.items()]
(spark.createDataFrame(pd.DataFrame(eval_rows)).write.mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{C}.ml.offline_evaluation"))
print("registered", versions, json.dumps(results, indent=1))
