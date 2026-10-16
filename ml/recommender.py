"""Two-stage recommender core, shared by training, batch inference, the A/B
simulator and the Model Serving pyfunc.

Stage 1 — candidate generation (up to 200 per request)
    * item-item collaborative filtering: cosine neighbours of implicit-ALS item
      factors, seeded by the session items (strong) and the user's last items (weak)
    * semantic neighbours of the session items from product description
      embeddings (databricks-gte-large-en) — the cold-start path
    * popularity within the user's / session's categories, then global popularity
Stage 2 — LightGBM LambdaRank over user × product × session-context features.

Everything here is numpy/pandas so the same code runs in a Spark driver, a
serverless job and a CPU serving container.
"""
from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass

import numpy as np
import pandas as pd

LOYALTY_ORD = {"none": 0, "bronze": 1, "silver": 2, "gold": 3, "platinum": 4}

BASE_FEATURES = [
    "f_cf", "f_sem", "f_pop", "f_conv", "f_price_rel_cat", "f_user_cat_aff", "f_session_cat_share",
    "f_last_cat_match", "f_session_len", "f_has_history", "f_recency_days", "f_n_orders",
    "f_is_cf", "f_is_sem", "f_is_pop",
]
CHALLENGER_FEATURES = BASE_FEATURES + ["f_brand_match", "f_price_vs_user", "f_loyalty"]


# ----------------------------------------------------------------------------- artifacts

@dataclass
class Artifacts:
    products: pd.DataFrame          # indexed 0..n-1, has product_id/category/brand/price/...
    cf_idx: np.ndarray              # (n, K) neighbour indices
    cf_sim: np.ndarray              # (n, K)
    sem_idx: np.ndarray             # (n, K2)
    sem_sim: np.ndarray             # (n, K2)

    def __post_init__(self):
        self.pid_to_idx = {p: i for i, p in enumerate(self.products["product_id"])}
        self.cat = self.products["category"].to_numpy()
        self.brand = self.products["brand"].to_numpy()
        self.price = self.products["price"].to_numpy(dtype=float)
        self.pop = self.products["popularity_score"].to_numpy(dtype=float)
        self.conv = self.products["conversion_rate"].to_numpy(dtype=float)
        med = self.products.groupby("category")["price"].transform("median").to_numpy(dtype=float)
        self.price_rel_cat = np.log(self.price / med)
        self.pop_order = np.argsort(-self.pop)
        self.cat_pop = {c: np.array([i for i in self.pop_order if self.cat[i] == c])
                        for c in np.unique(self.cat)}

    def save(self, d: str):
        os.makedirs(d, exist_ok=True)
        df = self.products.copy()
        df.attrs = {}  # Spark Connect toPandas() attaches non-serializable plan metrics
        df.to_parquet(f"{d}/products.parquet", index=False)
        np.savez_compressed(f"{d}/neighbors.npz", cf_idx=self.cf_idx, cf_sim=self.cf_sim,
                            sem_idx=self.sem_idx, sem_sim=self.sem_sim)

    @staticmethod
    def load(d: str) -> "Artifacts":
        nb = np.load(f"{d}/neighbors.npz")
        return Artifacts(pd.read_parquet(f"{d}/products.parquet"), nb["cf_idx"], nb["cf_sim"],
                         nb["sem_idx"], nb["sem_sim"])


def top_k_cosine(vecs: np.ndarray, k: int) -> tuple[np.ndarray, np.ndarray]:
    v = vecs / (np.linalg.norm(vecs, axis=1, keepdims=True) + 1e-9)
    sim = v @ v.T
    np.fill_diagonal(sim, -1)
    idx = np.argpartition(-sim, k, axis=1)[:, :k]
    s = np.take_along_axis(sim, idx, axis=1)
    order = np.argsort(-s, axis=1)
    return np.take_along_axis(idx, order, axis=1).astype(np.int32), np.take_along_axis(s, order, axis=1).astype(np.float32)


def implicit_als(user_idx: np.ndarray, item_idx: np.ndarray, conf: np.ndarray, n_users: int, n_items: int,
                 factors: int = 32, reg: float = 0.1, iters: int = 8, seed: int = 0) -> np.ndarray:
    """Hu-Koren-Volinsky implicit ALS. Returns item factors (n_items, factors)."""
    from scipy.sparse import csr_matrix
    rng = np.random.default_rng(seed)
    X = rng.normal(0, 0.01, (n_users, factors))
    Y = rng.normal(0, 0.01, (n_items, factors))
    Cui = csr_matrix((conf, (user_idx, item_idx)), shape=(n_users, n_items))
    Ciu = Cui.T.tocsr()
    I = reg * np.eye(factors)

    def step(A, B, C):
        BtB = B.T @ B
        for u in range(A.shape[0]):
            s, e = C.indptr[u], C.indptr[u + 1]
            if s == e:
                A[u] = 0
                continue
            ix = C.indices[s:e]
            c = C.data[s:e]
            Bi = B[ix]
            M = BtB + (Bi.T * c) @ Bi + I        # Bt (C - I) B with c = conf - 1 stored
            A[u] = np.linalg.solve(M, (Bi.T * (c + 1)).sum(axis=1))

    for _ in range(iters):
        step(X, Y, Cui)
        step(Y, X, Ciu)
    return Y


# ----------------------------------------------------------------------------- context

def parse_user_features(uf: dict | None) -> dict:
    uf = dict(uf or {})
    aff = uf.get("cat_affinity_json") or uf.get("cat_affinity") or "{}"
    uf["cat_affinity"] = json.loads(aff) if isinstance(aff, str) else dict(aff)
    li = uf.get("last_items")
    if isinstance(li, str):
        li = json.loads(li) if li.startswith("[") else [x for x in li.strip("{}").split(",") if x]
    uf["last_items"] = list(li) if li is not None else []
    return uf


def generate_candidates(art: Artifacts, uf: dict, session_items: list[str], n: int = 200):
    """Returns (cand_idx, cf_score, sem_score, pop_flag) over the union of sources."""
    S = [art.pid_to_idx[p] for p in session_items if p in art.pid_to_idx][:5]
    H = [art.pid_to_idx[p] for p in uf.get("last_items", []) if p in art.pid_to_idx][:20]
    n_items = len(art.products)
    cf = np.zeros(n_items, dtype=np.float32)
    sem = np.zeros(n_items, dtype=np.float32)
    for r, i in enumerate(S):
        w = 1.0 * 0.8 ** r
        cf[art.cf_idx[i]] += w * art.cf_sim[i]
        sem[art.sem_idx[i]] += w * art.sem_sim[i]
    for r, i in enumerate(H):
        w = 0.5 * 0.9 ** r
        cf[art.cf_idx[i]] += w * art.cf_sim[i]
        if not S and r < 3:
            sem[art.sem_idx[i]] += 0.5 * art.sem_sim[i]
    popflag = np.zeros(n_items, dtype=np.float32)
    cats = [art.cat[i] for i in S]
    aff = uf.get("cat_affinity", {})
    cats += [c for c, _ in sorted(aff.items(), key=lambda kv: -kv[1])[:3]]
    for c in dict.fromkeys(cats):
        if c in art.cat_pop:
            popflag[art.cat_pop[c][:40]] = 1
    popflag[art.pop_order[:40]] = 1
    pre = cf * 2 + sem + popflag * 0.05 * (1 + art.pop / (art.pop.max() + 1e-9))
    pre[S] = -1  # don't recommend what is on screen / just viewed
    cand = np.argpartition(-pre, n)[:n]
    cand = cand[pre[cand] > 0]
    return cand, cf[cand], sem[cand], popflag[cand]


def candidate_features(art: Artifacts, uf: dict, session_items: list[str], cand, cf, sem, popflag) -> pd.DataFrame:
    S = [art.pid_to_idx[p] for p in session_items if p in art.pid_to_idx][:5]
    sess_cats = [art.cat[i] for i in S]
    ccat = art.cat[cand]
    aff = uf.get("cat_affinity", {})
    avg_price = uf.get("avg_price_viewed")
    avg_price = float(avg_price) if avg_price not in (None, "", "None") and not (
        isinstance(avg_price, float) and math.isnan(avg_price)) else None
    rec = uf.get("recency_days")
    rec = float(rec) if rec not in (None, "", "None") and not (isinstance(rec, float) and math.isnan(rec)) else 999.0
    df = pd.DataFrame({
        "idx": cand,
        "f_cf": cf, "f_sem": sem, "f_pop": art.pop[cand], "f_conv": art.conv[cand],
        "f_price_rel_cat": art.price_rel_cat[cand],
        "f_user_cat_aff": np.array([float(aff.get(c, 0.0)) for c in ccat]),
        "f_session_cat_share": (np.array([sess_cats.count(c) for c in ccat]) / max(1, len(sess_cats))),
        "f_last_cat_match": (ccat == (sess_cats[0] if sess_cats else "__none__")).astype(float),
        "f_session_len": float(len(S)),
        "f_has_history": float(len(uf.get("last_items", [])) > 0),
        "f_recency_days": min(rec, 999.0),
        "f_n_orders": float(uf.get("n_orders_90d") or 0),
        "f_is_cf": (cf > 0).astype(float), "f_is_sem": (sem > 0).astype(float), "f_is_pop": popflag,
        "f_brand_match": (art.brand[cand] == uf.get("top_brand")).astype(float),
        "f_price_vs_user": np.log(art.price[cand] / avg_price) if avg_price else 0.0,
        "f_loyalty": float(LOYALTY_ORD.get(uf.get("loyalty_tier") or "none", 0)),
    })
    return df


def recommend(art: Artifacts, booster, features: list[str], uf: dict, session_items: list[str], k: int = 10,
              exclude: set[str] | None = None) -> list[dict]:
    cand, cf, sem, popflag = generate_candidates(art, uf, session_items)
    if len(cand) == 0:
        cand = art.pop_order[:50]
        cf = sem = np.zeros(len(cand), dtype=np.float32)
        popflag = np.ones(len(cand), dtype=np.float32)
    X = candidate_features(art, uf, session_items, cand, cf, sem, popflag)
    scores = booster.predict(X[features].to_numpy(dtype=np.float32)) if booster is not None else X["f_pop"].to_numpy()
    X["score"] = scores
    X = X.sort_values("score", ascending=False)
    out = []
    for r in X.itertuples():
        pid = art.products["product_id"].iat[r.idx]
        if exclude and pid in exclude:
            continue
        source = "session_cf" if r.f_cf > 0 and r.f_session_len > 0 else (
            "history_cf" if r.f_cf > 0 else ("semantic" if r.f_sem > 0 else "popular"))
        out.append({"product_id": pid, "score": round(float(r.score), 4), "source": source,
                    "signals": {"user_cat_affinity": round(float(r.f_user_cat_aff), 3),
                                "session_cat_share": round(float(r.f_session_cat_share), 3),
                                "cf": round(float(r.f_cf), 3), "semantic": round(float(r.f_sem), 3),
                                "brand_match": bool(r.f_brand_match), "popularity": round(float(r.f_pop), 2)}})
        if len(out) >= k:
            break
    return out


def popularity_recs(art: Artifacts, k: int = 10, exclude: set[str] | None = None) -> list[dict]:
    out = []
    for i in art.pop_order:
        pid = art.products["product_id"].iat[i]
        if exclude and pid in exclude:
            continue
        out.append({"product_id": pid, "score": round(float(art.pop[i]), 4), "source": "popular", "signals": {}})
        if len(out) >= k:
            break
    return out


# ----------------------------------------------------------------------------- MLflow pyfunc

try:
    import mlflow.pyfunc

    class RecommenderModel(mlflow.pyfunc.PythonModel):
        """Input rows: user_id, session_items (JSON list), user_features (JSON object), k.
        Output: one JSON string per row (list of {product_id, score, source, signals})."""

        def load_context(self, context):
            import lightgbm as lgb
            d = context.artifacts["bundle"]
            self.art = Artifacts.load(d)
            self.meta = json.load(open(f"{d}/meta.json"))
            self.booster = lgb.Booster(model_file=f"{d}/ranker.txt")
            self.features = self.meta["features"]

        def predict(self, context, model_input: pd.DataFrame, params=None):
            out = []
            for row in model_input.to_dict("records"):
                si = row.get("session_items") or "[]"
                si = json.loads(si) if isinstance(si, str) else list(si)
                ufr = row.get("user_features") or "{}"
                uf = parse_user_features(json.loads(ufr) if isinstance(ufr, str) else ufr)
                k = int(row.get("k") or 10)
                recs = recommend(self.art, self.booster, self.features, uf, si, k=k)
                out.append(json.dumps({"recommendations": recs, "model_variant": self.meta["variant"]}))
            return pd.Series(out, name="predictions")
except ImportError:  # pragma: no cover - mlflow absent in some local test envs
    RecommenderModel = None
