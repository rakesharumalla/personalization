"""Local smoke tests for the simulator and the recommender core (no Spark needed)."""
import json
import os
import sys
from datetime import datetime, timezone

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_gen.simulator import build_world, respond_to_recs, simulate_session  # noqa: E402
from ml.recommender import (BASE_FEATURES, CHALLENGER_FEATURES, Artifacts, candidate_features,  # noqa: E402
                            generate_candidates, implicit_als, parse_user_features, popularity_recs,
                            recommend, top_k_cosine)


def _world_and_events(n_users=400, n_products=300, sessions=1500):
    w = build_world(n_users, n_products)
    rng = np.random.default_rng(0)
    rows = []
    for s in range(sessions):
        uid = w.users[s % n_users]["user_id"]
        ev, _ = simulate_session(w, uid, datetime.now(timezone.utc), rng)
        rows += ev
    return w, pd.DataFrame(rows)


class _FakeBooster:
    def predict(self, X):
        return X[:, 0] + X[:, 5]


def _artifacts(w, ev):
    prods = pd.DataFrame([{k: v for k, v in p.items() if not k.startswith("_")} for p in w.products])
    prods["popularity_score"] = np.log1p(prods.product_id.map(ev.product_id.value_counts()).fillna(0))
    prods["conversion_rate"] = 0.01
    prods["price_tier"] = "mid"
    pid = {p: i for i, p in enumerate(prods.product_id)}
    pe = ev.dropna(subset=["product_id"])
    ui = pe.groupby(["user_id", "product_id"]).size().reset_index(name="w")
    uc, uu = pd.factorize(ui.user_id)
    item_f = implicit_als(uc, ui.product_id.map(pid).to_numpy(), 10.0 * ui.w.to_numpy(), len(uu), len(prods),
                          factors=8, iters=3)
    cf_idx, cf_sim = top_k_cosine(item_f, 20)
    sem_idx, sem_sim = top_k_cosine(np.random.default_rng(1).normal(size=(len(prods), 16)), 10)
    return Artifacts(prods[["product_id", "title", "category", "brand", "price", "popularity_score",
                            "conversion_rate", "price_tier"]], cf_idx, cf_sim, sem_idx, sem_sim)


def test_simulator_session_shape():
    w, ev = _world_and_events(sessions=200)
    assert {"event_id", "user_id", "session_id", "event_type", "ts"} <= set(ev.columns)
    assert ev.event_type.isin(["view", "add_to_cart", "purchase", "search"]).all()


def test_recommend_end_to_end(tmp_path):
    w, ev = _world_and_events()
    art = _artifacts(w, ev)
    art.save(str(tmp_path))
    art = Artifacts.load(str(tmp_path))
    uf = parse_user_features({"cat_affinity_json": json.dumps({"electronics": 0.7}), "last_items": "{P00001,P00002}",
                              "n_orders_90d": 2, "recency_days": 1.5, "avg_price_viewed": 50.0,
                              "top_brand": art.products.brand.iat[0], "loyalty_tier": "gold"})
    assert uf["last_items"] == ["P00001", "P00002"]
    session = ["P00010", "P00011"]
    cand, cf, sem, pf = generate_candidates(art, uf, session)
    assert 0 < len(cand) <= 200 and art.pid_to_idx["P00010"] not in set(cand)
    X = candidate_features(art, uf, session, cand, cf, sem, pf)
    assert set(CHALLENGER_FEATURES) <= set(X.columns)
    recs = recommend(art, _FakeBooster(), BASE_FEATURES, uf, session, k=10)
    assert len(recs) == 10 and len({r["product_id"] for r in recs}) == 10
    # cold start, empty session -> popularity fallback still fills the rail
    cold = recommend(art, _FakeBooster(), BASE_FEATURES, parse_user_features({}), [], k=10)
    assert len(cold) == 10
    assert len(popularity_recs(art, 5)) == 5


def test_rec_response_prefers_high_utility():
    w = build_world(50, 300)
    rng = np.random.default_rng(3)
    uid = w.users[0]["user_id"]
    best = list(w.p_ids[np.argsort(-w.utility(uid))[:10]])
    worst = list(w.p_ids[np.argsort(w.utility(uid))[:10]])
    clicks_best = sum(r["clicked"] for _ in range(300) for r in respond_to_recs(w, uid, best, rng, False))
    clicks_worst = sum(r["clicked"] for _ in range(300) for r in respond_to_recs(w, uid, worst, rng, False))
    assert clicks_best > clicks_worst * 3
