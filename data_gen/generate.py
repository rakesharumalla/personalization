"""Synthetic data generator job.

Modes
-----
backfill  Regenerates the catalog, the user registry and N days of history into
          the landing volume (idempotent: same seed -> same files), and writes the
          simulator's hidden ground truth to ``<catalog>.ml.sim_user_truth``.
stream    Emits a continuous stream of clickstream/order/user-CDC JSON files every
          ``--interval`` seconds at ~``--rate`` events/s for ``--minutes`` minutes.

Landing layout (UC volume ``<catalog>.bronze.raw_landing``)::

    products/  users/  events/  orders/
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone


def _args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=None, help="bundle file root (for imports)")
    ap.add_argument("--catalog", default="fevm_catalog_ra_test")
    ap.add_argument("--mode", choices=["backfill", "stream"], default="backfill")
    ap.add_argument("--users", type=int, default=10_000)
    ap.add_argument("--products", type=int, default=2_000)
    ap.add_argument("--days", type=int, default=28)
    ap.add_argument("--sessions_per_day", type=int, default=1_500)
    ap.add_argument("--rate", type=float, default=50.0, help="stream events per second")
    ap.add_argument("--interval", type=float, default=5.0, help="stream seconds between files")
    ap.add_argument("--minutes", type=float, default=30.0, help="stream duration")
    ap.add_argument("--seed", type=int, default=7)
    return ap.parse_known_args()[0]


args = _args()
if args.root:
    sys.path.insert(0, args.root)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))) if "__file__" in globals() else ".")

import numpy as np  # noqa: E402

from data_gen.simulator import (COLD_START_SHARE, LOYALTY_TIERS, World, build_world,  # noqa: E402
                                make_users, simulate_session, stable_rng, truth_rows)

LANDING = f"/Volumes/{args.catalog}/bronze/raw_landing"


def _write_jsonl(subdir: str, name: str, rows: list[dict]):
    if not rows:
        return
    rows = _inject_noise(subdir, rows, stable_rng("noise", name))
    d = f"{LANDING}/{subdir}"
    os.makedirs(d, exist_ok=True)
    tmp = f"{d}/.{name}.tmp"
    with open(tmp, "w") as f:
        for r in rows:
            f.write(json.dumps(r, default=str) + "\n")
    os.replace(tmp, f"{d}/{name}")


def _inject_noise(subdir: str, rows: list[dict], rng) -> list[dict]:
    """Realistic dirt so data-quality expectations have something to catch:
    ~0.3% null user_id, ~0.2% unknown event_type, ~0.3% zero price, ~0.2% duplicated events."""
    out = []
    for r in rows:
        x = rng.random()
        if subdir == "events":
            if x < 0.003:
                r = {**r, "user_id": None}
            elif x < 0.005:
                r = {**r, "event_type": "unknown"}
            elif x < 0.007:
                out.append(r)  # duplicate delivery
        elif subdir == "orders" and x < 0.003:
            r = {**r, "unit_price": 0.0}
        out.append(r)
    return out


def _public(p: dict) -> dict:
    return {k: v for k, v in p.items() if not k.startswith("_")}


def _user_cdc(u: dict, op: str, change_ts: datetime) -> dict:
    return {**u, "op": op, "change_ts": change_ts.isoformat()}


def _tier_for(purchases: int) -> str:
    return LOYALTY_TIERS[min(4, int(np.searchsorted([1, 3, 6, 10], purchases, side="right")))]


def backfill(world: World):
    now = datetime.now(timezone.utc).replace(microsecond=0)
    start = (now - timedelta(days=args.days)).replace(hour=0, minute=0, second=0)
    _write_jsonl("products", "products_000.json", [_public(p) for p in world.products])

    rng0 = stable_rng("initial_tiers", args.seed)
    base_users = []
    for u in world.users:
        act = world.truth[u["user_id"]]["activity"]
        prior_purchases = int(rng0.poisson(act * 1.5))
        base_users.append({**u, "loyalty_tier": _tier_for(prior_purchases)})
    _write_jsonl("users", "users_000_initial.json",
                 [_user_cdc(u, "INSERT", datetime.fromisoformat(u["signup_ts"])) for u in base_users])

    uids = np.array([u["user_id"] for u in base_users])
    activity = np.array([world.truth[u]["activity"] for u in uids])
    activity = activity / activity.sum()
    user_rows = {u["user_id"]: u for u in base_users}
    purchases = {u: 0 for u in uids}
    tier = {u["user_id"]: u["loyalty_tier"] for u in base_users}
    next_index = args.users
    for day in range(args.days):
        rng = stable_rng("day", args.seed, day)
        day_start = start + timedelta(days=day)
        # weekly seasonality: weekends +25%
        n_sessions = int(args.sessions_per_day * (1.25 if day_start.weekday() >= 5 else 1.0))
        events, orders, cdc = [], [], []
        for s in range(n_sessions):
            st = day_start + timedelta(seconds=float(rng.uniform(0, 86_400)))
            cold = rng.random() < COLD_START_SHARE
            if cold:
                new_users, new_truth = make_users(1, world.products, args.seed + 4, start_index=next_index,
                                                  signup_start=st - timedelta(minutes=5), signup_end=st)
                next_index += 1
                world.truth.update(new_truth)
                nu = new_users[0]
                world.users.append(nu)
                user_rows[nu["user_id"]] = nu
                cdc.append(_user_cdc(nu, "INSERT", st))
                purchases[nu["user_id"]] = 0
                tier[nu["user_id"]] = "none"
                uid = nu["user_id"]
            else:
                uid = str(uids[int(rng.choice(len(uids), p=activity))])
            ev, od = simulate_session(world, uid, st, rng, is_cold_start=cold)
            events += ev
            orders += od
            if od:
                purchases[uid] += 1
                new_tier = _tier_for(purchases[uid] + (2 if tier[uid] != "none" else 0))
                if LOYALTY_TIERS.index(new_tier) > LOYALTY_TIERS.index(tier[uid]):
                    tier[uid] = new_tier
                    user_rows[uid] = {**user_rows[uid], "loyalty_tier": new_tier}
                    cdc.append(_user_cdc(user_rows[uid], "UPDATE",
                                         datetime.fromisoformat(od[-1]["ts"]) + timedelta(seconds=1)))
        tag = day_start.strftime("%Y%m%d")
        _write_jsonl("events", f"events_{tag}.json", events)
        _write_jsonl("orders", f"orders_{tag}.json", orders)
        _write_jsonl("users", f"users_{tag}_cdc.json", cdc)
        print(f"day {tag}: sessions={n_sessions} events={len(events)} orders={len(orders)} cdc={len(cdc)}")
    return world


def write_truth(world: World):
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    rows = truth_rows(world.truth)
    df = spark.createDataFrame(rows, schema=(
        "user_id string, persona string, affinity_json string, price_sensitivity double, "
        "fav_brands array<string>, activity double"))
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{args.catalog}.ml.sim_user_truth")
    spark.sql(f"COMMENT ON TABLE {args.catalog}.ml.sim_user_truth IS "
              "'Hidden simulator ground truth (persona preferences). Used only by the A/B simulator, never by models.'")
    print(f"wrote {len(rows)} truth rows")


def stream(world: World):
    rng = stable_rng("stream", args.seed, int(time.time()))
    uids = np.array([u["user_id"] for u in world.users])
    activity = np.array([world.truth[u]["activity"] for u in uids])
    activity = activity / activity.sum()
    deadline = time.time() + args.minutes * 60
    next_index = 1_000_000 + int(time.time()) % 1_000_000 * 10
    tick = 0
    while time.time() < deadline:
        t0 = time.time()
        now = datetime.now(timezone.utc)
        target = int(args.rate * args.interval)
        events, orders, cdc = [], [], []
        while len(events) < target:
            cold = rng.random() < COLD_START_SHARE
            if cold:
                nu_list, nt = make_users(1, world.products, args.seed + 99, start_index=next_index,
                                         signup_start=now - timedelta(minutes=2), signup_end=now)
                next_index += 1
                world.truth.update(nt)
                uid = nu_list[0]["user_id"]
                cdc.append(_user_cdc(nu_list[0], "INSERT", now - timedelta(minutes=1)))
            else:
                uid = str(uids[int(rng.choice(len(uids), p=activity))])
            ev, od = simulate_session(world, uid, now, rng, is_cold_start=cold)
            if ev:  # shift so the session ends "now"
                shift = datetime.fromisoformat(ev[-1]["ts"]) - now
                for e in ev:
                    e["ts"] = (datetime.fromisoformat(e["ts"]) - shift).isoformat()
                for o in od:
                    o["ts"] = (datetime.fromisoformat(o["ts"]) - shift).isoformat()
            events += ev
            orders += od
        tag = now.strftime("%Y%m%dT%H%M%S") + f"_{tick:05d}"
        _write_jsonl("events", f"stream_{tag}.json", events)
        _write_jsonl("orders", f"stream_{tag}.json", orders)
        _write_jsonl("users", f"stream_{tag}_cdc.json", cdc)
        tick += 1
        if tick % 12 == 0:
            print(f"{now.isoformat()} tick={tick} events={len(events)} orders={len(orders)} new_users={len(cdc)}")
        time.sleep(max(0.0, args.interval - (time.time() - t0)))


if __name__ == "__main__":
    world = build_world(args.users, args.products, args.seed)
    if args.mode == "backfill":
        world = backfill(world)
        write_truth(world)
    else:
        stream(world)
