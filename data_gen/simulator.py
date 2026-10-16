"""Synthetic marketplace simulator.

Everything the rest of the solution learns from is generated here. The
simulator embeds *learnable* structure so the recommender earns its lift:

* products belong to categories/brands with a latent quality and a zipfian
  popularity prior;
* users belong to personas with category affinities, a price sensitivity and
  favourite brands;
* sessions have an intent category that drifts, and ~30% of sessions are
  cold-start (brand-new users with no history).

The same ground-truth preference function drives both the organic clickstream
and the response to recommendations in the A/B simulation
(``experiments/simulate_ab.py``), so offline lift reflects ranking quality
rather than hard-coded numbers.

Pure python + numpy: runs locally (tests) and on serverless jobs.
"""
from __future__ import annotations

import hashlib
import json
import math
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import numpy as np

CATEGORIES = [
    "electronics", "home_kitchen", "fashion_women", "fashion_men", "beauty",
    "sports_outdoors", "toys_games", "books", "grocery_gourmet", "pets",
    "health_wellness", "garden",
]

# median price per category (USD) — prices are lognormal around these
CATEGORY_MEDIAN_PRICE = {
    "electronics": 180, "home_kitchen": 55, "fashion_women": 48, "fashion_men": 45,
    "beauty": 26, "sports_outdoors": 70, "toys_games": 32, "books": 18,
    "grocery_gourmet": 14, "pets": 30, "health_wellness": 24, "garden": 40,
}

CATEGORY_NOUNS = {
    "electronics": ["Wireless Earbuds", "Smart Speaker", "4K Monitor", "Mechanical Keyboard", "Action Camera",
                    "Noise-Cancelling Headphones", "Portable Charger", "Smartwatch", "Gaming Mouse", "E-Reader"],
    "home_kitchen": ["Chef's Knife", "Cast Iron Skillet", "Espresso Maker", "Linen Duvet", "Air Fryer",
                     "Stand Mixer", "Ceramic Vase", "Throw Blanket", "Pour-Over Kettle", "Cutting Board"],
    "fashion_women": ["Wrap Dress", "Leather Tote", "Cashmere Sweater", "Ankle Boots", "Silk Scarf",
                      "High-Rise Jeans", "Trench Coat", "Ballet Flats", "Midi Skirt", "Gold Hoops"],
    "fashion_men": ["Oxford Shirt", "Chino Pants", "Leather Belt", "Wool Overcoat", "Suede Sneakers",
                    "Merino Crewneck", "Denim Jacket", "Chelsea Boots", "Polo Shirt", "Canvas Backpack"],
    "beauty": ["Vitamin C Serum", "Hydrating Moisturizer", "Matte Lipstick", "Clay Mask", "SPF 50 Sunscreen",
               "Hair Oil", "Eyeshadow Palette", "Retinol Cream", "Micellar Water", "Volumizing Mascara"],
    "sports_outdoors": ["Trail Running Shoes", "Yoga Mat", "Insulated Bottle", "Camping Tent", "Resistance Bands",
                        "Cycling Helmet", "Hiking Backpack", "Foam Roller", "Rain Shell", "Adjustable Dumbbells"],
    "toys_games": ["Building Blocks Set", "Strategy Board Game", "Plush Bear", "RC Car", "Puzzle 1000pc",
                   "Science Kit", "Card Game", "Wooden Train Set", "Art Easel", "Dollhouse"],
    "books": ["Mystery Novel", "Cookbook", "Sci-Fi Epic", "Business Memoir", "Kids Picture Book",
              "Self-Help Guide", "History Hardcover", "Poetry Collection", "Graphic Novel", "Travel Guide"],
    "grocery_gourmet": ["Single-Origin Coffee", "Olive Oil", "Dark Chocolate", "Matcha Powder", "Hot Sauce",
                        "Loose Leaf Tea", "Sea Salt Flakes", "Maple Syrup", "Granola", "Truffle Honey"],
    "pets": ["Dog Bed", "Cat Tree", "Grain-Free Kibble", "Chew Toy", "Pet Carrier",
             "Leash & Harness", "Cat Litter", "Aquarium Kit", "Pet Shampoo", "Treat Pouch"],
    "health_wellness": ["Multivitamin", "Protein Powder", "Massage Gun", "Sleep Mask", "Essential Oil Diffuser",
                        "Electric Toothbrush", "Fitness Tracker", "Collagen Peptides", "Meditation Cushion", "Blue Light Glasses"],
    "garden": ["Raised Planter", "Pruning Shears", "Hose Reel", "Seed Starter Kit", "Solar Lights",
               "Compost Bin", "Garden Gloves", "Bird Feeder", "Watering Can", "Herb Garden Kit"],
}

ADJECTIVES = ["Classic", "Pro", "Eco", "Premium", "Everyday", "Ultra", "Compact", "Signature",
              "Essential", "Deluxe", "Modern", "Vintage", "Lightweight", "Smart", "Organic"]

BRANDS_PER_CATEGORY = 6
BRAND_SYLLABLES = ["nor", "vex", "lumi", "kai", "ora", "zen", "tal", "bri", "mar", "quo", "sol", "fen",
                   "ari", "dex", "olo", "pix", "rho", "sky", "uma", "vio"]

PERSONAS = {
    # persona: (category -> alpha weight, price_sensitivity, share)
    "tech_enthusiast": ({"electronics": 6, "books": 1.5, "toys_games": 1, "health_wellness": 1}, 0.4, 0.14),
    "home_maker": ({"home_kitchen": 6, "garden": 2.5, "grocery_gourmet": 2}, 0.8, 0.14),
    "fashionista": ({"fashion_women": 5, "beauty": 3, "fashion_men": 1}, 0.6, 0.14),
    "athlete": ({"sports_outdoors": 6, "health_wellness": 3, "fashion_men": 1}, 0.7, 0.12),
    "parent": ({"toys_games": 5, "books": 2, "home_kitchen": 1.5, "health_wellness": 1}, 1.1, 0.12),
    "foodie": ({"grocery_gourmet": 6, "home_kitchen": 2.5, "books": 1}, 0.9, 0.1),
    "pet_lover": ({"pets": 6, "home_kitchen": 1, "garden": 1}, 0.9, 0.1),
    "bargain_hunter": ({c: 1.0 for c in CATEGORIES}, 2.2, 0.14),
}

COUNTRIES = (["US", "GB", "DE", "FR", "CA", "AU", "IN", "BR"], [0.38, 0.12, 0.1, 0.08, 0.1, 0.07, 0.08, 0.07])
AGE_BANDS = (["18-24", "25-34", "35-44", "45-54", "55+"], [0.18, 0.32, 0.25, 0.15, 0.10])
CHANNELS = (["paid_social", "paid_search", "organic", "referral", "email", "affiliate"], [0.3, 0.25, 0.2, 0.1, 0.08, 0.07])
DEVICES = (["mobile", "desktop", "tablet", "app"], [0.52, 0.3, 0.06, 0.12])
REFERRERS = ["google", "instagram", "tiktok", "direct", "newsletter", "partner_blog", "facebook"]
LOYALTY_TIERS = ["none", "bronze", "silver", "gold", "platinum"]

COLD_START_SHARE = 0.30
RESERVED_EMAIL_DOMAINS = ["example.com", "example.org", "example.net"]


def stable_rng(*parts) -> np.random.Generator:
    """Deterministic RNG keyed on arbitrary parts (idempotent regeneration)."""
    h = hashlib.sha256("|".join(map(str, parts)).encode()).digest()
    return np.random.default_rng(int.from_bytes(h[:8], "little"))


# ---------------------------------------------------------------------------
# Catalog & users
# ---------------------------------------------------------------------------

@dataclass
class World:
    products: list[dict]
    users: list[dict]
    truth: dict[str, dict]  # user_id -> hidden preferences
    # derived arrays for vectorised scoring
    p_ids: np.ndarray = field(default=None)
    p_cat_idx: np.ndarray = field(default=None)
    p_brand: np.ndarray = field(default=None)
    p_logprice_rel: np.ndarray = field(default=None)
    p_quality: np.ndarray = field(default=None)
    p_pop_prior: np.ndarray = field(default=None)

    def __post_init__(self):
        self.p_ids = np.array([p["product_id"] for p in self.products])
        self.p_cat_idx = np.array([CATEGORIES.index(p["category"]) for p in self.products])
        self.p_brand = np.array([p["brand"] for p in self.products])
        self.p_logprice_rel = np.array(
            [math.log(p["price"] / CATEGORY_MEDIAN_PRICE[p["category"]]) for p in self.products])
        self.p_quality = np.array([p["_quality"] for p in self.products])
        self.p_pop_prior = np.array([p["_pop_prior"] for p in self.products])
        self._log_pop_mean = float(np.log(self.p_pop_prior).mean())
        self.pid_to_idx = {pid: i for i, pid in enumerate(self.p_ids)}
        self.cat_to_pidx = {c: np.where(self.p_cat_idx == i)[0] for i, c in enumerate(CATEGORIES)}

    # ground-truth utility of every product for one user ------------------
    def utility(self, user_id: str) -> np.ndarray:
        t = self.truth[user_id]
        aff = np.array([t["affinity"][c] for c in CATEGORIES])
        # centred so a uniform-affinity, median-price, average product scores ~0
        u = 1.6 * np.log(aff[self.p_cat_idx] * len(CATEGORIES) + 1e-3)
        u += 0.9 * np.isin(self.p_brand, t["fav_brands"])
        u -= t["price_sensitivity"] * self.p_logprice_rel
        u += self.p_quality + 0.35 * (np.log(self.p_pop_prior) - self._log_pop_mean)
        return u


def _brand_names(rng) -> dict[str, list[str]]:
    out = {}
    used = set()
    for c in CATEGORIES:
        names = []
        while len(names) < BRANDS_PER_CATEGORY:
            n = "".join(rng.choice(BRAND_SYLLABLES, size=2)).capitalize()
            if n not in used:
                used.add(n)
                names.append(n)
        out[c] = names
    return out


def make_products(n_products: int, seed: int = 7) -> list[dict]:
    rng = stable_rng("products", seed)
    brands = _brand_names(rng)
    ranks = rng.permutation(n_products) + 1
    pop_prior = 1.0 / ranks ** 0.9
    pop_prior = pop_prior / pop_prior.max()
    products = []
    for i in range(n_products):
        cat = CATEGORIES[i % len(CATEGORIES)]
        brand = brands[cat][int(rng.integers(BRANDS_PER_CATEGORY))]
        noun = CATEGORY_NOUNS[cat][int(rng.integers(len(CATEGORY_NOUNS[cat])))]
        adj = ADJECTIVES[int(rng.integers(len(ADJECTIVES)))]
        price = round(float(CATEGORY_MEDIAN_PRICE[cat] * math.exp(rng.normal(0, 0.45))), 2)
        tier = "budget" if price < CATEGORY_MEDIAN_PRICE[cat] * 0.75 else (
            "premium" if price > CATEGORY_MEDIAN_PRICE[cat] * 1.4 else "mid")
        tags = sorted({cat.split("_")[0], tier, adj.lower(), noun.split()[-1].lower()})
        pid = f"P{i:05d}"
        products.append({
            "product_id": pid,
            "title": f"{brand} {adj} {noun}",
            "description": (f"The {adj.lower()} {noun.lower()} from {brand}. A {tier}-tier pick in "
                            f"{cat.replace('_', ' ')} loved for {', '.join(tags[:2])} qualities."),
            "category": cat,
            "brand": brand,
            "price": price,
            "image_url": f"/api/img/{pid}.svg",
            "tags": tags,
            "_quality": float(rng.normal(0, 0.5)),
            "_pop_prior": float(pop_prior[i]),
        })
    return products


def make_users(n_users: int, products: list[dict], seed: int = 11, start_index: int = 0,
               signup_start: datetime | None = None, signup_end: datetime | None = None):
    """Returns (users, truth). Users are public rows; truth holds hidden preferences."""
    signup_end = signup_end or datetime.now(timezone.utc)
    signup_start = signup_start or signup_end - timedelta(days=720)
    brands_by_cat: dict[str, list[str]] = {}
    for p in products:
        brands_by_cat.setdefault(p["category"], [])
        if p["brand"] not in brands_by_cat[p["category"]]:
            brands_by_cat[p["category"]].append(p["brand"])
    persona_names = list(PERSONAS)
    persona_share = np.array([PERSONAS[p][2] for p in persona_names])
    persona_share = persona_share / persona_share.sum()
    users, truth = [], {}
    span = (signup_end - signup_start).total_seconds()
    for k in range(start_index, start_index + n_users):
        rng = stable_rng("user", seed, k)
        persona = persona_names[int(rng.choice(len(persona_names), p=persona_share))]
        alpha_map, ps, _ = PERSONAS[persona]
        alpha = np.array([alpha_map.get(c, 0.15) for c in CATEGORIES]) * 3 + 0.2
        aff = rng.dirichlet(alpha)
        top_cats = [CATEGORIES[i] for i in np.argsort(-aff)[:2]]
        fav_brands = [str(rng.choice(brands_by_cat[c])) for c in top_cats]
        uid = f"U{k:06d}"
        signup = signup_start + timedelta(seconds=float(rng.uniform(0, span)))
        users.append({
            "user_id": uid,
            "email": f"shopper{k}@{RESERVED_EMAIL_DOMAINS[k % 3]}",
            "signup_ts": signup.isoformat(),
            "country": str(rng.choice(COUNTRIES[0], p=COUNTRIES[1])),
            "age_band": str(rng.choice(AGE_BANDS[0], p=AGE_BANDS[1])),
            "acquisition_channel": str(rng.choice(CHANNELS[0], p=CHANNELS[1])),
            "loyalty_tier": "none",
        })
        truth[uid] = {
            "persona": persona,
            "affinity": {c: float(a) for c, a in zip(CATEGORIES, aff)},
            "price_sensitivity": float(max(0.1, rng.normal(ps, 0.25))),
            "fav_brands": fav_brands,
            "activity": float(rng.lognormal(0, 0.8)),
        }
    return users, truth


def build_world(n_users: int = 10_000, n_products: int = 2_000, seed: int = 7) -> World:
    products = make_products(n_products, seed)
    users, truth = make_users(n_users, products, seed + 4)
    return World(products=products, users=users, truth=truth)


# ---------------------------------------------------------------------------
# Sessions
# ---------------------------------------------------------------------------

def _softmax_choice(rng, scores: np.ndarray, temp: float = 1.0) -> int:
    z = (scores - scores.max()) / temp
    p = np.exp(z)
    p /= p.sum()
    return int(rng.choice(len(scores), p=p))


def simulate_session(world: World, user_id: str, start: datetime, rng: np.random.Generator,
                     is_cold_start: bool = False) -> tuple[list[dict], list[dict]]:
    """Simulate one organic browsing session. Returns (events, order_lines)."""
    t = world.truth[user_id]
    util = world.utility(user_id)
    aff = np.array([t["affinity"][c] for c in CATEGORIES])
    session_id = uuid.UUID(int=int(rng.integers(0, 2**63)) << 64 | int(rng.integers(0, 2**63))).hex
    device = str(rng.choice(DEVICES[0], p=DEVICES[1]))
    referrer = str(rng.choice(REFERRERS))
    n_steps = int(min(25, 1 + rng.geometric(0.17)))
    intent = int(rng.choice(len(CATEGORIES), p=aff))
    ts = start
    events, orders, cart = [], [], []

    def ev(event_type, product_id, query=None):
        events.append({
            "event_id": uuid.UUID(int=int(rng.integers(0, 2**63)) << 64 | int(rng.integers(0, 2**63))).hex,
            "user_id": user_id, "session_id": session_id, "event_type": event_type,
            "product_id": product_id, "ts": ts.isoformat(), "device": device,
            "referrer": referrer, "search_query": query,
        })

    if rng.random() < 0.25:
        ev("search", None, query=CATEGORIES[intent].replace("_", " "))
    for _ in range(n_steps):
        ts += timedelta(seconds=float(rng.exponential(35)))
        if rng.random() < 0.18:  # intent drift
            intent = int(rng.choice(len(CATEGORIES), p=aff))
        pool = world.cat_to_pidx[CATEGORIES[intent]] if rng.random() < 0.85 else np.arange(len(world.p_ids))
        # shoppers browse what they like, but also what is merchandised (popularity)
        idx = pool[_softmax_choice(rng, util[pool] + 0.8 * np.log(world.p_pop_prior[pool]), temp=1.6)]
        pid = world.p_ids[idx]
        ev("view", pid)
        u = util[idx]
        if rng.random() < 1 / (1 + math.exp(-(u - 1.2))) * 0.18:
            ts += timedelta(seconds=float(rng.exponential(10)))
            ev("add_to_cart", pid)
            cart.append(idx)
    # checkout
    if cart:
        best = max(util[i] for i in cart)
        p_buy = (0.0085 if is_cold_start else 0.026) / (1 + math.exp(-(best - 0.5)))
        if rng.random() < p_buy:
            ts += timedelta(seconds=float(rng.exponential(60)))
            order_id = f"O{int(rng.integers(1e11, 1e12))}"
            for i in sorted(set(cart)):
                pid = world.p_ids[i]
                ev("purchase", pid)
                orders.append({
                    "order_id": order_id, "user_id": user_id, "product_id": pid,
                    "qty": int(1 + rng.poisson(0.2)), "unit_price": world.products[i]["price"],
                    "ts": ts.isoformat(),
                })
    return events, orders


# ---------------------------------------------------------------------------
# Response to recommendations (A/B simulation)
# ---------------------------------------------------------------------------

CLICK_BASE = -3.75     # logit intercept for a recommendation click
CLICK_SLOPE = 0.45     # sensitivity to ground-truth utility
POSITION_DECAY = 0.12  # logit penalty per slot
BUY_BASE = -4.6
BUY_SLOPE = 0.3


def respond_to_recs(world: World, user_id: str, rec_ids: list[str], rng: np.random.Generator,
                    is_cold_start: bool) -> list[dict]:
    """Simulate clicks/purchases on a rail of recommended products."""
    util = world.utility(user_id)
    out = []
    for pos, pid in enumerate(rec_ids):
        idx = world.pid_to_idx[pid]
        u = util[idx]
        p_click = 1 / (1 + math.exp(-(CLICK_BASE + CLICK_SLOPE * u - POSITION_DECAY * pos)))
        clicked = rng.random() < p_click
        purchased = False
        if clicked:
            p_buy = 1 / (1 + math.exp(-(BUY_BASE + BUY_SLOPE * u - (0.5 if is_cold_start else 0.0))))
            purchased = rng.random() < p_buy
        out.append({"product_id": pid, "position": pos, "clicked": bool(clicked),
                    "purchased": bool(purchased), "utility": float(u)})
    return out


def organic_conversion_prob(is_cold_start: bool) -> float:
    """Probability a session converts without help from the rec rail."""
    return 0.004 if is_cold_start else 0.011


def truth_rows(truth: dict[str, dict]) -> list[dict]:
    return [{"user_id": uid, "persona": t["persona"], "affinity_json": json.dumps(t["affinity"]),
             "price_sensitivity": t["price_sensitivity"], "fav_brands": t["fav_brands"],
             "activity": t["activity"]} for uid, t in truth.items()]
