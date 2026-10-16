"""UC functions used as agent tools (and directly queryable by analysts)."""
import argparse

from pyspark.sql import SparkSession

ap = argparse.ArgumentParser()
ap.add_argument("--catalog", default="fevm_catalog_ra_test")
ap.add_argument("--root", default=None)
args, _ = ap.parse_known_args()
C = args.catalog
spark = SparkSession.builder.getOrCreate()

spark.sql(f"""
CREATE OR REPLACE FUNCTION {C}.gold.simulate_campaign(
  p_category STRING COMMENT 'product category, or all',
  p_segment STRING COMMENT 'user segment (new, champion, buyer, engaged_browser, casual_browser, at_risk), or all',
  p_discount_pct DOUBLE COMMENT 'discount in percent, e.g. 10 for 10%')
RETURNS TABLE (
  category STRING, segment STRING, discount_pct DOUBLE, reach_users BIGINT, weekly_sessions DOUBLE,
  baseline_conversion DOUBLE, personalized_conversion DOUBLE, expected_conversion_with_discount DOUBLE,
  expected_weekly_orders DOUBLE, incremental_weekly_orders DOUBLE, expected_weekly_revenue DOUBLE)
COMMENT 'Estimate reach and expected lift of a personalized campaign. Reach from gold.user_segments; conversion \
from gold.experiment_results (popularity vs champion); discount elasticity: +2.5% relative conversion per discount \
point (capped at 50%), revenue net of discount.'
RETURN
  WITH reach AS (
    SELECT count(*) AS n, coalesce(avg(greatest(n_orders_90d, 0)), 0) AS avg_orders
    FROM {C}.gold.user_segments us
    WHERE (lower(p_segment) = 'all' OR us.segment = p_segment)
      AND (lower(p_category) = 'all' OR us.primary_category = p_category)),
  conv AS (
    SELECT
      coalesce(sum(CASE WHEN er.model_variant = 'popularity' THEN er.converted_sessions END)
               / nullif(sum(CASE WHEN er.model_variant = 'popularity' THEN er.sessions END), 0), 0.02) AS base,
      coalesce(sum(CASE WHEN er.model_variant = 'champion' THEN er.converted_sessions END)
               / nullif(sum(CASE WHEN er.model_variant = 'champion' THEN er.sessions END), 0), 0.026) AS pers,
      coalesce(sum(er.revenue) / nullif(sum(er.converted_sessions), 0), 60) AS aov
    FROM {C}.gold.experiment_results er
    WHERE (lower(p_segment) = 'all' OR er.segment = p_segment)),
  cat_aov AS (
    SELECT coalesce(avg(pf.price), (SELECT aov FROM conv)) AS price
    FROM {C}.gold.product_features pf WHERE lower(p_category) = 'all' OR pf.category = p_category)
  SELECT p_category, p_segment, p_discount_pct, reach.n,
         round(reach.n * 0.9, 1),
         round(conv.base, 5), round(conv.pers, 5),
         round(conv.pers * (1 + least(0.5, 0.025 * p_discount_pct)), 5),
         round(reach.n * 0.9 * conv.pers * (1 + least(0.5, 0.025 * p_discount_pct)), 1),
         round(reach.n * 0.9 * (conv.pers * (1 + least(0.5, 0.025 * p_discount_pct)) - conv.base), 1),
         round(reach.n * 0.9 * conv.pers * (1 + least(0.5, 0.025 * p_discount_pct))
               * cat_aov.price * (1 - p_discount_pct / 100), 2)
  FROM reach, conv, cat_aov
""")
print(spark.sql(f"SELECT * FROM {C}.gold.simulate_campaign('electronics', 'all', 10)").toPandas().T)

# Business case: translate the measured revenue-per-session lift into annual dollars and margin.
# Inputs are the customer's own numbers (defaults in the app/deck are Acme Retail assumptions).
BUSINESS_CASE_SQL = f"""
CREATE OR REPLACE FUNCTION {C}.gold.business_case(
  p_monthly_sessions DOUBLE COMMENT 'storefront sessions per month, e.g. 5000000',
  p_gross_margin DOUBLE COMMENT 'gross margin as a fraction, e.g. 0.35',
  p_realization DOUBLE COMMENT 'share of the simulated lift expected in production, e.g. 0.5',
  p_annual_run_cost DOUBLE COMMENT 'annual platform + team cost in USD, e.g. 750000')
RETURNS TABLE (
  model_variant STRING, baseline_revenue_per_session DOUBLE, personalized_revenue_per_session DOUBLE,
  uplift_per_session DOUBLE, annual_sessions DOUBLE, annual_incremental_revenue DOUBLE,
  annual_incremental_gross_profit DOUBLE, annual_net_benefit DOUBLE, roi_multiple DOUBLE, payback_months DOUBLE)
COMMENT 'Annual value of personalization: (personalized - popularity revenue per session) x sessions x realization, \
times gross margin, net of run cost. Revenue per session comes from gold.experiment_results (all sessions, \
cold start included). Owner: VP of E-commerce; approver: CFO.'
RETURN
  WITH rps AS (
    SELECT er.model_variant, sum(er.revenue) / sum(er.sessions) AS rps
    FROM {C}.gold.experiment_results er GROUP BY er.model_variant),
  base AS (SELECT rps AS b FROM rps WHERE model_variant = 'popularity'),
  v AS (
    SELECT rps.model_variant, base.b, rps.rps AS p, rps.rps - base.b AS u, p_monthly_sessions * 12 AS s
    FROM rps, base WHERE rps.model_variant <> 'popularity')
  SELECT v.model_variant, round(v.b, 4), round(v.p, 4), round(v.u, 4), v.s,
         round(v.u * v.s * p_realization, 0),
         round(v.u * v.s * p_realization * p_gross_margin, 0),
         round(v.u * v.s * p_realization * p_gross_margin - p_annual_run_cost, 0),
         round(v.u * v.s * p_realization * p_gross_margin / nullif(p_annual_run_cost, 0), 2),
         round(p_annual_run_cost / nullif(v.u * v.s * p_realization * p_gross_margin / 12, 0), 1)
  FROM v
"""
spark.sql(BUSINESS_CASE_SQL)
print(spark.sql(f"SELECT * FROM {C}.gold.business_case(5000000, 0.35, 0.5, 750000)").toPandas().T)
