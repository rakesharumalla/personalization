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
