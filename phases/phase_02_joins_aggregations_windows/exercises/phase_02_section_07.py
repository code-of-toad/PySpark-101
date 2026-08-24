from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    StructType,
    StructField,
)

from practice_data import (
    spark,
)
from phase_02_section_01 import (
    sales_lines_df,
)

"""
Phase 2, Section 7: Many-to-Many Joins & Pre-Aggregation
--------------------------------------------------------
1. why many-to-many joins multiply rows
2. why multiplied rows can corrupt measures
3. when pre-aggregation is necessary

QUESTIONS
=========
Q: Why does the direct promotion join overstate revenue?
Q: What determines whether pre-aggregation is the correct fix?
Q: What is the grain of `promotion_summary_df`?
Q: Why is many-to-many not universally wrong, even though it is dangerous?
"""


# =============================================================================
# 7.1 Create multiple promotions per product
# =============================================================================
# GRAIN:
#     one row per product-promotion
promotions_schema = StructType([
    StructField('promotion_id',   StringType(), False),
    StructField('product_id',     StringType(), False),
    StructField('promotion_type', StringType(), False),
])

promotions_df = spark.createDataFrame(
    [
        ('PROMO-01', 'P001', 'LOYALTY'),
        ('PROMO-02', 'P001', 'WEEKEND'),
        ('PROMO-03', 'P002', 'LOYALTY'),
    ],
    schema=promotions_schema,
)
# promotions_df.show(truncate=False)
# =>
"""
`promotions_df`
+------------+----------+--------------+                                        
|promotion_id|product_id|promotion_type|
+------------+----------+--------------+
|PROMO-01    |P001      |LOYALTY       |
|PROMO-02    |P001      |WEEKEND       |
|PROMO-03    |P002      |LOYALTY       |
+------------+----------+--------------+
"""


# =============================================================================
# 7.2 Direct many-to-many join
# ----------------------------
# Before running...
#
# `sales_lines_df`
# =============================================================================
# =>
"""
LEFT: `sales_lines_df`
+--------+-----------+----------+--------+----------+------------+-----------+---------------+---------+
|order_id|line_number|product_id|quantity|unit_price|discount_pct|gross_sales|discount_amount|net_sales|
+--------+-----------+----------+--------+----------+------------+-----------+---------------+---------+
|1001    |1          |P001      |2       |12.00     |0.0000      |24.00      |0.000000       |24.000000|
|1001    |2          |P002      |1       |30.00     |0.1000      |30.00      |3.000000       |27.000000|
|1002    |1          |P001      |3       |12.00     |0.0500      |36.00      |1.800000       |34.200000|
|1002    |2          |P003      |1       |50.00     |0.0000      |50.00      |0.000000       |50.000000|
|1003    |1          |P002      |2       |30.00     |0.0000      |60.00      |0.000000       |60.000000|
|1004    |1          |P003      |2       |50.00     |0.1000      |100.00     |10.000000      |90.000000|
|1004    |2          |P004      |4       |8.00      |0.0000      |32.00      |0.000000       |32.000000|
|1005    |1          |P001      |1       |12.00     |0.0000      |12.00      |0.000000       |12.000000|
|1005    |2          |P004      |5       |8.00      |0.0500      |40.00      |2.000000       |38.000000|
+--------+-----------+----------+--------+----------+------------+-----------+---------------+---------+

RIGHT: `promotions_df`
+------------+----------+--------------+                                        
|promotion_id|product_id|promotion_type|
+------------+----------+--------------+
|PROMO-01    |P001      |LOYALTY       |
|PROMO-02    |P001      |WEEKEND       |
|PROMO-03    |P002      |LOYALTY       |
+------------+----------+--------------+
"""
# Every P001 sales line matches BOTH P001 promotions.
# Thus, the resulting sales values are repeated once per matching promotion.
many_to_many_df = sales_lines_df.join(
    promotions_df,
    on='product_id',
    how='left',
).select(
    'product_id',
    'order_id',
    'line_number',
    'promotion_id',
    'net_sales',
)
# many_to_many_df.show(truncate=False)
# =>
"""
OUTPUT: `many_to_many_df`
+----------+--------+-----------+------------+---------+                        
|product_id|order_id|line_number|promotion_id|net_sales|
+----------+--------+-----------+------------+---------+
|P001      |1001    |1          |PROMO-02    |24.000000|
|P001      |1001    |1          |PROMO-01    |24.000000|
|P002      |1001    |2          |PROMO-03    |27.000000|
|P001      |1002    |1          |PROMO-02    |34.200000|
|P001      |1002    |1          |PROMO-01    |34.200000|
|P003      |1002    |2          |NULL        |50.000000|
|P002      |1003    |1          |PROMO-03    |60.000000|
|P003      |1004    |1          |NULL        |90.000000|
|P004      |1004    |2          |NULL        |32.000000|
|P004      |1005    |2          |NULL        |38.000000|
|P001      |1005    |1          |PROMO-02    |12.000000|
|P001      |1005    |1          |PROMO-01    |12.000000|
+----------+--------+-----------+------------+---------+
"""

source_revenue = (
    sales_lines_df
    .agg(
        F.sum('net_sales').alias('net_sales')
    )
    .first()['net_sales']
)
# print(f'Source revenue: {source_revenue}')
# =>
"""
Source revenue: 367.200000
"""
multiplied_revenue = (
    many_to_many_df
    .agg(
        F.sum('net_sales').alias('net_sales')
    )
    .first()['net_sales']
)
# print(f'After many-to-many join: {multiplied_revenue}')
# =>
"""
After many-to-many join: 437.400000
"""


# =============================================================================
# 7.3 Pre-aggregate before joining
# --------------------------------
# Collapse promotions to one row PER product because that is the level of
# information required by the sales-line enrichment.
#
# RIGHT GRAIN: one row PER product
#
# The join is now many-to-one from sales lines.
# =============================================================================
"""
`promotions_df` (before pre-aggregation)
+------------+----------+--------------+                                        
|promotion_id|product_id|promotion_type|
+------------+----------+--------------+
|PROMO-01    |P001      |LOYALTY       |
|PROMO-02    |P001      |WEEKEND       |
|PROMO-03    |P002      |LOYALTY       |
+------------+----------+--------------+
"""
promotion_summary_df = (
    promotions_df
    .groupBy('product_id')
    .agg(
        F.countDistinct('promotion_id').alias('promotion_count'),
        F.collect_set('promotion_type').alias('promotion_types'),
    )
)
# promotion_summary_df.show(truncate=False)
# =>
"""
LEFT: `sales_lines_df`
+--------+-----------+----------+--------+----------+------------+-----------+---------------+---------+
|order_id|line_number|product_id|quantity|unit_price|discount_pct|gross_sales|discount_amount|net_sales|
+--------+-----------+----------+--------+----------+------------+-----------+---------------+---------+
|1001    |1          |P001      |2       |12.00     |0.0000      |24.00      |0.000000       |24.000000|
|1001    |2          |P002      |1       |30.00     |0.1000      |30.00      |3.000000       |27.000000|
|1002    |1          |P001      |3       |12.00     |0.0500      |36.00      |1.800000       |34.200000|
|1002    |2          |P003      |1       |50.00     |0.0000      |50.00      |0.000000       |50.000000|
|1003    |1          |P002      |2       |30.00     |0.0000      |60.00      |0.000000       |60.000000|
|1004    |1          |P003      |2       |50.00     |0.1000      |100.00     |10.000000      |90.000000|
|1004    |2          |P004      |4       |8.00      |0.0000      |32.00      |0.000000       |32.000000|
|1005    |1          |P001      |1       |12.00     |0.0000      |12.00      |0.000000       |12.000000|
|1005    |2          |P004      |5       |8.00      |0.0500      |40.00      |2.000000       |38.000000|
+--------+-----------+----------+--------+----------+------------+-----------+---------------+---------+

RIGHT (Pre-Aggregate): `promotion_summary_df`
+----------+---------------+------------------+                                 
|product_id|promotion_count|promotion_types   |
+----------+---------------+------------------+
|P002      |1              |[LOYALTY]         |
|P001      |2              |[LOYALTY, WEEKEND]|
+----------+---------------+------------------+
"""
safe_promotion_join_df = sales_lines_df.join(
    promotion_summary_df,
    on='product_id',
    how='left',
)
# safe_promotion_join_df.show(truncate=False)
# =>
"""
OUTPUT: `safe_promotion_join_df`
+----------+--------+-----------+--------+----------+------------+-----------+---------------+---------+---------------+------------------+
|product_id|order_id|line_number|quantity|unit_price|discount_pct|gross_sales|discount_amount|net_sales|promotion_count|promotion_types   |
+----------+--------+-----------+--------+----------+------------+-----------+---------------+---------+---------------+------------------+
|P001      |1001    |1          |2       |12.00     |0.0000      |24.00      |0.000000       |24.000000|2              |[LOYALTY, WEEKEND]|
|P002      |1001    |2          |1       |30.00     |0.1000      |30.00      |3.000000       |27.000000|1              |[LOYALTY]         |
|P001      |1002    |1          |3       |12.00     |0.0500      |36.00      |1.800000       |34.200000|2              |[LOYALTY, WEEKEND]|
|P003      |1002    |2          |1       |50.00     |0.0000      |50.00      |0.000000       |50.000000|NULL           |NULL              |
|P002      |1003    |1          |2       |30.00     |0.0000      |60.00      |0.000000       |60.000000|1              |[LOYALTY]         |
|P003      |1004    |1          |2       |50.00     |0.1000      |100.00     |10.000000      |90.000000|NULL           |NULL              |
|P004      |1004    |2          |4       |8.00      |0.0000      |32.00      |0.000000       |32.000000|NULL           |NULL              |
|P004      |1005    |2          |5       |8.00      |0.0500      |40.00      |2.000000       |38.000000|NULL           |NULL              |
|P001      |1005    |1          |1       |12.00     |0.0000      |12.00      |0.000000       |12.000000|2              |[LOYALTY, WEEKEND]|
+----------+--------+-----------+--------+----------+------------+-----------+---------------+---------+---------------+------------------+
"""
# print(f'Before join: {sales_lines_df.count()}')
# print(f'After join: {safe_promotion_join_df.count()}')
# =>
"""
Before join: 9                                                                  
After join: 9  
"""
