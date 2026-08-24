from pyspark.sql import functions as F

from practice_data import (
    spark,
)
from phase_02_section_08 import (
    sales_enriched_df,
)

"""
Phase 2, Section 12: `explode`, Pivot, Unpivot
----------------------------------------------
1. `explode()`
2. grain expansion
3. pivot
4. unpivot
5. long vs. wide analytical shapes

QUESTIONS
=========
Q: How does `explode()` change grain?
Q: Why can exploding arrays before a sum duplicate parent-level measures?
Q: Why is long-form data often easier for generic analytical processing?
Q: Why can unpivot increase row count?
"""


# =============================================================================
# 12.1 `explode()`
# ----------------
# INPUT GRAIN: one row PER product
# OUTPUT GRAIN: one row PER product-tag
# =============================================================================
tagged_products_df = spark.createDataFrame(
    [
        ('P001', ['grocery', 'coffee']),
        ('P002', ['equipment', 'coffee']),
        ('P003', []),
        ('P004', None),
    ],
    ['product_id', 'tags'],
)
# tagged_products_df.show(truncate=False)
# =>
"""
INPUT: `tagged_products_df`
+----------+-------------------+                                                
|product_id|tags               |
+----------+-------------------+
|P001      |[grocery, coffee]  |
|P002      |[equipment, coffee]|
|P003      |[]                 |
|P004      |NULL               |
+----------+-------------------+
"""
product_tags_df = tagged_products_df.select(
    'product_id',
    F.explode('tags').alias('tag'),
)
# product_tags_df.show(truncate=False)
# =>
"""
OUTPUT: `product_tags_df`
+----------+---------+                                                          
|product_id|tag      |
+----------+---------+
|P001      |grocery  |
|P001      |coffee   |
|P002      |equipment|
|P002      |coffee   |
+----------+---------+
"""

# Compare with `explode_outer()`.
#
# `explode_outer()` can preserve a parent whose collection is NULL or empty by
# producing a row with a NULL child value.
"""
INPUT: `tagged_products_df`
+----------+-------------------+                                                
|product_id|tags               |
+----------+-------------------+
|P001      |[grocery, coffee]  |
|P002      |[equipment, coffee]|
|P003      |[]                 |
|P004      |NULL               |
+----------+-------------------+
"""
product_tags_outer_df = tagged_products_df.select(
    'product_id',
    F.explode_outer('tags').alias('tag'),
)
# product_tags_outer_df.show(truncate=False)
# =>
"""
`product_tags_outer_df`
+----------+---------+                                                          
|product_id|tag      |
+----------+---------+
|P001      |grocery  |
|P001      |coffee   |
|P002      |equipment|
|P002      |coffee   |
|P003      |NULL     |
|P004      |NULL     |
+----------+---------+
"""


# =============================================================================
# 12.2 Pivot
# =============================================================================
# Create monthly sales.
"""
INPUT: `sales_enriched_df`
+--------+----------+--------+-----------+--------+----------+------------+-----------+---------------+---------+-----------+----------+-------------------+------------+--------------+---------+---------+----------------+--------+
|store_id|product_id|order_id|line_number|quantity|unit_price|discount_pct|gross_sales|discount_amount|net_sales|customer_id|order_date|order_ts           |order_status|product_name  |category |unit_cost|store_name      |province|
+--------+----------+--------+-----------+--------+----------+------------+-----------+---------------+---------+-----------+----------+-------------------+------------+--------------+---------+---------+----------------+--------+
|S01     |P001      |1001    |1          |2       |12.00     |0.0000      |24.00      |0.000000       |24.000000|C001       |2026-01-03|2026-01-03 09:15:00|COMPLETED   |Coffee Beans  |GROCERY  |7.00     |Toronto Central |ON      |
|S01     |P002      |1001    |2          |1       |30.00     |0.1000      |30.00      |3.000000       |27.000000|C001       |2026-01-03|2026-01-03 09:15:00|COMPLETED   |Coffee Grinder|EQUIPMENT|18.00    |Toronto Central |ON      |
|S01     |P001      |1002    |1          |3       |12.00     |0.0500      |36.00      |1.800000       |34.200000|C002       |2026-01-05|2026-01-05 14:30:00|COMPLETED   |Coffee Beans  |GROCERY  |7.00     |Toronto Central |ON      |
|S01     |P003      |1002    |2          |1       |50.00     |0.0000      |50.00      |0.000000       |50.000000|C002       |2026-01-05|2026-01-05 14:30:00|COMPLETED   |Kettle        |EQUIPMENT|32.00    |Toronto Central |ON      |
|S02     |P002      |1003    |1          |2       |30.00     |0.0000      |60.00      |0.000000       |60.000000|C001       |2026-01-05|2026-01-05 17:45:00|CANCELLED   |Coffee Grinder|EQUIPMENT|18.00    |Mississauga West|ON      |
|S02     |P003      |1004    |1          |2       |50.00     |0.1000      |100.00     |10.000000      |90.000000|C003       |2026-01-12|2026-01-12 11:20:00|COMPLETED   |Kettle        |EQUIPMENT|32.00    |Mississauga West|ON      |
|S02     |P004      |1004    |2          |4       |8.00      |0.0000      |32.00      |0.000000       |32.000000|C003       |2026-01-12|2026-01-12 11:20:00|COMPLETED   |Paper Filters |GROCERY  |3.00     |Mississauga West|ON      |
|S01     |P001      |1005    |1          |1       |12.00     |0.0000      |12.00      |0.000000       |12.000000|C004       |2026-02-02|2026-02-02 10:05:00|COMPLETED   |Coffee Beans  |GROCERY  |7.00     |Toronto Central |ON      |
|S01     |P004      |1005    |2          |5       |8.00      |0.0500      |40.00      |2.000000       |38.000000|C004       |2026-02-02|2026-02-02 10:05:00|COMPLETED   |Paper Filters |GROCERY  |3.00     |Toronto Central |ON      |
+--------+----------+--------+-----------+--------+----------+------------+-----------+---------------+---------+-----------+----------+-------------------+------------+--------------+---------+---------+----------------+--------+
"""
monthly_store_sales_df = (
    sales_enriched_df
    .filter(
        F.col('order_status') == 'COMPLETED'
    )
    .withColumn(
        'month',
        F.date_format(
            F.col('order_date'),
            'yyyy-MM',
        ),
    )
    .groupBy(
        'store_id',
        'month',
    )
    .agg(
        F.sum('net_sales').alias('net_sales')
    )
)
# monthly_store_sales_df.show(truncate=False)
# =>
"""
INPUT: `monthly_store_sales_df`
+--------+-------+----------+                                                   
|store_id|month  |net_sales |
+--------+-------+----------+
|S01     |2026-02|50.000000 |
|S01     |2026-01|135.200000|
|S02     |2026-01|122.000000|
+--------+-------+----------+
"""
# Pivot.
#
# Convert values from month into separate output columns.
#
# Explicit pivot values make the intended output schema predictable.
wide_sales_df = (
    monthly_store_sales_df
    .groupBy('store_id')
    .pivot(
        'month',
        [
            '2026-01',
            '2026-02',
        ],
    )
    .agg(
        F.sum('net_sales')
    )
)
# wide_sales_df.show(truncate=False)
# =>
"""
OUTPUT: `wide_sales_df`
+--------+----------+---------+                                                 
|store_id|2026-01   |2026-02  |
+--------+----------+---------+
|S01     |135.200000|50.000000|
|S02     |122.000000|NULL     |
+--------+----------+---------+
"""


# =============================================================================
# 12.3 Unpivot
# ------------
# Convert wide month columns back into:
#     store_id, month, net_sales
#
# PySpark 4.2.0 provides DataFrame.unpivot().
# =============================================================================
"""
INPUT: `wide_sales_df`
+--------+----------+---------+                                                 
|store_id|2026-01   |2026-02  |
+--------+----------+---------+
|S01     |135.200000|50.000000|
|S02     |122.000000|NULL     |
+--------+----------+---------+
"""
long_sales_df = wide_sales_df.unpivot(
    ids='store_id',
    values=[
        '2026-01',
        '2026-02',
    ],
    variableColumnName='month',
    valueColumnName='net_sales',
)
# long_sales_df.show(truncate=False)
# =>
"""
OUTPUT: `long_sales_df`
+--------+-------+----------+                                                   
|store_id|month  |net_sales |
+--------+-------+----------+
|S01     |2026-01|135.200000|
|S01     |2026-02|50.000000 |
|S02     |2026-01|122.000000|
|S02     |2026-02|NULL      |
+--------+-------+----------+
"""
