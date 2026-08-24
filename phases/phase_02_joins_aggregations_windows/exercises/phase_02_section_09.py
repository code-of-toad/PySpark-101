from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructType,
    StructField,
    TimestampType,
)

from practice_data import (
    spark,
    orders_schema,
    orders_df,
    order_items_schema,
    order_items_df,
    products_schema,
    products_df,
    stores_schema,
    stores_df,
    inventory_schema,
    inventory_df,
)
from phase_02_section_01 import (
    sales_lines_df,
)
from phase_02_section_08 import (
    sales_enriched_df,
)

"""
Phase 2, Section 9: `row_number`, `rank`, `dense_rank`, Latest-Record Selection
-------------------------------------------------------------------------------
1. `Window.orderBy()`
2. `row_number()`
3. `rank()`
4. `dense_rank()`
5. deterministic latest-record selection

QUESTIONS
=========
Q: Why does `row_number()` need a tie-breaker for deterministic selection?
Q: What is the difference between `rank()` and `dense_rank()` under ties?
Q: Why is `dropDuplicates()` insufficient for latest-record selection?
Q: What is the target grain of `latest_inventory_df`?
"""


# =============================================================================
# 9.1 Rank products within category
# =============================================================================
# First, aggregate to product grain.
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
product_revenue_df = (
    sales_enriched_df
    .filter(
        F.col('order_status') == 'COMPLETED'
    )
    .groupBy(
        'category',
        'product_id',
        'product_name',
    )
    .agg(
        F.sum('net_sales').alias('net_sales')
    )
    .orderBy(
        'category',
        'product_id',
        'product_name',
    )
)
# product_revenue_df.show(truncate=False)
# =>
"""
OUTPUT: `product_revenue_df`
+---------+----------+--------------+----------+                                
|category |product_id|product_name  |net_sales |
+---------+----------+--------------+----------+
|EQUIPMENT|P002      |Coffee Grinder|27.000000 |
|EQUIPMENT|P003      |Kettle        |140.000000|
|GROCERY  |P001      |Coffee Beans  |70.200000 |
|GROCERY  |P004      |Paper Filters |70.000000 |
+---------+----------+--------------+----------+
"""
# Then, create ranking windows.

# `rank()` and `dense_rank()` should treat equal revenue values as true ties.
revenue_rank_window = (
    Window
    .partitionBy('category')
    .orderBy(F.col('net_sales').desc())
)
# `row_number()` must choose one deterministic sequence.
# `product_id` is added as a stable tie-breaker.
row_number_window = (
    Window
    .partitionBy('category')
    .orderBy(
        F.col('net_sales').desc(),
        F.col('product_id').asc(),
    )
)
"""
INPUT: `product_revenue_df`
+---------+----------+--------------+----------+                                
|category |product_id|product_name  |net_sales |
+---------+----------+--------------+----------+
|EQUIPMENT|P002      |Coffee Grinder|27.000000 |
|EQUIPMENT|P003      |Kettle        |140.000000|
|GROCERY  |P001      |Coffee Beans  |70.200000 |
|GROCERY  |P004      |Paper Filters |70.000000 |
+---------+----------+--------------+----------+
"""
ranked_products_df = (
    product_revenue_df
    .withColumn(
        'row_number',
        F.row_number().over(row_number_window),
    )
    .withColumn(
        'rank',
        F.rank().over(revenue_rank_window),
    )
    .withColumn(
        'dense_rank',
        F.dense_rank().over(revenue_rank_window),
    )
)
# ranked_products_df.show(truncate=False)
# =>
"""
OUTPUT: `ranked_products_df`
+---------+----------+--------------+----------+----------+----+----------+     
|category |product_id|product_name  |net_sales |row_number|rank|dense_rank|
+---------+----------+--------------+----------+----------+----+----------+
|EQUIPMENT|P003      |Kettle        |140.000000|1         |1   |1         |
|EQUIPMENT|P002      |Coffee Grinder|27.000000 |2         |2   |2         |
|GROCERY  |P001      |Coffee Beans  |70.200000 |1         |1   |1         |
|GROCERY  |P004      |Paper Filters |70.000000 |2         |2   |2         |
+---------+----------+--------------+----------+----------+----+----------+
"""


# =============================================================================
# 9.2 Latest inventory record
# ---------------------------
# TARGET GRAIN: one row PER snapshot date PER store PER product
#
# BUSINESS RULE: newest snapshot timestamp wins
#
# TIE-BREAKER: greatest ingestion_id wins
# =============================================================================
"""
INPUT: `inventory_df`
+-------------+-------------------+------------+--------+----------+----------------+-------------+
|snapshot_date|snapshot_ts        |ingestion_id|store_id|product_id|on_hand_quantity|reorder_point|
+-------------+-------------------+------------+--------+----------+----------------+-------------+
|2026-01-05   |2026-01-05 08:00:00|1           |S01     |P001      |15              |10           |
|2026-01-05   |2026-01-05 18:00:00|2           |S01     |P001      |12              |10           |
|2026-01-05   |2026-01-05 18:00:00|3           |S01     |P002      |4               |5            |
|2026-01-05   |2026-01-05 18:00:00|4           |S02     |P003      |0               |2            |
|2026-01-12   |2026-01-12 18:00:00|5           |S02     |P003      |5               |2            |
|2026-02-02   |2026-02-02 18:00:00|6           |S01     |P004      |7               |8            |
+-------------+-------------------+------------+--------+----------+----------------+-------------+
"""
latest_inventory_window = (
    Window
    .partitionBy(
        'snapshot_date',
        'store_id',
        'product_id',
    )
    .orderBy(
        F.col('snapshot_ts').desc(),
        F.col('ingestion_id').desc(),
    )
)
latest_inventory_df = (
    inventory_df
    .withColumn(
        '_row_number',
        F.row_number().over(latest_inventory_window),
    )
    .filter(
        F.col('_row_number') == 1
    )
    .orderBy(
        'snapshot_date',
        'store_id',
        'product_id',
    )
)
# latest_inventory_df.show(truncate=False)
# =>
"""
OUTPUT: `latest_inventory_df`
+-------------+-------------------+------------+--------+----------+----------------+-------------+-----------+
|snapshot_date|snapshot_ts        |ingestion_id|store_id|product_id|on_hand_quantity|reorder_point|_row_number|
+-------------+-------------------+------------+--------+----------+----------------+-------------+-----------+
|2026-01-05   |2026-01-05 18:00:00|2           |S01     |P001      |12              |10           |1          |
|2026-01-05   |2026-01-05 18:00:00|3           |S01     |P002      |4               |5            |1          |
|2026-01-05   |2026-01-05 18:00:00|4           |S02     |P003      |0               |2            |1          |
|2026-01-12   |2026-01-12 18:00:00|5           |S02     |P003      |5               |2            |1          |
|2026-02-02   |2026-02-02 18:00:00|6           |S01     |P004      |7               |8            |1          |
+-------------+-------------------+------------+--------+----------+----------------+-------------+-----------+
"""


# =============================================================================
# 9.3 Compare with `dropDuplicates()`
# =============================================================================
"""
INPUT: `inventory_df`
+-------------+-------------------+------------+--------+----------+----------------+-------------+
|snapshot_date|snapshot_ts        |ingestion_id|store_id|product_id|on_hand_quantity|reorder_point|
+-------------+-------------------+------------+--------+----------+----------------+-------------+
|2026-01-05   |2026-01-05 08:00:00|1           |S01     |P001      |15              |10           |
|2026-01-05   |2026-01-05 18:00:00|2           |S01     |P001      |12              |10           |
|2026-01-05   |2026-01-05 18:00:00|3           |S01     |P002      |4               |5            |
|2026-01-05   |2026-01-05 18:00:00|4           |S02     |P003      |0               |2            |
|2026-01-12   |2026-01-12 18:00:00|5           |S02     |P003      |5               |2            |
|2026-02-02   |2026-02-02 18:00:00|6           |S01     |P004      |7               |8            |
+-------------+-------------------+------------+--------+----------+----------------+-------------+
"""
# This keeps one row per business key, but does NOT encode which timestamp
# should survive.
arbitrary_inventory_df = inventory_df.dropDuplicates([
    'snapshot_date',
    'store_id',
    'product_id',
])
# arbitrary_inventory_df.show(truncate=False)
# =>
"""
OUTPUT: `arbitrary_inventory_df`
+-------------+-------------------+------------+--------+----------+----------------+-------------+
|snapshot_date|snapshot_ts        |ingestion_id|store_id|product_id|on_hand_quantity|reorder_point|
+-------------+-------------------+------------+--------+----------+----------------+-------------+
|2026-01-05   |2026-01-05 08:00:00|1           |S01     |P001      |15              |10           |
|2026-01-05   |2026-01-05 18:00:00|3           |S01     |P002      |4               |5            |
|2026-01-05   |2026-01-05 18:00:00|4           |S02     |P003      |0               |2            |
|2026-01-12   |2026-01-12 18:00:00|5           |S02     |P003      |5               |2            |
|2026-02-02   |2026-02-02 18:00:00|6           |S01     |P004      |7               |8            |
+-------------+-------------------+------------+--------+----------+----------------+-------------+
"""
