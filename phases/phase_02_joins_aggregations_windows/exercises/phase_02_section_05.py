from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructType,
    StructField,
)

from practice_data import (
    spark,
    orders_df,
    stores_df,
    inventory_df,
)

"""
Phase 2, Section 5: Multi-Column Joins & Duplicate Column Names
---------------------------------------------------------------
1. composite join keys
2. incomplete-key mistakes
3. aliases
4. explicit projection after joins

QUESTIONS
=========
1. What is the grain of `targets_df`?
2. Why is `product_id` alone an incomplete join key for that dataset?
3. Why are aliases useful when two DataFrames contain identically named columns?
4. When is `on=['store_id', 'product_id']` preferable to a boolean join expression?
"""
# GRAIN: one row PER store PER product
targets_schema = StructType([
    StructField('store_id', StringType(), False),
    StructField('product_id', StringType(), False),
    StructField('target_on_hand', IntegerType(), False),
])
targets_df = spark.createDataFrame(
    [
        ('S01', 'P001', 20),
        ('S02', 'P001', 9),   # Same product, different store.
        ('S01', 'P002', 8),
        ('S01', 'P004', 12),
        ('S02', 'P003', 6),
    ],
    schema=targets_schema,
)
# targets_df.show(truncate=False)
# =>
"""
`targets_df`
+--------+----------+--------------+                                            
|store_id|product_id|target_on_hand|
+--------+----------+--------------+
|S01     |P001      |20            |
|S02     |P001      |9             |
|S01     |P002      |8             |
|S01     |P004      |12            |
|S02     |P003      |6             |
+--------+----------+--------------+
"""


# =============================================================================
# 5.1 Multi-column join
# ---------------------
# Complete join key: (store_id, product_id)
# =============================================================================
"""
LEFT: `inventory_df`
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

RIGHT: `targets_df`
+--------+----------+--------------+                                            
|store_id|product_id|target_on_hand|
+--------+----------+--------------+
|S01     |P001      |20            |
|S02     |P001      |9             |
|S01     |P002      |8             |
|S01     |P004      |12            |
|S02     |P003      |6             |
+--------+----------+--------------+
"""
inventory_targets_df = inventory_df.join(
    targets_df,
    on=['store_id', 'product_id'],
    how='left',
).orderBy(
    'store_id',
    'product_id',
)
# inventory_targets_df.show(truncate=False)
# =>
"""
OUTPUT: `inventory_targets_df`
+--------+----------+-------------+-------------------+------------+----------------+-------------+--------------+
|store_id|product_id|snapshot_date|snapshot_ts        |ingestion_id|on_hand_quantity|reorder_point|target_on_hand|
+--------+----------+-------------+-------------------+------------+----------------+-------------+--------------+
|S01     |P001      |2026-01-05   |2026-01-05 08:00:00|1           |15              |10           |20            |
|S01     |P001      |2026-01-05   |2026-01-05 18:00:00|2           |12              |10           |20            |
|S01     |P002      |2026-01-05   |2026-01-05 18:00:00|3           |4               |5            |8             |
|S01     |P004      |2026-02-02   |2026-02-02 18:00:00|6           |7               |8            |12            |
|S02     |P003      |2026-01-05   |2026-01-05 18:00:00|4           |0               |2            |6             |
|S02     |P003      |2026-01-12   |2026-01-12 18:00:00|5           |5               |2            |6             |
+--------+----------+-------------+-------------------+------------+----------------+-------------+--------------+
"""

# Now, deliberately join on ONLY `product_id`.
incomplete_key_join_df = inventory_df.join(
    targets_df,
    on='product_id',
    how='left',
).orderBy(
    'product_id',
)
# incomplete_key_join_df.show(truncate=False)
# =>
"""
OUTPUT: `incomplete_key_join_df`
+----------+-------------+-------------------+------------+--------+----------------+-------------+--------+--------------+
|product_id|snapshot_date|snapshot_ts        |ingestion_id|store_id|on_hand_quantity|reorder_point|store_id|target_on_hand|
+----------+-------------+-------------------+------------+--------+----------------+-------------+--------+--------------+
|P001      |2026-01-05   |2026-01-05 08:00:00|1           |S01     |15              |10           |S02     |9             |
|P001      |2026-01-05   |2026-01-05 08:00:00|1           |S01     |15              |10           |S01     |20            |
|P001      |2026-01-05   |2026-01-05 18:00:00|2           |S01     |12              |10           |S02     |9             |
|P001      |2026-01-05   |2026-01-05 18:00:00|2           |S01     |12              |10           |S01     |20            |
|P002      |2026-01-05   |2026-01-05 18:00:00|3           |S01     |4               |5            |S01     |8             |
|P003      |2026-01-05   |2026-01-05 18:00:00|4           |S02     |0               |2            |S02     |6             |
|P003      |2026-01-12   |2026-01-12 18:00:00|5           |S02     |5               |2            |S02     |6             |
|P004      |2026-02-02   |2026-02-02 18:00:00|6           |S01     |7               |8            |S01     |12            |
+----------+-------------+-------------------+------------+--------+----------------+-------------+--------+--------------+
"""


# =============================================================================
# 5.2 Duplicate column names
# --------------------------
# A boolean join expression retains both sides' columns.
#
# Use aliases and explicit projection so every selected field has clear
# ownership and ambiguous names do NOT leak downstream.
# =============================================================================
orders = orders_df.alias('o')
stores = stores_df.alias('s')
"""
LEFT: `o`
+--------+--------+-----------+----------+-------------------+------------+     
|order_id|store_id|customer_id|order_date|order_ts           |order_status|
+--------+--------+-----------+----------+-------------------+------------+
|1001    |S01     |C001       |2026-01-03|2026-01-03 09:15:00|COMPLETED   |
|1002    |S01     |C002       |2026-01-05|2026-01-05 14:30:00|COMPLETED   |
|1003    |S02     |C001       |2026-01-05|2026-01-05 17:45:00|CANCELLED   |
|1004    |S02     |C003       |2026-01-12|2026-01-12 11:20:00|COMPLETED   |
|1005    |S01     |C004       |2026-02-02|2026-02-02 10:05:00|COMPLETED   |
+--------+--------+-----------+----------+-------------------+------------+

RIGHT: `s`
+--------+------------------+--------+                                          
|store_id|store_name        |province|
+--------+------------------+--------+
|S01     |Toronto Central   |ON      |
|S02     |Mississauga West  |ON      |
|S03     |Vancouver Downtown|BC      |
+--------+------------------+--------+
"""
orders_with_stores_df = (
    orders
    .join(
        stores,
        F.col('o.store_id') == F.col('s.store_id'),
        how='left',
    )
    .select(
        F.col('o.order_id'),
        F.col('o.store_id'),
        F.col('o.order_status'),
        F.col('s.store_name'),
        F.col('s.province'),
    )
).orderBy(
    'order_id',
    'store_id',
)
# orders_with_stores_df.show(truncate=False)
# =>
"""
OUTPUT: `orders_with_stores_df`
+--------+--------+------------+----------------+--------+                      
|order_id|store_id|order_status|store_name      |province|
+--------+--------+------------+----------------+--------+
|1001    |S01     |COMPLETED   |Toronto Central |ON      |
|1002    |S01     |COMPLETED   |Toronto Central |ON      |
|1003    |S02     |CANCELLED   |Mississauga West|ON      |
|1004    |S02     |COMPLETED   |Mississauga West|ON      |
|1005    |S01     |COMPLETED   |Toronto Central |ON      |
+--------+--------+------------+----------------+--------+
"""
