from decimal import Decimal

from pyspark.sql import functions as F

from practice_data import (
    spark,
    order_items_schema,
    order_items_df,
    products_df,
)

"""
Phase 2, Section 6: Join-Key Validation & Referential Integrity
---------------------------------------------------------------
1. duplicate-key detection
2. composite-key validation
3. referential-integrity checks
4. validating assumptions before joins

QUESTIONS
---------
Q: Why should a suppsed dimension key validated before a many-to-one join?
Q: What does an empty duplicate-key result mean?
Q: What does an empty anti-join result mean?
Q: Why is key validation part of metric correctness rather than merely data
   cleaning?
"""


# =============================================================================
# 6.1 Validate a supposed unique key
# ----------------------------------
# A non-empty result means `product_id` is NOT unique.
# =============================================================================
"""
INPUT: `products_df`
+----------+--------------+---------+---------+                                 
|product_id|product_name  |category |unit_cost|
+----------+--------------+---------+---------+
|P001      |Coffee Beans  |GROCERY  |7.00     |
|P002      |Coffee Grinder|EQUIPMENT|18.00    |
|P003      |Kettle        |EQUIPMENT|32.00    |
|P004      |Paper Filters |GROCERY  |3.00     |
+----------+--------------+---------+---------+
"""
duplicate_product_keys_df = (
    products_df
    .groupBy('product_id')
    .agg(
        F.count('*').alias('row_count')
    )
    .filter(
        F.col('row_count') > 1
    )
)
# duplicate_product_keys_df.show(truncate=False)
# =>
"""
OUTPUT: `duplicate_product_keys_df`
+----------+---------+                                                          
|product_id|row_count|
+----------+---------+
+----------+---------+
"""


# =============================================================================
# 6.2 Validate a composite key
# ----------------------------
# `order_items_df` is supposed to contain one row PER (order_id, line_number).
# =============================================================================
"""
INPUT: `order_items_df`
+--------+-----------+----------+--------+----------+------------+              
|order_id|line_number|product_id|quantity|unit_price|discount_pct|
+--------+-----------+----------+--------+----------+------------+
|1001    |1          |P001      |2       |12.00     |0.0000      |
|1001    |2          |P002      |1       |30.00     |0.1000      |
|1002    |1          |P001      |3       |12.00     |0.0500      |
|1002    |2          |P003      |1       |50.00     |0.0000      |
|1003    |1          |P002      |2       |30.00     |0.0000      |
|1004    |1          |P003      |2       |50.00     |0.1000      |
|1004    |2          |P004      |4       |8.00      |0.0000      |
|1005    |1          |P001      |1       |12.00     |0.0000      |
|1005    |2          |P004      |5       |8.00      |0.0500      |
+--------+-----------+----------+--------+----------+------------+
"""
duplicate_order_line_keys_df = (
    order_items_df
    .groupBy(
        'order_id',
        'line_number',
    )
    .agg(
        F.count('*').alias('row_count')
    )
    .filter(
        F.col('row_count') > 1
    )
)
# duplicate_order_line_keys_df.show(truncate=False)
# =>
"""
OUTPUT: `duplicate_order_line_keys_df`
+--------+-----------+---------+                                                
|order_id|line_number|row_count|
+--------+-----------+---------+
+--------+-----------+---------+
"""


# =============================================================================
# 6.3 Referential integrity with a left anti join
# -----------------------------------------------
# Find order-item rows whose `product_id` does NOT exist in products.
# =============================================================================
"""
LEFT: `order_items_df`
+--------+-----------+----------+--------+----------+------------+              
|order_id|line_number|product_id|quantity|unit_price|discount_pct|
+--------+-----------+----------+--------+----------+------------+
|1001    |1          |P001      |2       |12.00     |0.0000      |
|1001    |2          |P002      |1       |30.00     |0.1000      |
|1002    |1          |P001      |3       |12.00     |0.0500      |
|1002    |2          |P003      |1       |50.00     |0.0000      |
|1003    |1          |P002      |2       |30.00     |0.0000      |
|1004    |1          |P003      |2       |50.00     |0.1000      |
|1004    |2          |P004      |4       |8.00      |0.0000      |
|1005    |1          |P001      |1       |12.00     |0.0000      |
|1005    |2          |P004      |5       |8.00      |0.0500      |
+--------+-----------+----------+--------+----------+------------+

RIGHT: `products_df`
+----------+--------------+---------+---------+                                 
|product_id|product_name  |category |unit_cost|
+----------+--------------+---------+---------+
|P001      |Coffee Beans  |GROCERY  |7.00     |
|P002      |Coffee Grinder|EQUIPMENT|18.00    |
|P003      |Kettle        |EQUIPMENT|32.00    |
|P004      |Paper Filters |GROCERY  |3.00     |
+----------+--------------+---------+---------+
"""
orphan_product_items_df = order_items_df.join(
    products_df.select('product_id'),
    on='product_id',
    how='left_anti',
)
# orphan_product_items_df.show(truncate=False)
# =>
"""
OUTPUT: `orphan_product_items_df`
+----------+--------+-----------+--------+----------+------------+              
|product_id|order_id|line_number|quantity|unit_price|discount_pct|
+----------+--------+-----------+--------+----------+------------+
+----------+--------+-----------+--------+----------+------------+
"""


# =============================================================================
# 6.4 Create an orphan deliberately
# =============================================================================
orphan_item_df = spark.createDataFrame(
    [
        (9999, 1, 'P404', 1, Decimal('10.00'), Decimal('0.0000')),
    ],
    schema=order_items_schema,
)
order_items_with_orphan_df = order_items_df.unionByName(orphan_item_df)
# order_items_with_orphan_df.show(truncate=False)
# =>
"""
LEFT: `order_items_with_orphan_df`
+--------+-----------+----------+--------+----------+------------+              
|order_id|line_number|product_id|quantity|unit_price|discount_pct|
+--------+-----------+----------+--------+----------+------------+
|1001    |1          |P001      |2       |12.00     |0.0000      |
|1001    |2          |P002      |1       |30.00     |0.1000      |
|1002    |1          |P001      |3       |12.00     |0.0500      |
|1002    |2          |P003      |1       |50.00     |0.0000      |
|1003    |1          |P002      |2       |30.00     |0.0000      |
|1004    |1          |P003      |2       |50.00     |0.1000      |
|1004    |2          |P004      |4       |8.00      |0.0000      |
|1005    |1          |P001      |1       |12.00     |0.0000      |
|1005    |2          |P004      |5       |8.00      |0.0500      |
|9999    |1          |P404      |1       |10.00     |0.0000      |
+--------+-----------+----------+--------+----------+------------+

RIGHT: `products_df`
+----------+--------------+---------+---------+                                 
|product_id|product_name  |category |unit_cost|
+----------+--------------+---------+---------+
|P001      |Coffee Beans  |GROCERY  |7.00     |
|P002      |Coffee Grinder|EQUIPMENT|18.00    |
|P003      |Kettle        |EQUIPMENT|32.00    |
|P004      |Paper Filters |GROCERY  |3.00     |
+----------+--------------+---------+---------+
"""
orphan_product_items_df = order_items_with_orphan_df.join(
    products_df.select('product_id'),
    on='product_id',
    how='left_anti',
)
# orphan_product_items_df.show(truncate=False)
# =>
"""
OUTPUT: `orphan_product_items_df`
+----------+--------+-----------+--------+----------+------------+              
|product_id|order_id|line_number|quantity|unit_price|discount_pct|
+----------+--------+-----------+--------+----------+------------+
|P404      |9999    |1          |1       |10.00     |0.0000      |
+----------+--------+-----------+--------+----------+------------+
"""
