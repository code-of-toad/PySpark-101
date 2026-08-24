from pyspark.sql import Window
from pyspark.sql import functions as F

from practice_data import (
    orders_df,
    products_df,
    stores_df,
)
from phase_02_section_01 import (
    sales_lines_df,
)

"""
Phase 2, Section 8: `groupBy` vs. `Window.partitionBy`
------------------------------------------------------
1. why `groupBy()` collapses rows
2. why windows preserve rows
3. `Window.partitionBy()`

QUESTIONS
=========
Q: What does one row represent after the `groupBy()`?
Q: What does one row represent after the window?
Q: Why can both techniques compute store totals but produce different shapes?
Q: When would you prefer the window version?
"""


"""
`sales_lines_df`
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

`products_df`
+----------+--------------+---------+---------+                                 
|product_id|product_name  |category |unit_cost|
+----------+--------------+---------+---------+
|P001      |Coffee Beans  |GROCERY  |7.00     |
|P002      |Coffee Grinder|EQUIPMENT|18.00    |
|P003      |Kettle        |EQUIPMENT|32.00    |
|P004      |Paper Filters |GROCERY  |3.00     |
+----------+--------------+---------+---------+

`stores_df`
+--------+------------------+--------+                                          
|store_id|store_name        |province|
+--------+------------------+--------+
|S01     |Toronto Central   |ON      |
|S02     |Mississauga West  |ON      |
|S03     |Vancouver Downtown|BC      |
+--------+------------------+--------+
"""
# First, enrich sales lines with order/store information.
#
# Each join is many-to-one from order-line grain when right-side keys are valid,
# so the intended grain remains one row PER order line.
sales_enriched_df = (
    sales_lines_df
    .join(
        orders_df,
        on='order_id',
        how='inner',
    )
    .join(
        products_df,
        on='product_id',
        how='inner',
    )
    .join(
        stores_df,
        on='store_id',
        how='inner',
    )
    .orderBy(
        'order_id',
        'line_number',
    )
)
# sales_enriched_df.show(truncate=False)
# =>
"""
`sales_enriched_df`
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


# =============================================================================
# 8.1 `groupBy()` collapses rows
# ------------------------------
# INPUT GRAIN: one row PER order line
# OUTPUT GRAIN: one row PER store
# =============================================================================
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
store_totals_df = (
    sales_enriched_df
    .groupBy('store_id')
    .agg(F.sum('net_sales').alias('store_net_sales'))
)
# store_totals_df.show(truncate=False)
# =>
"""
OUTPUT: `store_totals_df`
+--------+---------------+                                                      
|store_id|store_net_sales|
+--------+---------------+
|S01     |185.200000     |
|S02     |182.000000     |
+--------+---------------+
"""


# =============================================================================
# 8.2 Window preserves rows
# -------------------------
# INPUT GRAIN: one row PER order line
# OUTPUT GRAIN: one row PER store
#
# `Window.partitionBy()` defines which rows can see one another.
# It does NOT collapse those rows.
# =============================================================================
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
store_window = Window.partitionBy('store_id')

sales_with_store_total_df = sales_enriched_df.withColumn(
    'store_net_sales',
    F.sum('net_sales').over(store_window)
).select(
    'order_id',
    'line_number',
    'store_id',
    'net_sales',
    'store_net_sales',
).orderBy(
    'order_id',
    'line_number',
)
# sales_with_store_total_df.show(truncate=False)
# =>
"""
OUTPUT: `sales_with_store_total_df`
+--------+-----------+--------+---------+---------------+                       
|order_id|line_number|store_id|net_sales|store_net_sales|
+--------+-----------+--------+---------+---------------+
|1001    |1          |S01     |24.000000|185.200000     |
|1001    |2          |S01     |27.000000|185.200000     |
|1002    |1          |S01     |34.200000|185.200000     |
|1002    |2          |S01     |50.000000|185.200000     |
|1003    |1          |S02     |60.000000|182.000000     |
|1004    |1          |S02     |90.000000|182.000000     |
|1004    |2          |S02     |32.000000|182.000000     |
|1005    |1          |S01     |12.000000|185.200000     |
|1005    |2          |S01     |38.000000|185.200000     |
+--------+-----------+--------+---------+---------------+
"""

# Compare counts:
# print(f'Source rows: {sales_enriched_df.count()}')
# print(f'groupBy rows: {store_totals_df.count()}')
# print(f'Window rows: {sales_with_store_total_df.count()}')
# =>
"""
Source rows: 9
groupBy rows: 2
Window rows: 9
"""
