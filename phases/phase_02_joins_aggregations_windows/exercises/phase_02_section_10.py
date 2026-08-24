from pyspark.sql import Window
from pyspark.sql import functions as F

from phase_02_section_08 import (
    sales_enriched_df,
)

"""
Phase 2, Section 10: `lag`, `lead`, Running Totals, Rolling Windows
-------------------------------------------------------------------
1. `lag()`
2. `lead()`
3. running aggregates
4. `rowsBetween()`
5. `rangeBetween()`
6. row-based vs. time/value-based windows

QUESTIONS
=========
Q: Why does `lag()` not automatically mean yesterday?
Q: What rows belong to the running window for the current row?
Q: What is the difference between `rowsBetween(-2, 0)` and `rangeBetween(-6, 0)`
   in these examples?
Q: Why is the ordering column part of the business meaning of a window?
"""


# =============================================================================
# 10.1 Daily store sales
# ----------------------
# INPUT GRAIN: one row PER order line
# OUTPUT GRAIN: one row PER store PER order date
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
daily_store_sales_df = (
    sales_enriched_df
    .filter(
        F.col('order_status') == 'COMPLETED'
    )
    .groupBy(
        'store_id',
        'order_date',
    )
    .agg(
        F.sum('net_sales').alias('daily_net_sales')
    )
    .orderBy(
        'store_id',
        'order_date',
    )
)
# daily_store_sales_df.show(truncate=False)
# =>
"""
OUTPUT: `daily_store_sales_df`
+--------+----------+---------------+                                           
|store_id|order_date|daily_net_sales|
+--------+----------+---------------+
|S01     |2026-01-03|51.000000      |
|S01     |2026-01-05|84.200000      |
|S01     |2026-02-02|50.000000      |
|S02     |2026-01-12|122.000000     |
+--------+----------+---------------+
"""


# =============================================================================
# 10.2 `lag()` and `lead()`
# -------------------------
# `lag()` means previous ordered ROW.
# It does NOT automatically mean previous calendar day.
# =============================================================================
# =>
"""
INPUT: `daily_store_sales_df`
+--------+----------+---------------+                                           
|store_id|order_date|daily_net_sales|
+--------+----------+---------------+
|S01     |2026-02-02|50.000000      |
|S01     |2026-01-05|84.200000      |
|S01     |2026-01-03|51.000000      |
|S02     |2026-01-12|122.000000     |
+--------+----------+---------------+
"""
store_day_window = (
    Window
    .partitionBy('store_id')
    .orderBy('order_date')
)

neighbor_sales_df = (
    daily_store_sales_df
    .withColumn(
        'previous_observed_sales',
        F.lag('daily_net_sales').over(store_day_window),
    )
    .withColumn(
        'next_observed_sales',
        F.lead('daily_net_sales').over(store_day_window),
    )
)
# neighbor_sales_df.show(truncate=False)
# =>
"""
OUTPUT: `neighbor_sales_df`
+--------+----------+---------------+-----------------------+-------------------+
|store_id|order_date|daily_net_sales|previous_observed_sales|next_observed_sales|
+--------+----------+---------------+-----------------------+-------------------+
|S01     |2026-01-03|51.000000      |NULL                   |84.200000          |
|S01     |2026-01-05|84.200000      |51.000000              |50.000000          |
|S01     |2026-02-02|50.000000      |84.200000              |NULL               |
|S02     |2026-01-12|122.000000     |NULL                   |NULL               |
+--------+----------+---------------+-----------------------+-------------------+
"""


# =============================================================================
# 10.3 Running total
# =============================================================================
"""
INPUT: `daily_store_sales_df`
+--------+----------+---------------+                                           
|store_id|order_date|daily_net_sales|
+--------+----------+---------------+
|S01     |2026-02-02|50.000000      |
|S01     |2026-01-05|84.200000      |
|S01     |2026-01-03|51.000000      |
|S02     |2026-01-12|122.000000     |
+--------+----------+---------------+
"""
running_window = (
    Window
    .partitionBy('store_id')
    .orderBy('order_date')
    .rowsBetween(
        Window.unboundedPreceding,
        Window.currentRow,
    )
)
running_sales_df = (
    daily_store_sales_df
    .withColumn(
        'running_net_sales',
        F.sum('daily_net_sales').over(running_window),
    )
)
# running_sales_df.show(truncate=False)
# =>
"""
OUTPUT: `running_sales_df`
+--------+----------+---------------+-----------------+
|store_id|order_date|daily_net_sales|running_net_sales|
+--------+----------+---------------+-----------------+
|S01     |2026-01-03|51.000000      |51.000000        |
|S01     |2026-01-05|84.200000      |135.200000       |
|S01     |2026-02-02|50.000000      |185.200000       |
|S02     |2026-01-12|122.000000     |122.000000       |
+--------+----------+---------------+-----------------+
"""


# =============================================================================
# 10.4 Three-row rolling calculation
# ----------------------------------
# The end result means: current row + previous two observed rows.
# It does NOT necessarily mean three calendar days.
# =============================================================================
"""
INPUT: `daily_store_sales_df`
+--------+----------+---------------+                                           
|store_id|order_date|daily_net_sales|
+--------+----------+---------------+
|S01     |2026-02-02|50.000000      |
|S01     |2026-01-05|84.200000      |
|S01     |2026-01-03|51.000000      |
|S02     |2026-01-12|122.000000     |
+--------+----------+---------------+
"""
rolling_3_row_window = (
    Window
    .partitionBy('store_id')
    .orderBy('order_date')
    .rowsBetween(-2, 0)
)
rolling_3_row_df = (
    daily_store_sales_df
    .withColumn(
        'rolling_3_observed_rows',
        F.sum('daily_net_sales').over(rolling_3_row_window),
    )
)
# rolling_3_row_df.show(truncate=False)
# =>
"""
OUTPUT: `rolling_3_row_df`
+--------+----------+---------------+-----------------------+
|store_id|order_date|daily_net_sales|rolling_3_observed_rows|
+--------+----------+---------------+-----------------------+
|S01     |2026-01-03|51.000000      |51.000000              |
|S01     |2026-01-05|84.200000      |135.200000             |
|S01     |2026-02-02|50.000000      |185.200000             |
|S02     |2026-01-12|122.000000     |122.000000             |
+--------+----------+---------------+-----------------------+
"""


# =============================================================================
# 10.5 Seven-calendar-day rolling calculation
# -------------------------------------------
# The end result means: current row + previous two observed rows.
# It does NOT necessarily mean three calendar days.
# =============================================================================
"""
INPUT: `daily_store_sales_df`
+--------+----------+---------------+                                           
|store_id|order_date|daily_net_sales|
+--------+----------+---------------+
|S01     |2026-02-02|50.000000      |
|S01     |2026-01-05|84.200000      |
|S01     |2026-01-03|51.000000      |
|S02     |2026-01-12|122.000000     |
+--------+----------+---------------+
"""
daily_with_day_number_df = (
    daily_store_sales_df
    .withColumn(
        # Convert the date to a numeric day offset because rangeBetween()
        # works with the ordering expression's values.
        'day_number',
        F.datediff(
            F.col('order_date'),
            F.lit('1970-01-01'),
        ),
    )
)
# daily_with_day_number_df.show(truncate=False)
# =>
"""
INPUT: `daily_with_day_number_df`
+--------+----------+---------------+----------+                                
|store_id|order_date|daily_net_sales|day_number|
+--------+----------+---------------+----------+
|S01     |2026-01-03|51.000000      |20456     |
|S01     |2026-01-05|84.200000      |20458     |
|S01     |2026-02-02|50.000000      |20486     |
|S02     |2026-01-12|122.000000     |20465     |
+--------+----------+---------------+----------+
"""
rolling_7_day_window = (
    Window
    .partitionBy('store_id')
    .orderBy('day_number')
    .rangeBetween(-6, 0)
)
rolling_7_day_df = (
    daily_with_day_number_df
    .withColumn(
        'rolling_7_day_sales',
        F.sum('daily_net_sales').over(rolling_7_day_window),
    )
    .drop('day_number')
)
# rolling_7_day_df.show(truncate=False)
# =>
"""
OUTPUT: `rolling_7_day_df`
+--------+----------+---------------+-------------------+                       
|store_id|order_date|daily_net_sales|rolling_7_day_sales|
+--------+----------+---------------+-------------------+
|S01     |2026-01-03|51.000000      |51.000000          |
|S01     |2026-01-05|84.200000      |135.200000         |
|S01     |2026-02-02|50.000000      |50.000000          |
|S02     |2026-01-12|122.000000     |122.000000         |
+--------+----------+---------------+-------------------+
"""
