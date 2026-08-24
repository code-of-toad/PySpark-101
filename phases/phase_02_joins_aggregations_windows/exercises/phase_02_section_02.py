from pyspark.sql import functions as F

from practice_data import (
    spark,
)
from phase_02_section_01 import (
    sales_lines_df,
)

"""
Phase 2, Section 2: Aggregate Functions & Conditional Aggregation
-----------------------------------------------------------------
1. `count`
2. `sum`
3. `avg`
4. `min`
5. `max`
6. `countDistinct`
7. conditional aggregation

QUESTIONS
---------
Q: Why does `count('*')` differ from `count('value')` when `value` contains
   NULL?
Q: What is th eoutput grain of `conditional_metrics_df`?
Q: In conditional aggregation, what does `when()` decide and what does `sum()`
   decide?
Q: Why can an average of subgroup averages be wrong for an overall average?
"""


# =============================================================================
# 2.1 Core aggregate functions
# ----------------------------
# INPUT GRAIN: one roe PER order line
# OUTPUT GRAIN: one row PER product
# =============================================================================
"""
INPUT: `sales_lines_df`
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
"""
product_metrics_df = (
    sales_lines_df
    .groupBy('product_id')
    .agg(
        F.count('*').alias('line_count'),

        F.sum('quantity').alias('units_sold'),
        F.sum('net_sales').alias('net_sales'),

        F.avg('unit_price').alias('avg_unit_price'),
        F.min('unit_price').alias('min_unit_price'),
        F.max('unit_price').alias('max_unit_price'),

        F.countDistinct('order_id').alias('order_count')
    )
)
# product_metrics_df.show(truncate=False)
# =>
"""
OUTPUT: `product_metrics_df`
+--------+----------+----------+---------+                                      
|order_id|product_id|units_sold|net_sales|
+--------+----------+----------+---------+
|1001    |P001      |2         |24.000000|
|1001    |P002      |1         |27.000000|
|1002    |P001      |3         |34.200000|
|1002    |P003      |1         |50.000000|
|1003    |P002      |2         |60.000000|
|1004    |P003      |2         |90.000000|
|1004    |P004      |4         |32.000000|
|1005    |P001      |1         |12.000000|
|1005    |P004      |5         |38.000000|
+--------+----------+----------+---------+
"""


# =============================================================================
# 2.2 `count(*)` vs. `count(column)`
# =============================================================================
count_demo_df = spark.createDataFrame(
    [
        ('A', 1),
        ('A', None),
        ('A', 3),
    ],
    ['group_id', 'value'],
)
# count_demo_df.show(truncate=False)
# =>
"""
INPUT: `count_demo_df`
+--------+-----+                                                                
|group_id|value|
+--------+-----+
|A       |1    |
|A       |NULL |
|A       |3    |
+--------+-----+
"""
count_result_df = (
    count_demo_df
    .groupBy('group_id')
    .agg(
        F.count('*').alias('row_count'),
        F.count('value').alias('non_null_value_count'),
    )
)
# count_result_df.show(truncate=False)
# =>
"""
OUTPUT: `count_results_df`
+--------+---------+--------------------+                                       
|group_id|row_count|non_null_value_count|
+--------+---------+--------------------+
|A       |3        |2                   |
+--------+---------+--------------------+
"""


# =============================================================================
# 2.3 Conditional Aggregation
# =============================================================================
"""
INPUT: `sales_lines_df`
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
"""
conditional_metrics_df = (
    sales_lines_df
    .groupBy('product_id')
    .agg(
        F.sum(
            F.when(
                F.col('discount_pct') > 0,
                F.col('net_sales'),
            ).otherwise(F.lit(0))
        ).alias('discounted_net_sales'),

        F.sum(
            F.when(
                F.col('discount_pct') > 0,
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias('discounted_line_count'),

        F.sum(
            F.when(
                F.col('quantity') >= 3,
                F.col('quantity'),
            ).otherwise(F.lit(0))
        ).alias('large_line_units'),
    )
)
# conditional_metrics_df.show(truncate=False)
# =>
"""
OUTPUT: `conditional_metrics_df`
+----------+--------------------+---------------------+----------------+        
|product_id|discounted_net_sales|discounted_line_count|large_line_units|
+----------+--------------------+---------------------+----------------+
|P001      |34.200000           |1                    |3               |
|P002      |27.000000           |1                    |0               |
|P003      |90.000000           |1                    |0               |
|P004      |38.000000           |1                    |9               |
+----------+--------------------+---------------------+----------------+
"""
