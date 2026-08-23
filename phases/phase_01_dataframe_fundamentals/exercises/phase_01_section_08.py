from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

"""
Phase 1, Section 8: Numeric Functions & Arithmetic
--------------------------------------------------
Practice typed arithmetic and decimal-safe calculations.
"""
spark = (
    SparkSession.builder
    .appName('phase_01_section_08')
    .master('local[*]')
    .getOrCreate()
)
schema = StructType([
    StructField('order_id', StringType(), False),
    StructField('quantity', IntegerType(), False),
    StructField('unit_price', DecimalType(10, 2), False),
    StructField('unit_cost', DecimalType(10, 2), False),
    StructField('discount_amount', DecimalType(10, 2), True),
])
data = [
    ('1001', 2, Decimal('12.99'), Decimal('7.50'), Decimal('2.00')),
    ('1002', 3, Decimal('8.50'), Decimal('5.25'), Decimal('0.00')),
    ('1003', 1, Decimal('19.99'), Decimal('12.40'), None),
    ('1004', -2, Decimal('4.99'), Decimal('2.50'), Decimal('1.00')),
]
df = spark.createDataFrame(data, schema)
# df.show(truncate=False)
# =>
"""
+--------+--------+----------+---------+---------------+                        
|order_id|quantity|unit_price|unit_cost|discount_amount|
+--------+--------+----------+---------+---------------+
|1001    |2       |12.99     |7.50     |2.00           |
|1002    |3       |8.50      |5.25     |0.00           |
|1003    |1       |19.99     |12.40    |NULL           |
|1004    |-2      |4.99      |2.50     |1.00           |
+--------+--------+----------+---------+---------------+
"""


# =============================================================================
# 8.1 Derived measures
# =============================================================================
measures_df = (
    df
    .withColumn(
        'gross_sales',
        F.col('quantity') * F.col('unit_price'),
    )
    .withColumn(
        'discount_amount_CLEAN',
        F.coalesce(F.col('discount_amount'), F.lit(0)),
    )
    .withColumn(
        'net_sales',
        F.col('gross_sales') - F.col('discount_amount_CLEAN'),
    )
    .withColumn(
        'gross_margin',
        F.col('net_sales') - (F.col('quantity') * F.col('unit_cost'))
    )
)
# measures_df.show(truncate=False)
# =>
"""
+--------+--------+----------+---------+---------------+-----------+---------------------+---------+------------+
|order_id|quantity|unit_price|unit_cost|discount_amount|gross_sales|discount_amount_CLEAN|net_sales|gross_margin|
+--------+--------+----------+---------+---------------+-----------+---------------------+---------+------------+
|1001    |2       |12.99     |7.50     |2.00           |25.98      |2.00                 |23.98    |8.98        |
|1002    |3       |8.50      |5.25     |0.00           |25.50      |0.00                 |25.50    |9.75        |
|1003    |1       |19.99     |12.40    |NULL           |19.99      |0.00                 |19.99    |7.59        |
|1004    |-2      |4.99      |2.50     |1.00           |-9.98      |1.00                 |-10.98   |-5.98       |
+--------+--------+----------+---------+---------------+-----------+---------------------+---------+------------+
"""


# =============================================================================
# 8.2 `round()` and `bround()`
# =============================================================================
rounded_df = measures_df.withColumn(
    'gross_margin_rounded',
    F.round(F.col('gross_margin'), 2),
)
# rounded_df.show(truncate=False)
# =>
"""
+--------+--------+----------+---------+---------------+-----------+---------------------+---------+------------+--------------------+
|order_id|quantity|unit_price|unit_cost|discount_amount|gross_sales|discount_amount_CLEAN|net_sales|gross_margin|gross_margin_rounded|
+--------+--------+----------+---------+---------------+-----------+---------------------+---------+------------+--------------------+
|1001    |2       |12.99     |7.50     |2.00           |25.98      |2.00                 |23.98    |8.98        |8.98                |
|1002    |3       |8.50      |5.25     |0.00           |25.50      |0.00                 |25.50    |9.75        |9.75                |
|1003    |1       |19.99     |12.40    |NULL           |19.99      |0.00                 |19.99    |7.59        |7.59                |
|1004    |-2      |4.99      |2.50     |1.00           |-9.98      |1.00                 |-10.98   |-5.98       |-5.98               |
+--------+--------+----------+---------+---------------+-----------+---------------------+---------+------------+--------------------+
"""

rounding_df = (
    spark.range(1)
    .select(
        F.round(F.lit(2.5), 0).alias('round_result'),
        F.bround(F.lit(2.5), 0).alias('bround_result'),
    )
)
# rounding_df.show(truncate=False)
# =>
"""
+------------+-------------+
|round_result|bround_result|
+------------+-------------+
|3.0         |2.0          |
+------------+-------------+
"""


# =============================================================================
# 8.3 `abs()`
# =============================================================================
difference_df = measures_df.withColumn(
    'margin_magnitude',
    F.abs(F.col('gross_margin')),
)
# difference_df.show(truncate=False)
# =>
"""
+--------+--------+----------+---------+---------------+-----------+---------------------+---------+------------+----------------+
|order_id|quantity|unit_price|unit_cost|discount_amount|gross_sales|discount_amount_CLEAN|net_sales|gross_margin|margin_magnitude|
+--------+--------+----------+---------+---------------+-----------+---------------------+---------+------------+----------------+
|1001    |2       |12.99     |7.50     |2.00           |25.98      |2.00                 |23.98    |8.98        |8.98            |
|1002    |3       |8.50      |5.25     |0.00           |25.50      |0.00                 |25.50    |9.75        |9.75            |
|1003    |1       |19.99     |12.40    |NULL           |19.99      |0.00                 |19.99    |7.59        |7.59            |
|1004    |-2      |4.99      |2.50     |1.00           |-9.98      |1.00                 |-10.98   |-5.98       |5.98            |
+--------+--------+----------+---------+---------------+-----------+---------------------+---------+------------+----------------+
"""


spark.stop()
