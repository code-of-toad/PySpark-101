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
Phase 1, Section 9: Dates & Timestamps
--------------------------------------
1. `try_to_date()`
2. `try_to_timestamp()`
3. `year()`
4. `month()`
5. `dayofmonth()`
6. `date_add()`
7. `datediff()`
8. `date_format()`

NOTE: `date_format()` returns a string.
"""
spark = (
    SparkSession.builder
    .appName('phase_01_section_09')
    .master('local[*]')
    .getOrCreate()
)
schema = StructType([
    StructField('order_id',       StringType(), False),
    StructField('order_date_RAW', StringType(), True),
    StructField('created_at_RAW', StringType(), True),
])
data = [
    ('1001', '2026-08-18', '2026-08-18 09:15:30'),
    ('1002', '2026-08-19', '2026-08-19 14:45:10'),
    ('1003', 'bad-date',   '2026-08-19 16:30:00'),
    ('1004', '2026-08-20', 'not-a-timestamp'),
]
df = spark.createDataFrame(data, schema)
# df.show(truncate=False)
# =>
"""
+--------+--------------+-------------------+                                   
|order_id|order_date_RAW|created_at_RAW     |
+--------+--------------+-------------------+
|1001    |2026-08-18    |2026-08-18 09:15:30|
|1002    |2026-08-19    |2026-08-19 14:45:10|
|1003    |bad-date      |2026-08-19 16:30:00|
|1004    |2026-08-20    |not-a-timestamp    |
+--------+--------------+-------------------+
"""


# =============================================================================
# 9.1 Parse safely
# =============================================================================
typed_df = (
    df
    .withColumn(
        'order_date',
        F.try_to_date(F.col('order_date_RAW'), 'yyyy-MM-dd')
    )
    .withColumn(
        'created_at',
        F.try_to_timestamp(F.col('created_at_RAW'), F.lit('yyyy-MM-dd HH:mm:ss'))
    )
)
# typed_df.show(truncate=False)
# =>
"""
+--------+--------------+-------------------+----------+-------------------+    
|order_id|order_date_RAW|created_at_RAW     |order_date|created_at         |
+--------+--------------+-------------------+----------+-------------------+
|1001    |2026-08-18    |2026-08-18 09:15:30|2026-08-18|2026-08-18 09:15:30|
|1002    |2026-08-19    |2026-08-19 14:45:10|2026-08-19|2026-08-19 14:45:10|
|1003    |bad-date      |2026-08-19 16:30:00|NULL      |2026-08-19 16:30:00|
|1004    |2026-08-20    |not-a-timestamp    |2026-08-20|NULL               |
+--------+--------------+-------------------+----------+-------------------+
"""


# =============================================================================
# 9.2 Extract date components
# =============================================================================
calendar_df = (
    typed_df
    .withColumn(
        'order_year',
        F.year(F.col('order_date')),
    )
    .withColumn(
        'order_month',
        F.month(F.col('order_date')),
    )
    .withColumn(
        'order_day',
        F.dayofmonth(F.col('order_date')),
    )
)
# calendar_df.show(truncate=False)
# =>
"""
+--------+--------------+-------------------+----------+-------------------+----------+-----------+---------+
|order_id|order_date_RAW|created_at_RAW     |order_date|created_at         |order_year|order_month|order_day|
+--------+--------------+-------------------+----------+-------------------+----------+-----------+---------+
|1001    |2026-08-18    |2026-08-18 09:15:30|2026-08-18|2026-08-18 09:15:30|2026      |8          |18       |
|1002    |2026-08-19    |2026-08-19 14:45:10|2026-08-19|2026-08-19 14:45:10|2026      |8          |19       |
|1003    |bad-date      |2026-08-19 16:30:00|NULL      |2026-08-19 16:30:00|NULL      |NULL       |NULL     |
|1004    |2026-08-20    |not-a-timestamp    |2026-08-20|NULL               |2026      |8          |20       |
+--------+--------------+-------------------+----------+-------------------+----------+-----------+---------+
"""


# =============================================================================
# 9.3 Date arithmetic
# =============================================================================
shipping_df = typed_df.withColumn(
    'expected_ship_date',
    F.date_add(F.col('order_date'), 2)
)
# shipping_df.show(truncate=False)
# =>
"""
+--------+--------------+-------------------+----------+-------------------+------------------+
|order_id|order_date_RAW|created_at_RAW     |order_date|created_at         |expected_ship_date|
+--------+--------------+-------------------+----------+-------------------+------------------+
|1001    |2026-08-18    |2026-08-18 09:15:30|2026-08-18|2026-08-18 09:15:30|2026-08-20        |
|1002    |2026-08-19    |2026-08-19 14:45:10|2026-08-19|2026-08-19 14:45:10|2026-08-21        |
|1003    |bad-date      |2026-08-19 16:30:00|NULL      |2026-08-19 16:30:00|NULL              |
|1004    |2026-08-20    |not-a-timestamp    |2026-08-20|NULL               |2026-08-22        |
+--------+--------------+-------------------+----------+-------------------+------------------+
"""


# =============================================================================
# Date difference
# =============================================================================
delivery_df = spark.createDataFrame(
    [
        ('1001', '2026-08-18', '2026-08-21'),
        ('1002', '2026-08-19', '2026-08-20'),
    ],
    [
        'order_id',
        'order_date_RAW',
        'delivery_date_RAW',
    ],
)
# delivery_df.show(truncate=False)
# =>
"""
+--------+--------------+-----------------+                                     
|order_id|order_date_RAW|delivery_date_RAW|
+--------+--------------+-----------------+
|1001    |2026-08-18    |2026-08-21       |
|1002    |2026-08-19    |2026-08-20       |
+--------+--------------+-----------------+
"""
delivery_df = (
    delivery_df
    .withColumn(
        'order_date',
        F.try_to_date(F.col('order_date_RAW'), 'yyyy-MM-dd'),
    )
    .withColumn(
        'delivery_date',
        F.try_to_date(F.col('delivery_date_RAW'), 'yyyy-MM-dd'),
    )
    .withColumn(
        'delivery_days',
        F.datediff(F.col('delivery_date'), F.col('order_date')),
    )
)
# delivery_df.show(truncate=False)
# =>
"""
+--------+--------------+-----------------+----------+-------------+-------------+
|order_id|order_date_RAW|delivery_date_RAW|order_date|delivery_date|delivery_days|
+--------+--------------+-----------------+----------+-------------+-------------+
|1001    |2026-08-18    |2026-08-21       |2026-08-18|2026-08-21   |3            |
|1002    |2026-08-19    |2026-08-20       |2026-08-19|2026-08-20   |1            |
+--------+--------------+-----------------+----------+-------------+-------------+
"""


# =============================================================================
# `date_format()`
# =============================================================================
formatted_df = typed_df.withColumn(
    'order_month_label',
    F.date_format(F.col('order_date'), 'yyyy-MM'),
)
# formatted_df.show(truncate=False)
# =>
"""
+--------+--------------+-------------------+----------+-------------------+-----------------+
|order_id|order_date_RAW|created_at_RAW     |order_date|created_at         |order_month_label|
+--------+--------------+-------------------+----------+-------------------+-----------------+
|1001    |2026-08-18    |2026-08-18 09:15:30|2026-08-18|2026-08-18 09:15:30|2026-08          |
|1002    |2026-08-19    |2026-08-19 14:45:10|2026-08-19|2026-08-19 14:45:10|2026-08          |
|1003    |bad-date      |2026-08-19 16:30:00|NULL      |2026-08-19 16:30:00|NULL             |
|1004    |2026-08-20    |not-a-timestamp    |2026-08-20|NULL               |2026-08          |
+--------+--------------+-------------------+----------+-------------------+-----------------+
"""


spark.stop()
