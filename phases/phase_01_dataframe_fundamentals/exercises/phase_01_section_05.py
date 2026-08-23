from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

"""
Phase 1, Section 5: Core DataFrame Operations
---------------------------------------------
1. `select()`
2. `alias()`
3. `filter()` / `where()`
4. `withColumn()`
5. `drop()`
6. `distinct()`
7. `dropDuplicates()`
8. `orderBy()`
9. `limit()`
"""
messy_csv_path = Path(
    'phases\\'
    'phase_01_dataframe_fundamentals\\'
    'exercises\\'
    'messy_orders.csv'
)
messy_csv_path.write_text(
    (
        'order_id,sku,quantity_RAW,unit_price_RAW,order_date_RAW\n'
        '1001,  SKU-001 ,2,12.99,2026-08-18\n'
        '1002,SKU-002,3,8.50,2026-08-18\n'
        '1003,SKU-003,abc,19.99,2026-08-19\n'
        '1004,SKU-004    ,-2,4.99,2026-08-19\n'
        '1005,     SKU-005 ,1,not-a-price,2026-08-20\n'
        '1006,SKU-006,4,29.99,not-a-date\n'
    ),
    encoding='utf-8'
)

spark = (
    SparkSession.builder
    .appName('phase_01_section_05')
    .master('local[*]')
    .getOrCreate()
)
raw_schema = StructType([
    StructField('order_id', StringType(), True),
    StructField('sku', StringType(), True),
    StructField('quantity_RAW', StringType(), True),
    StructField('unit_price_RAW', StringType(), True),
    StructField('order_date_RAW', StringType(), True),
])
raw_df = (
    spark.read
    .option('header', True)
    .schema(raw_schema)
    .csv(str(messy_csv_path))
)
# raw_df.printSchema()
# =>
"""
root
 |-- order_id: string (nullable = true)
 |-- sku: string (nullable = true)
 |-- quantity_RAW: string (nullable = true)
 |-- unit_price_RAW: string (nullable = true)
 |-- order_date_RAW: string (nullable = true)
"""
# raw_df.show(truncate=False)
# =>
"""
+--------+-------------+------------+--------------+--------------+
|order_id|sku          |quantity_RAW|unit_price_RAW|order_date_RAW|
+--------+-------------+------------+--------------+--------------+
|1001    |  SKU-001    |2           |12.99         |2026-08-18    |
|1002    |SKU-002      |3           |8.50          |2026-08-18    |
|1003    |SKU-003      |abc         |19.99         |2026-08-19    |
|1004    |SKU-004      |-2          |4.99          |2026-08-19    |
|1005    |     SKU-005 |1           |not-a-price   |2026-08-20    |
|1006    |SKU-006      |4           |29.99         |not-a-date    |
+--------+-------------+------------+--------------+--------------+
"""


# =============================================================================
# 5.1 Standardize strings
# =============================================================================
standardized_df = (
    raw_df
    .withColumn(
        'sku',
        F.upper(F.trim(F.col('sku')))
    )
)
# standardized_df.show(truncate=False)
# =>
"""
+--------+-------+------------+--------------+--------------+                   
|order_id|sku    |quantity_RAW|unit_price_RAW|order_date_RAW|
+--------+-------+------------+--------------+--------------+
|1001    |SKU-001|2           |12.99         |2026-08-18    |
|1002    |SKU-002|3           |8.50          |2026-08-18    |
|1003    |SKU-003|abc         |19.99         |2026-08-19    |
|1004    |SKU-004|-2          |4.99          |2026-08-19    |
|1005    |SKU-005|1           |not-a-price   |2026-08-20    |
|1006    |SKU-006|4           |29.99         |not-a-date    |
+--------+-------+------------+--------------+--------------+
"""


# =============================================================================
# 5.2 `select()` and `alias()`
# =============================================================================
selected_df = (
    standardized_df
    .select(
        F.col('order_id'),
        F.col('sku'),
        F.col('quantity_raw').alias('source_quantity'),
    )
)
# selected_df.show(truncate=False)
# =>
"""
+--------+-------+---------------+                                              
|order_id|sku    |source_quantity|
+--------+-------+---------------+
|1001    |SKU-001|2              |
|1002    |SKU-002|3              |
|1003    |SKU-003|abc            |
|1004    |SKU-004|-2             |
|1005    |SKU-005|1              |
|1006    |SKU-006|4              |
+--------+-------+---------------+
"""


# =============================================================================
# 5.3 `filter()` and `where()`
# =============================================================================
filtered_df = (
    standardized_df
    .filter(
        # INCORRECT
        # ---------
        # (F.col('quantity_RAW') != 'abc')
        # | (F.col('unit_price_RAW') != 'not-a-price')
        # | (F.col('order_date_RAW') != 'not-a-date')
        # ---------
        (F.col('quantity_RAW') != 'abc')
        & (F.col('unit_price_RAW') != 'not-a-price')
        & (F.col('order_date_RAW') != 'not-a-date')
    )
    # .where(
    #     (F.col('quantity_RAW') != 'abc')
    #     & (F.col('unit_price_RAW') != 'not-a-price')
    #     & (F.col('order_date_RAW') != 'not-a-date')
    # )
)
# filtered_df.show(truncate=False)
# =>
"""
+--------+-------+------------+--------------+--------------+                   
|order_id|sku    |quantity_RAW|unit_price_RAW|order_date_RAW|
+--------+-------+------------+--------------+--------------+
|1001    |SKU-001|2           |12.99         |2026-08-18    |
|1002    |SKU-002|3           |8.50          |2026-08-18    |
|1004    |SKU-004|-2          |4.99          |2026-08-19    |
+--------+-------+------------+--------------+--------------+
"""


# =============================================================================
# 5.4 `drop()`
# =============================================================================
biz_view_df = (
    standardized_df
    .drop(
        'quantity_RAW',
        'unit_price_RAW',
        'order_date_RAW',
    )
)
# biz_view_df.show(truncate=False)
# =>
"""
+--------+-------+                                                              
|order_id|sku    |
+--------+-------+
|1001    |SKU-001|
|1002    |SKU-002|
|1003    |SKU-003|
|1004    |SKU-004|
|1005    |SKU-005|
|1006    |SKU-006|
+--------+-------+
"""


# =============================================================================
# 5.5 `distinct()` vs. `dropDuplicates()`
# =============================================================================
duplicate_data = [
    ('1001', 'SKU-001', '2'),
    ('1001', 'SKU-001', '2'),
    ('1001', 'SKU-001', '3'),
    ('1002', 'SKU-002', '1'),
]

duplicate_df = spark.createDataFrame(
    duplicate_data,
    ['order_id', 'sku', 'quantity_raw'],
)
distinct_df = duplicate_df.distinct()
# distinct_df.show(truncate=False)
# =>
"""
+--------+-------+------------+                                                 
|order_id|sku    |quantity_raw|
+--------+-------+------------+
|1001    |SKU-001|2           |
|1001    |SKU-001|3           |
|1002    |SKU-002|1           |
+--------+-------+------------+
"""

deduplicated_df = duplicate_df.dropDuplicates(['order_id', 'sku'])
# deduplicated_df.show(truncate=False)
# =>
"""
+--------+-------+------------+                                                 
|order_id|sku    |quantity_raw|
+--------+-------+------------+
|1001    |SKU-001|2           |
|1002    |SKU-002|1           |
+--------+-------+------------+
"""

# =============================================================================
# 5.6 `orderBy()` and `limit()`
# =============================================================================
order_df = (
    duplicate_df
    .orderBy(
        # F.col('order_id').asc()
        F.col('order_id').desc()
    )
)
# order_df.show(truncate=False)
# =>
"""
+--------+-------+------------+                                                 
|order_id|sku    |quantity_raw|
+--------+-------+------------+
|1002    |SKU-002|1           |
|1001    |SKU-001|2           |
|1001    |SKU-001|3           |
|1001    |SKU-001|2           |
+--------+-------+------------+
"""

sample_df = duplicate_df.limit(2)
# sample_df.show(truncate=False)
# =>
"""
+--------+-------+------------+                                                 
|order_id|sku    |quantity_raw|
+--------+-------+------------+
|1001    |SKU-001|2           |
|1001    |SKU-001|2           |
+--------+-------+------------+
"""


spark.stop()
