from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

"""
Phase 1, Section 4: Preserve Raw Values & Parse Explicitly
----------------------------------------------------------
Goal: Build a more auditable ingestion pattern.
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
        '1001,SKU-001,2,12.99,2026-08-18\n'
        '1002,SKU-002,3,8.50,2026-08-18\n'
        '1003,SKU-003,abc,19.99,2026-08-19\n'
        '1004,SKU-004,-2,4.99,2026-08-19\n'
        '1005,SKU-005,1,not-a-price,2026-08-20\n'
        '1006,SKU-006,4,29.99,not-a-date\n'
    ),
    encoding='utf-8'
)


# =============================================================================
# 4.1 Read raw fields as strings
# =============================================================================
spark = (
    SparkSession.builder
    .appName('phase_01_section_04')
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
# 4.2 Create typed columns
# =============================================================================
typed_df = (
    raw_df
    .withColumn(
        'quantity',
        F.col('quantity_RAW').try_cast('int'),
    )
    .withColumn(
        'unit_price',
        F.col('unit_price_RAW').try_cast('decimal(10,2)'),
    )
    .withColumn(
        'order_date',
        F.try_to_date(
            F.col('order_date_RAW'),
            'yyyy-MM-dd',
        ),
    )
)
# typed_df.printSchema()
# =>
"""
root
 |-- order_id: string (nullable = true)
 |-- sku: string (nullable = true)
 |-- quantity_RAW: string (nullable = true)
 |-- unit_price_RAW: string (nullable = true)
 |-- order_date_RAW: string (nullable = true)
 |-- quantity: integer (nullable = true)
 |-- unit_price: decimal(10,2) (nullable = true)
 |-- order_date: date (nullable = true)
"""
# typed_df.show(truncate=False)
# =>
"""
+--------+-------+------------+--------------+--------------+--------+----------+----------+
|order_id|sku    |quantity_RAW|unit_price_RAW|order_date_RAW|quantity|unit_price|order_date|
+--------+-------+------------+--------------+--------------+--------+----------+----------+
|1001    |SKU-001|2           |12.99         |2026-08-18    |2       |12.99     |2026-08-18|
|1002    |SKU-002|3           |8.50          |2026-08-18    |3       |8.50      |2026-08-18|
|1003    |SKU-003|abc         |19.99         |2026-08-19    |NULL    |19.99     |2026-08-19|
|1004    |SKU-004|-2          |4.99          |2026-08-19    |-2      |4.99      |2026-08-19|
|1005    |SKU-005|1           |not-a-price   |2026-08-20    |1       |NULL      |2026-08-20|
|1006    |SKU-006|4           |29.99         |not-a-date    |4       |29.99     |NULL      |
+--------+-------+------------+--------------+--------------+--------+----------+----------+
"""


# =============================================================================
# 4.3 Detect conversion failures
# =============================================================================
missing_order_id = (
    F.col('order_id').isNull()
)

invalid_quantity_format = (
    F.col('quantity_raw').isNotNull()
    & F.col('quantity').isNull()
)

quantity_must_be_positive = (
    F.col('quantity') <= 0
)

invalid_unit_price_format = (
    F.col('unit_price_raw').isNotNull()
    & F.col('unit_price').isNull()
)

invalid_order_date_format = (
    F.col('order_date_raw').isNotNull()
    & F.col('order_date').isNull()
)


# =============================================================================
# 4.4 Add rejection reasons
# =============================================================================
validated_df = (
    typed_df
    .withColumn(
        'rejection_reason',
        F.when(
            missing_order_id,
            F.lit('missing_order_id'),
        )
        .when(
            invalid_quantity_format,
            F.lit('invalid_quantity_format'),
        )
        .when(
            quantity_must_be_positive,
            F.lit('quantity_must_be_positive'),
        )
        .when(
            invalid_unit_price_format,
            F.lit('invalid_unit_price_format'),
        )
        .when(
            invalid_order_date_format,
            F.lit('invalid_order_date_format'),
        )
    )
)
# validated_df.show(truncate=False)
# =>
"""
+--------+-------+------------+--------------+--------------+--------+----------+----------+-------------------------+
|order_id|sku    |quantity_RAW|unit_price_RAW|order_date_RAW|quantity|unit_price|order_date|rejection_reason         |
+--------+-------+------------+--------------+--------------+--------+----------+----------+-------------------------+
|1001    |SKU-001|2           |12.99         |2026-08-18    |2       |12.99     |2026-08-18|NULL                     |
|1002    |SKU-002|3           |8.50          |2026-08-18    |3       |8.50      |2026-08-18|NULL                     |
|1003    |SKU-003|abc         |19.99         |2026-08-19    |NULL    |19.99     |2026-08-19|invalid_quantity_format  |
|1004    |SKU-004|-2          |4.99          |2026-08-19    |-2      |4.99      |2026-08-19|quantity_must_be_positive|
|1005    |SKU-005|1           |not-a-price   |2026-08-20    |1       |NULL      |2026-08-20|invalid_unit_price_format|
|1006    |SKU-006|4           |29.99         |not-a-date    |4       |29.99     |NULL      |invalid_order_date_format|
+--------+-------+------------+--------------+--------------+--------+----------+----------+-------------------------+
"""


# =============================================================================
# 4.5 Split accepted and rejected rows
# =============================================================================
accepted_df = (
    validated_df
    .filter(
        F.col('rejection_reason').isNull()
    )
)
rejected_df = (
    validated_df
    .filter(
        F.col('rejection_reason').isNotNull()
    )
)
# accepted_df.show(truncate=False)
# =>
"""
+--------+-------+------------+--------------+--------------+--------+----------+----------+----------------+
|order_id|sku    |quantity_RAW|unit_price_RAW|order_date_RAW|quantity|unit_price|order_date|rejection_reason|
+--------+-------+------------+--------------+--------------+--------+----------+----------+----------------+
|1001    |SKU-001|2           |12.99         |2026-08-18    |2       |12.99     |2026-08-18|NULL            |
|1002    |SKU-002|3           |8.50          |2026-08-18    |3       |8.50      |2026-08-18|NULL            |
+--------+-------+------------+--------------+--------------+--------+----------+----------+----------------+
"""
# rejected_df.show(truncate=False)
# =>
"""
+--------+-------+------------+--------------+--------------+--------+----------+----------+-------------------------+
|order_id|sku    |quantity_RAW|unit_price_RAW|order_date_RAW|quantity|unit_price|order_date|rejection_reason         |
+--------+-------+------------+--------------+--------------+--------+----------+----------+-------------------------+
|1003    |SKU-003|abc         |19.99         |2026-08-19    |NULL    |19.99     |2026-08-19|invalid_quantity_format  |
|1004    |SKU-004|-2          |4.99          |2026-08-19    |-2      |4.99      |2026-08-19|quantity_must_be_positive|
|1005    |SKU-005|1           |not-a-price   |2026-08-20    |1       |NULL      |2026-08-20|invalid_unit_price_format|
|1006    |SKU-006|4           |29.99         |not-a-date    |4       |29.99     |NULL      |invalid_order_date_format|
+--------+-------+------------+--------------+--------------+--------+----------+----------+-------------------------+
"""


spark.stop()
