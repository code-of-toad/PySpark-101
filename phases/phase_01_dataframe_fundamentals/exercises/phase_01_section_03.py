from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

"""
Phase 1, Section 3: Messy CSV with Explicit Types
-------------------------------------------------
Goal: Observe behaviour when raw text must become typed Spark values.

Key Distinction
---------------
* `abc` ->  Parsing/type problem.
* `-2`  ->  Valid integer, potentially invalid business value.
"""
messy_csv_path = Path(
    'phases\\'
    'phase_01_dataframe_fundamentals\\'
    'exercises\\'
    'messy_orders.csv'
)
messy_csv_path.write_text(
    (
        'order_id,sku,quantity,unit_price,order_date\n'
        '1001,SKU-001,2,12.99,2026-08-18\n'
        '1002,SKU-002,3,8.50,2026-08-18\n'
        '1003,SKU-003,abc,19.99,2026-08-19\n'
        '1004,SKU-004,-2,4.99,2026-08-19\n'
        '1005,SKU-005,1,not-a-price,2026-08-20\n'
        '1006,SKU-006,4,29.99,not-a-date\n'
    ),
    encoding='utf-8'
)

spark = (
    SparkSession.builder
    .appName('phase_01_section_03')
    .master('local[*]')
    .getOrCreate()
)

schema = StructType([
    StructField('order_id', StringType(), True),
    StructField('sku', StringType(), True),
    StructField('quantity', IntegerType(), True),
    StructField('unit_price', DecimalType(10, 2), True),
    StructField('order_date', DateType(), True),
    StructField('_corrupt_record', StringType(), True),
])

df = (
    spark.read
    .option('header', True)
    .option('mode', 'PERMISSIVE')
    .option('columnNameOfCorruptRecord', '_corrupt_record')
    .option('dateFormat', 'yyyy-MM-dd')
    .schema(schema)
    .csv(str(messy_csv_path))
)

# df.printSchema()
# =>
"""
root
 |-- order_id: string (nullable = true)
 |-- sku: string (nullable = true)
 |-- quantity: integer (nullable = true)
 |-- unit_price: decimal(10,2) (nullable = true)
 |-- order_date: date (nullable = true)
 |-- _corrupt_record: string (nullable = true)
"""
# df.show(truncate=False)
# =>
"""
+--------+-------+--------+----------+----------+-------------------------------------+
|order_id|sku    |quantity|unit_price|order_date|_corrupt_record                      |
+--------+-------+--------+----------+----------+-------------------------------------+
|1001    |SKU-001|2       |12.99     |2026-08-18|NULL                                 |
|1002    |SKU-002|3       |8.50      |2026-08-18|NULL                                 |
|1003    |SKU-003|NULL    |19.99     |2026-08-19|1003,SKU-003,abc,19.99,2026-08-19    |
|1004    |SKU-004|-2      |4.99      |2026-08-19|NULL                                 |
|1005    |SKU-005|1       |NULL      |2026-08-20|1005,SKU-005,1,not-a-price,2026-08-20|
|1006    |SKU-006|4       |29.99     |NULL      |1006,SKU-006,4,29.99,not-a-date      |
+--------+-------+--------+----------+----------+-------------------------------------+
"""

corrupt_df = df.filter(
    F.col('_corrupt_record').isNotNull()
)
# corrupt_df.show(truncate=False)
"""
+--------+-------+--------+----------+----------+-------------------------------------+
|order_id|sku    |quantity|unit_price|order_date|_corrupt_record                      |
+--------+-------+--------+----------+----------+-------------------------------------+
|1003    |SKU-003|NULL    |19.99     |2026-08-19|1003,SKU-003,abc,19.99,2026-08-19    |
|1005    |SKU-005|1       |NULL      |2026-08-20|1005,SKU-005,1,not-a-price,2026-08-20|
|1006    |SKU-006|4       |29.99     |NULL      |1006,SKU-006,4,29.99,not-a-date      |
+--------+-------+--------+----------+----------+-------------------------------------+
"""

parsed_df = df.filter(
    F.col('_corrupt_record').isNull()
)
# parsed_df.show(truncate=False)
"""
+--------+-------+--------+----------+----------+---------------+
|order_id|sku    |quantity|unit_price|order_date|_corrupt_record|
+--------+-------+--------+----------+----------+---------------+
|1001    |SKU-001|2       |12.99     |2026-08-18|NULL           |
|1002    |SKU-002|3       |8.50      |2026-08-18|NULL           |
|1004    |SKU-004|-2      |4.99      |2026-08-19|NULL           |
+--------+-------+--------+----------+----------+---------------+
"""


spark.stop()
