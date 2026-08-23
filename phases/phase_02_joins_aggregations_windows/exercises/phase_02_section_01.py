from pyspark.sql import SparkSession
from pyspark.sql import functions as F

"""
Phase 1, Section 1: DataFrame Fundamentals
------------------------------------------
1. `SparkSession`
2. DataFrame creation
3. schema inference
4. transformations vs. actions
5. lazy evaluation
6. DataFrame immutability

QUESTIONS
---------
Q. Why did Spark infer `quantity` as a string?
Q. What is the execution difference between `filter()` and `show()`?
Q. Why does `df` mean unchanged after `df.drop('order_date')`?
Q. Why is an all-string schema dangerous in a production pipeline?
"""
# =============================================================================
# 1.1 Create a DataFrame w/ inferred types
# =============================================================================
spark = (
    SparkSession.builder
    .appName('phase_01_section_01')
    .master('local[*]')
    .getOrCreate()
)

data = [
    ('1001', ' SKU-001 ', '2', '12.99', '2026-08-18'),
    ('1002', 'SKU-002', '3', '8.50', '2026-08-18'),
    ('1003', 'SKU-003', 'abc', '19.99', '2026-08-19'),
]
schema = ['order_id', 'sku', 'quantity', 'unit_price', 'order_date']
df = spark.createDataFrame(data, schema)

# df.show()
# =>
"""
+--------+---------+--------+----------+----------+                             
|order_id|      sku|quantity|unit_price|order_date|
+--------+---------+--------+----------+----------+
|    1001| SKU-001 |       2|     12.99|2026-08-18|
|    1002|  SKU-002|       3|      8.50|2026-08-18|
|    1003|  SKU-003|     abc|     19.99|2026-08-19|
+--------+---------+--------+----------+----------+
"""
# df.printSchema()
# =>
"""
root
 |-- order_id: string (nullable = true)
 |-- sku: string (nullable = true)
 |-- quantity: string (nullable = true)
 |-- unit_price: string (nullable = true)
 |-- order_date: string (nullable = true)
"""


# =============================================================================
# 1.2 Transformation vs. Action
# =============================================================================
clean_df = df.filter(
    F.col('quantity') != 'abc'
)

# clean_df.show()
# =>
"""
+--------+---------+--------+----------+----------+                             
|order_id|      sku|quantity|unit_price|order_date|
+--------+---------+--------+----------+----------+
|    1001| SKU-001 |       2|     12.99|2026-08-18|
|    1002|  SKU-002|       3|      8.50|2026-08-18|
+--------+---------+--------+----------+----------+
"""


# =============================================================================
# 1.3 DataFrame immutability
# =============================================================================
df.drop('order_date')
# df.show()
# =>
"""
+--------+---------+--------+----------+----------+                             
|order_id|      sku|quantity|unit_price|order_date|
+--------+---------+--------+----------+----------+
|    1001| SKU-001 |       2|     12.99|2026-08-18|
|    1002|  SKU-002|       3|      8.50|2026-08-18|
|    1003|  SKU-003|     abc|     19.99|2026-08-19|
+--------+---------+--------+----------+----------+
"""
# df.printSchema()
# =>
"""
root
 |-- order_id: string (nullable = true)
 |-- sku: string (nullable = true)
 |-- quantity: string (nullable = true)
 |-- unit_price: string (nullable = true)
 |-- order_date: string (nullable = true)
"""

smaller_df = df.drop('order_date')
# smaller_df.show()
# =>
"""
+--------+---------+--------+----------+                             
|order_id|      sku|quantity|unit_price|
+--------+---------+--------+----------+
|    1001| SKU-001 |       2|     12.99|
|    1002|  SKU-002|       3|      8.50|
|    1003|  SKU-003|     abc|     19.99|
+--------+---------+--------+----------+
"""
# smaller_df.printSchema()
# =>
"""
root
 |-- order_id: string (nullable = true)
 |-- sku: string (nullable = true)
 |-- quantity: string (nullable = true)
 |-- unit_price: string (nullable = true)
"""


spark.stop()
