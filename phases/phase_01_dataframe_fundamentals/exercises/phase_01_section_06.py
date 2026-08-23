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
Phase 1, Section 6: NULL Handling and Conditional Expressions
-------------------------------------------------------------
1. `isNull()`
2. `isNotNull()`
3. `coalesce()`
4. `when()` / `otherwise()`
5. NULL propagation

NOTE:
* Prefer `isNull()` over `== None`.
* When aggregating, if either operand is NULL, the result is normally NULL.
"""
spark = (
    SparkSession.builder
    .appName('phase_01_section_06')
    .master('local[*]')
    .getOrCreate()
)
schema = StructType([
    StructField('order_id', StringType(), False),
    StructField('quantity', IntegerType(), True),
    StructField('unit_price', DecimalType(10, 2), True),
    StructField('province', StringType(), True),
])
data = [
    ('1001', 2, Decimal('12.99'), 'ON'),
    ('1002', None, Decimal('8.50'), 'ON'),
    ('1003', 3, None, 'QC'),
    ('1004', 1, Decimal('4.99'), None),
]
df = spark.createDataFrame(data, schema)
# df.show(truncate=False)
# =>
"""
+--------+--------+----------+--------+                                         
|order_id|quantity|unit_price|province|
+--------+--------+----------+--------+
|1001    |2       |12.99     |ON      |
|1002    |NULL    |8.50      |ON      |
|1003    |3       |NULL      |QC      |
|1004    |1       |4.99      |NULL    |
+--------+--------+----------+--------+
"""


# =============================================================================
# 6.1 NULL predicates
# =============================================================================
missing_quantity_df = df.filter(F.col('quantity').isNull())
# missing_quantity_df.show(truncate=False)
# =>
"""
+--------+--------+----------+--------+                                         
|order_id|quantity|unit_price|province|
+--------+--------+----------+--------+
|1002    |NULL    |8.50      |ON      |
+--------+--------+----------+--------+
"""
present_quantity_df = df.filter(F.col('quantity').isNotNull())
# present_quantity_df.show(truncate=False)
# =>
"""
+--------+--------+----------+--------+                                         
|order_id|quantity|unit_price|province|
+--------+--------+----------+--------+
|1001    |2       |12.99     |ON      |
|1003    |3       |NULL      |QC      |
|1004    |1       |4.99      |NULL    |
+--------+--------+----------+--------+
"""


# =============================================================================
# 6.2 `coalesce()`
# =============================================================================
filtered_df = df.withColumn(
    'province_clean',
    F.coalesce(
        F.col('province'),
        F.lit('UNKNOWN'),
    ),
)
# filtered_df.show(truncate=False)
# =>
"""
+--------+--------+----------+--------+--------------+                          
|order_id|quantity|unit_price|province|province_clean|
+--------+--------+----------+--------+--------------+
|1001    |2       |12.99     |ON      |ON            |
|1002    |NULL    |8.50      |ON      |ON            |
|1003    |3       |NULL      |QC      |QC            |
|1004    |1       |4.99      |NULL    |UNKNOWN       |
+--------+--------+----------+--------+--------------+
"""


# =============================================================================
# 6.3 `when()` / `otherwise()`
# =============================================================================
classified_df = df.withColumn(
    'quantity_status',
    F.when(
        F.col('quantity').isNull(),
        F.lit('MISSING'),
    )
    .when(
        F.col('quantity') <= 0,
        F.lit('INVALID'),
    )
    .otherwise(
        F.lit('VALID')
    ),
)
# classified_df.show(truncate=False)
# =>
"""
+--------+--------+----------+--------+---------------+                         
|order_id|quantity|unit_price|province|quantity_status|
+--------+--------+----------+--------+---------------+
|1001    |2       |12.99     |ON      |VALID          |
|1002    |NULL    |8.50      |ON      |MISSING        |
|1003    |3       |NULL      |QC      |VALID          |
|1004    |1       |4.99      |NULL    |VALID          |
+--------+--------+----------+--------+---------------+
"""


# =============================================================================
# 6.4 NULL propagation
# =============================================================================
bad_revenue_df = df.withColumn(
    'line_revenue',
    F.col('quantity') * F.col('unit_price'),
)
# bad_revenue_df.show(truncate=False)
# =>
"""
+--------+--------+----------+--------+------------+                            
|order_id|quantity|unit_price|province|line_revenue|
+--------+--------+----------+--------+------------+
|1001    |2       |12.99     |ON      |25.98       |
|1002    |NULL    |8.50      |ON      |NULL        |
|1003    |3       |NULL      |QC      |NULL        |
|1004    |1       |4.99      |NULL    |4.99        |
+--------+--------+----------+--------+------------+
"""
good_revenue_df = df.withColumn(
    'line_revenue',
    F.coalesce(F.col('quantity'), F.lit(0))
    * F.coalesce(F.col('unit_price'), F.lit(0)),
)
good_revenue_df.show(truncate=False)
# =>
"""
+--------+--------+----------+--------+------------+                            
|order_id|quantity|unit_price|province|line_revenue|
+--------+--------+----------+--------+------------+
|1001    |2       |12.99     |ON      |25.98       |
|1002    |NULL    |8.50      |ON      |0.00        |
|1003    |3       |NULL      |QC      |0.00        |
|1004    |1       |4.99      |NULL    |4.99        |
+--------+--------+----------+--------+------------+
"""


spark.stop()
