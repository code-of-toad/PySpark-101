from datetime import date
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

"""
Phase 1, Section 2: Explicit Schemas
------------------------------------
1. `StructType`
2. `StructField`
3. Spark data types
4. nullability
5. explicit schema enforcement

Key Distinction
---------------
`nullable=True` means Spark permits NULL structurally.
It does NOT mean that NULL satisfies business rules.
"""
# =============================================================================
# 2.1 Define an explicit schema
# =============================================================================
spark = (
    SparkSession.builder
    .appName('phase_01_section_02')
    .master('local[*]')
    .getOrCreate()
)

explicit_schema = StructType([
    StructField('order_id', StringType(), False),
    StructField('sku', StringType(), False),
    StructField('quantity', IntegerType(), True),
    StructField('unit_price', DecimalType(10, 2), True),
    StructField('order_date', DateType(), True),
])

# print(explicit_schema.treeString())
# =>
"""
root
 |-- order_id: string (nullable = false)
 |-- sku: string (nullable = false)
 |-- quantity: integer (nullable = true)
 |-- unit_price: decimal(10,2) (nullable = true)
 |-- order_date: date (nullable = true)
"""


# =============================================================================
# 2.2 Create correctly-typed rows
# =============================================================================
data =[
    ('1001', 'SKU-001', 2, Decimal('12.99'), date(2026, 8, 18)),
    ('1002', 'SKU-002', 3, Decimal('8.50'), date(2026, 8, 18)),
    ('1003', 'SKU-003', 1, Decimal('19.99'), date(2026, 8, 19)),
]
df = spark.createDataFrame(data, explicit_schema)

# df.printSchema()
# =>
"""
root
 |-- order_id: string (nullable = false)
 |-- sku: string (nullable = false)
 |-- quantity: integer (nullable = true)
 |-- unit_price: decimal(10,2) (nullable = true)
 |-- order_date: date (nullable = true)
"""
# df.show()
# =>
"""
+--------+-------+--------+----------+----------+                               
|order_id|    sku|quantity|unit_price|order_date|
+--------+-------+--------+----------+----------+
|    1001|SKU-001|       2|     12.99|2026-08-18|
|    1002|SKU-002|       3|      8.50|2026-08-18|
|    1003|SKU-003|       1|     19.99|2026-08-19|
+--------+-------+--------+----------+----------+
"""


# =============================================================================
# 2.3 Violate `IntegerType`
# =============================================================================
bad_data = [
    ('1001', 'SKU-001', '1', Decimal('19.99'), date(2026, 8, 19)),
]
# bad_df = spark.createDataFrame(bad_data, explicit_schema)
# =>
"""
pyspark.errors.exceptions.base.PySparkTypeError:
[FIELD_DATA_TYPE_UNACCEPTABLE_WITH_NAME] field quantity:
IntegerType() can not accept object '1' in type <class 'str'>.
"""


# =============================================================================
# 2.4 Test nullability
# =============================================================================
null_data_1 = [
    (None, 'SKU-001', '1', Decimal('19.99'), date(2026, 8, 19)),
]
# null_df_1 = spark.createDataFrame(null_data_1, explicit_schema)
# =>
"""
pyspark.errors.exceptions.base.PySparkValueError:
[FIELD_NOT_NULLABLE_WITH_NAME] field order_id:
This field is not nullable, but got None.
"""

null_data_2 = [
    ('1001', 'SKU-001', None, Decimal('19.99'), date(2026, 8, 19)),
]
null_df_2 = spark.createDataFrame(null_data_2, explicit_schema)
"""
SUCCESS
"""


spark.stop()
