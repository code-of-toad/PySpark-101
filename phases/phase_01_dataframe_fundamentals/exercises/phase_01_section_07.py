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
Phase 1, Section 7: String Functions
------------------------------------
Practice string normalization.
"""
spark = (
    SparkSession.builder
    .appName('phase_01_section_07')
    .master('local[*]')
    .getOrCreate()
)
schema = StructType([
    StructField('order_id',       StringType(), False),
    StructField('sku',            StringType(), True),
    StructField('province',       StringType(), True),
    StructField('customer_email', StringType(), True),
    StructField('product_code',   StringType(), True),
])
data = [
    ('1001', ' sku-001 ', 'on',   ' DANNY@EXAMPLE.COM ', 'prod_001'),
    ('1002', 'SKU-002',   ' ON ', 'user2@example.com',   'PROD-002'),
    ('1003', 'Sku-003',   'qc',   None,                  'prod 003'),
    ('1004', '  sku-004', 'Bc', ' USER4@EXAMPLE.COM',    'prod__004'),
]
df = spark.createDataFrame(data, schema)
# df.show(truncate=False)
# =>
"""
+--------+---------+--------+-------------------+------------+                  
|order_id|sku      |province|customer_email     |product_code|
+--------+---------+--------+-------------------+------------+
|1001    | sku-001 |on      | DANNY@EXAMPLE.COM |prod_001    |
|1002    |SKU-002  | ON     |user2@example.com  |PROD-002    |
|1003    |Sku-003  |qc      |NULL               |prod 003    |
|1004    |  sku-004|Bc      | USER4@EXAMPLE.COM |prod__004   |
+--------+---------+--------+-------------------+------------+
"""


# =============================================================================
# 7.1 Normalize strings
# =============================================================================
standardized_df = (
    df
    .withColumn(
        'sku',
        F.upper(F.trim(F.col('sku')))
    )
    .withColumn(
        'province',
        F.upper(F.trim(F.col('province')))
    )
    .withColumn(
        'customer_email',
        F.lower(F.trim(F.col('customer_email')))
    )
)
# standardized_df.show(truncate=False)
# =>
"""
+--------+-------+--------+-----------------+------------+                      
|order_id|sku    |province|customer_email   |product_code|
+--------+-------+--------+-----------------+------------+
|1001    |SKU-001|ON      |danny@example.com|prod_001    |
|1002    |SKU-002|ON      |user2@example.com|PROD-002    |
|1003    |SKU-003|QC      |NULL             |prod 003    |
|1004    |SKU-004|BC      |user4@example.com|prod__004   |
+--------+-------+--------+-----------------+------------+
"""


# =============================================================================
# 7.2 `regexp_replace()`
# =============================================================================
cleaned_df = (
    standardized_df
    .withColumn(
        'product_code',
        F.upper(F.regexp_replace(
            F.trim(F.col('product_code')),
            '[_ ]+',
            '-',
        )),
    )
)
# cleaned_df.show(truncate=False)
# =>
"""
+--------+-------+--------+-----------------+------------+                      
|order_id|sku    |province|customer_email   |product_code|
+--------+-------+--------+-----------------+------------+
|1001    |SKU-001|ON      |danny@example.com|PROD-001    |
|1002    |SKU-002|ON      |user2@example.com|PROD-002    |
|1003    |SKU-003|QC      |NULL             |PROD-003    |
|1004    |SKU-004|BC      |user4@example.com|PROD-004    |
+--------+-------+--------+-----------------+------------+
"""


# =============================================================================
# 7.3 `substring()`
# =============================================================================
sku_parts_df = cleaned_df.withColumn(
    'sku_number',
    F.substring(F.col('sku'), 5, 3),
)
# sku_parts_df.show(truncate=False)
# =>
"""
+--------+-------+--------+-----------------+------------+----------+           
|order_id|sku    |province|customer_email   |product_code|sku_number|
+--------+-------+--------+-----------------+------------+----------+
|1001    |SKU-001|ON      |danny@example.com|PROD-001    |001       |
|1002    |SKU-002|ON      |user2@example.com|PROD-002    |002       |
|1003    |SKU-003|QC      |NULL             |PROD-003    |003       |
|1004    |SKU-004|BC      |user4@example.com|PROD-004    |004       |
+--------+-------+--------+-----------------+------------+----------+
"""


# =============================================================================
# 7.4 `length()`
# =============================================================================
length_df = cleaned_df.withColumn('sku_length', F.length(F.col('sku')))
# length_df.show(truncate=False)
# =>
"""
+--------+-------+--------+-----------------+------------+----------+           
|order_id|sku    |province|customer_email   |product_code|sku_length|
+--------+-------+--------+-----------------+------------+----------+
|1001    |SKU-001|ON      |danny@example.com|PROD-001    |7         |
|1002    |SKU-002|ON      |user2@example.com|PROD-002    |7         |
|1003    |SKU-003|QC      |NULL             |PROD-003    |7         |
|1004    |SKU-004|BC      |user4@example.com|PROD-004    |7         |
+--------+-------+--------+-----------------+------------+----------+
"""


# =============================================================================
# 7.5 `concat_ws()`
# =============================================================================
reference_df = cleaned_df.withColumn(
    'order_sku_reference',
    F.concat_ws('-', F.col('order_id'), F.col('sku'))
)
reference_df.show(truncate=False)
# =>
"""
+--------+-------+--------+-----------------+------------+-------------------+  
|order_id|sku    |province|customer_email   |product_code|order_sku_reference|
+--------+-------+--------+-----------------+------------+-------------------+
|1001    |SKU-001|ON      |danny@example.com|PROD-001    |1001-SKU-001       |
|1002    |SKU-002|ON      |user2@example.com|PROD-002    |1002-SKU-002       |
|1003    |SKU-003|QC      |NULL             |PROD-003    |1003-SKU-003       |
|1004    |SKU-004|BC      |user4@example.com|PROD-004    |1004-SKU-004       |
+--------+-------+--------+-----------------+------------+-------------------+
"""


spark.stop()
