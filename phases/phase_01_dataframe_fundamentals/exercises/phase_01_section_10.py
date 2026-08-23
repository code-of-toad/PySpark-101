from pathlib import Path
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

"""
Phase 1, Section 10: Arrays, Structs, and Nested JSON
-----------------------------------------------------
Understand nested Spark SQL types.
"""
spark = (
    SparkSession.builder
    .appName('phase_01_section_10')
    .master('local[*]')
    .getOrCreate()
)


# =============================================================================
# 10.1 Arrays
# =============================================================================
schema = StructType([
    StructField('product_id', StringType(), False),
    StructField(
        'tags',
        ArrayType(
            StringType(),
            containsNull=False,
        ),
        True
    ),
])
data = [
    ('P001', ['electronics', 'sale']),
    ('P002', ['grocery', 'organic']),
    ('P003', []),
    ('P004', None),
]
df = spark.createDataFrame(data, schema)
# df.show(truncate=False)
# =>
"""
+----------+-------------------+                                                
|product_id|tags               |
+----------+-------------------+
|P001      |[electronics, sale]|
|P002      |[grocery, organic] |
|P003      |[]                 |
|P004      |NULL               |
+----------+-------------------+
"""


# =============================================================================
# 10.2 Create an array column
# =============================================================================
product_df = spark.createDataFrame(
    [
        ('P001', 'electronics', 'sale'),
        ('P002', 'grocery', 'organic'),
        ('P003', 'clothing', None),
    ],
    ['product_id', 'category', 'promotion_type'],
)
# product_df.show(truncate=False)
# =>
"""
+----------+-----------+--------------+                                         
|product_id|category   |promotion_type|
+----------+-----------+--------------+
|P001      |electronics|sale          |
|P002      |grocery    |organic       |
|P003      |clothing   |NULL          |
+----------+-----------+--------------+
"""

array_df = product_df.withColumn(
    'attributes',
    F.array(
        F.col('category'),
        F.col('promotion_type')
    ),
)
# array_df.show(truncate=False)
# =>
"""
+----------+-----------+--------------+-------------------+                     
|product_id|category   |promotion_type|attributes         |
+----------+-----------+--------------+-------------------+
|P001      |electronics|sale          |[electronics, sale]|
|P002      |grocery    |organic       |[grocery, organic] |
|P003      |clothing   |NULL          |[clothing, NULL]   |
+----------+-----------+--------------+-------------------+
"""
# array_df.printSchema()
# =>
"""
root
 |-- product_id: string (nullable = true)
 |-- category: string (nullable = true)
 |-- promotion_type: string (nullable = true)
 |-- attributes: array (nullable = false)
 |    |-- element: string (containsNull = true)
"""


# =============================================================================
# 10.3 Structs
# =============================================================================
flat_orders_df = spark.createDataFrame(
    [
        ('1001', 'S001', 'Toronto', 'ON'),
        ('1002', 'S002', 'Montreal', 'QC'),
    ],
    ['order_id', 'store_id', 'city', 'province'],
)
# flat_orders_df.show(truncate=False)
# =>
"""
+--------+--------+--------+--------+                                           
|order_id|store_id|city    |province|
+--------+--------+--------+--------+
|1001    |S001    |Toronto |ON      |
|1002    |S002    |Montreal|QC      |
+--------+--------+--------+--------+
"""

nested_df = flat_orders_df.withColumn(
    'store',
    F.struct(
        F.col('store_id'),
        F.col('city'),
        F.col('province'),
    ),
)
# nested_df.show(truncate=False)
# =>
"""
+--------+--------+--------+--------+--------------------+                      
|order_id|store_id|city    |province|store               |
+--------+--------+--------+--------+--------------------+
|1001    |S001    |Toronto |ON      |{S001, Toronto, ON} |
|1002    |S002    |Montreal|QC      |{S002, Montreal, QC}|
+--------+--------+--------+--------+--------------------+
"""
# nested_df.printSchema()
# =>
"""
root
 |-- order_id: string (nullable = true)
 |-- store_id: string (nullable = true)
 |-- city: string (nullable = true)
 |-- province: string (nullable = true)
 |-- store: struct (nullable = false)
 |    |-- store_id: string (nullable = true)
 |    |-- city: string (nullable = true)
 |    |-- province: string (nullable = true)
"""

store_view_df = nested_df.select(
    F.col('order_id'),
    F.col('store.store_id').alias('nested_store_id'),
    F.col('store.city').alias('store_city'),
    F.col('store.province').alias('store_province'),
)
# store_view_df.show(truncate=False)
# =>
"""
+--------+---------------+----------+--------------+                            
|order_id|nested_store_id|store_city|store_province|
+--------+---------------+----------+--------------+
|1001    |S001           |Toronto   |ON            |
|1002    |S002           |Montreal  |QC            |
+--------+---------------+----------+--------------+
"""
# store_view_df.printSchema()
# =>
"""
root
 |-- order_id: string (nullable = true)
 |-- nested_store_id: string (nullable = true)
 |-- store_city: string (nullable = true)
 |-- store_province: string (nullable = true)
"""


# =============================================================================
# 10.4 Nested JSON schemas
# =============================================================================
nested_orders_path = Path(
    'phases\\'
    'phase_01_dataframe_fundamentals\\'
    'exercises\\'
    'nested_orders.json'
)
nested_orders_path.write_text(
    (
        '{"order_id":"1001",'
        '"customer":{"customer_id":"C001","province":"ON"},'
        '"items":['
        '{"sku":"SKU-001","quantity":2},'
        '{"sku":"SKU-002","quantity":1}'
        ']}\n'
        '{"order_id":"1002",'
        '"customer":{"customer_id":"C002","province":"QC"},'
        '"items":['
        '{"sku":"SKU-003","quantity":4}'
        ']}\n'
    ),
    encoding='utf-8',
)
"""
nested_orders.json
------------------
{"order_id":"1001","customer":{"customer_id":"C001","province":"ON"},"items":[{"sku":"SKU-001","quantity":2},{"sku":"SKU-002","quantity":1}]}
{"order_id":"1002","customer":{"customer_id":"C002","province":"QC"},"items":[{"sku":"SKU-003","quantity":4}]}

"""
customer_schema = StructType([
    StructField('customer_id', StringType(), True),
    StructField('province',    StringType(), True),
])
item_schema = StructType([
    StructField('sku',      StringType(),  True),
    StructField('quantity', IntegerType(), True),
])
order_schema = StructType([
    StructField('order_id', StringType(), False),
    StructField(
        'customer',
        customer_schema,
        True,
    ),
    StructField(
        'items',
        ArrayType(
            item_schema,
            containsNull=False,
        ),
        True,
    ),
])

orders_df = (
    spark.read
    .schema(order_schema)
    .json(str(nested_orders_path))
)
# orders_df.show(truncate=False)
# =>
"""
+--------+----------+----------------------------+
|order_id|customer  |items                       |
+--------+----------+----------------------------+
|1001    |{C001, ON}|[{SKU-001, 2}, {SKU-002, 1}]|
|1002    |{C002, QC}|[{SKU-003, 4}]              |
+--------+----------+----------------------------+
"""
# orders_df.printSchema()
# =>
"""
root
 |-- order_id: string (nullable = true)
 |-- customer: struct (nullable = true)
 |    |-- customer_id: string (nullable = true)
 |    |-- province: string (nullable = true)
 |-- items: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- sku: string (nullable = true)
 |    |    |-- quantity: integer (nullable = true)
"""


# =============================================================================
# 10.5 `explode()`
# =============================================================================
items_df = (
    orders_df
    .withColumn(
        'item',
        F.explode(F.col('items')),
    )
    .select(
        F.col('order_id'),
        F.col('item.sku').alias('sku'),
        F.col('item.quantity').alias('quantity'),
    )
)
# items_df.show(truncate=False)
# =>
"""
+--------+-------+--------+
|order_id|sku    |quantity|
+--------+-------+--------+
|1001    |SKU-001|2       |
|1001    |SKU-002|1       |
|1002    |SKU-003|4       |
+--------+-------+--------+
"""
# items_df.printSchema()
# =>
"""
root
 |-- order_id: string (nullable = true)
 |-- sku: string (nullable = true)
 |-- quantity: integer (nullable = true)
"""


# =============================================================================
# 10.6 `from_json()`
# =============================================================================
raw_json_df = spark.createDataFrame(
    [
        (
            '1001',
            '{"customer_id":"C001","province":"ON"}',
        ),
        (
            '1002',
            '{"customer_id":"C002","province":"QC"}',
        ),
    ],
    ['order_id', 'customer_json'],
)
# raw_json_df.show(truncate=False)
# =>
"""
+--------+--------------------------------------+                               
|order_id|customer_json                         |
+--------+--------------------------------------+
|1001    |{"customer_id":"C001","province":"ON"}|
|1002    |{"customer_id":"C002","province":"QC"}|
+--------+--------------------------------------+
"""
# raw_json_df.printSchema()
# =>
"""
root
 |-- order_id: string (nullable = true)
 |-- customer_json: string (nullable = true)
"""

parsed_json_df = raw_json_df.withColumn(
    'customer',
    F.from_json(
        F.col('customer_json'),
        customer_schema,
    )
)
# parsed_json_df.show(truncate=False)
# =>
"""
+--------+--------------------------------------+----------+                    
|order_id|customer_json                         |customer  |
+--------+--------------------------------------+----------+
|1001    |{"customer_id":"C001","province":"ON"}|{C001, ON}|
|1002    |{"customer_id":"C002","province":"QC"}|{C002, QC}|
+--------+--------------------------------------+----------+
"""
# parsed_json_df.printSchema()
# =>
"""
root
 |-- order_id: string (nullable = true)
 |-- customer_json: string (nullable = true)
 |-- customer: struct (nullable = true)
 |    |-- customer_id: string (nullable = true)
 |    |-- province: string (nullable = true)
"""


spark.stop()
