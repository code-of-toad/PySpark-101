from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Create one shared SparkSession for the Phase 2 exercises.
spark = (
    SparkSession.builder
    .appName('phase02-practice')
    .master('local[*]')
    .getOrCreate()
)

# orders
# ------
# Define orders_schema and orders_df.
# GRAIN: one row PER order
orders_schema = StructType([
    StructField('order_id',     LongType(),      False),
    StructField('store_id',     StringType(),    False),
    StructField('customer_id',  StringType(),    True),
    StructField('order_date',   DateType(),      False),
    StructField('order_ts',     TimestampType(), False),
    StructField('order_status', StringType(),    False),
])
orders_df = spark.createDataFrame(
    [
        (
            1001,
            'S01',
            'C001',
            date(2026, 1, 3),
            datetime(2026, 1, 3, 9, 15),
            'COMPLETED',
        ),
        (
            1002,
            'S01',
            'C002',
            date(2026, 1, 5),
            datetime(2026, 1, 5, 14, 30),
            'COMPLETED',
        ),
        (
            1003,
            'S02',
            'C001',
            date(2026, 1, 5),
            datetime(2026, 1, 5, 17, 45),
            'CANCELLED',
        ),
        (
            1004,
            'S02',
            'C003',
            date(2026, 1, 12),
            datetime(2026, 1, 12, 11, 20),
            'COMPLETED',
        ),
        (
            1005,
            'S01',
            'C004',
            date(2026, 2, 2),
            datetime(2026, 2, 2, 10, 5),
            'COMPLETED',
        ),
    ],
    schema=orders_schema,
)

# order_items
# -----------
# Define order_items_schema and order_items_df.
# GRAIN: one row PER order line
order_items_schema = StructType([
    StructField('order_id',     LongType(),    False),
    StructField('line_number',  IntegerType(), False),
    StructField('product_id',   StringType(),  False),
    StructField('quantity',     IntegerType(), False),
    StructField('unit_price',   DecimalType(12, 2), False),
    StructField('discount_pct', DecimalType(5, 4),  False),
])
order_items_df = spark.createDataFrame(
    [
        (1001, 1, 'P001', 2, Decimal('12.00'), Decimal('0.0000')),
        (1001, 2, 'P002', 1, Decimal('30.00'), Decimal('0.1000')),
        (1002, 1, 'P001', 3, Decimal('12.00'), Decimal('0.0500')),
        (1002, 2, 'P003', 1, Decimal('50.00'), Decimal('0.0000')),
        (1003, 1, 'P002', 2, Decimal('30.00'), Decimal('0.0000')),
        (1004, 1, 'P003', 2, Decimal('50.00'), Decimal('0.1000')),
        (1004, 2, 'P004', 4, Decimal('8.00'), Decimal('0.0000')),
        (1005, 1, 'P001', 1, Decimal('12.00'), Decimal('0.0000')),
        (1005, 2, 'P004', 5, Decimal('8.00'), Decimal('0.0500')),
    ],
    schema=order_items_schema,
)

# products
# --------
# Define products_schema and products_df.
# GRAIN: one row PER product
products_schema = StructType([
    StructField('product_id',   StringType(), False),
    StructField('product_name', StringType(), False),
    StructField('category',     StringType(), False),
    StructField('unit_cost',    DecimalType(12, 2), False),
])
products_df = spark.createDataFrame(
    [
        ('P001', 'Coffee Beans', 'GROCERY', Decimal('7.00')),
        ('P002', 'Coffee Grinder', 'EQUIPMENT', Decimal('18.00')),
        ('P003', 'Kettle', 'EQUIPMENT', Decimal('32.00')),
        ('P004', 'Paper Filters', 'GROCERY', Decimal('3.00')),
    ],
    schema=products_schema,
)

# stores
# ------
# Define stores_schema and stores_df.
# GRAIN: one row PER store
stores_schema = StructType([
    StructField('store_id',   StringType(), False),
    StructField('store_name', StringType(), False),
    StructField('province',   StringType(), False),
])

stores_df = spark.createDataFrame(
    [
        ('S01', 'Toronto Central', 'ON'),
        ('S02', 'Mississauga West', 'ON'),
        ('S03', 'Vancouver Downtown', 'BC'),
    ],
    schema=stores_schema,
)

# inventory_snapshots
# -------------------
# Define inventory_schema and inventory_df.
# GRAIN: one row PER observed inventory record
#     -> Target Daily Grain: (snapshot_date, store_id, product_id)
inventory_schema = StructType([
    StructField('snapshot_date',    DateType(),      False),
    StructField('snapshot_ts',      TimestampType(), False),
    StructField('ingestion_id',     LongType(),      False),
    StructField('store_id',         StringType(),    False),
    StructField('product_id',       StringType(),    False),
    StructField('on_hand_quantity', IntegerType(),   False),
    StructField('reorder_point',    IntegerType(),   False),
])

inventory_df = spark.createDataFrame(
    [
        (
            date(2026, 1, 5),
            datetime(2026, 1, 5, 8, 0),
            1,
            'S01',
            'P001',
            15,
            10,
        ),
        (
            date(2026, 1, 5),
            datetime(2026, 1, 5, 18, 0),
            2,
            'S01',
            'P001',
            12,
            10,
        ),
        (
            date(2026, 1, 5),
            datetime(2026, 1, 5, 18, 0),
            3,
            'S01',
            'P002',
            4,
            5,
        ),
        (
            date(2026, 1, 5),
            datetime(2026, 1, 5, 18, 0),
            4,
            'S02',
            'P003',
            0,
            2,
        ),
        (
            date(2026, 1, 12),
            datetime(2026, 1, 12, 18, 0),
            5,
            'S02',
            'P003',
            5,
            2,
        ),
        (
            date(2026, 2, 2),
            datetime(2026, 2, 2, 18, 0),
            6,
            'S01',
            'P004',
            7,
            8,
        ),
    ],
    schema=inventory_schema,
) 

# =======
# SCHEMAS
# =======
"""
`orders_df`
root
 |-- order_id: long (nullable = false)
 |-- store_id: string (nullable = false)
 |-- customer_id: string (nullable = true)
 |-- order_date: date (nullable = false)
 |-- order_ts: timestamp (nullable = false)
 |-- order_status: string (nullable = false)

`order_items_df`
root
 |-- order_id: long (nullable = false)
 |-- line_number: integer (nullable = false)
 |-- product_id: string (nullable = false)
 |-- quantity: integer (nullable = false)
 |-- unit_price: decimal(12,2) (nullable = false)
 |-- discount_pct: decimal(5,4) (nullable = false)

`products_df`
root
 |-- product_id: string (nullable = false)
 |-- product_name: string (nullable = false)
 |-- category: string (nullable = false)
 |-- unit_cost: decimal(12,2) (nullable = false)

`stores_df`
root
 |-- store_id: string (nullable = false)
 |-- store_name: string (nullable = false)
 |-- province: string (nullable = false)

`inventory_df`
 root
 |-- snapshot_date: date (nullable = false)
 |-- snapshot_ts: timestamp (nullable = false)
 |-- ingestion_id: long (nullable = false)
 |-- store_id: string (nullable = false)
 |-- product_id: string (nullable = false)
 |-- on_hand_quantity: integer (nullable = false)
 |-- reorder_point: integer (nullable = false)
""" 

# ==========
# DataFrames
# ==========
"""
`orders_df`
+--------+--------+-----------+----------+-------------------+------------+     
|order_id|store_id|customer_id|order_date|order_ts           |order_status|
+--------+--------+-----------+----------+-------------------+------------+
|1001    |S01     |C001       |2026-01-03|2026-01-03 09:15:00|COMPLETED   |
|1002    |S01     |C002       |2026-01-05|2026-01-05 14:30:00|COMPLETED   |
|1003    |S02     |C001       |2026-01-05|2026-01-05 17:45:00|CANCELLED   |
|1004    |S02     |C003       |2026-01-12|2026-01-12 11:20:00|COMPLETED   |
|1005    |S01     |C004       |2026-02-02|2026-02-02 10:05:00|COMPLETED   |
+--------+--------+-----------+----------+-------------------+------------+

`order_items_df`
+--------+-----------+----------+--------+----------+------------+              
|order_id|line_number|product_id|quantity|unit_price|discount_pct|
+--------+-----------+----------+--------+----------+------------+
|1001    |1          |P001      |2       |12.00     |0.0000      |
|1001    |2          |P002      |1       |30.00     |0.1000      |
|1002    |1          |P001      |3       |12.00     |0.0500      |
|1002    |2          |P003      |1       |50.00     |0.0000      |
|1003    |1          |P002      |2       |30.00     |0.0000      |
|1004    |1          |P003      |2       |50.00     |0.1000      |
|1004    |2          |P004      |4       |8.00      |0.0000      |
|1005    |1          |P001      |1       |12.00     |0.0000      |
|1005    |2          |P004      |5       |8.00      |0.0500      |
+--------+-----------+----------+--------+----------+------------+

`products_df`
+----------+--------------+---------+---------+                                 
|product_id|product_name  |category |unit_cost|
+----------+--------------+---------+---------+
|P001      |Coffee Beans  |GROCERY  |7.00     |
|P002      |Coffee Grinder|EQUIPMENT|18.00    |
|P003      |Kettle        |EQUIPMENT|32.00    |
|P004      |Paper Filters |GROCERY  |3.00     |
+----------+--------------+---------+---------+

`stores_df`
+--------+------------------+--------+                                          
|store_id|store_name        |province|
+--------+------------------+--------+
|S01     |Toronto Central   |ON      |
|S02     |Mississauga West  |ON      |
|S03     |Vancouver Downtown|BC      |
+--------+------------------+--------+

`inventory_df`
+-------------+-------------------+------------+--------+----------+----------------+-------------+
|snapshot_date|snapshot_ts        |ingestion_id|store_id|product_id|on_hand_quantity|reorder_point|
+-------------+-------------------+------------+--------+----------+----------------+-------------+
|2026-01-05   |2026-01-05 08:00:00|1           |S01     |P001      |15              |10           |
|2026-01-05   |2026-01-05 18:00:00|2           |S01     |P001      |12              |10           |
|2026-01-05   |2026-01-05 18:00:00|3           |S01     |P002      |4               |5            |
|2026-01-05   |2026-01-05 18:00:00|4           |S02     |P003      |0               |2            |
|2026-01-12   |2026-01-12 18:00:00|5           |S02     |P003      |5               |2            |
|2026-02-02   |2026-02-02 18:00:00|6           |S01     |P004      |7               |8            |
+-------------+-------------------+------------+--------+----------+----------------+-------------+
"""
