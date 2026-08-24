from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructType,
    StructField,
)

from practice_data import (
    spark,
    orders_df,
    products_df,
    stores_df,
)
from phase_02_section_09 import (
    latest_inventory_df,
)

"""
Phase 2, Section 13: Dimensional Modeling
-----------------------------------------
Connect Phase 2 transformations to:

1.  grain;
2.  facts;
3.  dimensions;
4.  transaction facts;
5.  snapshot facts;
6.  business keys;
7.  surrogate keys;
8.  star schemas;
9.  conformed dimensions;
10. slowly changing dimensions;
11. derived measures.

QUESTIONS
=========
Q: Why is `fact_sales` a transaction fact?
Q: Why is `fact_inventory_snapshot` a snapshot fact?
Q: What makes `dim_products` conformed?
Q: Why should surrogate keys not be generated with an arbitrary fresh row
   number on every run?
Q: What must be true about each dimension join if `fact_sales` is to remain
   at order-line grain?
"""


# =============================================================================
# 13.1 Intended analytical model
# ------------------------------
#   Raw Orders
#   Raw Order Items
#   Raw Products
#   Raw Stores
#   Inventory Snapshots
#        ↓
#     PySpark
#        ↓
#   dim_product
#   dim_store
#   dim_date
#   fact_sales
#   fact_inventory_snapshot
#
#
# dim_product: one row PER current product
#
# dim_store: one row PER current store
#
# dim_date: one row PER calendar date
#
# fact_sales: one row PER order line
#
# fact_inventory_snapshot: one row PER date PER store PER product
# =============================================================================


# =============================================================================
# 13.2 Business keys vs. Surrogate keys
# -------------------------------------
#
# Business key:
# -------------
# identifier from the source/business domain
#
# Examples:
# ---------
# product_id
# store_id
# order_id
# -----------------------------------------------------------------------------
#
# Surrogate key:
# --------------
# warehouse-managed identifier for a dimension row
#
# Examples:
# ---------
# product_key
# store_key
# date_key
# =============================================================================
# Fixed mappings make the key assignment stable for the experiment.
#
# A production warehouse needs a persistent surrogate-key strategy.
#
# For practice, use the following fixed surrogate-key maps.
product_key_map_df = spark.createDataFrame(
    [
        (101, 'P001'),
        (102, 'P002'),
        (103, 'P003'),
        (104, 'P004'),
    ],
    StructType([
        StructField('product_key', IntegerType(), False),
        StructField('product_id', StringType(), False),
    ]),
)
# product_key_map_df.show(truncate=False)
# =>
"""
`product_key_map_df`
+-----------+----------+                                                        
|product_key|product_id|
+-----------+----------+
|101        |P001      |
|102        |P002      |
|103        |P003      |
|104        |P004      |
+-----------+----------+
"""
store_key_map_df = spark.createDataFrame(
    [
        (201, 'S01'),
        (202, 'S02'),
        (203, 'S03'),
    ],
    StructType([
        StructField('store_key', IntegerType(), False),
        StructField('store_id', StringType(), False),
    ]),
)
# store_key_map_df.show(truncate=False)
# =>
"""
`store_key_map_df`
+---------+--------+                                                            
|store_key|store_id|
+---------+--------+
|201      |S01     |
|202      |S02     |
|203      |S03     |
+---------+--------+
"""


# =============================================================================
# 13.3 Build dimensions
# =============================================================================
# INPUT GRAIN: one row PER product
# OUTPUT GRAIN: one row PER current product
"""
INPUT: `product_key_map_df`
+-----------+----------+                                                        
|product_key|product_id|
+-----------+----------+
|101        |P001      |
|102        |P002      |
|103        |P003      |
|104        |P004      |
+-----------+----------+
"""
dim_product_df = (
    product_key_map_df
    .join(
        products_df,
        on='product_id',
        how='inner',
    )
)
# dim_product_df.show(truncate=False)
# =>
"""
OUTPUT: `dim_product_df`
+----------+-----------+--------------+---------+---------+                     
|product_id|product_key|product_name  |category |unit_cost|
+----------+-----------+--------------+---------+---------+
|P001      |101        |Coffee Beans  |GROCERY  |7.00     |
|P002      |102        |Coffee Grinder|EQUIPMENT|18.00    |
|P003      |103        |Kettle        |EQUIPMENT|32.00    |
|P004      |104        |Paper Filters |GROCERY  |3.00     |
+----------+-----------+--------------+---------+---------+
"""


# INPUT GRAIN: one row PER store
# OUTPUT GRAIN: one row PER current store
"""
INPUT: `store_key_map_df`
+---------+--------+                                                            
|store_key|store_id|
+---------+--------+
|201      |S01     |
|202      |S02     |
|203      |S03     |
+---------+--------+
"""
dim_store_df = (
    store_key_map_df
    .join(
        stores_df,
        on='store_id',
        how='inner',
    )
)
# dim_store_df.show(truncate=False)
# =>
"""
OUTPUT: `dim_store_df`
+--------+---------+------------------+--------+                                
|store_id|store_key|store_name        |province|
+--------+---------+------------------+--------+
|S01     |201      |Toronto Central   |ON      |
|S02     |202      |Mississauga West  |ON      |
|S03     |203      |Vancouver Downtown|BC      |
+--------+---------+------------------+--------+
"""


# Build a conformed date dimension.
sales_dates_df = orders_df.select(
    F.col('order_date').alias('full_date')
)
# sales_dates_df.show(truncate=False)
# =>
"""
`sales_dates_df`
+----------+                                                                    
|full_date |
+----------+
|2026-01-03|
|2026-01-05|
|2026-01-05|
|2026-01-12|
|2026-02-02|
+----------+
"""
inventory_dates_df = latest_inventory_df.select(
    F.col('snapshot_date').alias('full_date')
)
# inventory_dates_df.show(truncate=False)
# =>
"""
`inventory_dates_df`
+----------+                                                                    
|full_date |
+----------+
|2026-01-05|
|2026-01-05|
|2026-01-05|
|2026-01-12|
|2026-02-02|
+----------+
"""
# UNION because both inputs represent the same conceptual row type:
# one calendar date.
dim_date_df = (
    sales_dates_df
    .unionByName(inventory_dates_df)
    .distinct()
    .withColumn(
        'date_key',
        F.date_format(
            F.col('full_date'),
            'yyyyMMdd',
        ).cast('int'),
    )
    .withColumn(
        'year',
        F.year('full_date'),
    )
    .withColumn(
        'month',
        F.month('full_date'),
    )
    .withColumn(
        'day',
        F.dayofmonth('full_date'),
    )
)
# dim_date_df.show(truncate=False)
# =>
"""
`dim_date_df`
+----------+--------+----+-----+---+                                            
|full_date |date_key|year|month|day|
+----------+--------+----+-----+---+
|2026-01-03|20260103|2026|1    |3  |
|2026-01-05|20260105|2026|1    |5  |
|2026-01-12|20260112|2026|1    |12 |
|2026-02-02|20260202|2026|2    |2  |
+----------+--------+----+-----+---+
"""


# =============================================================================
# 13.4 Transaction fact concept
# -----------------------------
# fact_sales: one row PER order line
#
# Typical measures:
# -----------------
#   quantity
#   unit_price
#   gross_sales
#   discount_amount
#   net_sales
#   gross_margin
#
# Each dimension join should be many-to-one from the fact grain.
# =============================================================================


# =============================================================================
# 13.5  Snapshot fact concept
# ---------------------------
# fact_inventory_snapshot: one row PER date PER store PER product
#
# Typical measures:
# -----------------
#   on_hand_quantity
#   reorder_point
#
# The source inventory events must first be reduced deterministically to the
# target snapshot grain.
# =============================================================================


# =============================================================================
# 13.6 Conformed dimensions
# -------------------------
# `dim_product`, `dim_store`, and `dim_date` are conformed when both facts
# use the same definitions and keys for those dimensions.
#
# That makes analyses across business processes consistent:
#
# sales by product category
# inventory by product category
#
# sales by store
# inventory by store
#
# sales by date
# inventory by date
# =============================================================================


# =============================================================================
# 13.7 Slowly changing dimensions
# -------------------------------
# Type 1: Overwrite the old dimension attributes.
# Type 2: Insert a new historical version and preserve the old version.
#
# E.g., product P001 changes category.
#
# Type 1 keeps only the current category.
# Type 2 preserves both historical versions so that old fact rows can remain
#        associated with the category that was valid at the time.
#
# No full SCD implementation is required in this phase.
# =============================================================================
