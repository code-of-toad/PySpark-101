#!/usr/bin/env python
'''PySpark 101 — Phase 3 lecture.

Spark SQL, DataFrame equivalence, CTEs, joins, aggregations, windows,
validation, and reconciliation.

This is lecture-only code. It deliberately reuses one coherent retail domain
so that SQL syntax is always connected to grain, keys, and engineering
correctness. The Phase 3 mastery/capstone pipeline is intentionally omitted.
'''
from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
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


# =============================================================================
# 0. SPARK SESSION AND PHASE 3 MENTAL MODEL
# =============================================================================

spark = (
    SparkSession.builder
    .appName('phase_03_spark_sql')
    .getOrCreate()
)

# Phase 3 adds another interface, not another execution engine.
#
#     DataFrame API                         Spark SQL
#          |                                    |
#          +------------ Spark -----------------+
#                           |
#                     query planning
#                           |
#                  distributed execution
#
# The same engineering questions therefore apply to BOTH interfaces:
#
#     What is the input grain?
#     What is the target grain?
#     What keys define the relationship?
#     Can rows collapse, disappear, or multiply?
#     How will correctness be validated and reconciled?


# =============================================================================
# 1. COHERENT RETAIL SOURCE DATA
# =============================================================================

# -----------------------------------------------------------------------------
# 1.1 Orders
# -----------------------------------------------------------------------------

# GRAIN:
#     one row = one order
#
# BUSINESS KEY:
#     order_id
orders_schema = StructType([
    StructField('order_id', LongType(), nullable=False),
    StructField('store_id', StringType(), nullable=False),
    StructField('customer_id', StringType(), nullable=True),
    StructField('order_date', DateType(), nullable=False),
    StructField('order_ts', TimestampType(), nullable=False),
    StructField('order_status', StringType(), nullable=False),
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


# -----------------------------------------------------------------------------
# 1.2 Order items
# -----------------------------------------------------------------------------

# GRAIN:
#     one row = one line within one order
#
# COMPOSITE BUSINESS KEY:
#     (order_id, line_number)
order_items_schema = StructType([
    StructField('order_id', LongType(), nullable=False),
    StructField('line_number', IntegerType(), nullable=False),
    StructField('product_id', StringType(), nullable=False),
    StructField('quantity', IntegerType(), nullable=False),
    StructField('unit_price', DecimalType(12, 2), nullable=False),
    StructField('discount_pct', DecimalType(5, 4), nullable=False),
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


# -----------------------------------------------------------------------------
# 1.3 Products
# -----------------------------------------------------------------------------

# GRAIN:
#     one row = one product
#
# BUSINESS KEY:
#     product_id
products_schema = StructType([
    StructField('product_id', StringType(), nullable=False),
    StructField('product_name', StringType(), nullable=False),
    StructField('category', StringType(), nullable=False),
    StructField('unit_cost', DecimalType(12, 2), nullable=False),
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


# -----------------------------------------------------------------------------
# 1.4 Stores
# -----------------------------------------------------------------------------

# GRAIN:
#     one row = one store
#
# BUSINESS KEY:
#     store_id
stores_schema = StructType([
    StructField('store_id', StringType(), nullable=False),
    StructField('store_name', StringType(), nullable=False),
    StructField('province', StringType(), nullable=False),
])

stores_df = spark.createDataFrame(
    [
        ('S01', 'Toronto Central', 'ON'),
        ('S02', 'Mississauga West', 'ON'),
        ('S03', 'Vancouver Downtown', 'BC'),
    ],
    schema=stores_schema,
)


# -----------------------------------------------------------------------------
# 1.5 Inventory source observations
# -----------------------------------------------------------------------------

# SOURCE GRAIN:
#     one row = one observed inventory record
#
# TARGET DAILY SNAPSHOT GRAIN:
#     (snapshot_date, store_id, product_id)
#
# Two P001/S01 records intentionally share the same target key on 2026-01-05.
# The latest-record examples will select the survivor deterministically.
inventory_schema = StructType([
    StructField('snapshot_date', DateType(), nullable=False),
    StructField('snapshot_ts', TimestampType(), nullable=False),
    StructField('ingestion_id', LongType(), nullable=False),
    StructField('store_id', StringType(), nullable=False),
    StructField('product_id', StringType(), nullable=False),
    StructField('on_hand_quantity', IntegerType(), nullable=False),
    StructField('reorder_point', IntegerType(), nullable=False),
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


# -----------------------------------------------------------------------------
# 1.6 Store-product inventory targets
# -----------------------------------------------------------------------------

# GRAIN:
#     one row = one target for one store-product pair
#
# COMPOSITE BUSINESS KEY:
#     (store_id, product_id)
store_product_targets_schema = StructType([
    StructField('store_id', StringType(), nullable=False),
    StructField('product_id', StringType(), nullable=False),
    StructField('target_on_hand', IntegerType(), nullable=False),
])

store_product_targets_df = spark.createDataFrame(
    [
        ('S01', 'P001', 20),
        ('S01', 'P002', 8),
        ('S01', 'P004', 12),
        ('S02', 'P003', 6),
    ],
    schema=store_product_targets_schema,
)


# =============================================================================
# 2. TEMP VIEWS AND spark.sql()
# =============================================================================

# A temporary view is a SESSION-SCOPED SQL NAME for a relation.
#
# createOrReplaceTempView() does NOT copy all rows, write a table, or cache the
# DataFrame. It registers a name that SQL can resolve to the DataFrame's logical
# relation/query plan.
orders_df.createOrReplaceTempView('orders')
order_items_df.createOrReplaceTempView('order_items')
products_df.createOrReplaceTempView('products')
stores_df.createOrReplaceTempView('stores')
inventory_df.createOrReplaceTempView('inventory')
store_product_targets_df.createOrReplaceTempView('store_product_targets')

# spark.sql() returns a DataFrame for a SELECT query.
#
# The query remains lazy in the same sense as a DataFrame transformation chain:
# this statement defines a relation, but no Spark job scans all data yet.
completed_orders_sql_df = spark.sql(
    '''
    SELECT
        order_id,
        store_id,
        customer_id,
        order_date,
        order_ts,
        order_status
    FROM orders
    WHERE order_status = 'COMPLETED'
    '''
)

# Because spark.sql() returned a normal DataFrame, we can immediately continue
# with the DataFrame API. The two interfaces can be mixed deliberately.
completed_orders_mixed_df = completed_orders_sql_df.select(
    'order_id',
    'store_id',
    'order_date',
)

# An ACTION such as count(), show(), collect(), or write triggers execution.
# Keep this action small and explicit so the lazy model is visible.
completed_order_count = completed_orders_sql_df.count()

# A temporary view normally lives only for the Spark session that owns it.
# dropTempView() removes the SQL name; it does not delete the original Python
# DataFrame variable.
completed_orders_mixed_df.createOrReplaceTempView('completed_orders_mixed')
spark.catalog.dropTempView('completed_orders_mixed')


# =============================================================================
# 3. SQL / DATAFRAME EQUIVALENCE: CORE EXPRESSIONS
# =============================================================================

# -----------------------------------------------------------------------------
# 3.1 select() <-> SELECT
# -----------------------------------------------------------------------------

# DATAFRAME API:
# Projection changes carried columns but normally preserves row grain.
selected_df_api = order_items_df.select(
    'order_id',
    'line_number',
    'product_id',
    'quantity',
)

# SPARK SQL:
selected_df_sql = spark.sql(
    '''
    SELECT
        order_id,
        line_number,
        product_id,
        quantity
    FROM order_items
    '''
)


# -----------------------------------------------------------------------------
# 3.2 filter() / where() <-> WHERE
# -----------------------------------------------------------------------------

# DATAFRAME API:
filtered_df_api = orders_df.filter(
    F.col('order_status') == 'COMPLETED'
)

# SPARK SQL:
filtered_df_sql = spark.sql(
    '''
    SELECT *
    FROM orders
    WHERE order_status = 'COMPLETED'
    '''
)


# -----------------------------------------------------------------------------
# 3.3 Derived columns <-> SQL expressions
# -----------------------------------------------------------------------------

# INPUT GRAIN:
#     one row per order line
#
# OUTPUT GRAIN:
#     still one row per order line
#
# The expressions add measures but do not aggregate.
order_item_measures_api_df = (
    order_items_df
    .withColumn(
        'gross_sales',
        F.col('quantity') * F.col('unit_price'),
    )
    .withColumn(
        'discount_amount',
        F.col('gross_sales') * F.col('discount_pct'),
    )
    .withColumn(
        'net_sales',
        F.col('gross_sales') - F.col('discount_amount'),
    )
)

order_item_measures_sql_df = spark.sql(
    '''
    WITH gross AS (
        SELECT
            *,
            quantity * unit_price AS gross_sales
        FROM order_items
    ),
    discounted AS (
        SELECT
            *,
            gross_sales * discount_pct AS discount_amount
        FROM gross
    )
    SELECT
        *,
        gross_sales - discount_amount AS net_sales
    FROM discounted
    '''
)

# Register both implementations so later reconciliation can compare them.
order_item_measures_api_df.createOrReplaceTempView('order_item_measures_api')
order_item_measures_sql_df.createOrReplaceTempView('order_item_measures_sql')


# -----------------------------------------------------------------------------
# 3.4 when()/otherwise() <-> CASE WHEN
# -----------------------------------------------------------------------------

# DATAFRAME API:
inventory_status_api_df = inventory_df.withColumn(
    'inventory_status',
    # Condition order matters because zero also satisfies <= reorder_point.
    F.when(
        F.col('on_hand_quantity') == 0,
        F.lit('OUT_OF_STOCK'),
    )
    .when(
        F.col('on_hand_quantity') <= F.col('reorder_point'),
        F.lit('LOW_STOCK'),
    )
    .otherwise(F.lit('HEALTHY')),
)

# SPARK SQL:
inventory_status_sql_df = spark.sql(
    '''
    SELECT
        *,
        CASE
            WHEN on_hand_quantity = 0 THEN 'OUT_OF_STOCK'
            WHEN on_hand_quantity <= reorder_point THEN 'LOW_STOCK'
            ELSE 'HEALTHY'
        END AS inventory_status
    FROM inventory
    '''
)


# =============================================================================
# 4. JOINS: SAME GRAIN DISCIPLINE, DIFFERENT INTERFACE
# =============================================================================

# BEFORE EVERY JOIN, answer:
#
#     What is the grain of each input?
#     What are the actual join keys?
#     What is the cardinality?
#     Can this join multiply rows?


# -----------------------------------------------------------------------------
# 4.1 Inner join: orders -> order items
# -----------------------------------------------------------------------------

# LEFT GRAIN:
#     one row per order
#
# RIGHT GRAIN:
#     one row per order line
#
# CARDINALITY:
#     one-to-many
#
# TARGET GRAIN:
#     one row per order line
orders_with_items_api_df = orders_df.join(
    order_item_measures_api_df,
    on='order_id',
    how='inner',
)

orders_with_items_sql_df = spark.sql(
    '''
    SELECT
        o.order_id,
        o.store_id,
        o.customer_id,
        o.order_date,
        o.order_ts,
        o.order_status,
        i.line_number,
        i.product_id,
        i.quantity,
        i.unit_price,
        i.discount_pct,
        i.gross_sales,
        i.discount_amount,
        i.net_sales
    FROM orders o
    INNER JOIN order_item_measures_sql i
        ON o.order_id = i.order_id
    '''
)


# -----------------------------------------------------------------------------
# 4.2 Left join: order lines -> products
# -----------------------------------------------------------------------------

# This should preserve order-line grain only because products.product_id is
# expected to be unique. A duplicated right-side key could multiply rows.
items_with_products_api_df = order_item_measures_api_df.join(
    products_df,
    on='product_id',
    how='left',
)

items_with_products_sql_df = spark.sql(
    '''
    SELECT
        i.order_id,
        i.line_number,
        i.product_id,
        i.quantity,
        i.unit_price,
        i.discount_pct,
        i.gross_sales,
        i.discount_amount,
        i.net_sales,
        p.product_name,
        p.category,
        p.unit_cost
    FROM order_item_measures_sql i
    LEFT JOIN products p
        ON i.product_id = p.product_id
    '''
)


# -----------------------------------------------------------------------------
# 4.3 Aliases and qualified references
# -----------------------------------------------------------------------------

# SQL aliases make column ownership explicit: o.store_id vs s.store_id.
orders_with_store_sql_df = spark.sql(
    '''
    SELECT
        o.order_id,
        o.store_id,
        o.order_status,
        s.store_name,
        s.province
    FROM orders o
    LEFT JOIN stores s
        ON o.store_id = s.store_id
    '''
)

# The equivalent DataFrame pattern uses aliases plus qualified F.col() calls.
orders_with_store_api_df = (
    orders_df.alias('o')
    .join(
        stores_df.alias('s'),
        F.col('o.store_id') == F.col('s.store_id'),
        how='left',
    )
    .select(
        F.col('o.order_id').alias('order_id'),
        F.col('o.store_id').alias('store_id'),
        F.col('o.order_status').alias('order_status'),
        F.col('s.store_name').alias('store_name'),
        F.col('s.province').alias('province'),
    )
)


# -----------------------------------------------------------------------------
# 4.4 Semi join
# -----------------------------------------------------------------------------

# LEFT SEMI answers:
#     Which order-item rows HAVE a matching product?
#
# Only left-side columns survive.
valid_product_items_api_df = order_item_measures_api_df.join(
    products_df,
    on='product_id',
    how='left_semi',
)

valid_product_items_sql_df = spark.sql(
    '''
    SELECT i.*
    FROM order_item_measures_sql i
    LEFT SEMI JOIN products p
        ON i.product_id = p.product_id
    '''
)


# -----------------------------------------------------------------------------
# 4.5 Anti join
# -----------------------------------------------------------------------------

# LEFT ANTI answers:
#     Which order-item rows have NO matching product?
#
# This is one of the cleanest referential-integrity validation patterns.
invalid_product_items_api_df = order_item_measures_api_df.join(
    products_df,
    on='product_id',
    how='left_anti',
)

invalid_product_items_sql_df = spark.sql(
    '''
    SELECT i.*
    FROM order_item_measures_sql i
    LEFT ANTI JOIN products p
        ON i.product_id = p.product_id
    '''
)


# -----------------------------------------------------------------------------
# 4.6 Multi-column join
# -----------------------------------------------------------------------------

# TARGETS GRAIN:
#     one row per store-product
#
# The complete relationship key is therefore (store_id, product_id).
inventory_with_targets_api_df = inventory_df.join(
    store_product_targets_df,
    on=['store_id', 'product_id'],
    how='left',
)

inventory_with_targets_sql_df = spark.sql(
    '''
    SELECT
        i.snapshot_date,
        i.snapshot_ts,
        i.ingestion_id,
        i.store_id,
        i.product_id,
        i.on_hand_quantity,
        i.reorder_point,
        t.target_on_hand
    FROM inventory i
    LEFT JOIN store_product_targets t
        ON i.store_id = t.store_id
       AND i.product_id = t.product_id
    '''
)

# Joining only on product_id would ignore store_id, part of the target grain,
# and could create false cross-store matches.


# =============================================================================
# 5. AGGREGATIONS: groupBy().agg() <-> GROUP BY
# =============================================================================

# -----------------------------------------------------------------------------
# 5.1 Core aggregates
# -----------------------------------------------------------------------------

# INPUT GRAIN:
#     one row per order line
#
# GROUP KEY:
#     product_id
#
# OUTPUT GRAIN:
#     one row per product
product_summary_api_df = (
    order_item_measures_api_df
    .groupBy('product_id')
    .agg(
        F.count('*').alias('line_count'),
        F.sum('quantity').alias('units_sold'),
        F.sum('gross_sales').alias('gross_sales'),
        F.sum('net_sales').alias('net_sales'),
        F.avg('unit_price').alias('avg_unit_price'),
        F.min('unit_price').alias('min_unit_price'),
        F.max('unit_price').alias('max_unit_price'),
        F.countDistinct('order_id').alias('distinct_order_count'),
    )
)

product_summary_sql_df = spark.sql(
    '''
    SELECT
        product_id,
        COUNT(*) AS line_count,
        SUM(quantity) AS units_sold,
        SUM(gross_sales) AS gross_sales,
        SUM(net_sales) AS net_sales,
        AVG(unit_price) AS avg_unit_price,
        MIN(unit_price) AS min_unit_price,
        MAX(unit_price) AS max_unit_price,
        COUNT(DISTINCT order_id) AS distinct_order_count
    FROM order_item_measures_sql
    GROUP BY product_id
    '''
)


# -----------------------------------------------------------------------------
# 5.2 Conditional aggregation
# -----------------------------------------------------------------------------

product_conditional_api_df = (
    order_item_measures_api_df
    .groupBy('product_id')
    .agg(
        F.sum(
            F.when(
                F.col('discount_pct') > 0,
                F.col('net_sales'),
            ).otherwise(F.lit(0))
        ).alias('discounted_net_sales'),
        F.sum(
            F.when(
                F.col('discount_pct') > 0,
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias('discounted_line_count'),
    )
)

product_conditional_sql_df = spark.sql(
    '''
    SELECT
        product_id,
        SUM(
            CASE
                WHEN discount_pct > 0 THEN net_sales
                ELSE 0
            END
        ) AS discounted_net_sales,
        SUM(
            CASE
                WHEN discount_pct > 0 THEN 1
                ELSE 0
            END
        ) AS discounted_line_count
    FROM order_item_measures_sql
    GROUP BY product_id
    '''
)


# -----------------------------------------------------------------------------
# 5.3 WHERE vs HAVING
# -----------------------------------------------------------------------------

# WHERE filters INPUT rows before grouping.
# HAVING filters OUTPUT groups after aggregation.
completed_product_sales_sql_df = spark.sql(
    '''
    SELECT
        i.product_id,
        SUM(i.net_sales) AS net_sales
    FROM order_item_measures_sql i
    INNER JOIN orders o
        ON i.order_id = o.order_id
    WHERE o.order_status = 'COMPLETED'
    GROUP BY i.product_id
    HAVING SUM(i.net_sales) >= 40
    '''
)


# =============================================================================
# 6. CTES: NAMED RELATIONAL STEPS, NOT AUTOMATIC PERSISTENCE
# =============================================================================

# WITH gives intermediate relations meaningful names inside ONE SQL statement.
# It improves readability but does NOT automatically cache, write, or persist
# those intermediate results.
#
# This query makes grain transitions explicit:
#
#     measured_lines      -> order-line grain
#     completed_lines     -> order-line grain after filtering
#     product_sales       -> product grain after GROUP BY
#     product_sales_named -> product grain after many-to-one enrichment
product_sales_cte_df = spark.sql(
    '''
    WITH measured_lines AS (
        SELECT
            order_id,
            line_number,
            product_id,
            quantity,
            unit_price,
            discount_pct,
            quantity * unit_price AS gross_sales,
            quantity * unit_price * discount_pct AS discount_amount,
            quantity * unit_price
                - quantity * unit_price * discount_pct AS net_sales
        FROM order_items
    ),
    completed_lines AS (
        SELECT
            i.*
        FROM measured_lines i
        INNER JOIN orders o
            ON i.order_id = o.order_id
        WHERE o.order_status = 'COMPLETED'
    ),
    product_sales AS (
        SELECT
            product_id,
            SUM(quantity) AS units_sold,
            SUM(net_sales) AS net_sales
        FROM completed_lines
        GROUP BY product_id
    ),
    product_sales_named AS (
        SELECT
            s.product_id,
            p.product_name,
            p.category,
            s.units_sold,
            s.net_sales
        FROM product_sales s
        INNER JOIN products p
            ON s.product_id = p.product_id
    )
    SELECT *
    FROM product_sales_named
    '''
)


# =============================================================================
# 7. BUILD THE ENRICHED ORDER-LINE RELATION FOR WINDOW EXAMPLES
# =============================================================================

# Start from order-line measures because later analyses should keep this grain
# until an aggregation deliberately changes it.
sales_enriched_api_df = (
    order_item_measures_api_df.alias('i')
    .join(
        orders_df.alias('o'),
        on='order_id',
        how='inner',
    )
    .join(
        products_df.alias('p'),
        on='product_id',
        how='inner',
    )
    .join(
        stores_df.alias('s'),
        on='store_id',
        how='inner',
    )
    .select(
        'order_id',
        'line_number',
        'store_id',
        'store_name',
        'province',
        'customer_id',
        'order_date',
        'order_ts',
        'order_status',
        'product_id',
        'product_name',
        'category',
        'quantity',
        'unit_price',
        'unit_cost',
        'discount_pct',
        'gross_sales',
        'discount_amount',
        'net_sales',
    )
    .withColumn(
        'gross_margin',
        F.col('net_sales')
        - (F.col('quantity') * F.col('unit_cost')),
    )
)

sales_enriched_sql_df = spark.sql(
    '''
    SELECT
        o.order_id,
        i.line_number,
        o.store_id,
        s.store_name,
        s.province,
        o.customer_id,
        o.order_date,
        o.order_ts,
        o.order_status,
        i.product_id,
        p.product_name,
        p.category,
        i.quantity,
        i.unit_price,
        p.unit_cost,
        i.discount_pct,
        i.gross_sales,
        i.discount_amount,
        i.net_sales,
        i.net_sales - (i.quantity * p.unit_cost) AS gross_margin
    FROM order_item_measures_sql i
    INNER JOIN orders o
        ON i.order_id = o.order_id
    INNER JOIN products p
        ON i.product_id = p.product_id
    INNER JOIN stores s
        ON o.store_id = s.store_id
    '''
)

sales_enriched_api_df.createOrReplaceTempView('sales_enriched_api')
sales_enriched_sql_df.createOrReplaceTempView('sales_enriched_sql')


# =============================================================================
# 8. WINDOW FUNCTIONS
# =============================================================================

# Core distinction:
#
#     GROUP BY -> collapses rows
#     WINDOW   -> preserves rows


# -----------------------------------------------------------------------------
# 8.1 GROUP BY versus window aggregate
# -----------------------------------------------------------------------------

# GROUP BY changes grain to one row per store.
store_totals_sql_df = spark.sql(
    '''
    SELECT
        store_id,
        SUM(net_sales) AS store_net_sales
    FROM sales_enriched_sql
    GROUP BY store_id
    '''
)

# A window keeps every order line and attaches the store total to each row.
sales_with_store_total_sql_df = spark.sql(
    '''
    SELECT
        *,
        SUM(net_sales) OVER (
            PARTITION BY store_id
        ) AS store_net_sales
    FROM sales_enriched_sql
    '''
)


# -----------------------------------------------------------------------------
# 8.2 ROW_NUMBER, RANK, and DENSE_RANK
# -----------------------------------------------------------------------------

# First aggregate to PRODUCT grain because the business question is:
#     rank products by completed revenue within category.
product_revenue_api_df = (
    sales_enriched_api_df
    .filter(F.col('order_status') == 'COMPLETED')
    .groupBy(
        'category',
        'product_id',
        'product_name',
    )
    .agg(F.sum('net_sales').alias('net_sales'))
)

product_revenue_api_df.createOrReplaceTempView('product_revenue')

# RANK and DENSE_RANK intentionally order only by net_sales so equal sales stay
# tied. ROW_NUMBER adds product_id as a deterministic tie-breaker.
ranked_products_sql_df = spark.sql(
    '''
    SELECT
        *,
        RANK() OVER (
            PARTITION BY category
            ORDER BY net_sales DESC
        ) AS rank,
        DENSE_RANK() OVER (
            PARTITION BY category
            ORDER BY net_sales DESC
        ) AS dense_rank,
        ROW_NUMBER() OVER (
            PARTITION BY category
            ORDER BY net_sales DESC, product_id ASC
        ) AS row_number
    FROM product_revenue
    '''
)

# Direct DataFrame equivalent of the deterministic ROW_NUMBER specification.
product_order_window = (
    Window
    .partitionBy('category')
    .orderBy(
        F.col('net_sales').desc(),
        F.col('product_id').asc(),
    )
)

ranked_products_api_df = product_revenue_api_df.withColumn(
    'row_number',
    F.row_number().over(product_order_window),
)


# -----------------------------------------------------------------------------
# 8.3 LAG and LEAD
# -----------------------------------------------------------------------------

# Aggregate first because the business sequence is STORE-DAY sales.
daily_store_sales_sql_df = spark.sql(
    '''
    SELECT
        store_id,
        order_date,
        SUM(net_sales) AS daily_net_sales
    FROM sales_enriched_sql
    WHERE order_status = 'COMPLETED'
    GROUP BY store_id, order_date
    '''
)

daily_store_sales_sql_df.createOrReplaceTempView('daily_store_sales')

# LAG/LEAD refer to neighboring OBSERVED rows in the specified ordering.
daily_neighbors_sql_df = spark.sql(
    '''
    SELECT
        *,
        LAG(daily_net_sales) OVER (
            PARTITION BY store_id
            ORDER BY order_date
        ) AS previous_observed_sales,
        LEAD(daily_net_sales) OVER (
            PARTITION BY store_id
            ORDER BY order_date
        ) AS next_observed_sales
    FROM daily_store_sales
    '''
)

# Direct mapping of SQL PARTITION BY / ORDER BY into the PySpark Window API.
store_day_window = (
    Window
    .partitionBy('store_id')
    .orderBy('order_date')
)

daily_neighbors_api_df = (
    daily_store_sales_sql_df
    .withColumn(
        'previous_observed_sales',
        F.lag('daily_net_sales').over(store_day_window),
    )
    .withColumn(
        'next_observed_sales',
        F.lead('daily_net_sales').over(store_day_window),
    )
)


# -----------------------------------------------------------------------------
# 8.4 Running aggregate
# -----------------------------------------------------------------------------

running_sales_sql_df = spark.sql(
    '''
    SELECT
        *,
        SUM(daily_net_sales) OVER (
            PARTITION BY store_id
            ORDER BY order_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_net_sales
    FROM daily_store_sales
    '''
)


# -----------------------------------------------------------------------------
# 8.5 Rolling ROW frame
# -----------------------------------------------------------------------------

rolling_3_observation_sql_df = spark.sql(
    '''
    SELECT
        *,
        SUM(daily_net_sales) OVER (
            PARTITION BY store_id
            ORDER BY order_date
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS rolling_3_observation_sales
    FROM daily_store_sales
    '''
)

# This is three OBSERVED rows, not necessarily three calendar days.


# -----------------------------------------------------------------------------
# 8.6 Rolling RANGE frame for a real seven-calendar-day calculation
# -----------------------------------------------------------------------------

rolling_7_day_sql_df = spark.sql(
    '''
    WITH dated AS (
        SELECT
            *,
            DATEDIFF(order_date, DATE '1970-01-01') AS day_number
        FROM daily_store_sales
    )
    SELECT
        store_id,
        order_date,
        daily_net_sales,
        SUM(daily_net_sales) OVER (
            PARTITION BY store_id
            ORDER BY day_number
            RANGE BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS rolling_7_day_sales
    FROM dated
    '''
)


# -----------------------------------------------------------------------------
# 8.7 Deterministic latest-record selection
# -----------------------------------------------------------------------------

# TARGET GRAIN:
#     one row per snapshot_date, store_id, product_id
#
# RECENCY:
#     latest snapshot_ts
#
# TIE-BREAKER:
#     greatest ingestion_id
latest_inventory_sql_df = spark.sql(
    '''
    WITH ranked_inventory AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY snapshot_date, store_id, product_id
                ORDER BY snapshot_ts DESC, ingestion_id DESC
            ) AS row_number
        FROM inventory
    )
    SELECT
        snapshot_date,
        snapshot_ts,
        ingestion_id,
        store_id,
        product_id,
        on_hand_quantity,
        reorder_point
    FROM ranked_inventory
    WHERE row_number = 1
    '''
)

# Direct DataFrame equivalent of the same survivor rule.
latest_inventory_window = (
    Window
    .partitionBy(
        'snapshot_date',
        'store_id',
        'product_id',
    )
    .orderBy(
        F.col('snapshot_ts').desc(),
        F.col('ingestion_id').desc(),
    )
)

latest_inventory_api_df = (
    inventory_df
    .withColumn(
        '_row_number',
        F.row_number().over(latest_inventory_window),
    )
    .filter(F.col('_row_number') == 1)
    .drop('_row_number')
)

latest_inventory_sql_df.createOrReplaceTempView('latest_inventory_sql')
latest_inventory_api_df.createOrReplaceTempView('latest_inventory_api')


# =============================================================================
# 9. SQL AS A DATA-ENGINEERING VALIDATION LANGUAGE
# =============================================================================

# Good validation queries expose VIOLATIONS. For uniqueness, NULL, and orphan
# checks, the expected healthy result is often an empty DataFrame.


# -----------------------------------------------------------------------------
# 9.1 Row counts
# -----------------------------------------------------------------------------

orders_count_sql_df = spark.sql(
    '''
    SELECT COUNT(*) AS row_count
    FROM orders
    '''
)

order_items_count_sql_df = spark.sql(
    '''
    SELECT COUNT(*) AS row_count
    FROM order_items
    '''
)


# -----------------------------------------------------------------------------
# 9.2 Primary-key duplicate detection
# -----------------------------------------------------------------------------

# Expected healthy result: zero rows.
duplicate_orders_sql_df = spark.sql(
    '''
    SELECT
        order_id,
        COUNT(*) AS row_count
    FROM orders
    GROUP BY order_id
    HAVING COUNT(*) > 1
    '''
)


# -----------------------------------------------------------------------------
# 9.3 Composite-key duplicate detection
# -----------------------------------------------------------------------------

# Expected healthy result: zero rows because order_items is one row per
# (order_id, line_number).
duplicate_order_lines_sql_df = spark.sql(
    '''
    SELECT
        order_id,
        line_number,
        COUNT(*) AS row_count
    FROM order_items
    GROUP BY order_id, line_number
    HAVING COUNT(*) > 1
    '''
)

# Validate the latest inventory target grain as well.
duplicate_latest_inventory_sql_df = spark.sql(
    '''
    SELECT
        snapshot_date,
        store_id,
        product_id,
        COUNT(*) AS row_count
    FROM latest_inventory_sql
    GROUP BY snapshot_date, store_id, product_id
    HAVING COUNT(*) > 1
    '''
)


# -----------------------------------------------------------------------------
# 9.4 Required-field / NULL validation
# -----------------------------------------------------------------------------

# Expected healthy result: zero rows.
required_nulls_sql_df = spark.sql(
    '''
    SELECT *
    FROM sales_enriched_sql
    WHERE order_id IS NULL
       OR line_number IS NULL
       OR store_id IS NULL
       OR product_id IS NULL
       OR net_sales IS NULL
    '''
)


# -----------------------------------------------------------------------------
# 9.5 Referential-integrity / orphan validation
# -----------------------------------------------------------------------------

# LEFT ANTI directly asks which child rows lack a parent match.
# Expected healthy result: zero rows.
orphan_products_sql_df = spark.sql(
    '''
    SELECT i.*
    FROM order_items i
    LEFT ANTI JOIN products p
        ON i.product_id = p.product_id
    '''
)

# Equivalent DataFrame expression.
orphan_products_api_df = order_items_df.join(
    products_df.select('product_id'),
    on='product_id',
    how='left_anti',
)


# -----------------------------------------------------------------------------
# 9.6 Reusable DataFrame duplicate-key helper
# -----------------------------------------------------------------------------


def find_duplicate_keys(df: DataFrame, keys: list[str]) -> DataFrame:
    # WHAT: return key groups appearing more than once.
    # WHY: a non-empty result disproves the claimed uniqueness contract.
    return (
        df
        .groupBy(*keys)
        .agg(F.count('*').alias('row_count'))
        .filter(F.col('row_count') > 1)
    )


# The DataFrame helper expresses the same validation as GROUP BY ... HAVING.
duplicate_order_lines_api_df = find_duplicate_keys(
    order_items_df,
    ['order_id', 'line_number'],
)


# =============================================================================
# 10. RECONCILIATION: PROVE TWO REPRESENTATIONS AGREE
# =============================================================================

# Validation asks whether ONE dataset satisfies its own contract.
# Reconciliation asks whether TWO representations agree according to the
# transformation contract.


# -----------------------------------------------------------------------------
# 10.1 Row-count reconciliation for grain-preserving enrichment
# -----------------------------------------------------------------------------

# The enriched sales relation should keep one row per order line because each
# order line joins to exactly one order, one product, and one store when parent
# keys are valid and unique.
row_count_reconciliation_sql_df = spark.sql(
    '''
    SELECT
        (SELECT COUNT(*) FROM order_items) AS source_line_count,
        (SELECT COUNT(*) FROM sales_enriched_sql) AS enriched_line_count
    '''
)

# IMPORTANT:
# Row counts should NOT be expected to reconcile when the transformation
# intentionally changes grain, such as order-line -> store-day aggregation.


# -----------------------------------------------------------------------------
# 10.2 Measure reconciliation
# -----------------------------------------------------------------------------

# Create a SMALL lecture fact projection at completed order-line grain.
# This is not the Phase 3 mastery pipeline; it exists only to demonstrate the
# source-measure -> analytical-fact reconciliation pattern.
fact_sales_example_sql_df = spark.sql(
    '''
    SELECT
        order_id,
        line_number,
        store_id,
        product_id,
        order_date,
        quantity,
        net_sales,
        gross_margin
    FROM sales_enriched_sql
    WHERE order_status = 'COMPLETED'
    '''
)

fact_sales_example_sql_df.createOrReplaceTempView('fact_sales_example')

# Compare completed-line source revenue against the fact-table projection.
# The same eligibility predicate appears on both sides so the business scope is
# aligned before totals are compared.
measure_reconciliation_sql_df = spark.sql(
    '''
    SELECT
        source.completed_net_sales AS source_completed_net_sales,
        fact.completed_net_sales AS fact_completed_net_sales,
        source.completed_net_sales - fact.completed_net_sales AS difference
    FROM (
        SELECT
            SUM(i.net_sales) AS completed_net_sales
        FROM order_item_measures_sql i
        INNER JOIN orders o
            ON i.order_id = o.order_id
        WHERE o.order_status = 'COMPLETED'
    ) source
    CROSS JOIN (
        SELECT
            SUM(net_sales) AS completed_net_sales
        FROM fact_sales_example
    ) fact
    '''
)


# -----------------------------------------------------------------------------
# 10.3 Exact DataFrame-vs-SQL mismatch detection
# -----------------------------------------------------------------------------

# A sample from .show() is not proof of equality.
#
# exceptAll() is duplicate-sensitive. Compare BOTH directions because one
# direction only finds rows overrepresented or unique on that side.
measures_api_only_df = order_item_measures_api_df.exceptAll(
    order_item_measures_sql_df
)

measures_sql_only_df = order_item_measures_sql_df.exceptAll(
    order_item_measures_api_df
)

# The SQL equivalent uses EXCEPT ALL in both directions.
measures_api_only_sql_df = spark.sql(
    '''
    SELECT *
    FROM order_item_measures_api
    EXCEPT ALL
    SELECT *
    FROM order_item_measures_sql
    '''
)

measures_sql_only_sql_df = spark.sql(
    '''
    SELECT *
    FROM order_item_measures_sql
    EXCEPT ALL
    SELECT *
    FROM order_item_measures_api
    '''
)

# Exact row reconciliation is satisfied only when BOTH mismatch sets are empty.
# These actions intentionally convert the conceptual validation into evidence.
assert measures_api_only_df.count() == 0
assert measures_sql_only_df.count() == 0


# -----------------------------------------------------------------------------
# 10.4 Exact latest-inventory reconciliation
# -----------------------------------------------------------------------------

# The SQL and DataFrame implementations use the same partition key, recency
# ordering, and deterministic ingestion_id tie-breaker, so exact rows should
# reconcile after aligning column order.
latest_inventory_columns = [
    'snapshot_date',
    'snapshot_ts',
    'ingestion_id',
    'store_id',
    'product_id',
    'on_hand_quantity',
    'reorder_point',
]

latest_inventory_api_aligned_df = latest_inventory_api_df.select(
    *latest_inventory_columns
)

latest_inventory_sql_aligned_df = latest_inventory_sql_df.select(
    *latest_inventory_columns
)

latest_api_only_df = latest_inventory_api_aligned_df.exceptAll(
    latest_inventory_sql_aligned_df
)

latest_sql_only_df = latest_inventory_sql_aligned_df.exceptAll(
    latest_inventory_api_aligned_df
)

assert latest_api_only_df.count() == 0
assert latest_sql_only_df.count() == 0


# -----------------------------------------------------------------------------
# 10.5 Schema reconciliation
# -----------------------------------------------------------------------------

# Exact rows are only part of the contract. Downstream systems may also depend
# on column names, order, Spark types, and nullability expectations.
#
# Compare the StructType objects when the implementations are intended to be
# schema-identical.
assert order_item_measures_api_df.schema == order_item_measures_sql_df.schema


# =============================================================================
# 11. PHASE 3 TAKEAWAY
# =============================================================================

# Phase 3 is NOT complete merely because the syntax below looks familiar:
#
#     df.groupBy(...).agg(...)
#                  <->
#     SELECT ... GROUP BY ...
#
# The professional standard is stronger:
#
#     1. State input and output grain.
#     2. Validate keys before depending on join cardinality.
#     3. Choose SQL or DataFrame API based on clarity and maintainability.
#     4. Make CTE steps reflect meaningful relational stages.
#     5. Use GROUP BY when grain must collapse.
#     6. Use windows when calculations need related rows but row grain remains.
#     7. Make latest-record logic deterministic.
#     8. Use SQL to express executable data-quality checks.
#     9. Reconcile counts/measures only where the transformation contract says
#        they should agree.
#    10. Prove equivalent implementations with exact mismatch detection rather
#        than eyeballing samples.
#
# The Phase 3 mastery pipeline is intentionally NOT implemented in this lecture.
