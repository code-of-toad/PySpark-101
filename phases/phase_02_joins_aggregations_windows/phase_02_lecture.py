#!/usr/bin/env python
'''PySpark 101 — Phase 2 lecture.

Joins, aggregations, windows, transformation patterns, and dimensional modeling.

This is lecture-only code. It intentionally keeps the examples in one coherent
retail domain so that each API can be understood in terms of grain, keys, and
data-engineering correctness.
'''
from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
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
# 0. SPARK SESSION AND PHASE 2 MENTAL MODEL
# =============================================================================

spark = (
    SparkSession.builder
    .appName('phase_02_joins_aggregations_windows')
    .getOrCreate()
)

# Phase 2 starts with one question:
#
#     What does one row represent?
#
# That definition is the DATA GRAIN.
#
# Most transformations can then be reasoned about by their effect on grain:
#
#     groupBy + agg  -> collapses rows to one row per group
#     window         -> calculates across related rows but preserves input rows
#     many-to-one join -> should preserve left grain when the right key is unique
#     one-to-many join -> expands the left row into multiple child rows
#     explode        -> expands one parent row into one row per array/map element
#     union          -> stacks rows representing the same conceptual row type
#
# Before every join, explicitly answer:
#
#     What is the grain of the left DataFrame?
#     What is the grain of the right DataFrame?
#     Can this join multiply rows?


# =============================================================================
# 1. COHERENT RETAIL SOURCE DATA
# =============================================================================

# -----------------------------------------------------------------------------
# 1.1 Raw orders
# -----------------------------------------------------------------------------

# GRAIN:
#     one row = one order
#
# BUSINESS KEY:
#     order_id
#
# store_id is a foreign key to the store entity.
# customer_id is retained as a transaction attribute for this phase.
orders_schema = StructType([
    StructField('order_id',     LongType(),      nullable=False),
    StructField('store_id',     StringType(),    nullable=False),
    StructField('customer_id',  StringType(),    nullable=True),
    StructField('order_date',   DateType(),      nullable=False),
    StructField('order_ts',     TimestampType(), nullable=False),
    StructField('order_status', StringType(),    nullable=False),
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
# 1.2 Raw order items
# -----------------------------------------------------------------------------

# GRAIN:
#     one row = one line within one order
#
# COMPOSITE BUSINESS KEY:
#     (order_id, line_number)
#
# This is finer grain than orders_df. One order can therefore have many lines.
order_items_schema = StructType([
    StructField('order_id',     LongType(),         nullable=False),
    StructField('line_number',  IntegerType(),      nullable=False),
    StructField('product_id',   StringType(),       nullable=False),
    StructField('quantity',     IntegerType(),      nullable=False),
    StructField('unit_price',   DecimalType(12, 2), nullable=False),
    StructField('discount_pct', DecimalType(5, 4),  nullable=False),
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
# 1.3 Raw products
# -----------------------------------------------------------------------------

# GRAIN:
#     one row = one product
#
# BUSINESS KEY:
#     product_id
#
# tags is included so that explode can later demonstrate a deliberate grain
# change from product grain to product-tag grain.
products_schema = StructType([
    StructField('product_id',   StringType(),       nullable=False),
    StructField('product_name', StringType(),       nullable=False),
    StructField('category',     StringType(),       nullable=False),
    StructField('unit_cost',    DecimalType(12, 2), nullable=False),
    StructField(
        'tags',
        ArrayType(
            StringType(),
            containsNull=False,
        ),
        nullable=True,
    ),
])

products_df = spark.createDataFrame(
    [
        (
            'P001',
            'Coffee Beans',
            'GROCERY',
            Decimal('7.00'),
            ['grocery', 'beverage'],
        ),
        (
            'P002',
            'Coffee Grinder',
            'EQUIPMENT',
            Decimal('18.00'),
            ['equipment', 'coffee'],
        ),
        (
            'P003',
            'Kettle',
            'EQUIPMENT',
            Decimal('32.00'),
            ['equipment', 'kitchen'],
        ),
        (
            'P004',
            'Paper Filters',
            'GROCERY',
            Decimal('3.00'),
            ['grocery', 'coffee'],
        ),
    ],
    schema=products_schema,
)


# -----------------------------------------------------------------------------
# 1.4 Raw stores
# -----------------------------------------------------------------------------

# GRAIN:
#     one row = one store
#
# BUSINESS KEY:
#     store_id
stores_schema = StructType([
    StructField('store_id',   StringType(), nullable=False),
    StructField('store_name', StringType(), nullable=False),
    StructField('province',   StringType(), nullable=False),
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
# 1.5 Inventory source events
# -----------------------------------------------------------------------------

# SOURCE GRAIN:
#     one row = one observed inventory record
#
# TARGET DAILY SNAPSHOT GRAIN:
#     (snapshot_date, store_id, product_id)
#
# Two P001/S01 records intentionally exist for 2026-01-05. They represent
# multiple arrivals for the same target grain. Later, a window chooses the
# latest record deterministically.
inventory_schema = StructType([
    StructField('snapshot_date',    DateType(),      nullable=False),
    StructField('snapshot_ts',      TimestampType(), nullable=False),
    StructField('ingestion_id',     LongType(),      nullable=False),
    StructField('store_id',         StringType(),    nullable=False),
    StructField('product_id',       StringType(),    nullable=False),
    StructField('on_hand_quantity', IntegerType(),   nullable=False),
    StructField('reorder_point',    IntegerType(),   nullable=False),
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
# 1.6 Two helper patterns for key validation
# -----------------------------------------------------------------------------

def find_duplicate_keys(df: DataFrame, keys: list[str]) -> DataFrame:
    # A uniqueness check groups by the claimed key and exposes key groups that
    # contain more than one row. A non-empty result disproves uniqueness.
    return (
        df
        .groupBy(*keys)
        .agg(F.count('*').alias('row_count'))
        .filter(F.col('row_count') > 1)
    )


def find_orphans(
    child_df: DataFrame,
    parent_df: DataFrame,
    keys: list[str],
) -> DataFrame:
    # LEFT ANTI keeps child rows with no parent match.
    #
    # This directly models referential-integrity validation:
    # every non-null foreign-key value should resolve to a valid parent.
    return child_df.join(
        parent_df.select(*keys).dropDuplicates(keys),
        on=keys,
        how='left_anti',
    )


# Validate keys BEFORE depending on them in joins.
#
# These result DataFrames should be empty for the intended contracts.
duplicate_orders_df = find_duplicate_key(orders_df, ['order_id'])

duplicate_order_lines_df = find_duplicate_keys(
    order_items_df,
    ['order_id', 'line_number'],
)

duplicate_products_df = find_duplicate_keys(
    products_df,
    ['product_id'],
)

duplicate_stores_df = find_duplicate_keys(
    stores_df,
    ['store_id'],
)

orphan_order_products_df = find_orphans(
    order_items_df,
    products_df,
    ['product_id'],
)


# =============================================================================
# 2. AGGREGATIONS
# =============================================================================

# -----------------------------------------------------------------------------
# 2.1 Create line-level measures without changing grain
# -----------------------------------------------------------------------------

# INPUT GRAIN:
#     one row per order line
#
# OUTPUT GRAIN:
#     still one row per order line
#
# withColumn adds expressions; it does not aggregate or collapse rows.
order_item_measures_df = (
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


# -----------------------------------------------------------------------------
# 2.2 groupBy + agg
# -----------------------------------------------------------------------------

# INPUT GRAIN:
#     one row per order line
#
# GROUP KEY:
#     product_id
#
# OUTPUT GRAIN:
#     one row per product
#
# Every non-grouped output value must be reduced by an aggregate function.
product_sales_summary_df = (
    order_item_measures_df
    .groupBy('product_id')
    .agg(
        # count('*') counts rows, including rows containing NULL elsewhere.
        F.count('*').alias('line_count'),

        # count('column') counts only non-NULL values in that column.
        F.count('discount_pct').alias('non_null_discount_count'),

        F.sum('quantity').alias('units_sold'),
        F.sum('gross_sales').alias('gross_sales'),
        F.sum('net_sales').alias('net_sales'),
        F.avg('unit_price').alias('avg_unit_price'),
        F.min('unit_price').alias('min_unit_price'),
        F.max('unit_price').alias('max_unit_price'),

        # countDistinct answers how many distinct orders contained the product.
        F.countDistinct('order_id').alias('distinct_order_count'),
    )
)


# -----------------------------------------------------------------------------
# 2.3 Conditional aggregation
# -----------------------------------------------------------------------------

# Conditional aggregation lets one grouped pass calculate metrics for subsets
# of rows. The WHEN expression defines each row's contribution; SUM reduces
# those contributions within the product group.
product_conditional_metrics_df = (
    order_item_measures_df
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

        F.sum(
            F.when(
                F.col('quantity') >= 3,
                F.col('quantity'),
            ).otherwise(F.lit(0))
        ).alias('units_from_large_lines'),
    )
)


# -----------------------------------------------------------------------------
# 2.4 Aggregation grain is defined by every grouped column
# -----------------------------------------------------------------------------

# OUTPUT GRAIN:
#     one row per order AND product
#
# This is deliberately finer than product_sales_summary_df.
order_product_summary_df = (
    order_item_measures_df
    .groupBy(
        'order_id',
        'product_id',
    )
    .agg(
        F.sum('quantity').alias('quantity'),
        F.sum('net_sales').alias('net_sales'),
    )
)


# =============================================================================
# 3. JOINS
# =============================================================================

# -----------------------------------------------------------------------------
# 3.1 One-to-many join: orders -> order items
# -----------------------------------------------------------------------------

# LEFT GRAIN:
#     one row per order
#
# RIGHT GRAIN:
#     one row per order line
#
# RELATIONSHIP:
#     one order -> many order lines
#
# TARGET GRAIN:
#     one row per order line
#
# Row expansion is correct here because the target intentionally becomes the
# finer order-line grain.
orders_with_items_df = orders_df.join(
    order_item_measures_df,
    on='order_id',
    how='inner',
)


# -----------------------------------------------------------------------------
# 3.2 Many-to-one join: order lines -> products
# -----------------------------------------------------------------------------

# LEFT GRAIN:
#     one row per order line
#
# RIGHT GRAIN:
#     one row per product
#
# products_df.product_id was validated as unique, so each left row can match
# at most one product. This should preserve order-line grain.
items_with_products_df = order_item_measures_df.join(
    products_df,
    on='product_id',
    how='left',
)


# -----------------------------------------------------------------------------
# 3.3 Inner, left, right, and full joins
# -----------------------------------------------------------------------------

# INNER keeps only matched rows from both sides.
inner_product_join_df = order_item_measures_df.join(
    products_df,
    on='product_id',
    how='inner',
)

# LEFT preserves every order-item row, but it preserves ROW COUNT only when
# the right-side product key is unique. A duplicated dimension key can still
# multiply left rows.
left_product_join_df = order_item_measures_df.join(
    products_df,
    on='product_id',
    how='left',
)

# RIGHT preserves every product row. Products with no order lines remain.
right_product_join_df = order_item_measures_df.join(
    products_df,
    on='product_id',
    how='right',
)

# FULL preserves unmatched rows from both sides.
full_product_join_df = order_item_measures_df.join(
    products_df,
    on='product_id',
    how='full',
)


# -----------------------------------------------------------------------------
# 3.4 Semi and anti joins
# -----------------------------------------------------------------------------

# LEFT SEMI answers:
#     Which order-item rows have a valid product match?
#
# It returns only left-side columns.
valid_product_items_df = order_item_measures_df.join(
    products_df,
    on='product_id',
    how='left_semi',
)

# LEFT ANTI answers:
#     Which order-item rows have NO valid product match?
#
# This is a direct referential-integrity pattern.
invalid_product_items_df = order_item_measures_df.join(
    products_df,
    on='product_id',
    how='left_anti',
)


# -----------------------------------------------------------------------------
# 3.5 Multi-column join
# -----------------------------------------------------------------------------

# TARGETS GRAIN:
#     one row per store AND product
#
# The complete relationship key is therefore:
#     (store_id, product_id)
store_product_targets_schema = StructType([
    StructField('store_id',       StringType(),  nullable=False),
    StructField('product_id',     StringType(),  nullable=False),
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

inventory_with_targets_df = inventory_df.join(
    store_product_targets_df,
    on=['store_id', 'product_id'],
    how='left',
)

# Joining only on product_id would ignore part of the right-side grain.
# If the same product had target rows for multiple stores, that incomplete
# join could match one inventory row to several unrelated store targets.


# -----------------------------------------------------------------------------
# 3.6 Handling duplicate non-key column names
# -----------------------------------------------------------------------------

# Boolean join expressions can leave columns with the same names from both
# sides. Aliases plus an explicit select make ownership unambiguous.
orders_alias = orders_df.alias('o')
stores_alias = stores_df.alias('s')

orders_with_store_names_df = (
    orders_alias
    .join(
        stores_alias,
        F.col('o.store_id') == F.col('s.store_id'),
        how='left',
    )
    .select(
        F.col('o.order_id'),
        F.col('o.store_id'),
        F.col('o.order_status'),
        F.col('s.store_name'),
        F.col('s.province'),
    )
)


# -----------------------------------------------------------------------------
# 3.7 Cross join
# -----------------------------------------------------------------------------

# A cross join produces every combination.
#
# If date_df has D rows and stores_df has S rows:
#     output rows = D * S
#
# This is correct only when the target grain intentionally contains every
# date-store combination, such as a completeness scaffold.
scaffold_dates_df = spark.createDataFrame(
    [
        (date(2026, 1, 5),),
        (date(2026, 1, 6),),
    ],
    schema=StructType([
        StructField('calendar_date', DateType(), nullable=False),
    ]),
)

date_store_scaffold_df = scaffold_dates_df.crossJoin(
    stores_df.select(
        'store_id',
        'store_name',
    )
)


# -----------------------------------------------------------------------------
# 3.8 Many-to-many danger and pre-aggregation
# -----------------------------------------------------------------------------

# PROMOTIONS GRAIN:
#     one row per product-promotion
#
# ORDER ITEMS GRAIN:
#     one row per order line
#
# Both sides can repeat product_id, so a direct product_id join is many-to-many.
product_promotions_schema = StructType([
    StructField('promotion_id',   StringType(), nullable=False),
    StructField('product_id',     StringType(), nullable=False),
    StructField('promotion_type', StringType(), nullable=False),
])

product_promotions_df = spark.createDataFrame(
    [
        ('PROMO-01', 'P001', 'LOYALTY'),
        ('PROMO-02', 'P001', 'WEEKEND'),
        ('PROMO-03', 'P002', 'LOYALTY'),
    ],
    schema=product_promotions_schema,
)

# This join is intentionally dangerous for sales measures.
#
# Every P001 order line is repeated once for each matching P001 promotion.
# Summing net_sales after this join would overstate revenue.
many_to_many_example_df = order_item_measures_df.join(
    product_promotions_df,
    on='product_id',
    how='inner',
)

# If the business requirement needs only product-level promotion attributes,
# aggregate promotions FIRST to one row per product.
promotion_summary_df = (
    product_promotions_df
    .groupBy('product_id')
    .agg(
        F.countDistinct('promotion_id').alias('promotion_count'),
        F.collect_set('promotion_type').alias('promotion_types'),
    )
)

# RIGHT GRAIN is now one row per product, so this is many-to-one from lines.
items_with_promotion_summary_df = order_item_measures_df.join(
    promotion_summary_df,
    on='product_id',
    how='left',
)


# -----------------------------------------------------------------------------
# 3.9 Build an enriched line-level DataFrame for later windows
# -----------------------------------------------------------------------------

# Start from order-line measures because fact_sales will eventually keep this
# grain. Every enrichment below is many-to-one when source keys are valid.
sales_enriched_df = (
    order_item_measures_df.alias('i')
    .join(
        orders_df.alias('o'),
        on='order_id',
        how='inner',
    )
    .join(
        products_df.select(
            'product_id',
            'product_name',
            'category',
            'unit_cost',
        ).alias('p'),
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
        # Gross margin is derived at the SAME order-line grain as sales.
        'gross_margin',
        F.col('net_sales')
        - (F.col('quantity') * F.col('unit_cost')),
    )
)


# =============================================================================
# 4. WINDOW FUNCTIONS
# =============================================================================

# Window functions preserve rows. The partition and ordering define which rows
# participate in the calculation and in what sequence.


# -----------------------------------------------------------------------------
# 4.1 groupBy versus Window.partitionBy
# -----------------------------------------------------------------------------

# groupBy COLLAPSES order lines to one row per store.
store_totals_df = (
    sales_enriched_df
    .groupBy('store_id')
    .agg(
        F.sum('net_sales').alias('store_net_sales'),
    )
)

# Window.partitionBy keeps every order line and attaches a store-level result
# to each line.
store_partition = Window.partitionBy('store_id')

sales_with_store_total_df = sales_enriched_df.withColumn(
    'store_net_sales',
    F.sum('net_sales').over(store_partition),
)


# -----------------------------------------------------------------------------
# 4.2 row_number, rank, and dense_rank
# -----------------------------------------------------------------------------

# First aggregate to PRODUCT grain because the ranking requirement is:
#     rank products by revenue within category.
product_revenue_df = (
    sales_enriched_df
    .filter(F.col('order_status') == 'COMPLETED')
    .groupBy(
        'category',
        'product_id',
        'product_name',
    )
    .agg(
        F.sum('net_sales').alias('net_sales'),
    )
)

# rank and dense_rank should treat equal revenue values as ties, so the ranking
# window orders by revenue only.
revenue_rank_window = (
    Window
    .partitionBy('category')
    .orderBy(F.col('net_sales').desc())
)

ranked_products_df = (
    product_revenue_df
    .withColumn(
        'rank',
        F.rank().over(revenue_rank_window),
    )
    .withColumn(
        'dense_rank',
        F.dense_rank().over(revenue_rank_window),
    )
)

# row_number must choose exactly one sequence position per row.
#
# Adding product_id as a stable tie-breaker makes the sequence deterministic
# when two products have equal revenue.
deterministic_product_order_window = (
    Window
    .partitionBy('category')
    .orderBy(
        F.col('net_sales').desc(),
        F.col('product_id').asc(),
    )
)

ranked_products_df = ranked_products_df.withColumn(
    'row_number',
    F.row_number().over(deterministic_product_order_window),
)


# -----------------------------------------------------------------------------
# 4.3 lag and lead
# -----------------------------------------------------------------------------

# Aggregate first because the business sequence we want is STORE-DAY sales.
#
# OUTPUT GRAIN:
#     one row per store and order date
daily_store_sales_df = (
    sales_enriched_df
    .filter(F.col('order_status') == 'COMPLETED')
    .groupBy(
        'store_id',
        'order_date',
    )
    .agg(
        F.sum('net_sales').alias('daily_net_sales'),
    )
)

store_day_order_window = (
    Window
    .partitionBy('store_id')
    .orderBy('order_date')
)

daily_store_sales_with_neighbors_df = (
    daily_store_sales_df
    .withColumn(
        'previous_day_sales',
        F.lag('daily_net_sales').over(store_day_order_window),
    )
    .withColumn(
        'next_day_sales',
        F.lead('daily_net_sales').over(store_day_order_window),
    )
    .withColumn(
        # This compares with the previous OBSERVED store-day row.
        'change_from_previous_observed_day',
        F.col('daily_net_sales') - F.col('previous_day_sales'),
    )
)


# -----------------------------------------------------------------------------
# 4.4 Running aggregate
# -----------------------------------------------------------------------------

running_sales_window = (
    Window
    .partitionBy('store_id')
    .orderBy('order_date')
    .rowsBetween(
        Window.unboundedPreceding,
        Window.currentRow,
    )
)

daily_store_sales_running_df = daily_store_sales_df.withColumn(
    'running_net_sales',
    F.sum('daily_net_sales').over(running_sales_window),
)


# -----------------------------------------------------------------------------
# 4.5 Rolling ROW window
# -----------------------------------------------------------------------------

rolling_3_observation_window = (
    Window
    .partitionBy('store_id')
    .orderBy('order_date')
    .rowsBetween(-2, 0)
)

daily_store_sales_rolling_rows_df = daily_store_sales_df.withColumn(
    # This means the current row plus the previous two OBSERVED rows.
    #
    # It does NOT necessarily mean three calendar days because source dates
    # can have gaps.
    'rolling_3_observation_sales',
    F.sum('daily_net_sales').over(rolling_3_observation_window),
)


# -----------------------------------------------------------------------------
# 4.6 Rolling RANGE window for a real seven-day calculation
# -----------------------------------------------------------------------------

daily_store_sales_with_day_number_df = daily_store_sales_df.withColumn(
    # rangeBetween uses ordering VALUES rather than row positions.
    #
    # Convert date to a numeric day offset so that -6 through 0 represents a
    # seven-calendar-day interval including the current date.
    'day_number',
    F.datediff(
        F.col('order_date'),
        F.lit('1970-01-01'),
    ),
)

rolling_7_day_window = (
    Window
    .partitionBy('store_id')
    .orderBy('day_number')
    .rangeBetween(-6, 0)
)

daily_store_sales_rolling_days_df = (
    daily_store_sales_with_day_number_df
    .withColumn(
        'rolling_7_day_sales',
        F.sum('daily_net_sales').over(rolling_7_day_window),
    )
    .drop('day_number')
)


# -----------------------------------------------------------------------------
# 4.7 Latest-record selection and deterministic deduplication
# -----------------------------------------------------------------------------

# TARGET GRAIN:
#     one row per snapshot_date, store_id, product_id
#
# RECENCY:
#     newest snapshot_ts wins
#
# TIE-BREAKER:
#     greatest ingestion_id wins if timestamps tie
#
# The tie-breaker is essential for deterministic output.
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

latest_inventory_df = (
    inventory_df
    .withColumn(
        '_row_number',
        F.row_number().over(latest_inventory_window),
    )
    .filter(F.col('_row_number') == 1)
    .drop('_row_number')
)


# =============================================================================
# 5. OTHER TRANSFORMATION PATTERNS
# =============================================================================

# -----------------------------------------------------------------------------
# 5.1 dropDuplicates versus deterministic row_number selection
# -----------------------------------------------------------------------------

duplicate_event_schema = StructType([
    StructField('event_id',        StringType(),    nullable=False),
    StructField('event_ts',        TimestampType(), nullable=False),
    StructField('payload_version', IntegerType(),   nullable=False),
])

duplicate_events_df = spark.createDataFrame(
    [
        ('E001', datetime(2026, 1, 1, 10, 0), 1),
        ('E001', datetime(2026, 1, 1, 10, 5), 2),
        ('E002', datetime(2026, 1, 1, 11, 0), 1),
    ],
    schema=duplicate_event_schema,
)

# dropDuplicates guarantees one surviving row per event_id, but it does not
# encode WHICH event version the business wants to survive.
arbitrary_event_survivor_df = duplicate_events_df.dropDuplicates(
    ['event_id']
)

# A deterministic window encodes the actual business rule:
# keep the newest event, then greatest payload version under timestamp ties.
event_survivor_window = (
    Window
    .partitionBy('event_id')
    .orderBy(
        F.col('event_ts').desc(),
        F.col('payload_version').desc(),
    )
)

latest_event_df = (
    duplicate_events_df
    .withColumn(
        '_row_number',
        F.row_number().over(event_survivor_window),
    )
    .filter(F.col('_row_number') == 1)
    .drop('_row_number')
)


# -----------------------------------------------------------------------------
# 5.2 Conditional transformations
# -----------------------------------------------------------------------------

inventory_status_df = latest_inventory_df.withColumn(
    'inventory_status',

    # Condition order matters because zero also satisfies a typical
    # less-than-or-equal-to reorder-point test.
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


# -----------------------------------------------------------------------------
# 5.3 union versus unionByName
# -----------------------------------------------------------------------------

# A UNION stacks ROWS. A JOIN combines COLUMNS based on a row relationship.
#
# These tiny batches deliberately use string columns so that positional union
# can silently demonstrate the danger of reordered columns.
batch_a_df = spark.createDataFrame(
    [
        ('R001', 'READY'),
        ('R002', 'READY'),
    ],
    schema=StructType([
        StructField('record_id', StringType(), nullable=False),
        StructField('status',    StringType(), nullable=False),
    ]),
)

batch_b_reordered_df = spark.createDataFrame(
    [
        ('READY', 'R003'),
        ('FAILED', 'R004'),
    ],
    schema=StructType([
        StructField('status',    StringType(), nullable=False),
        StructField('record_id', StringType(), nullable=False),
    ]),
)

# union resolves columns BY POSITION.
#
# This executes, but the second batch's semantic values land under the wrong
# column names because the schemas are ordered differently.
unsafe_positional_union_df = batch_a_df.union(
    batch_b_reordered_df
)

# unionByName resolves columns BY NAME and is therefore the safer default for
# pipeline batches that represent the same conceptual row type.
safe_named_union_df = batch_a_df.unionByName(
    batch_b_reordered_df
)

batch_c_with_extra_column_df = spark.createDataFrame(
    [
        ('R005', 'READY', 'API'),
    ],
    schema=StructType([
        StructField('record_id',     StringType(), nullable=False),
        StructField('status',        StringType(), nullable=False),
        StructField('source_system', StringType(), nullable=False),
    ]),
)

# allowMissingColumns=True fills absent columns with NULL while still aligning
# existing columns by name.
evolving_schema_union_df = batch_a_df.unionByName(
    batch_c_with_extra_column_df,
    allowMissingColumns=True,
)


# -----------------------------------------------------------------------------
# 5.4 explode and its effect on grain
# -----------------------------------------------------------------------------

# INPUT GRAIN:
#     one row per product
#
# OUTPUT GRAIN:
#     one row per product-tag
#
# A two-tag product therefore becomes two rows.
product_tags_df = products_df.select(
    'product_id',
    'product_name',
    F.explode('tags').alias('tag'),
)

# explode_outer is the related pattern when a NULL or empty collection should
# still preserve a parent row with a NULL child value.
product_tags_outer_df = products_df.select(
    'product_id',
    F.explode_outer('tags').alias('tag'),
)


# -----------------------------------------------------------------------------
# 5.5 Pivot
# -----------------------------------------------------------------------------

monthly_store_sales_df = (
    sales_enriched_df
    .filter(F.col('order_status') == 'COMPLETED')
    .withColumn(
        'month',
        F.date_format(
            F.col('order_date'),
            'yyyy-MM',
        ),
    )
    .groupBy(
        'store_id',
        'month',
    )
    .agg(
        F.sum('net_sales').alias('net_sales'),
    )
)

# Pivot changes long data into a reporting-oriented wide shape.
#
# Supplying known pivot values makes the intended schema explicit and avoids
# requiring Spark to discover distinct pivot values first.
store_sales_pivot_df = (
    monthly_store_sales_df
    .groupBy('store_id')
    .pivot(
        'month',
        ['2026-01', '2026-02'],
    )
    .agg(
        F.sum('net_sales')
    )
)


# -----------------------------------------------------------------------------
# 5.6 Unpivot
# -----------------------------------------------------------------------------

# PySpark 4.2.0 supports DataFrame.unpivot().
#
# One wide store row can become multiple long rows, so unpivot can increase
# row count by the number of value columns being unpivoted.
store_sales_unpivot_df = store_sales_pivot_df.unpivot(
    ids='store_id',
    values=['2026-01', '2026-02'],
    variableColumnName='month',
    valueColumnName='net_sales',
)


# =============================================================================
# 6. DATA MODELING
# =============================================================================

# Target star-style analytical model:
#
#                 dim_product
#                      |
#     dim_date --- fact_sales --- dim_store
#         |            |
#         |            |
#         +---- fact_inventory_snapshot
#                      |
#                 shared dimensions
#
# Facts record business events/states.
# Dimensions describe reusable analytical entities.
#
# The product, store, and date dimensions are CONFORMED because both fact
# tables use the same definitions of those entities.


# -----------------------------------------------------------------------------
# 6.1 Durable surrogate-key mappings for the lecture
# -----------------------------------------------------------------------------

# A surrogate key is warehouse-managed and independent of the source business
# key. In a real warehouse, key assignment must be persistent and stable.
#
# Do NOT generate warehouse surrogate keys with a fresh arbitrary row number on
# every run. That can change existing keys when the input changes.
product_key_map_schema = StructType([
    StructField('product_key', IntegerType(), nullable=False),
    StructField('product_id',  StringType(),  nullable=False),
])

product_key_map_df = spark.createDataFrame(
    [
        (101, 'P001'),
        (102, 'P002'),
        (103, 'P003'),
        (104, 'P004'),
    ],
    schema=product_key_map_schema,
)

store_key_map_schema = StructType([
    StructField('store_key', IntegerType(), nullable=False),
    StructField('store_id',  StringType(),  nullable=False),
])

store_key_map_df = spark.createDataFrame(
    [
        (201, 'S01'),
        (202, 'S02'),
        (203, 'S03'),
    ],
    schema=store_key_map_schema,
)


# -----------------------------------------------------------------------------
# 6.2 dim_product
# -----------------------------------------------------------------------------

# GRAIN:
#     one row per current product
#
# BUSINESS KEY:
#     product_id
#
# SURROGATE KEY:
#     product_key
#
# This lecture models a current-state dimension, conceptually similar to a
# Type 1 view. Type 2 history would create multiple versioned rows per business
# key with effective dates/current-state metadata.
dim_product_df = (
    product_key_map_df
    .join(
        products_df,
        on='product_id',
        how='inner',
    )
    .select(
        'product_key',
        'product_id',
        'product_name',
        'category',
        'unit_cost',
    )
)


# -----------------------------------------------------------------------------
# 6.3 dim_store
# -----------------------------------------------------------------------------

# GRAIN:
#     one row per current store
#
# BUSINESS KEY:
#     store_id
#
# SURROGATE KEY:
#     store_key
dim_store_df = (
    store_key_map_df
    .join(
        stores_df,
        on='store_id',
        how='inner',
    )
    .select(
        'store_key',
        'store_id',
        'store_name',
        'province',
    )
)


# -----------------------------------------------------------------------------
# 6.4 dim_date
# -----------------------------------------------------------------------------

# GRAIN:
#     one row per calendar date
#
# Pull dates needed by BOTH facts, union them by name, then deduplicate to the
# date grain. The same date dimension can therefore be shared across facts.
sales_dates_df = orders_df.select(
    F.col('order_date').alias('full_date')
)

inventory_dates_df = latest_inventory_df.select(
    F.col('snapshot_date').alias('full_date')
)

dim_date_df = (
    sales_dates_df
    .unionByName(inventory_dates_df)
    .distinct()
    .withColumn(
        # YYYYMMDD is deterministic and human-readable as a date key.
        'date_key',
        F.date_format(
            F.col('full_date'),
            'yyyyMMdd',
        ).cast(IntegerType()),
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
    .select(
        'date_key',
        'full_date',
        'year',
        'month',
        'day',
    )
)


# -----------------------------------------------------------------------------
# 6.5 fact_sales — transaction fact
# -----------------------------------------------------------------------------

# TARGET GRAIN:
#     one row per order line
#
# NATURAL GRAIN KEY:
#     (order_id, line_number)
#
# Each enrichment below must therefore be many-to-one from the line fact.
fact_sales_df = (
    order_item_measures_df.alias('i')
    .join(
        orders_df.alias('o'),
        on='order_id',
        how='inner',
    )
    .join(
        dim_product_df.select(
            'product_id',
            'product_key',
            'unit_cost',
        ).alias('p'),
        on='product_id',
        how='inner',
    )
    .join(
        dim_store_df.select(
            'store_id',
            'store_key',
        ).alias('s'),
        on='store_id',
        how='inner',
    )
    .join(
        dim_date_df.select(
            'date_key',
            'full_date',
        ).alias('d'),
        F.col('o.order_date') == F.col('d.full_date'),
        how='inner',
    )
    .select(
        F.col('i.order_id'),
        F.col('i.line_number'),
        F.col('d.date_key'),
        F.col('p.product_key'),
        F.col('s.store_key'),
        F.col('o.customer_id'),
        F.col('o.order_status'),
        F.col('i.quantity'),
        F.col('i.unit_price'),
        F.col('i.discount_pct'),
        F.col('i.gross_sales'),
        F.col('i.discount_amount'),
        F.col('i.net_sales'),
        (
            F.col('i.net_sales')
            - (F.col('i.quantity') * F.col('p.unit_cost'))
        ).alias('gross_margin'),
    )
)


# -----------------------------------------------------------------------------
# 6.6 fact_inventory_snapshot — snapshot fact
# -----------------------------------------------------------------------------

# TARGET GRAIN:
#     one row per date, store, and product
#
# latest_inventory_df already reduced multiple source arrivals to the intended
# daily business grain before dimension-key resolution.
fact_inventory_snapshot_df = (
    latest_inventory_df.alias('i')
    .join(
        dim_product_df.select(
            'product_id',
            'product_key',
        ).alias('p'),
        on='product_id',
        how='inner',
    )
    .join(
        dim_store_df.select(
            'store_id',
            'store_key',
        ).alias('s'),
        on='store_id',
        how='inner',
    )
    .join(
        dim_date_df.select(
            'date_key',
            'full_date',
        ).alias('d'),
        F.col('i.snapshot_date') == F.col('d.full_date'),
        how='inner',
    )
    .select(
        F.col('d.date_key'),
        F.col('s.store_key'),
        F.col('p.product_key'),
        F.col('i.on_hand_quantity'),
        F.col('i.reorder_point'),
    )
)


# =============================================================================
# 7. MODEL VALIDATION PATTERNS
# =============================================================================

# Model correctness is not proven by successful execution. Validate the grain
# and the dimension relationships that the model claims to have.


# -----------------------------------------------------------------------------
# 7.1 Fact grain checks
# -----------------------------------------------------------------------------

duplicate_fact_sales_keys_df = find_duplicate_keys(
    fact_sales_df,
    ['order_id', 'line_number'],
)

duplicate_inventory_fact_keys_df = find_duplicate_keys(
    fact_inventory_snapshot_df,
    [
        'date_key',
        'store_key',
        'product_key',
    ],
)


# -----------------------------------------------------------------------------
# 7.2 Dimension key checks
# -----------------------------------------------------------------------------

duplicate_product_dimension_keys_df = find_duplicate_keys(
    dim_product_df,
    ['product_key'],
)

duplicate_product_business_keys_df = find_duplicate_keys(
    dim_product_df,
    ['product_id'],
)

duplicate_store_dimension_keys_df = find_duplicate_keys(
    dim_store_df,
    ['store_key'],
)


# -----------------------------------------------------------------------------
# 7.3 Referential-integrity checks from facts to dimensions
# -----------------------------------------------------------------------------

orphan_sales_products_df = find_orphans(
    fact_sales_df,
    dim_product_df,
    ['product_key'],
)

orphan_sales_stores_df = find_orphans(
    fact_sales_df,
    dim_store_df,
    ['store_key'],
)

orphan_sales_dates_df = find_orphans(
    fact_sales_df,
    dim_date_df,
    ['date_key'],
)

orphan_inventory_products_df = find_orphans(
    fact_inventory_snapshot_df,
    dim_product_df,
    ['product_key'],
)

orphan_inventory_stores_df = find_orphans(
    fact_inventory_snapshot_df,
    dim_store_df,
    ['store_key'],
)

orphan_inventory_dates_df = find_orphans(
    fact_inventory_snapshot_df,
    dim_date_df,
    ['date_key'],
)


# -----------------------------------------------------------------------------
# 7.4 Row-count and measure reconciliation
# -----------------------------------------------------------------------------

# Because fact_sales is supposed to preserve accepted order-line grain, its
# row count should equal the order-item source row count when every order,
# product, store, and date resolves successfully.
source_line_count = order_items_df.count()
fact_sales_count = fact_sales_df.count()

print(
    f'order-item source rows: {source_line_count}; '
    f'fact_sales rows: {fact_sales_count}'
)

# Revenue reconciliation checks that dimension enrichment did not multiply or
# lose line-level measures.
source_net_sales = (
    order_item_measures_df
    .agg(
        F.sum('net_sales').alias('net_sales')
    )
    .first()['net_sales']
)

fact_net_sales = (
    fact_sales_df
    .agg(
        F.sum('net_sales').alias('net_sales')
    )
    .first()['net_sales']
)

print(
    f'source net sales: {source_net_sales}; '
    f'fact net sales: {fact_net_sales}'
)


# =============================================================================
# 8. COMPACT OUTPUTS TO INSPECT WHILE STUDYING
# =============================================================================

# show() calls are ACTIONS. They are included because this is a runnable lecture
# file and the datasets are deliberately tiny. In production code, avoid using
# repeated actions as ad hoc validation over large datasets.

print('\n=== AGGREGATION: ONE ROW PER PRODUCT ===')
product_sales_summary_df.orderBy('product_id').show(
    truncate=False
)

print('\n=== CONDITIONAL AGGREGATION ===')
product_conditional_metrics_df.orderBy('product_id').show(
    truncate=False
)

print('\n=== MANY-TO-MANY EXAMPLE: ROW MULTIPLICATION ===')
many_to_many_example_df.select(
    'product_id',
    'order_id',
    'line_number',
    'promotion_id',
    'net_sales',
).orderBy(
    'product_id',
    'order_id',
    'line_number',
    'promotion_id',
).show(
    truncate=False
)

print('\n=== PRE-AGGREGATED PROMOTION JOIN ===')
items_with_promotion_summary_df.select(
    'product_id',
    'order_id',
    'line_number',
    'promotion_count',
    'net_sales',
).orderBy(
    'product_id',
    'order_id',
    'line_number',
).show(
    truncate=False
)

print('\n=== PRODUCT RANKING WITH WINDOW FUNCTIONS ===')
ranked_products_df.orderBy(
    'category',
    'row_number',
).show(
    truncate=False
)

print('\n=== RUNNING STORE SALES ===')
daily_store_sales_running_df.orderBy(
    'store_id',
    'order_date',
).show(
    truncate=False
)

print('\n=== ROLLING SEVEN-DAY STORE SALES ===')
daily_store_sales_rolling_days_df.orderBy(
    'store_id',
    'order_date',
).show(
    truncate=False
)

print('\n=== LATEST INVENTORY RECORD PER TARGET GRAIN ===')
latest_inventory_df.orderBy(
    'snapshot_date',
    'store_id',
    'product_id',
).show(
    truncate=False
)

print('\n=== POSITIONAL UNION: INTENTIONALLY UNSAFE ===')
unsafe_positional_union_df.show(
    truncate=False
)

print('\n=== UNION BY NAME: CORRECT ALIGNMENT ===')
safe_named_union_df.show(
    truncate=False
)

print('\n=== EXPLODE: PRODUCT GRAIN -> PRODUCT-TAG GRAIN ===')
product_tags_df.orderBy(
    'product_id',
    'tag',
).show(
    truncate=False
)

print('\n=== PIVOT ===')
store_sales_pivot_df.orderBy('store_id').show(
    truncate=False
)

print('\n=== UNPIVOT ===')
store_sales_unpivot_df.orderBy(
    'store_id',
    'month',
).show(
    truncate=False
)

print('\n=== DIM_PRODUCT ===')
dim_product_df.orderBy('product_key').show(
    truncate=False
)

print('\n=== DIM_STORE ===')
dim_store_df.orderBy('store_key').show(
    truncate=False
)

print('\n=== DIM_DATE ===')
dim_date_df.orderBy('date_key').show(
    truncate=False
)

print('\n=== FACT_SALES: ONE ROW PER ORDER LINE ===')
fact_sales_df.orderBy(
    'order_id',
    'line_number',
).show(
    truncate=False
)

print('\n=== FACT_INVENTORY_SNAPSHOT ===')
fact_inventory_snapshot_df.orderBy(
    'date_key',
    'store_key',
    'product_key',
).show(
    truncate=False
)


# =============================================================================
# 9. PHASE 2 DECISION CHECKLIST
# =============================================================================

# Before an aggregation:
#     current grain?
#     target grain?
#     which columns define the group?
#     which measures are valid to aggregate?
#
# Before a join:
#     left grain?
#     right grain?
#     complete join key?
#     one-to-one / one-to-many / many-to-one / many-to-many?
#     can rows multiply?
#     expected unmatched behavior?
#
# Before a window:
#     partition keys?
#     business ordering?
#     deterministic tie-breakers?
#     whole partition / running frame / rolling row frame / range frame?
#
# Before deduplication:
#     what defines a duplicate?
#     does any survivor work?
#     if not, what deterministic ordering selects the survivor?
#
# Before explode:
#     what does one collection element represent?
#     what will one output row represent afterward?
#
# Before union:
#     do both inputs represent the same conceptual row type?
#     should columns align by position or by name?
#
# Before writing any output table:
#     WHAT DOES ONE ROW REPRESENT?


spark.stop()
