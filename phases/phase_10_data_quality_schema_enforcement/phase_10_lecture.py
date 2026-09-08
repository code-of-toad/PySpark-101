#!/usr/bin/env python
'''PySpark 101 — Phase 10 lecture.

Data quality and schema enforcement through explicit contracts, reusable
validation helpers, diagnostic rejection reasons, accepted/rejected outputs,
quarantine patterns, validation metrics, key checks, and referential integrity.

This is lecture-only code.

The governing principle is:

    raw data
        -> schema enforcement
        -> data-quality validation
        -> accepted / rejected split
            -> accepted -> transformations
            -> rejected -> quarantine

The file keeps the complete teaching flow in one place. A production-style
application would normally place reusable schemas and validation functions in
the appropriate Phase 9 application modules rather than creating a separate
framework merely for appearance.

Examples target PySpark 4.2.0 and deterministic retail-domain data.
'''

from datetime import date
from decimal import Decimal

from pyspark.sql import Column
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType
from pyspark.sql.types import DecimalType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType


# =============================================================================
# 0. SPARK SESSION AND PHASE 10 MENTAL MODEL
# =============================================================================

spark = (
    SparkSession.builder
    .appName('phase_10_data_quality_schema_enforcement')
    .master('local[4]')
    # Keep the teaching workload small and predictable.
    .config('spark.sql.shuffle.partitions', '8')
    .getOrCreate()
)

spark.sparkContext.setLogLevel('WARN')

# Phase 10 is NOT:
#
#     raw_df
#         -> filter bad rows
#         -> forget what disappeared
#         -> continue
#
# It IS:
#
#     raw
#       -> structural contract
#       -> row-level checks
#       -> dataset-level checks
#       -> cross-dataset checks
#       -> rejection reasons
#       -> accepted / rejected split
#           -> accepted -> transformations
#           -> rejected -> quarantine
#
# Validation scopes:
#
#     row-level
#         required fields
#         domains
#         ranges
#         cross-column business rules
#
#     dataset-level
#         primary-key uniqueness
#         composite-key uniqueness
#         exact duplicate detection
#
#     cross-dataset
#         referential integrity


# =============================================================================
# 1. EXPLICIT STRUCTURAL SCHEMAS
# =============================================================================

# Incoming schemas deliberately allow NULL values so malformed business rows
# can exist long enough to be diagnosed and quarantined. Required-field rules
# below decide whether a row is acceptable.
ORDERS_SCHEMA = StructType(
    [
        StructField('order_id', StringType(), nullable=True),
        StructField('customer_id', StringType(), nullable=True),
        StructField('order_date', DateType(), nullable=True),
        StructField('order_status', StringType(), nullable=True),
        StructField('net_sales', DecimalType(12, 2), nullable=True),
    ]
)

CUSTOMERS_SCHEMA = StructType(
    [
        StructField('customer_id', StringType(), nullable=True),
        StructField('customer_name', StringType(), nullable=True),
        StructField('province', StringType(), nullable=True),
        StructField('customer_segment', StringType(), nullable=True),
    ]
)

INVENTORY_SCHEMA = StructType(
    [
        StructField('snapshot_date', DateType(), nullable=True),
        StructField('store_id', StringType(), nullable=True),
        StructField('product_id', StringType(), nullable=True),
        StructField('quantity_on_hand', IntegerType(), nullable=True),
    ]
)

# Structural schema enforcement and semantic validation solve different
# problems:
#
#     schema
#     -> expected columns + Spark data types
#
#     semantic validation
#     -> required values + domains + ranges + keys + relationships


# =============================================================================
# 2. DETERMINISTIC RETAIL PRACTICE DATA
# =============================================================================

ORDERS_ROWS = [
    # Valid row.
    ('O001', 'C001', date(2026, 9, 1), 'COMPLETED', Decimal('125.00')),

    # Invalid: missing primary key.
    (None, 'C002', date(2026, 9, 1), 'COMPLETED', Decimal('80.00')),

    # Invalid: order_status is outside the approved domain.
    ('O003', 'C001', date(2026, 9, 2), 'UNKNOWN', Decimal('45.00')),

    # Invalid: negative net sales violate the allowed range.
    ('O004', 'C003', date(2026, 9, 2), 'COMPLETED', Decimal('-20.00')),

    # Invalid: customer does not exist in customers_df.
    ('O005', 'C999', date(2026, 9, 3), 'COMPLETED', Decimal('30.00')),

    # Invalid pair: same business key with conflicting values.
    ('O006', 'C004', date(2026, 9, 3), 'COMPLETED', Decimal('10.00')),
    ('O006', 'C004', date(2026, 9, 3), 'COMPLETED', Decimal('15.00')),

    # Invalid exact duplicate pair.
    ('O007', 'C004', date(2026, 9, 4), 'PENDING', Decimal('20.00')),
    ('O007', 'C004', date(2026, 9, 4), 'PENDING', Decimal('20.00')),

    # Invalid: cancelled orders should carry zero sales under this contract.
    ('O008', 'C003', date(2026, 9, 4), 'CANCELLED', Decimal('12.00')),

    # Valid row.
    ('O009', 'C002', date(2026, 9, 5), 'CANCELLED', Decimal('0.00')),
]

CUSTOMERS_ROWS = [
    ('C001', 'Alice Wong', 'ON', 'CONSUMER'),
    ('C002', 'Ben Tremblay', 'QC', 'CONSUMER'),
    ('C003', 'Carla Singh', 'BC', 'BUSINESS'),
    ('C004', 'Diego Martin', 'ON', 'CONSUMER'),
]

INVENTORY_ROWS = [
    # Valid rows.
    (date(2026, 9, 5), 'S001', 'P001', 10),
    (date(2026, 9, 5), 'S001', 'P002', 7),

    # Invalid pair: duplicate composite key.
    (date(2026, 9, 5), 'S002', 'P001', 5),
    (date(2026, 9, 5), 'S002', 'P001', 8),

    # Invalid: negative quantity.
    (date(2026, 9, 5), 'S003', 'P003', -1),
]


def build_practice_data(
    spark_session: SparkSession,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    '''Create deterministic DataFrames containing intentional quality defects.'''

    orders_df = spark_session.createDataFrame(
        ORDERS_ROWS,
        schema=ORDERS_SCHEMA,
    )
    customers_df = spark_session.createDataFrame(
        CUSTOMERS_ROWS,
        schema=CUSTOMERS_SCHEMA,
    )
    inventory_df = spark_session.createDataFrame(
        INVENTORY_ROWS,
        schema=INVENTORY_SCHEMA,
    )

    return orders_df, customers_df, inventory_df


# Declared grains:
#
#     orders_df
#     -> one row per order_id
#
#     customers_df
#     -> one row per customer_id
#
#     inventory_df
#     -> one row per snapshot_date + store_id + product_id
#
# Duplicate keys intentionally violate those declared grains.


# =============================================================================
# 3. STRUCTURAL SCHEMA VALIDATION
# =============================================================================


def validate_schema(
    df: DataFrame,
    expected_schema: StructType,
    label: str,
    allow_extra_columns: bool = False,
) -> None:
    '''Validate required columns and Spark data types without scanning row data.'''

    # Schema metadata is already available on the driver, so this structural
    # check does not need a distributed Spark job.
    expected_fields = {
        field.name: field.dataType
        for field in expected_schema.fields
    }
    actual_fields = {
        field.name: field.dataType
        for field in df.schema.fields
    }

    missing_columns = set(expected_fields) - set(actual_fields)
    unexpected_columns = set(actual_fields) - set(expected_fields)

    type_mismatches = {
        column_name: (
            expected_fields[column_name],
            actual_fields[column_name],
        )
        for column_name in expected_fields.keys() & actual_fields.keys()
        if expected_fields[column_name] != actual_fields[column_name]
    }

    if missing_columns:
        raise ValueError(
            f'{label} is missing required columns: {sorted(missing_columns)}'
        )

    if type_mismatches:
        raise ValueError(
            f'{label} contains type mismatches: {type_mismatches}'
        )

    if unexpected_columns and not allow_extra_columns:
        raise ValueError(
            f'{label} contains unexpected columns: {sorted(unexpected_columns)}'
        )


# Structural schema validation answers:
#
#     Are the required columns present?
#     Are their Spark data types correct?
#     Are extra columns allowed?
#
# It does NOT answer:
#
#     Are required values non-null?
#     Are domains/ranges valid?
#     Are keys unique?
#     Do foreign keys exist?


# =============================================================================
# 4. SMALL REUSABLE VALIDATION HELPERS
# =============================================================================


def missing_string(column_name: str) -> Column:
    '''Return an invalidity condition for a required string field.'''

    # A blank identifier is structurally present but still unusable.
    return (
        F.col(column_name).isNull()
        | (F.trim(F.col(column_name)) == '')
    )


def find_duplicate_keys(
    df: DataFrame,
    key_columns: list[str],
) -> DataFrame:
    '''Return one row per duplicated business key.'''

    # groupBy() requires equal keys to be colocated, so this is a shuffle.
    return (
        df
        .groupBy(*key_columns)
        .count()
        .filter(F.col('count') > 1)
        .select(*key_columns)
    )


def find_exact_duplicate_rows(df: DataFrame) -> DataFrame:
    '''Return one row per exact duplicated record value.'''

    # Exact duplicates and duplicate business keys are different concepts.
    return (
        df
        .groupBy(*df.columns)
        .count()
        .filter(F.col('count') > 1)
    )


def add_rejection_reasons(
    df: DataFrame,
    rules: list[tuple[str, Column]],
) -> DataFrame:
    '''Attach every row-level rejection reason that applies to each record.'''

    # Each rule contributes its reason only when the invalidity condition holds.
    reason_columns = [
        F.when(invalid_condition, F.lit(reason))
        for reason, invalid_condition in rules
    ]

    return df.withColumn(
        'rejection_reasons',
        # Remove NULL placeholders so accepted rows receive an empty array.
        F.filter(
            F.array(*reason_columns),
            lambda reason: reason.isNotNull(),
        ),
    )


def append_reason(
    df: DataFrame,
    reason: str,
    invalid_condition: Column,
) -> DataFrame:
    '''Append one additional reason while preserving existing diagnostics.'''

    return df.withColumn(
        'rejection_reasons',
        F.when(
            invalid_condition,
            F.array_union(
                F.col('rejection_reasons'),
                F.array(F.lit(reason)),
            ),
        ).otherwise(F.col('rejection_reasons')),
    )


def split_accepted_rejected(
    validated_df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    '''Split rows only after all required quality rules are evaluated.'''

    accepted_df = (
        validated_df
        .filter(F.size('rejection_reasons') == 0)
        .drop('rejection_reasons')
    )

    rejected_df = validated_df.filter(
        F.size('rejection_reasons') > 0
    )

    return accepted_df, rejected_df


# These helpers deliberately solve mechanics only.
#
# Business-specific contracts such as allowed order statuses remain explicit in
# the dataset validation function instead of being hidden in a generic framework.


# =============================================================================
# 5. ORDERS — ROW-LEVEL RULES
# =============================================================================

ALLOWED_ORDER_STATUSES = (
    'COMPLETED',
    'CANCELLED',
    'PENDING',
)


def add_orders_row_level_reasons(orders_df: DataFrame) -> DataFrame:
    '''Evaluate required fields, domain checks, range checks, and business rules.'''

    rules = [
        (
            'MISSING_ORDER_ID',
            missing_string('order_id'),
        ),
        (
            'MISSING_CUSTOMER_ID',
            missing_string('customer_id'),
        ),
        (
            'MISSING_ORDER_DATE',
            F.col('order_date').isNull(),
        ),
        (
            'MISSING_ORDER_STATUS',
            missing_string('order_status'),
        ),
        (
            'MISSING_NET_SALES',
            F.col('net_sales').isNull(),
        ),
        (
            'INVALID_ORDER_STATUS',
            # Guard against NULL so missing and invalid domain remain distinct.
            F.col('order_status').isNotNull()
            & ~F.col('order_status').isin(*ALLOWED_ORDER_STATUSES),
        ),
        (
            'NEGATIVE_NET_SALES',
            F.col('net_sales').isNotNull()
            & (F.col('net_sales') < F.lit(Decimal('0.00'))),
        ),
        (
            'INVALID_CANCELLED_AMOUNT',
            # Cross-column business rule: cancelled orders should carry zero sales.
            (F.col('order_status') == 'CANCELLED')
            & F.col('net_sales').isNotNull()
            & (F.col('net_sales') != F.lit(Decimal('0.00'))),
        ),
    ]

    return add_rejection_reasons(orders_df, rules)


# Atomic rule identifiers make rejected data useful:
#
#     MISSING_ORDER_ID
#     INVALID_ORDER_STATUS
#     NEGATIVE_NET_SALES
#     INVALID_CANCELLED_AMOUNT
#
# A row can retain multiple reasons instead of being collapsed to INVALID_ROW.


# =============================================================================
# 6. ORDERS — PRIMARY-KEY UNIQUENESS
# =============================================================================


def add_duplicate_order_reason(validated_orders_df: DataFrame) -> DataFrame:
    '''Mark every row whose non-null order_id violates primary-key uniqueness.'''

    duplicate_keys_df = (
        find_duplicate_keys(
            validated_orders_df,
            key_columns=['order_id'],
        )
        # NULL order_id is already handled by MISSING_ORDER_ID.
        .filter(F.col('order_id').isNotNull())
        .withColumn('_duplicate_order_id', F.lit(True))
    )

    result_df = (
        validated_orders_df
        .join(
            duplicate_keys_df,
            on='order_id',
            how='left',
        )
        .withColumn(
            '_duplicate_order_id',
            F.coalesce(F.col('_duplicate_order_id'), F.lit(False)),
        )
    )

    result_df = append_reason(
        result_df,
        reason='DUPLICATE_ORDER_ID',
        invalid_condition=F.col('_duplicate_order_id'),
    )

    return result_df.drop('_duplicate_order_id')


# Why mark ALL rows sharing an unexplained duplicate key?
#
#     O006 | ... | 10.00
#     O006 | ... | 15.00
#
# Without an explicit survivorship rule, the validation layer should not guess
# which business row is the correct one.


# =============================================================================
# 7. ORDERS — REFERENTIAL INTEGRITY
# =============================================================================


def add_orphan_customer_reason(
    validated_orders_df: DataFrame,
    customers_df: DataFrame,
) -> DataFrame:
    '''Mark order rows whose customer_id has no parent customer record.'''

    # Validate the parent grain separately before relying on the relationship.
    duplicate_customer_keys_df = find_duplicate_keys(
        customers_df,
        key_columns=['customer_id'],
    )

    duplicate_parent_exists = (
        duplicate_customer_keys_df
        .limit(1)
        .count()
        > 0
    )

    if duplicate_parent_exists:
        raise ValueError(
            'customers_df violates expected grain: customer_id is not unique.'
        )

    parent_keys_df = (
        customers_df
        .select('customer_id')
        .filter(F.col('customer_id').isNotNull())
        # The preceding grain check means this should already be unique.
        .withColumn('_customer_exists', F.lit(True))
    )

    result_df = (
        validated_orders_df
        .join(
            parent_keys_df,
            on='customer_id',
            how='left',
        )
        .withColumn(
            '_customer_exists',
            F.coalesce(F.col('_customer_exists'), F.lit(False)),
        )
    )

    # Do not label NULL customer_id as an orphan; it already has its own
    # required-field rejection reason.
    orphan_condition = (
        F.col('customer_id').isNotNull()
        & ~F.col('_customer_exists')
    )

    result_df = append_reason(
        result_df,
        reason='ORPHAN_CUSTOMER_ID',
        invalid_condition=orphan_condition,
    )

    return result_df.drop('_customer_exists')


# Referential integrity proves:
#
#     a parent exists
#
# Parent-key uniqueness proves:
#
#     exactly one parent is expected
#
# Both matter because a duplicated parent dimension can multiply rows during a
# downstream join even when the foreign key itself technically matches.


# =============================================================================
# 8. COMPLETE REUSABLE ORDERS QUALITY LAYER
# =============================================================================


def validate_orders(
    orders_df: DataFrame,
    customers_df: DataFrame,
) -> DataFrame:
    '''Return order-grain rows annotated with all required rejection reasons.'''

    # Fail fast when the dataset structure itself is not understandable.
    validate_schema(
        orders_df,
        expected_schema=ORDERS_SCHEMA,
        label='orders_df',
    )
    validate_schema(
        customers_df,
        expected_schema=CUSTOMERS_SCHEMA,
        label='customers_df',
    )

    # Evaluate row-local rules first without dropping any records.
    validated_df = add_orders_row_level_reasons(orders_df)

    # Add dataset-level primary-key violations.
    validated_df = add_duplicate_order_reason(validated_df)

    # Add cross-dataset relationship violations.
    validated_df = add_orphan_customer_reason(
        validated_df,
        customers_df,
    )

    return validated_df


# The validation layer owns source correctness.
#
# Downstream transformation functions should consume accepted_orders_df rather
# than reimplementing the same quality filters in multiple places.


# =============================================================================
# 9. INVENTORY — COMPOSITE KEY VALIDATION
# =============================================================================


def add_inventory_reasons(inventory_df: DataFrame) -> DataFrame:
    '''Validate inventory values and its composite-grain contract.'''

    validate_schema(
        inventory_df,
        expected_schema=INVENTORY_SCHEMA,
        label='inventory_df',
    )

    row_rules = [
        (
            'MISSING_SNAPSHOT_DATE',
            F.col('snapshot_date').isNull(),
        ),
        (
            'MISSING_STORE_ID',
            missing_string('store_id'),
        ),
        (
            'MISSING_PRODUCT_ID',
            missing_string('product_id'),
        ),
        (
            'MISSING_QUANTITY_ON_HAND',
            F.col('quantity_on_hand').isNull(),
        ),
        (
            'NEGATIVE_QUANTITY_ON_HAND',
            F.col('quantity_on_hand').isNotNull()
            & (F.col('quantity_on_hand') < 0),
        ),
    ]

    validated_df = add_rejection_reasons(
        inventory_df,
        rules=row_rules,
    )

    composite_key = [
        'snapshot_date',
        'store_id',
        'product_id',
    ]

    duplicate_keys_df = (
        find_duplicate_keys(
            validated_df,
            key_columns=composite_key,
        )
        .withColumn('_duplicate_inventory_key', F.lit(True))
    )

    validated_df = (
        validated_df
        .join(
            duplicate_keys_df,
            on=composite_key,
            how='left',
        )
        .withColumn(
            '_duplicate_inventory_key',
            F.coalesce(
                F.col('_duplicate_inventory_key'),
                F.lit(False),
            ),
        )
    )

    validated_df = append_reason(
        validated_df,
        reason='DUPLICATE_INVENTORY_KEY',
        invalid_condition=F.col('_duplicate_inventory_key'),
    )

    return validated_df.drop('_duplicate_inventory_key')


# Composite grain:
#
#     snapshot_date + store_id + product_id
#
# None of those columns needs to be individually unique. The COMBINATION must
# identify one inventory snapshot row.


# =============================================================================
# 10. ACCEPTED VS. REJECTED OUTPUTS
# =============================================================================


def validate_and_split_orders(
    orders_df: DataFrame,
    customers_df: DataFrame,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    '''Return validated, accepted, and rejected order populations.'''

    validated_df = validate_orders(
        orders_df,
        customers_df,
    )

    accepted_df, rejected_df = split_accepted_rejected(validated_df)

    return validated_df, accepted_df, rejected_df


# Core classification invariant:
#
#     every interpretable input row
#     -> accepted OR rejected
#
# Rows are split only AFTER required diagnostics are attached.


# =============================================================================
# 11. QUARANTINE PATTERN
# =============================================================================


def build_quarantine(
    rejected_df: DataFrame,
    source_dataset: str,
    validation_run_date: date,
) -> DataFrame:
    '''Add deterministic operational metadata to rejected source records.'''

    return (
        rejected_df
        # Preserve original business columns and rejection reasons.
        .withColumn('source_dataset', F.lit(source_dataset))
        # Inject run metadata instead of calling current_date() implicitly.
        .withColumn(
            'validation_run_date',
            F.lit(validation_run_date).cast('date'),
        )
    )


# Quarantine should preserve enough evidence to answer:
#
#     What source record arrived?
#     Which rules failed?
#     Under which validation run?
#     Can the row be investigated or replayed later?
#
# Quarantine is not a silent delete path.


# =============================================================================
# 12. VALIDATION METRICS
# =============================================================================


def build_validation_summary(
    validated_df: DataFrame,
    dataset_name: str,
) -> DataFrame:
    '''Build one lazy summary row for accepted/rejected reconciliation.'''

    # The aggregation remains lazy until shown, collected, or written.
    return (
        validated_df
        .agg(
            F.count(F.lit(1)).alias('input_count'),
            F.sum(
                F.when(
                    F.size('rejection_reasons') == 0,
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).alias('accepted_count'),
            F.sum(
                F.when(
                    F.size('rejection_reasons') > 0,
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).alias('rejected_count'),
        )
        .withColumn('dataset_name', F.lit(dataset_name))
        .withColumn(
            'acceptance_rate',
            F.col('accepted_count') / F.col('input_count'),
        )
        .withColumn(
            'rejection_rate',
            F.col('rejected_count') / F.col('input_count'),
        )
        .select(
            'dataset_name',
            'input_count',
            'accepted_count',
            'rejected_count',
            'acceptance_rate',
            'rejection_rate',
        )
    )


def build_rejection_reason_counts(
    rejected_df: DataFrame,
) -> DataFrame:
    '''Count violations by atomic machine-readable rejection reason.'''

    return (
        rejected_df
        .select(
            F.explode('rejection_reasons').alias('rejection_reason')
        )
        .groupBy('rejection_reason')
        .count()
    )


def assert_validation_reconciles(validated_df: DataFrame) -> None:
    '''Prove that input rows were classified exactly once.'''

    # This action is deliberate because reconciliation is a correctness check.
    counts = (
        validated_df
        .agg(
            F.count(F.lit(1)).alias('input_count'),
            F.sum(
                F.when(
                    F.size('rejection_reasons') == 0,
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).alias('accepted_count'),
            F.sum(
                F.when(
                    F.size('rejection_reasons') > 0,
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).alias('rejected_count'),
        )
        .first()
    )

    assert counts['input_count'] == (
        counts['accepted_count'] + counts['rejected_count']
    )


# Important:
#
#     sum(rejection counts by reason)
#     can be greater than
#     rejected row count
#
# because one row can fail multiple rules.


# =============================================================================
# 13. WHY dropDuplicates() IS NOT A QUALITY STRATEGY
# =============================================================================


def demonstrate_duplicate_diagnostics(orders_df: DataFrame) -> None:
    '''Show exact duplicates and duplicate business keys separately.'''

    print('\nDUPLICATE BUSINESS KEYS')
    (
        find_duplicate_keys(
            orders_df,
            key_columns=['order_id'],
        )
        .orderBy('order_id')
        .show(truncate=False)
    )

    print('\nEXACT DUPLICATE ROWS')
    (
        find_exact_duplicate_rows(orders_df)
        .orderBy('order_id')
        .show(truncate=False)
    )


# Avoid:
#
#     df.dropDuplicates(['order_id'])
#
# as a generic fix.
#
# It hides:
#
#     how many collisions existed
#     whether records disagreed
#     which row survived
#     whether the survivor was deterministic
#     whether the source should be corrected
#
# Deduplication becomes legitimate only when an explicit deterministic
# survivorship rule is part of the business contract.


# =============================================================================
# 14. DISTRIBUTED-EXECUTION REFERENCE
# =============================================================================

DISTRIBUTED_EXECUTION_REFERENCE = '''
ROW-LEVEL RULES
    isNull()
    trim()
    isin()
    numeric comparisons
    cross-column conditions

Usually narrow expressions:
    each partition can evaluate its own rows.

UNIQUENESS
    groupBy(key).count()

Usually wide:
    equal keys must be redistributed together.
    Expect a shuffle / Exchange.

REFERENTIAL INTEGRITY
    child.join(parent_keys, ...)

Distributed relationship check:
    physical strategy depends on data sizes, statistics, configuration, and AQE.
    Could become BroadcastHashJoin, SortMergeJoin, or another valid strategy.

METRICS
    count / aggregation

Actions or downstream writes materialize Spark work.

Principle:
    correctness comes first.
    optimize expensive validation with evidence rather than deleting checks.
'''


# =============================================================================
# 15. VALIDATION VS. BUSINESS TRANSFORMATION
# =============================================================================


def transform_accepted_orders(
    accepted_orders_df: DataFrame,
    customers_df: DataFrame,
) -> DataFrame:
    '''Example downstream transformation that assumes validation already passed.'''

    # Phase 10 keeps source-quality logic outside this business transformation.
    customer_projection_df = customers_df.select(
        'customer_id',
        'province',
        'customer_segment',
    )

    return accepted_orders_df.join(
        customer_projection_df,
        on='customer_id',
        how='left',
    )


# Preferred architecture:
#
#     raw_orders_df
#         -> validate_orders()
#         -> accepted_orders_df
#         -> transform_accepted_orders()
#
# Avoid:
#
#     transform_orders()
#         -> filter null keys
#         -> filter invalid statuses
#         -> filter negative sales
#         -> dedupe
#         -> join
#         -> actual business logic
#
# Scattered validation makes rules difficult to diagnose, reuse, and maintain.


# =============================================================================
# 16. COMMON FAILURE MODES
# =============================================================================

COMMON_FAILURE_MODES = '''
1. Silent filters
   Bad rows disappear and leave no diagnostic evidence.

2. Schema-only thinking
   Correct Spark types do not prove requiredness, domains, ranges, keys, or RI.

3. dropDuplicates() as cleanup
   Key collisions disappear without an explicit survivorship contract.

4. Foreign-key checks without parent-grain checks
   The parent exists but duplicate parents can still multiply downstream rows.

5. One opaque reason such as INVALID_ROW
   Operators cannot tell which contract actually failed.

6. Sequential filtering before diagnostics
   Later violations on the same row are never observed.

7. collect() on rejected populations
   Large bad-data sets can overwhelm the driver.

8. Rejecting anomalies without a contract
   Statistical weirdness is not automatically invalid data.

9. Overengineered validation frameworks
   Too much abstraction makes ordinary business rules harder to inspect.

10. No reconciliation
    Rows can disappear through implementation mistakes even when rejects exist.
'''


# =============================================================================
# 17. PHASE 10 DESIGN REVIEW CHECKLIST
# =============================================================================

DESIGN_REVIEW_CHECKLIST = '''
CONTRACT
[ ] Grain is stated explicitly.
[ ] Required columns and Spark data types are explicit.
[ ] Required values are validated independently of schema metadata.
[ ] Domain, range, and business-rule ownership is clear.

KEYS AND RELATIONSHIPS
[ ] Primary-key uniqueness is validated.
[ ] Composite keys are validated as combinations.
[ ] Exact duplicates and duplicate business keys are distinguished.
[ ] Referential integrity uses the correct relationship key.
[ ] Parent/dimension grain is validated before downstream joins rely on it.

REJECTIONS
[ ] Invalid rows are not silently discarded.
[ ] Rejection reasons are machine-readable and diagnostic.
[ ] One row can retain multiple reasons where useful.
[ ] Quarantine preserves original business evidence and run metadata.

ACCEPTED DATA
[ ] Only contract-valid rows enter downstream transformations.
[ ] Validation does not accidentally change accepted-record grain.

METRICS
[ ] input_count = accepted_count + rejected_count.
[ ] Rejection counts are available by reason.
[ ] Per-reason counts may exceed rejected-row count when rows fail multiple rules.

ARCHITECTURE
[ ] Validation mechanics are reusable.
[ ] Dataset-specific business rules remain visible.
[ ] Validation is separate from business transformations.
[ ] Abstraction exists only where repeated requirements justify it.

SPARK EXECUTION
[ ] Row rules, uniqueness shuffles, and RI joins are understood separately.
[ ] Metrics/actions are deliberate.
[ ] Large reject populations stay distributed.
[ ] Persistence is justified by real reuse rather than habit.
'''


# =============================================================================
# 18. RUN THE LECTURE
# =============================================================================


if __name__ == '__main__':
    practice_orders_df, practice_customers_df, practice_inventory_df = (
        build_practice_data(spark)
    )

    print('\nRAW ORDERS')
    practice_orders_df.orderBy(
        F.col('order_id').asc_nulls_first()
    ).show(truncate=False)

    print('\nCUSTOMERS')
    practice_customers_df.orderBy('customer_id').show(truncate=False)

    print('\nINVENTORY')
    practice_inventory_df.orderBy(
        'snapshot_date',
        'store_id',
        'product_id',
    ).show(truncate=False)

    # -------------------------------------------------------------------------
    # ORDERS QUALITY LAYER
    # -------------------------------------------------------------------------

    validated_orders_df, accepted_orders_df, rejected_orders_df = (
        validate_and_split_orders(
            practice_orders_df,
            practice_customers_df,
        )
    )

    print('\nVALIDATED ORDERS WITH REJECTION REASONS')
    (
        validated_orders_df
        .orderBy(F.col('order_id').asc_nulls_first())
        .show(truncate=False)
    )

    print('\nACCEPTED ORDERS')
    accepted_orders_df.orderBy('order_id').show(truncate=False)

    print('\nREJECTED ORDERS')
    (
        rejected_orders_df
        .orderBy(F.col('order_id').asc_nulls_first())
        .show(truncate=False)
    )

    # Reconciliation proves every input row was classified.
    assert_validation_reconciles(validated_orders_df)

    print('\nORDERS VALIDATION SUMMARY')
    (
        build_validation_summary(
            validated_orders_df,
            dataset_name='orders',
        )
        .show(truncate=False)
    )

    print('\nORDERS REJECTION COUNTS BY REASON')
    (
        build_rejection_reason_counts(rejected_orders_df)
        .orderBy(F.col('count').desc(), 'rejection_reason')
        .show(truncate=False)
    )

    print('\nORDERS QUARANTINE')
    orders_quarantine_df = build_quarantine(
        rejected_orders_df,
        source_dataset='orders',
        validation_run_date=date(2026, 9, 7),
    )
    (
        orders_quarantine_df
        .orderBy(F.col('order_id').asc_nulls_first())
        .show(truncate=False)
    )

    # -------------------------------------------------------------------------
    # DUPLICATE DIAGNOSTICS
    # -------------------------------------------------------------------------

    demonstrate_duplicate_diagnostics(practice_orders_df)

    # -------------------------------------------------------------------------
    # COMPOSITE-KEY VALIDATION
    # -------------------------------------------------------------------------

    validated_inventory_df = add_inventory_reasons(
        practice_inventory_df
    )
    accepted_inventory_df, rejected_inventory_df = (
        split_accepted_rejected(validated_inventory_df)
    )

    print('\nVALIDATED INVENTORY')
    (
        validated_inventory_df
        .orderBy('snapshot_date', 'store_id', 'product_id')
        .show(truncate=False)
    )

    print('\nACCEPTED INVENTORY')
    (
        accepted_inventory_df
        .orderBy('snapshot_date', 'store_id', 'product_id')
        .show(truncate=False)
    )

    print('\nREJECTED INVENTORY')
    (
        rejected_inventory_df
        .orderBy('snapshot_date', 'store_id', 'product_id')
        .show(truncate=False)
    )

    assert_validation_reconciles(validated_inventory_df)

    # -------------------------------------------------------------------------
    # DOWNSTREAM TRANSFORMATION RECEIVES ACCEPTED DATA ONLY
    # -------------------------------------------------------------------------

    print('\nDOWNSTREAM ENRICHED ACCEPTED ORDERS')
    (
        transform_accepted_orders(
            accepted_orders_df,
            practice_customers_df,
        )
        .orderBy('order_id')
        .show(truncate=False)
    )

    print('\nDISTRIBUTED EXECUTION REFERENCE')
    print(DISTRIBUTED_EXECUTION_REFERENCE)

    print('\nCOMMON FAILURE MODES')
    print(COMMON_FAILURE_MODES)

    print('\nPHASE 10 DESIGN REVIEW CHECKLIST')
    print(DESIGN_REVIEW_CHECKLIST)

    spark.stop()
