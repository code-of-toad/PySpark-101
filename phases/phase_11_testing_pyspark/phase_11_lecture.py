#!/usr/bin/env python
'''PySpark 101 — Phase 11 lecture.

Testing PySpark through deterministic fixtures, focused transformation checks,
schema assertions, grain and uniqueness tests, referential-integrity tests,
numerical reconciliation, accepted/rejected validation checks, integration
checks, edge cases, and mutation-style regression detection.

This is lecture-only code.

The governing principle is:

    business/data contract
        -> deterministic fixture
        -> production responsibility
        -> assertion
        -> regression protection

The Phase 11 distinction is:

    unit test
        -> one transformation or validation responsibility

    integration test
        -> multiple meaningful pipeline responsibilities working together

The lecture deliberately reuses the existing Phase 9 and Phase 10 lecture
modules as the subjects under test. The actual pytest suite should import
production/reusable logic rather than copy business logic into tests.

Examples target PySpark 4.2.0 and deterministic retail-domain data.
'''

from datetime import date
from decimal import Decimal
import importlib.util
from pathlib import Path
from types import ModuleType

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType


# =============================================================================
# 0. SPARK SESSION AND PHASE 11 MENTAL MODEL
# =============================================================================

spark = (
    SparkSession.builder
    .appName('phase_11_testing_pyspark')
    .master('local[2]')
    # Keep tiny teaching shuffles predictable and inexpensive.
    .config('spark.sql.shuffle.partitions', '2')
    .getOrCreate()
)

spark.sparkContext.setLogLevel('WARN')

# Phase 11 is NOT:
#
#     result_df = transform(...)
#     assert result_df is not None
#
# It IS:
#
#     contract
#       -> smallest deterministic fixture
#       -> production responsibility
#       -> focused assertion
#       -> meaningful regression protection
#
# Correctness layers:
#
#     structure
#         schema
#         columns
#         Spark data types
#
#     row semantics
#         filters
#         derived measures
#         rejection reasons
#
#     dataset semantics
#         row population
#         grain
#         uniqueness
#
#     relationships
#         referential integrity
#         parent-key uniqueness
#
#     reconciliation
#         input = accepted + rejected
#         input measures = aggregated output measures
#
#     determinism
#         same logical input -> same logical output


# =============================================================================
# 1. LOAD EXISTING PHASE 9 / 10 SUBJECTS UNDER TEST
# =============================================================================


def load_module_from_path(
    module_name: str,
    module_path: Path,
) -> ModuleType:
    '''Load one existing lecture module so tests call real preserved logic.'''

    # WHAT: load the existing phase module directly from its file path.
    # WHY: Phase 11 should test the logic already built instead of copying that
    # logic into the test implementation.
    spec = importlib.util.spec_from_file_location(
        module_name,
        module_path,
    )

    if spec is None or spec.loader is None:
        raise ImportError(f'Unable to load module from {module_path}.')

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    return module


phase_11_dir = Path(__file__).resolve().parent
phases_dir = phase_11_dir.parent

phase_09 = load_module_from_path(
    'phase_09_lecture_for_testing',
    phases_dir
    / 'phase_09_pyspark_application_architecture'
    / 'phase_09_lecture.py',
)

phase_10 = load_module_from_path(
    'phase_10_lecture_for_testing',
    phases_dir
    / 'phase_10_data_quality_schema_enforcement'
    / 'phase_10_lecture.py',
)

# Important:
#
# The lecture modules are convenient preserved teaching subjects.
#
# A production-style repository would normally expose reusable logic through
# importable modules such as:
#
#     src/retail_pipeline/transformations.py
#     src/retail_pipeline/validation.py
#     src/retail_pipeline/schemas.py
#
# Then pytest modules would import those package modules directly.


# =============================================================================
# 2. PYTEST STRUCTURE REFERENCE
# =============================================================================

PYTEST_STRUCTURE_REFERENCE = r'''
A practical test layout:

tests/
|-- conftest.py
|-- test_transformations.py
|-- test_validation.py
'-- test_pipeline_integration.py


conftest.py:

    import pytest

    from pyspark.sql import SparkSession


    @pytest.fixture(scope='session')
    def spark():
        # Reuse one local Spark runtime across the test session.
        spark_session = (
            SparkSession.builder
            .master('local[2]')
            .appName('phase_11_tests')
            .config('spark.sql.shuffle.partitions', '2')
            .getOrCreate()
        )

        spark_session.sparkContext.setLogLevel('WARN')

        yield spark_session

        spark_session.stop()


test module:

    def test_filter_orders_keeps_only_qualifying_rows(spark):
        # Arrange.
        orders_df = ...

        # Act.
        actual_df = filter_orders(...)

        # Assert.
        assert ...


Core shape:

    ARRANGE
    deterministic input + expected contract

    ACT
    call production code

    ASSERT
    prove observable business/data correctness
'''


# =============================================================================
# 3. SMALL DETERMINISTIC FIXTURE BUILDERS
# =============================================================================


def build_filter_fixture() -> DataFrame:
    '''Build the smallest fixture that distinguishes three filter branches.'''

    schema = StructType(
        [
            StructField('order_id', StringType(), nullable=False),
            StructField('customer_id', StringType(), nullable=False),
            StructField('order_status', StringType(), nullable=False),
            StructField(
                'net_sales',
                DecimalType(12, 2),
                nullable=False,
            ),
        ]
    )

    rows = [
        # Included status + valid amount -> keep.
        ('O001', 'C001', 'COMPLETED', Decimal('125.00')),
        # Excluded status -> remove.
        ('O002', 'C002', 'CANCELLED', Decimal('80.00')),
        # Included status but below minimum -> remove.
        ('O003', 'C003', 'COMPLETED', Decimal('-1.00')),
    ]

    return spark.createDataFrame(rows, schema=schema)


def build_province_sales_fixture() -> DataFrame:
    '''Build a tiny enriched order dataset for aggregation testing.'''

    schema = StructType(
        [
            StructField('order_id', StringType(), nullable=False),
            StructField('province', StringType(), nullable=False),
            StructField(
                'net_sales',
                DecimalType(12, 2),
                nullable=False,
            ),
        ]
    )

    rows = [
        ('O001', 'ON', Decimal('125.00')),
        ('O002', 'ON', Decimal('75.00')),
        ('O003', 'BC', Decimal('200.00')),
    ]

    return spark.createDataFrame(rows, schema=schema)


def build_orphan_fixture() -> tuple[DataFrame, DataFrame]:
    '''Build one matched child and one orphan child relationship.'''

    orders_rows = [
        ('O001', 'C001', date(2026, 9, 1), 'COMPLETED', Decimal('25.00')),
        ('O002', 'C999', date(2026, 9, 1), 'COMPLETED', Decimal('30.00')),
    ]

    customers_rows = [
        ('C001', 'Alice Wong', 'ON', 'CONSUMER'),
    ]

    orders_df = spark.createDataFrame(
        orders_rows,
        schema=phase_10.ORDERS_SCHEMA,
    )

    customers_df = spark.createDataFrame(
        customers_rows,
        schema=phase_10.CUSTOMERS_SCHEMA,
    )

    return orders_df, customers_df


# Deterministic fixture principles:
#
#     small enough to inspect mentally
#     exact values
#     explicit schemas
#     explicit dates
#     exact Decimal currency
#     stable business keys
#     one deliberate scenario per unit fixture
#
# Large generated data is unnecessary unless scale itself is under test.


# =============================================================================
# 4. REUSABLE TEST ASSERTION HELPERS
# =============================================================================


def collect_rows_by(
    df: DataFrame,
    *ordering_columns: str,
) -> list:
    '''Collect a tiny test DataFrame after deterministic ordering.'''

    # WHAT: sort only tiny assertion data before collecting.
    # WHY: Spark does not guarantee row presentation order.
    return df.orderBy(*ordering_columns).collect()


def assert_unique_key(
    df: DataFrame,
    key_columns: list[str],
) -> None:
    '''Assert that the declared business/composite key is unique.'''

    duplicate_exists = (
        df
        .groupBy(*key_columns)
        .count()
        .filter(F.col('count') > 1)
        .limit(1)
        .count()
        > 0
    )

    assert not duplicate_exists, (
        f'Expected unique key {key_columns}, but duplicate keys exist.'
    )


def assert_no_orphans(
    child_df: DataFrame,
    parent_df: DataFrame,
    key_columns: list[str],
) -> None:
    '''Assert that every child key has a matching parent key.'''

    orphan_exists = (
        child_df
        .select(*key_columns)
        .join(
            parent_df.select(*key_columns),
            on=key_columns,
            how='left_anti',
        )
        .limit(1)
        .count()
        > 0
    )

    assert not orphan_exists, (
        f'Orphan child key exists for relationship {key_columns}.'
    )


def assert_decimal_measure_reconciles(
    input_df: DataFrame,
    output_df: DataFrame,
    column_name: str,
) -> None:
    '''Assert exact measure conservation across a transformation boundary.'''

    input_value = (
        input_df
        .agg(F.sum(column_name).alias('value'))
        .first()['value']
    )

    output_value = (
        output_df
        .agg(F.sum(column_name).alias('value'))
        .first()['value']
    )

    assert output_value == input_value, (
        f'{column_name} failed reconciliation: '
        f'input={input_value}, output={output_value}.'
    )


# Test helpers should express repeated assertion mechanics only.
#
# Avoid helpers so abstract that a failing test no longer communicates the
# business/data contract being protected.


# =============================================================================
# 5. UNIT TEST — FILTER TRANSFORMATION
# =============================================================================


def check_filter_orders() -> None:
    '''Unit check for one Phase 9 filtering responsibility.'''

    orders_df = build_filter_fixture()

    actual_df = phase_09.filter_orders(
        orders_df,
        included_statuses=('COMPLETED',),
        minimum_net_sales=Decimal('0.00'),
    )

    actual_order_ids = {
        row['order_id']
        for row in actual_df.select('order_id').collect()
    }

    # Exact business identity matters more than a count of one.
    assert actual_order_ids == {'O001'}

    # Filtering should not create duplicate business keys.
    assert_unique_key(actual_df, ['order_id'])


# This is a unit check because it protects one responsibility:
#
#     filter_orders()
#
# It does not involve:
#
#     readers
#     writers
#     validation
#     aggregation
#     orchestration


# =============================================================================
# 6. UNIT TEST — DETERMINISTIC LATEST-RECORD SELECTION
# =============================================================================


def check_latest_customer_record() -> None:
    '''Unit check for the Phase 9 deterministic window tie-breaker.'''

    history_df = spark.createDataFrame(
        [
            # The effective dates tie deliberately.
            ('R001', 'C001', date(2026, 5, 1), 'ON'),
            ('R002', 'C001', date(2026, 5, 1), 'QC'),
        ],
        schema=phase_09.CUSTOMER_HISTORY_SCHEMA,
    )

    actual_df = phase_09.select_latest_customer_record(history_df)

    actual_row = actual_df.first()

    # The production contract uses customer_record_id DESC as the stable
    # secondary tie-breaker, so R002 must win.
    assert actual_row['customer_record_id'] == 'R002'
    assert actual_row['province'] == 'QC'


# A test without tied effective_date values would NOT prove deterministic
# tie-breaking.


# =============================================================================
# 7. UNIT TEST — AGGREGATION VALUES, GRAIN, AND RECONCILIATION
# =============================================================================


def check_sales_by_province() -> None:
    '''Unit check for province-grain aggregation correctness.'''

    enriched_orders_df = build_province_sales_fixture()

    actual_df = phase_09.build_sales_by_province(enriched_orders_df)

    actual_rows = collect_rows_by(actual_df, 'province')

    actual = [
        (
            row['province'],
            row['order_count'],
            row['net_sales'],
        )
        for row in actual_rows
    ]

    expected = [
        ('BC', 1, Decimal('200.00')),
        ('ON', 2, Decimal('200.00')),
    ]

    # Protect exact groups and measures.
    assert actual == expected

    # Protect the declared output grain: one row per province.
    assert_unique_key(actual_df, ['province'])

    # Aggregation should conserve the qualifying net_sales measure.
    assert_decimal_measure_reconciles(
        enriched_orders_df,
        actual_df,
        column_name='net_sales',
    )


# Row count alone would be incomplete:
#
#     two output rows
#
# does not prove:
#
#     correct provinces
#     correct order_count
#     correct net_sales
#     correct grain
#     measure conservation


# =============================================================================
# 8. SCHEMA TESTING
# =============================================================================


def check_processing_date_schema() -> None:
    '''Assert both data and Spark type for an injected processing date.'''

    source_df = spark.createDataFrame(
        [
            ('ON', 2, Decimal('200.00')),
        ],
        schema=StructType(
            [
                StructField('province', StringType(), nullable=False),
                StructField('order_count', StringType(), nullable=False),
                StructField(
                    'net_sales',
                    DecimalType(12, 2),
                    nullable=False,
                ),
            ]
        ),
    )

    # The input order_count type is irrelevant to this focused unit check.
    actual_df = phase_09.add_processing_date(
        source_df,
        run_date=date(2026, 9, 7),
    )

    actual_types = {
        field.name: field.dataType.simpleString()
        for field in actual_df.schema.fields
    }

    # Protect structural contract.
    assert actual_types['processing_date'] == 'date'

    # Protect actual deterministic value.
    assert actual_df.first()['processing_date'] == date(2026, 9, 7)


# Schema assertions and data assertions solve different problems:
#
#     processing_date is a date
#
#     processing_date equals configured run date
#
# Both can matter.


# =============================================================================
# 9. PHASE 10 UNIT TEST — ROW-LEVEL REJECTION REASONS
# =============================================================================


def check_row_level_rejection_reasons() -> None:
    '''Unit check for Phase 10 row-local validation behavior.'''

    orders_df = spark.createDataFrame(
        [
            (
                'O001',
                'C001',
                date(2026, 9, 1),
                'COMPLETED',
                Decimal('25.00'),
            ),
            (
                'O002',
                None,
                date(2026, 9, 1),
                'COMPLETED',
                Decimal('-1.00'),
            ),
        ],
        schema=phase_10.ORDERS_SCHEMA,
    )

    actual_df = phase_10.add_orders_row_level_reasons(orders_df)

    rejected_row = (
        actual_df
        .filter(F.col('order_id') == 'O002')
        .first()
    )

    actual_reasons = set(rejected_row['rejection_reasons'])

    # One source row can legitimately preserve multiple diagnostics.
    assert actual_reasons == {
        'MISSING_CUSTOMER_ID',
        'NEGATIVE_NET_SALES',
    }


# Do not test a multiple-reason contract by depending on accidental array order
# unless reason order itself is explicitly part of the business contract.


# =============================================================================
# 10. PHASE 10 UNIT TEST — DUPLICATE PRIMARY KEY
# =============================================================================


def check_duplicate_order_reason() -> None:
    '''Unit check that every conflicting duplicate-key row is marked.'''

    orders_df = spark.createDataFrame(
        [
            ('O001', 'C001', date(2026, 9, 1), 'COMPLETED', Decimal('10.00')),
            ('O001', 'C001', date(2026, 9, 1), 'COMPLETED', Decimal('15.00')),
            ('O002', 'C001', date(2026, 9, 1), 'COMPLETED', Decimal('20.00')),
        ],
        schema=phase_10.ORDERS_SCHEMA,
    )

    row_validated_df = phase_10.add_orders_row_level_reasons(orders_df)

    actual_df = phase_10.add_duplicate_order_reason(row_validated_df)

    duplicate_rows = (
        actual_df
        .filter(F.col('order_id') == 'O001')
        .select('rejection_reasons')
        .collect()
    )

    # Both conflicting business rows should be diagnosed.
    assert len(duplicate_rows) == 2

    for row in duplicate_rows:
        assert 'DUPLICATE_ORDER_ID' in row['rejection_reasons']


# A test that checks only one surviving O001 row after dropDuplicates() would
# hide the exact contract violation Phase 10 was designed to expose.


# =============================================================================
# 11. PHASE 10 UNIT TEST — REFERENTIAL INTEGRITY
# =============================================================================


def check_orphan_customer_reason() -> None:
    '''Unit check for cross-dataset foreign-key validation.'''

    orders_df, customers_df = build_orphan_fixture()

    validated_df = phase_10.validate_orders(
        orders_df,
        customers_df,
    )

    accepted_df, rejected_df = phase_10.split_accepted_rejected(
        validated_df
    )

    # Matched child remains accepted.
    accepted_ids = {
        row['order_id']
        for row in accepted_df.select('order_id').collect()
    }
    assert accepted_ids == {'O001'}

    # Orphan child is rejected with the correct diagnostic.
    orphan_row = (
        rejected_df
        .filter(F.col('order_id') == 'O002')
        .first()
    )

    assert 'ORPHAN_CUSTOMER_ID' in orphan_row['rejection_reasons']

    # Accepted child data must satisfy referential integrity directly.
    assert_no_orphans(
        accepted_df,
        customers_df,
        ['customer_id'],
    )


# Referential integrity:
#
#     child has a parent
#
# Parent grain:
#
#     expected parent key is unique
#
# These are separate contracts and should be tested separately.


# =============================================================================
# 12. PHASE 10 UNIT TEST — COMPOSITE KEY
# =============================================================================


def check_composite_inventory_key() -> None:
    '''Unit check for composite-key uniqueness diagnostics.'''

    inventory_df = spark.createDataFrame(
        [
            (date(2026, 9, 5), 'S001', 'P001', 10),
            (date(2026, 9, 5), 'S001', 'P001', 12),
            (date(2026, 9, 5), 'S001', 'P002', 7),
        ],
        schema=phase_10.INVENTORY_SCHEMA,
    )

    validated_df = phase_10.add_inventory_reasons(inventory_df)

    duplicate_rows = (
        validated_df
        .filter(
            (F.col('snapshot_date') == F.lit(date(2026, 9, 5)))
            & (F.col('store_id') == 'S001')
            & (F.col('product_id') == 'P001')
        )
        .collect()
    )

    assert len(duplicate_rows) == 2

    for row in duplicate_rows:
        assert 'DUPLICATE_INVENTORY_KEY' in row['rejection_reasons']


# The contract is:
#
#     snapshot_date + store_id + product_id
#
# Each component may repeat independently. The combination must be unique.


# =============================================================================
# 13. INTEGRATION TEST — VALIDATE, SPLIT, TRANSFORM, AGGREGATE
# =============================================================================


def check_validation_transformation_integration() -> None:
    '''Integration check across Phase 10 validation and Phase 9 transformation.'''

    orders_df = spark.createDataFrame(
        [
            # Valid ON order.
            ('O001', 'C001', date(2026, 9, 1), 'COMPLETED', Decimal('100.00')),
            # Valid BC order.
            ('O002', 'C002', date(2026, 9, 1), 'COMPLETED', Decimal('50.00')),
            # Invalid orphan order.
            ('O003', 'C999', date(2026, 9, 1), 'COMPLETED', Decimal('25.00')),
        ],
        schema=phase_10.ORDERS_SCHEMA,
    )

    customers_df = spark.createDataFrame(
        [
            ('C001', 'Alice Wong', 'ON', 'CONSUMER'),
            ('C002', 'Carla Singh', 'BC', 'BUSINESS'),
        ],
        schema=phase_10.CUSTOMERS_SCHEMA,
    )

    # Responsibility 1: validate the source contract.
    validated_df = phase_10.validate_orders(
        orders_df,
        customers_df,
    )

    # Responsibility 2: classify source rows.
    accepted_df, rejected_df = phase_10.split_accepted_rejected(
        validated_df
    )

    # Responsibility 3: transform only accepted rows.
    enriched_df = phase_10.transform_accepted_orders(
        accepted_df,
        customers_df,
    )

    # Responsibility 4: aggregate accepted business measures.
    result_df = phase_09.build_sales_by_province(enriched_df)

    result_rows = collect_rows_by(result_df, 'province')

    actual = [
        (row['province'], row['order_count'], row['net_sales'])
        for row in result_rows
    ]

    expected = [
        ('BC', 1, Decimal('50.00')),
        ('ON', 1, Decimal('100.00')),
    ]

    assert actual == expected

    # Classification must reconcile every interpretable source row.
    assert orders_df.count() == accepted_df.count() + rejected_df.count()

    # Final province output must preserve accepted sales value exactly.
    assert_decimal_measure_reconciles(
        accepted_df,
        result_df,
        column_name='net_sales',
    )

    # One row per province.
    assert_unique_key(result_df, ['province'])


# This is an integration test because multiple meaningful responsibilities must
# work together:
#
#     Phase 10 validation
#     -> accepted/rejected classification
#     -> Phase 10 accepted-order enrichment
#     -> Phase 9 aggregation
#     -> reconciliation
#
# It is still intentionally tiny. Integration does not mean production scale.


# =============================================================================
# 14. EDGE-CASE TESTS
# =============================================================================


def check_empty_input() -> None:
    '''Edge check: explicit schema keeps empty input testable.'''

    empty_orders_df = spark.createDataFrame(
        [],
        schema=phase_10.ORDERS_SCHEMA,
    )

    customers_df = spark.createDataFrame(
        [],
        schema=phase_10.CUSTOMERS_SCHEMA,
    )

    validated_df = phase_10.validate_orders(
        empty_orders_df,
        customers_df,
    )

    accepted_df, rejected_df = phase_10.split_accepted_rejected(
        validated_df
    )

    assert validated_df.count() == 0
    assert accepted_df.count() == 0
    assert rejected_df.count() == 0


def check_numeric_boundary() -> None:
    '''Edge check for the exact minimum-sales boundary.'''

    orders_df = spark.createDataFrame(
        [
            ('O001', 'C001', 'COMPLETED', Decimal('-0.01')),
            ('O002', 'C001', 'COMPLETED', Decimal('0.00')),
            ('O003', 'C001', 'COMPLETED', Decimal('0.01')),
        ],
        schema=StructType(
            [
                StructField('order_id', StringType(), nullable=False),
                StructField('customer_id', StringType(), nullable=False),
                StructField('order_status', StringType(), nullable=False),
                StructField(
                    'net_sales',
                    DecimalType(12, 2),
                    nullable=False,
                ),
            ]
        ),
    )

    actual_df = phase_09.filter_orders(
        orders_df,
        included_statuses=('COMPLETED',),
        minimum_net_sales=Decimal('0.00'),
    )

    actual_ids = {
        row['order_id']
        for row in actual_df.select('order_id').collect()
    }

    # Boundary contract: >= 0.00.
    assert actual_ids == {'O002', 'O003'}


# Boundary values often reveal defects that happy-path values never exercise.


# =============================================================================
# 15. DETERMINISTIC DATAFRAME COMPARISON REFERENCE
# =============================================================================

DATAFRAME_COMPARISON_REFERENCE = r'''
Spark DataFrames do not guarantee row presentation order.

FRAGILE:

    assert actual_df.collect() == expected_df.collect()


GOOD FOR TINY FIXTURES WITH A STABLE SORT KEY:

    actual_rows = actual_df.orderBy('order_id').collect()
    expected_rows = expected_df.orderBy('order_id').collect()

    assert actual_rows == expected_rows


PYSPARK TESTING HELPER:

    from pyspark.testing.utils import assertDataFrameEqual

    assertDataFrameEqual(
        actual_df,
        expected_df,
        checkRowOrder=False,
    )


SCHEMA HELPER:

    from pyspark.testing.utils import assertSchemaEqual

    assertSchemaEqual(
        actual_df.schema,
        EXPECTED_SCHEMA,
    )


Principles:

    compare logical data, not incidental partition output order
    keep assertions focused on the contract under test
    do not weaken schema/numeric checks merely to make tests pass
    do not add expensive production orderBy() only for test convenience
'''


# =============================================================================
# 16. DISTRIBUTED-EXECUTION TESTING REFERENCE
# =============================================================================

DISTRIBUTED_TESTING_REFERENCE = r'''
A PySpark test still executes Spark work.

ACTIONS USED IN TESTS:

    count()
    collect()
    first()

These can launch Spark jobs.

Example:

    df.groupBy('province').agg(...)
        -> lazy transformation

    df.count()
        -> action
        -> Spark job


TESTING PRINCIPLES:

    keep fixtures tiny
    understand that assertions may materialize Spark work
    collect only intentionally small test populations
    do not cache every fixture by habit
    do not assume row order
    do not couple business tests to incidental physical-plan choices
    restore mutable shared SparkSession state when a test changes it


LOCAL MODE:

    local[2]

still exercises:

    Catalyst planning
    joins
    shuffles
    aggregations
    Spark SQL types
    lazy evaluation

A cluster is not required to prove most logical correctness defects.
'''


# =============================================================================
# 17. COMMON BAD TESTING PATTERNS
# =============================================================================

COMMON_BAD_TESTING_PATTERNS = r'''
1. assert result_df is not None
   Proves almost nothing about business correctness.

2. Row-count-only assertions
   The wrong rows can still produce the expected count.

3. Copying production logic into expected-result code
   Production and test can reproduce the same bug.

4. Comparing unordered collect() results
   Spark does not guarantee row presentation order.

5. Huge generated fixtures
   Slow feedback and poor diagnostic clarity.

6. One giant dirty fixture for every unit test
   Unrelated defects make failures hard to understand.

7. Mocking DataFrames instead of using tiny real Spark DataFrames
   Transformation semantics are better tested against Spark itself.

8. Testing implementation syntax
   Equivalent safe refactoring should not break business tests.

9. Schema-only testing
   Correct types do not prove correct values.

10. Data-only testing
    Correct values do not prove the intended output schema.

11. Loose numeric tolerances without justification
    Can hide real measure corruption.

12. dropDuplicates() inside the test
    Can hide a grain violation instead of detecting it.

13. Wall-clock dates or unseeded randomness
    Make tests non-replayable.

14. Only happy-path tests
    Validation behavior cannot be proven without invalid input.

15. One giant end-to-end test
    Failure diagnosis becomes unnecessarily difficult.
'''


# =============================================================================
# 18. MUTATION-STYLE MASTERY DEMONSTRATION
# =============================================================================


def deliberately_broken_sales_by_province(
    enriched_orders_df: DataFrame,
) -> DataFrame:
    '''Deliberately wrong aggregation used only to prove regression detection.'''

    # WRONG ON PURPOSE:
    # avg(net_sales) corrupts a measure that should use sum(net_sales).
    return (
        enriched_orders_df
        .groupBy('province')
        .agg(
            F.countDistinct('order_id').alias('order_count'),
            F.avg('net_sales').alias('net_sales'),
        )
    )


def check_regression_detection() -> None:
    '''Prove that a meaningful bad pipeline change triggers an assertion.'''

    input_df = build_province_sales_fixture()

    broken_df = deliberately_broken_sales_by_province(input_df)

    regression_detected = False

    try:
        # The business contract requires measure conservation.
        assert_decimal_measure_reconciles(
            input_df,
            broken_df,
            column_name='net_sales',
        )
    except AssertionError:
        regression_detected = True

    assert regression_detected, (
        'Expected the reconciliation test to detect the deliberate regression.'
    )


# This is the Phase 11 mastery idea:
#
#     passing suite
#         -> introduce meaningful wrong change
#         -> relevant test fails
#         -> violated contract is identifiable
#         -> restore correct implementation
#         -> suite passes again
#
# Tests existing is not enough.
#
# The suite must be capable of detecting actual data corruption.


# =============================================================================
# 19. PHASE 11 DESIGN REVIEW CHECKLIST
# =============================================================================

DESIGN_REVIEW_CHECKLIST = r'''
TEST ORGANIZATION
[ ] pytest discovers the suite cleanly.
[ ] Shared Spark setup is centralized in a reusable fixture.
[ ] Production logic is imported instead of copied.
[ ] Unit and integration responsibilities are distinguishable.

FIXTURES
[ ] Fixtures are small and deterministic.
[ ] Explicit production schemas are reused where appropriate.
[ ] Runtime dates/randomness/ties are controlled.
[ ] Unit fixtures contain only the rows needed to prove one behavior.

TRANSFORMATIONS
[ ] Filters preserve exactly the correct rows.
[ ] Joins preserve intended grain.
[ ] Aggregations produce correct groups and measures.
[ ] Window tie-breaking is tested deliberately.

DATAFRAME COMPARISON
[ ] Tests do not assume undefined row order.
[ ] Whole-DataFrame equality is used only when appropriate.
[ ] Focused projections/keys are preferred when clearer.

SCHEMA
[ ] Required columns and important Spark types are protected.
[ ] Nullability is tested only when contractually meaningful.
[ ] Schema assertions do not replace data assertions.

GRAIN AND KEYS
[ ] Primary-key uniqueness is protected.
[ ] Composite keys are tested as combinations.
[ ] Parent/dimension grain is protected before joins rely on it.
[ ] Row-count assertions are supported by identity/value assertions.

REFERENTIAL INTEGRITY
[ ] Accepted child rows have valid parents.
[ ] Orphan cases are introduced deliberately.
[ ] Orphan rejection reasons are verified.

DATA QUALITY
[ ] Known-valid rows reach accepted output.
[ ] Known-invalid rows reach rejected output.
[ ] Correct rejection reasons are preserved.
[ ] Multiple simultaneous rejection reasons are tested.
[ ] Duplicate conflicting rows are all diagnosed according to policy.

RECONCILIATION
[ ] input_count = accepted_count + rejected_count.
[ ] Aggregated measures reconcile to qualifying input measures.
[ ] Derived measures satisfy row-level formulas.
[ ] Decimal business values use exact assertions when appropriate.

EDGE CASES
[ ] Empty input is covered.
[ ] Boundary values are covered.
[ ] Duplicate and orphan scenarios are covered.
[ ] All-accepted / all-rejected / no-match populations are considered.

SPARK EXECUTION
[ ] collect() is limited to tiny fixtures.
[ ] Assertions that trigger Spark jobs are understood.
[ ] Shared SparkSession state does not leak unpredictably.
[ ] Business tests are not coupled to incidental plan choices.

REGRESSION PROTECTION
[ ] Important past defects become permanent tests.
[ ] A meaningful deliberately bad change causes the suite to fail.
[ ] The failing test identifies the violated contract.
'''


# =============================================================================
# 20. RUN THE LECTURE CHECKS
# =============================================================================


if __name__ == '__main__':
    print('\nPYTEST STRUCTURE REFERENCE')
    print(PYTEST_STRUCTURE_REFERENCE)

    checks = [
        ('filter_orders', check_filter_orders),
        ('latest_customer_record', check_latest_customer_record),
        ('sales_by_province', check_sales_by_province),
        ('processing_date_schema', check_processing_date_schema),
        ('row_level_rejection_reasons', check_row_level_rejection_reasons),
        ('duplicate_order_reason', check_duplicate_order_reason),
        ('orphan_customer_reason', check_orphan_customer_reason),
        ('composite_inventory_key', check_composite_inventory_key),
        (
            'validation_transformation_integration',
            check_validation_transformation_integration,
        ),
        ('empty_input', check_empty_input),
        ('numeric_boundary', check_numeric_boundary),
        ('regression_detection', check_regression_detection),
    ]

    for check_name, check_function in checks:
        check_function()
        print(f'PASS | {check_name}')

    print('\nDATAFRAME COMPARISON REFERENCE')
    print(DATAFRAME_COMPARISON_REFERENCE)

    print('\nDISTRIBUTED TESTING REFERENCE')
    print(DISTRIBUTED_TESTING_REFERENCE)

    print('\nCOMMON BAD TESTING PATTERNS')
    print(COMMON_BAD_TESTING_PATTERNS)

    print('\nPHASE 11 DESIGN REVIEW CHECKLIST')
    print(DESIGN_REVIEW_CHECKLIST)

    spark.stop()
