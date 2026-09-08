#!/usr/bin/env python
'''PySpark 101 — Phase 9 lecture.

PySpark application architecture through separation of concerns, reusable
transformations, explicit configuration, deterministic behavior, logging,
exception handling, dependency management, packaging, and testable design.

This is lecture-only code.

The file deliberately teaches all application layers in one place so their
relationships are easy to inspect. A production-style application would split
these responsibilities into modules such as:

    config.py
    schemas.py
    readers.py
    validation.py
    transformations.py
    writers.py
    pipeline.py

The governing principle is:

    DataFrame(s)
        -> business transformation
        -> DataFrame

Business logic should not own paths, credentials, environment detection,
write destinations, scheduling, or deployment concerns.

Examples target PySpark 4.2.0 and deterministic retail-domain data.
'''

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
import logging
from pathlib import Path
from tempfile import TemporaryDirectory

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import DateType
from pyspark.sql.types import DecimalType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType


# =============================================================================
# 0. SPARK SESSION AND PHASE 9 MENTAL MODEL
# =============================================================================

spark = (
    SparkSession.builder
    .appName('phase_09_pyspark_application_architecture')
    # Local mode keeps the lecture runnable while preserving Spark semantics.
    .master('local[4]')
    .config('spark.sql.shuffle.partitions', '8')
    .getOrCreate()
)

spark.sparkContext.setLogLevel('WARN')

# Phase 9 is NOT:
#
#     giant_script.py
#         -> reads
#         -> validates
#         -> transforms
#         -> writes
#         -> knows every path
#         -> knows every environment
#         -> catches every exception
#
# It IS:
#
#     configuration
#         -> orchestration
#         -> readers
#         -> validation
#         -> transformations
#         -> writers
#
# The stable center is:
#
#     DataFrame(s)
#         -> transformation function
#         -> DataFrame
#
# That shape makes business logic easier to understand, reuse, and test.


# =============================================================================
# 1. LOGGING
# =============================================================================


def configure_logging() -> logging.Logger:
    '''Create one application logger with a concise, useful format.'''

    # Configure once at the application boundary rather than inside every
    # transformation function.
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    )

    return logging.getLogger('phase_09')


logger = configure_logging()

# Useful logging usually describes:
#
#     pipeline start / finish
#     environment
#     important input/output locations
#     selected run parameters
#     major processing boundaries
#     failures with context
#
# Avoid actions such as df.count() only to decorate a log line. Spark actions
# are real distributed work and should be justified independently.


# =============================================================================
# 2. EXCEPTIONS
# =============================================================================


class ConfigurationError(ValueError):
    '''Raised when application configuration is invalid.'''


class PipelineExecutionError(RuntimeError):
    '''Raised when orchestration adds useful context to a pipeline failure.'''


# Do not wrap every helper in try/except.
#
# Catch an exception only where the code can:
#
#     recover meaningfully
#     add important context
#     translate it into an application-level failure
#
# Otherwise, let the original exception propagate.


# =============================================================================
# 3. CONFIGURATION
# =============================================================================


@dataclass(frozen=True)
class PipelineConfig:
    '''Immutable run configuration for one pipeline execution.'''

    environment: str
    orders_path: str
    customers_path: str
    output_path: str
    included_statuses: tuple[str, ...]
    minimum_net_sales: Decimal
    run_date: date
    write_mode: str = 'overwrite'


def build_config(environment: str, base_dir: Path) -> PipelineConfig:
    '''Build environment-specific settings without changing business logic.'''

    # Fail fast if the caller asks for an unsupported environment.
    allowed_environments = {'dev', 'test', 'prod'}
    if environment not in allowed_environments:
        raise ConfigurationError(
            f'Unsupported environment {environment!r}. '
            f'Expected one of {sorted(allowed_environments)}.'
        )

    # Only infrastructure/configuration details vary by environment here.
    environment_root = base_dir / environment

    return PipelineConfig(
        environment=environment,
        orders_path=str(environment_root / 'input' / 'orders'),
        customers_path=str(environment_root / 'input' / 'customers'),
        output_path=str(environment_root / 'output' / 'sales_by_province'),
        included_statuses=('COMPLETED',),
        minimum_net_sales=Decimal('0.00'),
        # Inject run-dependent values instead of calling current_date() inside
        # business logic. This keeps reruns and tests deterministic.
        run_date=date(2026, 9, 7),
    )


# Configuration should contain values that can legitimately vary between runs
# or environments.
#
# Good examples:
#
#     paths
#     table names
#     environment names
#     run dates
#     business thresholds that are truly configurable
#     write modes where operationally appropriate
#
# Poor examples:
#
#     passwords committed to source control
#     transformation implementation details
#     every tiny constant merely to make everything 'configurable'
#
# Secrets belong in a secret-management mechanism, not ordinary config files.


# =============================================================================
# 4. SCHEMAS
# =============================================================================


ORDERS_SCHEMA = StructType(
    [
        StructField('order_id', StringType(), nullable=False),
        StructField('customer_id', StringType(), nullable=False),
        StructField('order_date', DateType(), nullable=False),
        StructField('order_status', StringType(), nullable=False),
        StructField('net_sales', DecimalType(12, 2), nullable=False),
    ]
)

CUSTOMERS_SCHEMA = StructType(
    [
        StructField('customer_id', StringType(), nullable=False),
        StructField('customer_name', StringType(), nullable=False),
        StructField('province', StringType(), nullable=False),
        StructField('customer_segment', StringType(), nullable=False),
    ]
)

CUSTOMER_HISTORY_SCHEMA = StructType(
    [
        StructField('customer_record_id', StringType(), nullable=False),
        StructField('customer_id', StringType(), nullable=False),
        StructField('effective_date', DateType(), nullable=False),
        StructField('province', StringType(), nullable=False),
    ]
)

# Schema definitions form a clear application boundary.
#
# They should be reusable by readers, fixture builders, and tests without being
# hidden inside orchestration code.


# =============================================================================
# 5. SMALL DETERMINISTIC RETAIL FIXTURES
# =============================================================================


ORDERS_ROWS = [
    ('O001', 'C001', date(2026, 9, 1), 'COMPLETED', Decimal('125.00')),
    ('O002', 'C002', date(2026, 9, 1), 'CANCELLED', Decimal('80.00')),
    ('O003', 'C001', date(2026, 9, 2), 'COMPLETED', Decimal('45.00')),
    ('O004', 'C003', date(2026, 9, 2), 'COMPLETED', Decimal('200.00')),
    ('O005', 'C004', date(2026, 9, 3), 'COMPLETED', Decimal('30.00')),
]

CUSTOMERS_ROWS = [
    ('C001', 'Alice Wong', 'ON', 'CONSUMER'),
    ('C002', 'Ben Tremblay', 'QC', 'CONSUMER'),
    ('C003', 'Carla Singh', 'BC', 'BUSINESS'),
    ('C004', 'Diego Martin', 'ON', 'CONSUMER'),
]

CUSTOMER_HISTORY_ROWS = [
    ('R001', 'C001', date(2026, 1, 1), 'ON'),
    ('R002', 'C001', date(2026, 6, 1), 'QC'),
    # Same effective date is intentional; customer_record_id is the stable
    # tie-breaker used to make latest-record selection deterministic.
    ('R003', 'C002', date(2026, 5, 1), 'AB'),
    ('R004', 'C002', date(2026, 5, 1), 'QC'),
]


def build_practice_data(
    spark_session: SparkSession,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    '''Create deterministic in-memory DataFrames for architecture examples.'''

    orders_df = spark_session.createDataFrame(ORDERS_ROWS, schema=ORDERS_SCHEMA)
    customers_df = spark_session.createDataFrame(
        CUSTOMERS_ROWS,
        schema=CUSTOMERS_SCHEMA,
    )
    customer_history_df = spark_session.createDataFrame(
        CUSTOMER_HISTORY_ROWS,
        schema=CUSTOMER_HISTORY_SCHEMA,
    )

    return orders_df, customers_df, customer_history_df


# Grains:
#
#     orders_df
#     -> one row per order_id
#
#     customers_df
#     -> one row per customer_id
#
#     customer_history_df
#     -> one row per customer_record_id


# =============================================================================
# 6. VALIDATION
# =============================================================================



def require_columns(df: DataFrame, required_columns: set[str], label: str) -> None:
    '''Validate a structural DataFrame contract without triggering a Spark job.'''

    missing_columns = required_columns - set(df.columns)

    if missing_columns:
        raise ValueError(
            f'{label} is missing required columns: {sorted(missing_columns)}'
        )


def require_non_null_keys(df: DataFrame, key_columns: list[str], label: str) -> None:
    '''Fail when required business keys contain NULL values.'''

    # This is intentionally an action because it validates actual row data.
    null_condition = F.lit(False)
    for column_name in key_columns:
        null_condition = null_condition | F.col(column_name).isNull()

    invalid_exists = df.filter(null_condition).limit(1).count() > 0

    if invalid_exists:
        raise ValueError(
            f'{label} contains NULL values in required key columns {key_columns}.'
        )


# Phase 9 only needs enough validation to establish architecture boundaries.
# Phase 10 will deepen reusable data-quality rules such as domain checks,
# uniqueness, referential integrity, quarantine, and rejection reasons.


# =============================================================================
# 7. REUSABLE BUSINESS TRANSFORMATIONS
# =============================================================================



def transform_orders(orders_df: DataFrame, customers_df: DataFrame) -> DataFrame:
    '''Enrich order-grain rows with customer attributes.'''

    # WHAT: validate only the columns this transformation requires.
    # WHY: the function states its contract without depending on file layout.
    require_columns(
        orders_df,
        {
            'order_id',
            'customer_id',
            'order_date',
            'order_status',
            'net_sales',
        },
        'orders_df',
    )
    require_columns(
        customers_df,
        {'customer_id', 'province', 'customer_segment'},
        'customers_df',
    )

    # Customer dimension grain is expected to be one row per customer_id.
    # Phase 11 will formalize automated grain/uniqueness tests.
    customer_projection_df = customers_df.select(
        'customer_id',
        'province',
        'customer_segment',
    )

    # A left join preserves the order grain when customer_id is unique on the
    # dimension side.
    return orders_df.join(
        customer_projection_df,
        on='customer_id',
        how='left',
    )


def filter_orders(
    orders_df: DataFrame,
    included_statuses: tuple[str, ...],
    minimum_net_sales: Decimal,
) -> DataFrame:
    '''Apply true run/business parameters without reading global configuration.'''

    # The function receives only the values it needs. It does not receive an
    # entire application object or reach into module-level mutable state.
    return orders_df.filter(
        F.col('order_status').isin(*included_statuses)
        & (F.col('net_sales') >= F.lit(minimum_net_sales))
    )


def build_sales_by_province(enriched_orders_df: DataFrame) -> DataFrame:
    '''Aggregate order-grain rows to one row per province.'''

    require_columns(
        enriched_orders_df,
        {'province', 'order_id', 'net_sales'},
        'enriched_orders_df',
    )

    # Output grain changes deliberately from order to province.
    return (
        enriched_orders_df
        .groupBy('province')
        .agg(
            F.countDistinct('order_id').alias('order_count'),
            F.sum('net_sales').alias('net_sales'),
        )
    )


def add_processing_date(result_df: DataFrame, run_date: date) -> DataFrame:
    '''Add an injected processing date instead of using runtime wall-clock time.'''

    # Injecting run_date makes replay behavior explicit and testable.
    return result_df.withColumn(
        'processing_date',
        F.lit(run_date).cast('date'),
    )


def select_latest_customer_record(customer_history_df: DataFrame) -> DataFrame:
    '''Select one deterministic latest history row per customer.'''

    require_columns(
        customer_history_df,
        {'customer_id', 'customer_record_id', 'effective_date', 'province'},
        'customer_history_df',
    )

    latest_window = (
        Window
        .partitionBy('customer_id')
        # Stable secondary ordering removes ambiguity when effective dates tie.
        .orderBy(
            F.col('effective_date').desc(),
            F.col('customer_record_id').desc(),
        )
    )

    return (
        customer_history_df
        .withColumn('row_number', F.row_number().over(latest_window))
        .filter(F.col('row_number') == 1)
        .drop('row_number')
    )


# Reusable transformation design checklist:
#
#     accepts DataFrame(s) and explicit scalar parameters
#     returns a DataFrame
#     states/validates required input columns
#     preserves or deliberately changes grain
#     does not read files
#     does not write files
#     does not contain credentials
#     does not inspect the environment
#     does not decide scheduling
#     avoids hidden mutable global state
#
# That is the practical meaning of testable business logic.


# =============================================================================
# 8. READERS
# =============================================================================



def read_orders(spark_session: SparkSession, path: str) -> DataFrame:
    '''Read the orders dataset with its explicit schema.'''

    # Reader code owns file-format mechanics, not business aggregations.
    return (
        spark_session.read
        .schema(ORDERS_SCHEMA)
        .parquet(path)
    )


def read_customers(spark_session: SparkSession, path: str) -> DataFrame:
    '''Read the customer dataset with its explicit schema.'''

    return (
        spark_session.read
        .schema(CUSTOMERS_SCHEMA)
        .parquet(path)
    )


# A reader may reasonably own:
#
#     file format
#     read options
#     schema application
#     table/path resolution passed in by configuration
#
# It should generally not own:
#
#     completed-order filtering
#     business joins
#     revenue calculations
#     output persistence


# =============================================================================
# 9. WRITERS
# =============================================================================



def write_sales_by_province(
    result_df: DataFrame,
    path: str,
    mode: str,
) -> None:
    '''Persist the final province-grain result.'''

    # Writer code owns persistence behavior and keeps business rules upstream.
    (
        result_df.write
        .mode(mode)
        .parquet(path)
    )


# A writer may reasonably own:
#
#     output format
#     write mode
#     partitionBy() when it is a storage responsibility
#     table/path destination passed in explicitly
#
# It should not silently change business grain or redefine measures.


# =============================================================================
# 10. THIN ORCHESTRATION
# =============================================================================



def run_pipeline(
    spark_session: SparkSession,
    config: PipelineConfig,
) -> DataFrame:
    '''Coordinate application layers while keeping business logic elsewhere.'''

    logger.info('Pipeline started | environment=%s', config.environment)

    try:
        logger.info('Reading orders | path=%s', config.orders_path)
        orders_df = read_orders(spark_session, config.orders_path)

        logger.info('Reading customers | path=%s', config.customers_path)
        customers_df = read_customers(spark_session, config.customers_path)

        # Boundary validations make assumptions explicit before business logic.
        require_non_null_keys(orders_df, ['order_id', 'customer_id'], 'orders_df')
        require_non_null_keys(customers_df, ['customer_id'], 'customers_df')

        filtered_orders_df = filter_orders(
            orders_df,
            included_statuses=config.included_statuses,
            minimum_net_sales=config.minimum_net_sales,
        )

        enriched_orders_df = transform_orders(
            filtered_orders_df,
            customers_df,
        )

        sales_by_province_df = build_sales_by_province(enriched_orders_df)

        final_df = add_processing_date(
            sales_by_province_df,
            run_date=config.run_date,
        )

        logger.info('Writing result | path=%s', config.output_path)
        write_sales_by_province(
            final_df,
            path=config.output_path,
            mode=config.write_mode,
        )

    except ConfigurationError:
        # ConfigurationError is already specific enough; preserve its type.
        raise
    except Exception as exc:
        # The orchestration boundary adds useful application context while
        # preserving the original exception as the cause.
        raise PipelineExecutionError(
            f'Phase 9 pipeline failed in environment {config.environment!r}.'
        ) from exc

    logger.info('Pipeline completed | environment=%s', config.environment)

    # Returning the final DataFrame is useful for teaching, inspection, and
    # integration tests. Production entry points may choose a different API.
    return final_df


# Thin orchestration should read like a workflow:
#
#     read
#     validate
#     transform
#     aggregate
#     write
#
# If pipeline.py contains hundreds of lines of join/filter/window details, the
# business logic has leaked back into orchestration.
#
# Orchestration in this phase means in-application sequencing. It is not the
# same responsibility as Airflow, Cloud Composer, Databricks Workflows, or a
# scheduler deciding when the Spark application runs. Phase 15 covers that
# platform-level orchestration distinction in depth.


# =============================================================================
# 11. ENVIRONMENT-SPECIFIC CONFIGURATION DEMONSTRATION
# =============================================================================



def print_config_reference(base_dir: Path) -> None:
    '''Show that environment changes do not alter transformation code.'''

    for environment in ('dev', 'test', 'prod'):
        config = build_config(environment, base_dir)
        print(f'\nENVIRONMENT: {environment}')
        print(f'orders_path={config.orders_path}')
        print(f'customers_path={config.customers_path}')
        print(f'output_path={config.output_path}')
        print(f'run_date={config.run_date}')


# Notice what does NOT change between environments:
#
#     transform_orders()
#     filter_orders()
#     build_sales_by_province()
#     select_latest_customer_record()
#
# That separation is the architectural win.


# =============================================================================
# 12. DETERMINISM
# =============================================================================



def demonstrate_deterministic_selection(customer_history_df: DataFrame) -> None:
    '''Show stable latest-record selection with an explicit tie-breaker.'''

    print('\nDETERMINISTIC LATEST CUSTOMER RECORD')

    (
        select_latest_customer_record(customer_history_df)
        # orderBy() is used only for human-readable display. Spark DataFrames do
        # not otherwise guarantee row presentation order.
        .orderBy('customer_id')
        .show(truncate=False)
    )


# Deterministic pipeline design includes more than output sorting.
#
# Ask:
#
#     Are window ties resolved explicitly?
#     Are run-dependent timestamps/dates injected?
#     Are random operations seeded when randomness is intentional?
#     Are business keys stable?
#     Does the same logical input produce the same logical output?
#     Does rerunning depend on hidden process state?
#
# Example seeded randomness when deliberately needed:
#
#     df.withColumn('sample_value', F.rand(seed=17))
#
# Do not use unseeded randomness in logic that must replay identically.


# =============================================================================
# 13. TESTABLE TRANSFORMATION DESIGN
# =============================================================================



def run_small_transformation_checks(
    orders_df: DataFrame,
    customers_df: DataFrame,
) -> None:
    '''Demonstrate lightweight checks enabled by pure-ish DataFrame functions.'''

    completed_orders_df = filter_orders(
        orders_df,
        included_statuses=('COMPLETED',),
        minimum_net_sales=Decimal('0.00'),
    )

    enriched_orders_df = transform_orders(
        completed_orders_df,
        customers_df,
    )

    province_sales_df = build_sales_by_province(enriched_orders_df)

    # These actions are acceptable because this is a tiny deterministic fixture.
    # Larger production validation should stay distributed where practical.
    actual_rows = (
        province_sales_df
        .orderBy('province')
        .collect()
    )

    expected = [
        ('BC', 1, Decimal('200.00')),
        ('ON', 3, Decimal('200.00')),
    ]

    actual = [
        (row['province'], row['order_count'], row['net_sales'])
        for row in actual_rows
    ]

    assert actual == expected

    # Grain check: one output row per province.
    duplicate_province_exists = (
        province_sales_df
        .groupBy('province')
        .count()
        .filter(F.col('count') > 1)
        .limit(1)
        .count()
        > 0
    )

    assert not duplicate_province_exists


# Why this code is testable:
#
#     no temporary directory is required for transform_orders()
#     no credentials are required
#     no environment variable is required
#     no output path is required
#     no write operation is required
#
# Tests can construct tiny DataFrames, call the function, and assert business
# correctness directly. Phase 11 will turn this pattern into formal pytest
# suites, reusable Spark fixtures, schema checks, and integration tests.


# =============================================================================
# 14. THE MONOLITHIC ANTI-PATTERN
# =============================================================================


MONOLITHIC_ANTI_PATTERN = '''
# Anti-pattern sketch only; do not execute this design.

def build_sales_report(spark):
    orders_df = spark.read.parquet('C:/hard-coded/dev/orders')
    customers_df = spark.read.parquet('C:/hard-coded/dev/customers')

    result_df = (
        orders_df
        .filter(F.col('order_status') == 'COMPLETED')
        .join(customers_df, on='customer_id', how='left')
        .groupBy('province')
        .agg(F.sum('net_sales').alias('net_sales'))
    )

    result_df.write.mode('overwrite').parquet(
        'C:/hard-coded/dev/sales_by_province'
    )
'''

# Problems:
#
#     hard-coded environment
#     I/O mixed with business logic
#     function cannot be tested without filesystem setup
#     storage change requires editing transformation code
#     business logic is harder to reuse
#     orchestration and transformation are indistinguishable
#
# Splitting code is useful only when the split follows real responsibilities.
# Creating ten tiny files with arbitrary boundaries is not automatically better.


# =============================================================================
# 15. DEPENDENCY DIRECTION AND COUPLING
# =============================================================================

# Prefer dependencies that point inward toward stable business logic:
#
#     pipeline.py
#       |-- config.py
#       |-- readers.py
#       |-- validation.py
#       |-- transformations.py
#       '-- writers.py
#
#     transformations.py
#       '-- PySpark DataFrame API
#
# Business transformations should not need to import pipeline.py, inspect CLI
# arguments, access cloud credentials, or know where files live.
#
# Pass only what a function needs:
#
#     good
#     transform_orders(orders_df, customers_df)
#
#     weaker
#     transform_orders(application_context)
#
# The larger the object passed everywhere, the more hidden coupling each
# function acquires.


# =============================================================================
# 16. DEPENDENCY MANAGEMENT
# =============================================================================


DEPENDENCY_REFERENCE = '''
Repository dependency declaration:

requirements.txt
    pyspark==4.2.0
    ipykernel

Production principle:
    declare dependencies explicitly
    pin runtime-critical versions deliberately
    install from declared dependencies
    do not rely on packages that happen to exist on one laptop

Conceptual separation:
    Python dependency declaration
    !=
    packaged application code
    !=
    Spark cluster/runtime configuration
'''

# Dependency management answers:
#
#     Which third-party packages does the application require?
#     Which versions are expected?
#     Can another environment reproduce the Python runtime?
#
# It does NOT by itself package your source code or configure executor memory.


# =============================================================================
# 17. PACKAGING
# =============================================================================


PACKAGE_REFERENCE = '''
A maintainable src-layout could become:

phase_09_application/
|-- pyproject.toml
|-- src/
|   '-- retail_pipeline/
|       |-- __init__.py
|       |-- config.py
|       |-- schemas.py
|       |-- readers.py
|       |-- validation.py
|       |-- transformations.py
|       |-- writers.py
|       '-- pipeline.py
'-- tests/
    |-- test_transformations.py
    '-- test_validation.py

Packaging makes application code installable/importable as a coherent unit.

Example conceptual workflow:
    python -m pip install -e .
    python -m retail_pipeline.pipeline

For distributed deployment, the packaged code must also be available to the
Spark runtime according to the target platform's submission mechanism.
'''

# Packaging is not an excuse to overengineer a small learning project.
#
# Use a package when the code has become an application with reusable modules,
# tests, deployment boundaries, or multiple entry points. The key lesson is
# that imports and deployment should not depend on manually editing sys.path or
# running from one magical working directory.


# =============================================================================
# 18. END-TO-END LOCAL APPLICATION DEMONSTRATION
# =============================================================================



def seed_local_inputs(
    orders_df: DataFrame,
    customers_df: DataFrame,
    config: PipelineConfig,
) -> None:
    '''Write deterministic source data so readers can exercise real I/O.'''

    # Seed data creation is teaching/test setup, not part of business logic.
    orders_df.write.mode('overwrite').parquet(config.orders_path)
    customers_df.write.mode('overwrite').parquet(config.customers_path)


def inspect_output(spark_session: SparkSession, config: PipelineConfig) -> None:
    '''Read the written result only for lecture verification.'''

    output_df = spark_session.read.parquet(config.output_path)

    print('\nFINAL OUTPUT')
    (
        output_df
        .orderBy('province')
        .show(truncate=False)
    )


# =============================================================================
# 19. DESIGN REVIEW CHECKLIST
# =============================================================================


DESIGN_REVIEW_CHECKLIST = '''
STRUCTURE
[ ] I/O, schemas, validation, transformations, configuration, and orchestration
    have clear ownership.

TRANSFORMATIONS
[ ] Business functions accept DataFrames and explicit parameters.
[ ] They return DataFrames.
[ ] They do not embed paths, credentials, writes, or environment detection.
[ ] Grain is preserved or changed deliberately.

CONFIGURATION
[ ] Environment differences are values, not duplicated code branches.
[ ] Invalid configuration fails early.
[ ] Secrets are not ordinary source-controlled configuration.

LOGGING
[ ] Major boundaries and failures are logged.
[ ] Logging does not trigger unnecessary Spark actions.
[ ] Sensitive row data is not dumped casually.

EXCEPTIONS
[ ] Errors are caught only where context/recovery is meaningful.
[ ] Original causes are preserved.
[ ] Failures are not swallowed.

DETERMINISM
[ ] Window/order ties have stable tie-breakers.
[ ] Run-dependent values are injected when replayability matters.
[ ] Randomness is seeded when intentional.
[ ] DataFrame row order is not assumed implicitly.

DEPENDENCIES AND PACKAGING
[ ] Third-party dependencies are declared.
[ ] Runtime-critical versions are deliberate.
[ ] Application modules can be imported without path hacks.
[ ] Package/deployment concerns stay outside business transformations.

MAINTAINABILITY
[ ] Another engineer can identify where to change business rules, I/O,
    configuration, validation, and orchestration independently.
'''


# =============================================================================
# 20. RUN THE LECTURE
# =============================================================================


if __name__ == '__main__':
    logger.info('Phase 9 lecture started')

    practice_orders_df, practice_customers_df, practice_history_df = (
        build_practice_data(spark)
    )

    print('\nPRACTICE DATA')
    practice_orders_df.orderBy('order_id').show(truncate=False)
    practice_customers_df.orderBy('customer_id').show(truncate=False)

    demonstrate_deterministic_selection(practice_history_df)

    print('\nTESTABLE TRANSFORMATION CHECKS')
    run_small_transformation_checks(
        practice_orders_df,
        practice_customers_df,
    )
    print('Transformation checks passed.')

    print('\nCONFIGURATION REFERENCE')
    with TemporaryDirectory() as temporary_directory:
        base_dir = Path(temporary_directory)

        print_config_reference(base_dir)

        # Run one complete local application using the dev configuration.
        dev_config = build_config('dev', base_dir)

        seed_local_inputs(
            practice_orders_df,
            practice_customers_df,
            dev_config,
        )

        final_df = run_pipeline(spark, dev_config)

        # The returned DataFrame is still available for integration-style
        # verification even though the pipeline has already persisted output.
        print('\nRETURNED FINAL DATAFRAME')
        final_df.orderBy('province').show(truncate=False)

        inspect_output(spark, dev_config)

    print('\nDEPENDENCY MANAGEMENT REFERENCE')
    print(DEPENDENCY_REFERENCE)

    print('\nPACKAGING REFERENCE')
    print(PACKAGE_REFERENCE)

    print('\nPHASE 9 DESIGN REVIEW CHECKLIST')
    print(DESIGN_REVIEW_CHECKLIST)

    logger.info('Phase 9 lecture completed')

    spark.stop()
