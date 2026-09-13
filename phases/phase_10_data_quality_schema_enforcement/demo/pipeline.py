from datetime import date
from decimal import Decimal

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

from phases.phase_10_data_quality_schema_enforcement.demo.rules import INVENTORY_RULES
from rules import ORDER_RULES
from schemas import CUSTOMERS_SCHEMA
from schemas import INVENTORY_SCHEMA
from schemas import ORDERS_SCHEMA
from transformations import transform_accepted_orders
from validation import add_duplicate_key_reason
from validation import add_orphan_customer_reason
from validation import add_rejection_reasons
from validation import assert_validation_reconciles
from validation import build_quarantine
from validation import build_rejection_reason_counts
from validation import build_validation_summary
from validation import find_duplicate_keys
from validation import find_duplicate_rows
from validation import find_exact_duplicate_rows
from validation import split_accepted_rejected
from validation import validate_schema


def build_seed_data(
    spark: SparkSession,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Create deterministic retail inputs containing deliberate DQ failures."""

    # -------------------------------------------------------------------------
    # orders_df
    #
    # Grain:
    #     one row per order_id
    #
    # PK:
    #     order_id
    #
    # FK:
    #     customer_id -> customers.customer_id
    # -------------------------------------------------------------------------

    orders_df = spark.createDataFrame(
        [
            # Valid.
            (
                'O001',
                'C001',
                date(2026, 9, 1),
                'COMPLETED',
                Decimal('125.00'),
            ),

            # Domain failure.
            (
                'O002',
                'C002',
                date(2026, 9, 1),
                'UNKNOWN',
                Decimal('80.00'),
            ),

            # Referential-integrity failure.
            (
                'O003',
                'C999',
                date(2026, 9, 2),
                'COMPLETED',
                Decimal('45.00'),
            ),

            # Range failure.
            (
                'O004',
                'C003',
                date(2026, 9, 2),
                'COMPLETED',
                Decimal('-20.00'),
            ),

            # Duplicate PK with conflicting payload values.
            (
                'O005',
                'C004',
                date(2026, 9, 3),
                'COMPLETED',
                Decimal('10.00'),
            ),
            (
                'O005',
                'C004',
                date(2026, 9, 3),
                'COMPLETED',
                Decimal('15.00'),
            ),

            # Exact duplicate pair. These rows are also duplicate PK rows.
            (
                'O006',
                'C004',
                date(2026, 9, 4),
                'PENDING',
                Decimal('20.00'),
            ),
            (
                'O006',
                'C004',
                date(2026, 9, 4),
                'PENDING',
                Decimal('20.00'),
            ),

            # Cross-column business-rule failure.
            (
                'O007',
                'C003',
                date(2026, 9, 4),
                'CANCELLED',
                Decimal('12.00'),
            ),

            # Required-field failure.
            (
                'O008',
                None,
                date(2026, 9, 4),
                'PENDING',
                Decimal('30.00'),
            ),

            # Valid.
            (
                'O009',
                'C002',
                date(2026, 9, 5),
                'CANCELLED',
                Decimal('0.00'),
            ),
        ],
        schema=ORDERS_SCHEMA,
    )

    # -------------------------------------------------------------------------
    # customers_df
    #
    # Grain:
    #     one row per customer_id
    #
    # Parent key:
    #     customer_id
    # -------------------------------------------------------------------------

    customers_df = spark.createDataFrame(
        [
            ('C001', 'Alice Wong', 'ON'),
            ('C002', 'Ben Tremblay', 'QC'),
            ('C003', 'Carla Singh', 'BC'),
            ('C004', 'Diego Martin', 'ON'),
        ],
        schema=CUSTOMERS_SCHEMA,
    )

    # -------------------------------------------------------------------------
    # inventory_df
    #
    # Grain:
    #     one row per snapshot_date + store_id + product_id
    #
    # Composite key:
    #     snapshot_date + store_id + product_id
    # -------------------------------------------------------------------------

    inventory_df = spark.createDataFrame(
        [
            # Valid.
            (
                date(2026, 9, 5),
                'S001',
                'P001',
                10,
            ),

            # Valid.
            (
                date(2026, 9, 5),
                'S001',
                'P002',
                7,
            ),

            # Duplicate composite-key pair.
            (
                date(2026, 9, 5),
                'S002',
                'P001',
                5,
            ),
            (
                date(2026, 9, 5),
                'S002',
                'P001',
                8,
            ),

            # Range failure.
            (
                date(2026, 9, 5),
                'S003',
                'P003',
                -1,
            ),
        ],
        schema=INVENTORY_SCHEMA,
    )

    return (
        orders_df,
        customers_df,
        inventory_df,
    )


def validate_orders(
    orders_df: DataFrame,
    customers_df: DataFrame,
) -> DataFrame:
    """
    Run the complete orders data-quality contract.
    """
    # Fail fast when the physical structure does not match the contract.
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

    # Evaluate row-local required, domain, range, and business rules.
    validated_df = add_rejection_reasons(
        orders_df,
        rules=ORDER_RULES,
    )

    # Enforce the declared one-row-per-order_id grain.
    validated_df = add_duplicate_key_reason(
        validated_df,
        key_columns=['order_id'],
        rejection_reason='DUPLICATE_ORDER_ID',
    )

    # Enforce orders.customer_id -> customers.customer_id.
    validated_df = add_orphan_customer_reason(
        validated_df,
        customers_df,
    )

    return validated_df


def validate_inventory(
    inventory_df: DataFrame,
) -> DataFrame:
    """
    Run the complete inventory data-quality contract.
    """
    validate_schema(
        inventory_df,
        expected_schema=INVENTORY_SCHEMA,
        label='inventory_df',
    )

    # Evaluate row-local required and range rules.
    validated_df = add_rejection_reasons(
        inventory_df,
        rules=INVENTORY_RULES,
    )

    # Enforce the full grain-defining composite key.
    validated_df = add_duplicate_key_reason(
        validated_df,
        key_columns=[
            'snapshot_date',
            'store_id',
            'product_id',
        ],
        rejection_reason='DUPLICATE_INVENTORY_KEY',
    )

    return validated_df


def run_pipeline(
    spark: SparkSession,
    validation_run_date: date,
) -> dict[str, DataFrame]:
    """
    Run the complete separated Phase 10 demonstration.
    """
    (
        orders_df,
        customers_df,
        inventory_df,
    ) = build_seed_data(spark)

    # =========================================================================
    # 1. VALIDATE ORDERS
    # =========================================================================

    validated_orders_df = validate_orders(orders_df, customers_df)

    (
        accepted_orders_df,
        rejected_orders_df,
    ) = split_accepted_rejected(validated_orders_df)

    # Only accepted data is permitted into downstream business logic.
    transformed_orders_df = transform_accepted_orders(
        accepted_orders_df
    )

    # Rejected records are retained with reasons and operational metadata.
    orders_quarantine_df = build_quarantine(
        rejected_orders_df,
        source_dataset='orders',
        validation_run_date=validation_run_date,
    )

    # Operational DQ observability.
    orders_validation_summary_df = build_validation_summary(
        validated_orders_df,
        dataset_name='orders',
    )

    orders_rejection_reason_counts_df = build_rejection_reason_counts(
        rejected_orders_df
    )

    # Correctness invariant: no source row silently disappears.
    assert_validation_reconciles(validated_orders_df)

    # =========================================================================
    # 2. VALIDATE INVENTORY
    # =========================================================================

    validated_inventory_df = validate_inventory(inventory_df)

    (
        accepted_inventory_df,
        rejected_inventory_df,
    ) = split_accepted_rejected(validated_inventory_df)

    inventory_quarantine_df = build_quarantine(
        rejected_inventory_df,
        source_dataset='inventory',
        validation_run_date=validation_run_date,
    )

    inventory_validation_summary_df = build_validation_summary(
        validated_inventory_df,
        dataset_name='inventory',
    )

    inventory_rejection_reason_counts_df = build_rejection_reason_counts(
        rejected_inventory_df
    )

    assert_validation_reconciles(validated_inventory_df)

    # =========================================================================
    # 3. DUPLICATE DIAGNOSTICS
    # =========================================================================

    # Business-key duplicates show grain violations.
    duplicate_order_keys_df = find_duplicate_keys(
        orders_df,
        key_columns=['order_id'],
    )

    duplicate_order_rows_df = find_duplicate_rows(
        orders_df,
        key_columns=['order_id'],
    )

    # Exact duplicates explain whether repeated PKs are byte-for-byte-equivalent
    # business records or conflicting records sharing the same business key.
    exact_duplicate_orders_df = find_exact_duplicate_rows(orders_df)

    duplicate_inventory_keys_df = find_duplicate_keys(
        inventory_df,
        key_columns=[
            'snapshot_date',
            'store_id',
            'product_id',
        ],
    )

    duplicate_inventory_rows_df = find_duplicate_rows(
        inventory_df,
        key_columns=[
            'snapshot_date',
            'store_id',
            'product_id',
        ],
    )

    # Named outputs make the pipeline easy to inspect and test in Phase 11.
    return {
        'validated_orders': validated_orders_df,
        'accepted_orders': accepted_orders_df,
        'transformed_orders': transformed_orders_df,
        'rejected_orders': rejected_orders_df,
        'orders_quarantine': orders_quarantine_df,
        'orders_validation_summary': orders_validation_summary_df,
        'orders_rejection_reason_counts': orders_rejection_reason_counts_df,
        'duplicate_order_keys': duplicate_order_keys_df,
        'duplicate_order_rows': duplicate_order_rows_df,
        'exact_duplicate_orders': exact_duplicate_orders_df,
        'validated_inventory': validated_inventory_df,
        'accepted_inventory': accepted_inventory_df,
        'rejected_inventory': rejected_inventory_df,
        'inventory_quarantine': inventory_quarantine_df,
        'inventory_validation_summary': inventory_validation_summary_df,
        'inventory_rejection_reason_counts': inventory_rejection_reason_counts_df,
        'duplicate_inventory_keys': duplicate_inventory_keys_df,
        'duplicate_inventory_rows': duplicate_inventory_rows_df,
    }


if __name__ == '__main__':
    print('ay yoooooooooooooooooooooooo')
    spark = (
        SparkSession.builder
        .appName('phase-10-data-quality')
        .getOrCreate()
    )

    outputs = run_pipeline(
        spark,
        validation_run_date=date(2026, 9, 10),
    )

    # In a production application, writer functions would persist these outputs.
    for output_name in [
        'validated_orders',
        'accepted_orders',
        'transformed_orders',
        'rejected_orders',
        'orders_quarantine',
        'orders_validation_summary',
        'orders_rejection_reason_counts',
        'duplicate_order_keys',
        'duplicate_order_rows',
        'exact_duplicate_orders',
        'validated_inventory',
        'accepted_inventory',
        'rejected_inventory',
        'inventory_quarantine',
        'inventory_validation_summary',
        'inventory_rejection_reason_counts',
        'duplicate_inventory_keys',
        'duplicate_inventory_rows',
    ]:
        print(f'\n=== {output_name.upper()} ===')
        outputs[output_name].show(truncate=False)

    spark.stop()
