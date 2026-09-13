from datetime import date

from pyspark.sql import functions as F
from pyspark.sql import DataFrame, Column, Row
from pyspark.sql.types import StringType, StructType


def validate_schema(
    df: DataFrame,
    expected_schema: StructType,
    label: str,
    allow_extra_columns: bool = False,
) -> None:
    """Validate required columns and Spark data types."""

    # Schema metadata is available without scanning every source row.
    expected_fields = {
        field.name: field.dataType
        for field in expected_schema.fields
    }

    actual_fields = {
        field.name: field.dataType
        for field in df.schema.fields
    }

    missing_columns = (
        set(expected_fields)
        - set(actual_fields)
    )

    unexpected_columns = (
        set(actual_fields)
        - set(expected_fields)
    )

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
            f'{label} is missing required columns: '
            f'{sorted(missing_columns)}'
        )

    if type_mismatches:
        raise ValueError(
            f'{label} contains type mismatches: '
            f'{type_mismatches}'
        )

    if unexpected_columns and not allow_extra_columns:
        raise ValueError(
            f'{label} contains unexpected columns: '
            f'{sorted(unexpected_columns)}'
        )


def missing_string(
    column_name: str,
) -> Column:
    """Return True when a required string is NULL or blank."""

    return (
        F.col(column_name).isNull()
        | (
            F.trim(
                F.col(column_name)
            )
            == ''
        )
    )


def add_rejection_reasons(
    df: DataFrame,
    rules: list[tuple[str, Column]],
) -> DataFrame:
    """Attach every row-level rejection reason that applies."""

    # Every rule follows the same contract:
    #
    #     (rejection_reason, invalid_condition)
    reason_columns = [
        F.when(
            invalid_condition,
            F.lit(reason),
        )
        for reason, invalid_condition in rules
    ]

    return df.withColumn(
        'rejection_reasons',

        # Remove NULL placeholders so the array contains only failed rules.
        F.filter(
            F.array(
                *reason_columns
            ),
            lambda reason: reason.isNotNull(),
            Column
        ),
    )


def append_reason(
    df: DataFrame,
    reason: str,
    invalid_condition: Column,
) -> DataFrame:
    """Append one reason while preserving existing rejection reasons."""

    return df.withColumn(
        'rejection_reasons',
        F.when(
            invalid_condition,

            # array_union() also prevents an accidental duplicate reason.
            F.array_union(
                F.col('rejection_reasons'),
                F.array(
                    F.lit(reason)
                ),
            ),
        ).otherwise(
            F.col('rejection_reasons')
        ),
    )


def _usable_key_condition(
    df: DataFrame,
    key_columns: list[str],
) -> Column:
    """Return True when every business-key component is usable."""

    condition = F.lit(True)

    for column_name in key_columns:
        column_condition = F.col(
            column_name
        ).isNotNull()

        # Blank strings are missing-key defects, so uniqueness validation should
        # not double-label them as duplicate business keys.
        if isinstance(
            df.schema[column_name].dataType,
            StringType,
        ):
            column_condition = (
                column_condition
                & (
                    F.trim(
                        F.col(column_name)
                    )
                    != ''
                )
            )

        condition = (
            condition
            & column_condition
        )

    return condition


def find_duplicate_keys(
    df: DataFrame,
    key_columns: list[str],
) -> DataFrame:
    """Return usable primary or composite keys occurring more than once."""

    return (
        df

        # Missing key components are owned by required-field validation.
        .filter(
            _usable_key_condition(
                df,
                key_columns,
            )
        )

        # Equal keys must be colocated, so this aggregation requires a shuffle.
        .groupBy(
            *key_columns
        )

        .count()

        # More than one source row violates the declared grain.
        .filter(
            F.col('count') > 1
        )

        .select(
            *key_columns
        )
    )


def find_duplicate_rows(
    df: DataFrame,
    key_columns: list[str],
) -> DataFrame:
    """Return source rows participating in a duplicate business key."""

    duplicate_keys_df = find_duplicate_keys(
        df,
        key_columns,
    )

    return df.join(
        duplicate_keys_df,
        on=key_columns,

        # Keep left-side rows only when their complete key is duplicated.
        how='left_semi',
    )


def find_exact_duplicate_rows(
    df: DataFrame,
) -> DataFrame:
    """Return complete row values appearing more than once."""

    return (
        df

        # Group by every source column to distinguish exact duplicates from
        # records that merely share the same PK or composite key.
        .groupBy(
            *df.columns
        )

        .count()

        .filter(
            F.col('count') > 1
        )
    )


def add_duplicate_key_reason(
    validated_df: DataFrame,
    key_columns: list[str],
    rejection_reason: str,
) -> DataFrame:
    """Mark every row participating in a duplicate PK or composite key."""

    duplicate_keys_df = (
        find_duplicate_keys(
            validated_df,
            key_columns,
        )

        # Add a temporary marker so a left join can preserve every source row.
        .withColumn(
            '_duplicate_key',
            F.lit(True),
        )
    )

    result_df = (
        validated_df

        .join(
            duplicate_keys_df,
            on=key_columns,
            how='left',
        )

        # Non-matching keys become False rather than NULL.
        .withColumn(
            '_duplicate_key',
            F.coalesce(
                F.col('_duplicate_key'),
                F.lit(False),
            ),
        )
    )

    result_df = append_reason(
        result_df,
        reason=rejection_reason,
        invalid_condition=F.col(
            '_duplicate_key'
        ),
    )

    return result_df.drop(
        '_duplicate_key'
    )


def _assert_parent_key_unique(
    parent_df: DataFrame,
    key_columns: list[str],
    label: str,
) -> None:
    """Fail when a parent dataset cannot safely support relationship checks."""

    duplicate_parent_keys_df = find_duplicate_keys(
        parent_df,
        key_columns,
    )

    duplicate_parent_exists = (
        duplicate_parent_keys_df

        # Only enough data is materialized to answer whether any defect exists.
        .limit(1)

        .count()

        > 0
    )

    if duplicate_parent_exists:
        raise ValueError(
            f'{label} violates expected grain: '
            f'{key_columns} is not unique.'
        )


def add_orphan_customer_reason(
    validated_orders_df: DataFrame,
    customers_df: DataFrame,
) -> DataFrame:
    """Mark non-null customer IDs having no matching parent customer."""

    # Referential integrity is trustworthy only when the parent key is unique.
    _assert_parent_key_unique(
        customers_df,
        key_columns=['customer_id'],
        label='customers_df',
    )

    parent_customer_keys_df = (
        customers_df

        # Only the relationship key is needed for an existence check.
        .select(
            'customer_id'
        )

        .filter(
            F.col('customer_id').isNotNull()
            & (
                F.trim(
                    F.col('customer_id')
                )
                != ''
            )
        )

        .withColumn(
            '_customer_exists',
            F.lit(True),
        )
    )

    result_df = (
        validated_orders_df

        # Preserve every child row while attaching parent-existence metadata.
        .join(
            parent_customer_keys_df,
            on='customer_id',
            how='left',
        )

        .withColumn(
            '_customer_exists',
            F.coalesce(
                F.col('_customer_exists'),
                F.lit(False),
            ),
        )
    )

    orphan_condition = (
        # Missing FKs already receive MISSING_CUSTOMER_ID.
        F.col('customer_id').isNotNull()
        & (
            F.trim(
                F.col('customer_id')
            )
            != ''
        )
        & ~F.col('_customer_exists')
    )

    result_df = append_reason(
        result_df,
        reason='ORPHAN_CUSTOMER_ID',
        invalid_condition=orphan_condition,
    )

    return result_df.drop(
        '_customer_exists'
    )


def split_accepted_rejected(
    validated_df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Split one validated population into accepted and rejected records."""

    accepted_df = (
        validated_df

        # An empty rejection array means the record passed every required rule.
        .filter(
            F.size('rejection_reasons') == 0
        )

        # Business transformations should not depend on DQ diagnostics.
        .drop(
            'rejection_reasons'
        )
    )

    rejected_df = (
        validated_df

        # Preserve every failed record and every diagnostic reason.
        .filter(
            F.size('rejection_reasons') > 0
        )
    )

    return accepted_df, rejected_df


def build_quarantine(
    rejected_df: DataFrame,
    source_dataset: str,
    validation_run_date: date,
) -> DataFrame:
    """Add deterministic operational metadata to rejected records."""

    return (
        rejected_df

        # Preserve which source contract produced this quarantine record.
        .withColumn(
            'source_dataset',
            F.lit(source_dataset),
        )

        # Inject run metadata instead of relying on current_date().
        .withColumn(
            'validation_run_date',
            F.lit(
                validation_run_date
            ).cast('date'),
        )
    )


def _validation_counts_aggregation(
    validated_df: DataFrame,
) -> DataFrame:
    """Build the common row-count aggregation used by metrics and assertions."""

    return validated_df.agg(
        F.count(
            F.lit(1)
        ).alias(
            'input_count'
        ),

        # coalesce() keeps zero-row datasets deterministic instead of NULL.
        F.coalesce(
            F.sum(
                F.when(
                    F.size('rejection_reasons') == 0,
                    F.lit(1),
                ).otherwise(
                    F.lit(0)
                )
            ),
            F.lit(0),
        ).alias(
            'accepted_count'
        ),

        F.coalesce(
            F.sum(
                F.when(
                    F.size('rejection_reasons') > 0,
                    F.lit(1),
                ).otherwise(
                    F.lit(0)
                )
            ),
            F.lit(0),
        ).alias(
            'rejected_count'
        ),
    )


def build_validation_summary(
    validated_df: DataFrame,
    dataset_name: str,
) -> DataFrame:
    """Build accepted/rejected counts and quality rates."""

    return (
        _validation_counts_aggregation(
            validated_df
        )

        .withColumn(
            'dataset_name',
            F.lit(dataset_name),
        )

        .withColumn(
            'acceptance_rate',
            F.when(
                F.col('input_count') > 0,
                F.col('accepted_count')
                / F.col('input_count'),
            ),
        )

        .withColumn(
            'rejection_rate',
            F.when(
                F.col('input_count') > 0,
                F.col('rejected_count')
                / F.col('input_count'),
            ),
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
    """Count individual validation-rule failures."""

    return (
        rejected_df

        # One rejected row may contribute multiple reason occurrences.
        .select(
            F.explode(
                'rejection_reasons'
            ).alias(
                'rejection_reason'
            )
        )

        .groupBy(
            'rejection_reason'
        )

        .count()
    )


def _get_validation_counts(
    validated_df: DataFrame,
) -> Row:
    """Materialize the counts required for reconciliation."""

    return (
        _validation_counts_aggregation(
            validated_df
        )

        # This action is intentional because reconciliation is an assertion.
        .first()
    )


def assert_validation_reconciles(
    validated_df: DataFrame,
) -> None:
    """Verify that every input row is classified exactly once."""

    counts = _get_validation_counts(
        validated_df
    )

    assert counts['input_count'] == (
        counts['accepted_count']
        + counts['rejected_count']
    )
