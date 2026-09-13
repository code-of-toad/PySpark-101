from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform_accepted_orders(
    accepted_orders_df: DataFrame,
) -> DataFrame:
    """Apply downstream business logic only to quality-approved orders."""

    return (
        accepted_orders_df

        # This transformation deliberately happens after DQ routing.
        .withColumn(
            'order_month',
            F.date_format(
                F.col('order_date'),
                'yyyy-MM',
            ),
        )

        # Example downstream derivation for an accepted business record.
        .withColumn(
            'is_completed_order',
            F.col('order_status')
            == 'COMPLETED',
        )
    )
