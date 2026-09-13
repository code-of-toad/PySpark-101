from collections.abc import Callable
from decimal import Decimal

from pyspark.sql import Column
from pyspark.sql import functions as F

from validation import missing_string


# IMPORTANT:
# Rules store CALLABLES rather than Column objects.
#
# pipeline.py imports this module before SparkSession creation. If F.col(...)
# were evaluated directly here, PySpark would require an active SparkContext
# during module import and raise AssertionError.
RuleBuilder = Callable[[], Column]
ValidationRule = tuple[str, RuleBuilder]


ALLOWED_ORDER_STATUSES = (
    'COMPLETED',
    'CANCELLED',
    'PENDING',
)


ORDER_RULES: list[ValidationRule] = [
    # Required fields.
    ('MISSING_ORDER_ID', lambda: missing_string('order_id')),
    (
        'MISSING_CUSTOMER_ID',
        lambda: missing_string(
            'customer_id'
        ),
    ),
    (
        'MISSING_ORDER_DATE',
        lambda: F.col(
            'order_date'
        ).isNull(),
    ),
    (
        'MISSING_ORDER_STATUS',
        lambda: missing_string(
            'order_status'
        ),
    ),
    (
        'MISSING_NET_SALES',
        lambda: F.col(
            'net_sales'
        ).isNull(),
    ),

    # Domain validation. Blank strings are diagnosed only as missing.
    (
        'INVALID_ORDER_STATUS',
        lambda: (
            F.col(
                'order_status'
            ).isNotNull()
            & (
                F.trim(
                    F.col(
                        'order_status'
                    )
                )
                != ''
            )
            & ~F.col(
                'order_status'
            ).isin(
                *ALLOWED_ORDER_STATUSES
            )
        ),
    ),

    # Range validation.
    (
        'NEGATIVE_NET_SALES',
        lambda: (
            F.col('net_sales').isNotNull()
            & (F.col('net_sales') < F.lit(Decimal('0.00')))
        ),
    ),

    # Cross-column business rule.
    (
        'INVALID_CANCELLED_AMOUNT',
        lambda: (
            (
                F.col(
                    'order_status'
                )
                == 'CANCELLED'
            )
            & F.col(
                'net_sales'
            ).isNotNull()
            & (
                F.col(
                    'net_sales'
                )
                != F.lit(
                    Decimal('0.00')
                )
            )
        ),
    ),
]


INVENTORY_RULES: list[ValidationRule] = [
    # Required fields.
    (
        'MISSING_SNAPSHOT_DATE',
        lambda: F.col(
            'snapshot_date'
        ).isNull(),
    ),
    (
        'MISSING_STORE_ID',
        lambda: missing_string(
            'store_id'
        ),
    ),
    (
        'MISSING_PRODUCT_ID',
        lambda: missing_string(
            'product_id'
        ),
    ),
    (
        'MISSING_QUANTITY_ON_HAND',
        lambda: F.col(
            'quantity_on_hand'
        ).isNull(),
    ),

    # Range validation.
    (
        'NEGATIVE_QUANTITY_ON_HAND',
        lambda: (
            F.col(
                'quantity_on_hand'
            ).isNotNull()
            & (
                F.col(
                    'quantity_on_hand'
                )
                < 0
            )
        ),
    ),
]
