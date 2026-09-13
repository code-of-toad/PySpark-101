from decimal import Decimal

from pyspark.sql import functions as F

from phases.phase_10_data_quality_schema_enforcement.demo.validation import missing_string


ALLOWED_ORDER_STATUSES = (
    'COMPLETED',
    'CANCELLED',
    'PENDING',
)


# Every rule uses:
#
#     (rejection_reason, invalid_condition)
#
# Keeping rule declarations separate from validation mechanics makes the
# business contract easy to inspect and change.

ORDER_RULES = [
    # Required fields.
    ('MISSING_ORDER_ID', missing_string('order_id')),
    ('MISSING_CUSTOMER_ID', missing_string('customer_id')),
    ('MISSING_ORDER_DATE', F.col('order_date').isNull()),
    ('MISSING_ORDER_STATUS', missing_string('order_status')),
    ('MISSING_NET_SALES', F.col('net_sales').isNull()),

    # Domain validation. Blank strings are diagnosed only as missing.
    (
        'INVALID_ORDER_STATUS',
        F.col('order_status').isNotNull()
        & (F.trim(F.col('order_status')) != '')
        & ~F.col('order_status').isin(*ALLOWED_ORDER_STATUSES),
    ),

    # Range validation.
    (
        'NEGATIVE_NET_SALES',
        F.col('net_sales').isNotNull()
        & (F.col('net_sales') < F.lit(Decimal('0.00'))),
    ),

    # Cross-column business rule.
    (
        'INVALID_CANCELLED_AMOUNT',
        (F.col('order_status') == 'CANCELLED')
        & F.col('net_sales').isNotNull()
        & (F.col('net_sales') != F.lit(Decimal('0.00'))),
    ),
]


INVENTORY_RULES = [
    # Required fields.
    ('MISSING_SNAPSHOT_DATE', F.col('snapshot_date').isNull()),
    ('MISSING_STORE_ID', missing_string('store_id')),
    ('MISSING_PRODUCT_ID', missing_string('product_id')),
    ('MISSING_QUANTITY_ON_HAND', F.col('quantity_on_hand').isNull()),

    # Range validation.
    (
        'NEGATIVE_QUANTITY_ON_HAND',
        F.col('quantity_on_hand').isNotNull()
        & (F.col('quantity_on_hand') < 0),
    ),
]
