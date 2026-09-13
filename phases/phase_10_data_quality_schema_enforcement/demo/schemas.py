from pyspark.sql.types import DateType
from pyspark.sql.types import DecimalType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType


# Incoming schemas intentionally allow NULL values so malformed business records
# can reach the DQ layer and receive explicit rejection reasons.

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
