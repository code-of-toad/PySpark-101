from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    try_cast,
    try_to_date,
    upper,
)
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

# Create the Spark entry point for the Phase 1 applied pipeline.
spark = (
    SparkSession.builder
    .appName('phase01-retail-cleaning')
    .master('local[*]')
    .getOrCreate()
)

# TODO: define the explicit raw schema.

# TODO: read raw_orders.csv using that schema.

# TODO: standardize sku and province.

# TODO: safely parse quantity, unit_price, and order_date.

# TODO: print the resulting schema and rows.

spark.stop()
