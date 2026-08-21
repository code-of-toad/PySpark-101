"""
Phase 1 — DataFrame Fundamentals, Schemas & I/O
PySpark 4.2.0 — compact lecture file

This file is intentionally lecture-only:
- no applied mastery project;
- no capstone;
- no exercises;
- no assignment scaffolding.

Use it as a pen-and-paper study source: copy the code patterns and the
surrounding comments into your notes.

Phase 1 mental model
--------------------
Raw data
   -> known read options + schema
   -> standardize
   -> type safely
   -> validate
   -> write typed analytical data
"""

from pathlib import Path
from tempfile import TemporaryDirectory

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


# =============================================================================
# 1. SPARK FUNDAMENTALS
# =============================================================================

# SparkSession is the main entry point for DataFrame and Spark SQL work.
spark = (
    SparkSession.builder
    .appName('phase_01_lecture')
    .master('local[*]')
    .getOrCreate()
)


# -----------------------------------------------------------------------------
# Creating DataFrames
# -----------------------------------------------------------------------------

rows = [
    (1001, ' SKU-001 ', '12.99'),
    (1002, 'SKU-002', '8.50'),
]

# Without an explicit schema, Spark infers types from the Python values.
inferred_df = spark.createDataFrame(
    rows,
    ['product_id', 'sku', 'unit_price_raw'],
)

inferred_df.printSchema()


# -----------------------------------------------------------------------------
# DataFrame vs. RDD
# -----------------------------------------------------------------------------

# DataFrame:
# - structured rows;
# - named columns;
# - schema-aware;
# - preferred for structured/semi-structured data engineering.
#
# RDD:
# - lower-level distributed collection;
# - less schema information available to Spark SQL;
# - learn conceptually, but do not default to it for normal DE pipelines.


# -----------------------------------------------------------------------------
# Transformations vs. actions
# -----------------------------------------------------------------------------

# Transformations describe new DataFrames.
# They are lazy: Spark builds a logical plan first.
clean_df = (
    inferred_df
    .filter(F.col('product_id').isNotNull())
    .select('product_id', 'sku')
)

# Actions trigger execution.
clean_df.show()
print(clean_df.count())

# collect() is also an action, but moves all result rows to the driver.
# Only use it when the result is known to be small.
small_rows = clean_df.limit(2).collect()

# Writes also trigger execution.
# df.write.parquet(...) is therefore an action-like boundary.


# -----------------------------------------------------------------------------
# DataFrame immutability
# -----------------------------------------------------------------------------

# DataFrames are immutable.
# drop() returns a new DataFrame rather than mutating inferred_df.
smaller_df = inferred_df.drop('unit_price_raw')


# =============================================================================
# 2. SCHEMAS AND DATA TYPES
# =============================================================================

# A schema is a contract:
# column name + data type + nullability.

product_schema = StructType([
    StructField('product_id', IntegerType(), nullable=False),
    StructField('sku', StringType(), nullable=False),
    StructField(
        'unit_price',
        DecimalType(12, 2),
        nullable=True,
    ),
])

print(product_schema.treeString())


# -----------------------------------------------------------------------------
# Common Spark data types
# -----------------------------------------------------------------------------

# StringType
#     text, codes, raw source values
#
# IntegerType / LongType
#     whole-number quantities and identifiers
#
# DecimalType(p, s)
#     exact decimal values such as money
#
# DateType
#     calendar date
#
# TimestampType / TimestampNTZType
#     timestamps
#
# ArrayType
#     arrays
#
# StructType
#     row schemas and nested objects
#
# MapType
#     key/value structures
#
# For exact monetary values, prefer DecimalType over floating point.


# -----------------------------------------------------------------------------
# StructType and StructField
# -----------------------------------------------------------------------------

typed_rows = [
    (1001, 'SKU-001', None),
    (1002, 'SKU-002', None),
]

typed_df = spark.createDataFrame(
    typed_rows,
    schema=product_schema,
)

typed_df.printSchema()


# -----------------------------------------------------------------------------
# Nullability
# -----------------------------------------------------------------------------

# nullable=False expresses that a field is not expected to be NULL.
#
# Important:
# schema nullability is not a complete data-quality framework.
# Correctness-critical pipelines should still validate required fields.

invalid_required_df = typed_df.filter(
    F.col('product_id').isNull()
    | F.col('sku').isNull()
)


# -----------------------------------------------------------------------------
# Schema inference vs. explicit schema application
# -----------------------------------------------------------------------------

# Inference:
#
# spark.read
#     .option('inferSchema', True)
#     .csv(...)
#
# Spark inspects values and guesses types.
# Convenient for exploration; weaker as a production contract.
#
# Explicit schema:
#
# spark.read
#     .schema(product_schema)
#     .csv(...)
#
# Prefer explicit schemas for controlled/understood inputs because assumptions
# become visible, stable, and testable.


# -----------------------------------------------------------------------------
# Type casting and safe parsing
# -----------------------------------------------------------------------------

cast_source_df = spark.createDataFrame(
    [
        ('12.99',),
        ('not-a-price',),
        (None,),
    ],
    ['unit_price_raw'],
)

# cast() is appropriate when values are expected to be compatible.
cast_example_df = cast_source_df.withColumn(
    'unit_price_cast',
    F.col('unit_price_raw').cast(
        DecimalType(12, 2)
    ),
)

# PySpark 4.x uses ANSI behavior by default.
# For messy input where malformed values should be retained rather than aborting
# the batch, use deliberate safe parsing.
safe_parse_df = cast_source_df.withColumn(
    'unit_price',
    F.expr(
        'try_cast(unit_price_raw AS DECIMAL(12,2))'
    ),
)

# Distinguish:
# raw value missing
# vs.
# raw value present but unparseable.
safe_parse_df = safe_parse_df.withColumn(
    'price_parse_failed',
    F.col('unit_price_raw').isNotNull()
    & F.col('unit_price').isNull(),
)

safe_parse_df.show(truncate=False)


# =============================================================================
# 3. READING AND WRITING DATA
# =============================================================================

with TemporaryDirectory(
    prefix='pyspark_phase_01_'
) as temp_directory:
    temp = Path(temp_directory)

    # =========================================================================
    # CSV
    # =========================================================================

    csv_path = temp / 'orders.csv'

    csv_path.write_text(
        (
            'order_id,sku,quantity,unit_price,order_date\n'
            '1001,SKU-001,2,12.99,2026-08-18\n'
            '1002,SKU-002,3,8.50,2026-08-19\n'
        ),
        encoding='utf-8',
    )

    order_schema = StructType([
        StructField('order_id', StringType(), True),
        StructField('sku', StringType(), True),
        StructField('quantity', IntegerType(), True),
        StructField(
            'unit_price',
            DecimalType(12, 2),
            True,
        ),
        StructField('order_date', DateType(), True),
    ])

    # CSV is row-oriented text.
    # Its schema is not embedded in the file.
    orders_df = (
        spark.read
        .option('header', True)
        .option('mode', 'PERMISSIVE')
        .option('dateFormat', 'yyyy-MM-dd')
        .schema(order_schema)
        .csv(str(csv_path))
    )

    orders_df.show(truncate=False)

    # Useful CSV options:
    #
    # .option('header', True)
    # .option('sep', ',')
    # .option('quote', '"')
    # .option('escape', '"')
    # .option('nullValue', '')
    # .option('dateFormat', 'yyyy-MM-dd')
    # .option('timestampFormat', 'yyyy-MM-dd HH:mm:ss')
    #
    # Make important parsing options explicit when they belong to the source
    # contract.


    # =========================================================================
    # JSON
    # =========================================================================

    json_path = temp / 'events.json'

    # Spark expects newline-delimited JSON by default:
    # one complete JSON object per line.
    json_path.write_text(
        (
            '{"order_id":"1001","event_type":"PLACED"}\n'
            '{"order_id":"1002","event_type":"SHIPPED"}\n'
        ),
        encoding='utf-8',
    )

    event_schema = StructType([
        StructField('order_id', StringType(), True),
        StructField('event_type', StringType(), True),
    ])

    events_df = (
        spark.read
        .schema(event_schema)
        .json(str(json_path))
    )

    events_df.show(truncate=False)


    # =========================================================================
    # PARQUET
    # =========================================================================

    parquet_path = temp / 'orders_parquet'

    # Parquet is a self-describing columnar format.
    (
        orders_df.write
        .mode('overwrite')
        .parquet(str(parquet_path))
    )

    parquet_df = spark.read.parquet(
        str(parquet_path)
    )

    parquet_df.printSchema()
    parquet_df.show(truncate=False)

    # Spark normally writes a DATASET DIRECTORY containing part files,
    # not one familiar single local file.


    # =========================================================================
    # GENERIC LOAD/SAVE STYLE
    # =========================================================================

    generic_read_df = (
        spark.read
        .format('csv')
        .option('header', True)
        .schema(order_schema)
        .load(str(csv_path))
    )

    generic_output_path = temp / 'generic_parquet'

    (
        generic_read_df.write
        .format('parquet')
        .mode('overwrite')
        .save(str(generic_output_path))
    )


    # =========================================================================
    # WRITE MODES
    # =========================================================================

    # error / errorifexists
    #     fail if target already exists
    #
    # overwrite
    #     replace existing output according to writer semantics
    #
    # append
    #     add new output
    #
    # ignore
    #     do nothing if target already exists


    # =============================================================================
    # 4. READ MODES AND MALFORMED RECORDS
    # =============================================================================

    malformed_path = temp / 'malformed_orders.csv'

    malformed_path.write_text(
        (
            'order_id,sku,quantity\n'
            '1001,SKU-001,2\n'
            '1002,SKU-002,abc\n'
            '1003,SKU-003,3,EXTRA\n'
        ),
        encoding='utf-8',
    )

    malformed_schema = StructType([
        StructField('order_id', StringType(), True),
        StructField('sku', StringType(), True),
        StructField('quantity', IntegerType(), True),
        StructField(
            '_corrupt_record',
            StringType(),
            True,
        ),
    ])

    # PERMISSIVE:
    # attempts to preserve the row and can store malformed source content
    # in a corrupt-record field.
    permissive_df = (
        spark.read
        .option('header', True)
        .option('mode', 'PERMISSIVE')
        .option(
            'columnNameOfCorruptRecord',
            '_corrupt_record',
        )
        .schema(malformed_schema)
        .csv(str(malformed_path))
    )

    permissive_df.show(truncate=False)

    # DROPMALFORMED:
    # drops malformed records.
    #
    # Trade-off:
    # convenient, but rejected evidence is lost and data loss can be hidden.

    drop_schema = StructType([
        StructField('order_id', StringType(), True),
        StructField('sku', StringType(), True),
        StructField('quantity', IntegerType(), True),
    ])

    drop_df = (
        spark.read
        .option('header', True)
        .option('mode', 'DROPMALFORMED')
        .schema(drop_schema)
        .csv(str(malformed_path))
    )

    print(drop_df.count())

    # FAILFAST:
    # aborts when malformed data is encountered.
    failfast_df = (
        spark.read
        .option('header', True)
        .option('mode', 'FAILFAST')
        .schema(drop_schema)
        .csv(str(malformed_path))
    )

    # Lazy evaluation still applies:
    # defining failfast_df does not necessarily trigger the error.
    #
    # An action such as:
    #
    # failfast_df.show()
    #
    # forces execution.


# -----------------------------------------------------------------------------
# Malformed record vs. business-invalid record
# -----------------------------------------------------------------------------

# Parse failure:
#     a source value cannot be converted or parsed correctly.
#
# Business-invalid:
#     the row parses correctly but violates a business rule.
#
# Example:
#     quantity_raw = 'abc'
#         -> parse/type failure
#
#     quantity = -5
#         -> valid integer, but possibly business-invalid
#
# Keep parsing concerns separate from business validation.


# =============================================================================
# 5. CORE DATAFRAME OPERATIONS
# =============================================================================

operations_df = spark.createDataFrame(
    [
        ('1001', ' sku-001 ', 2, 12.99),
        ('1001', ' sku-001 ', 2, 12.99),
        ('1002', 'SKU-002', 3, 8.50),
    ],
    [
        'order_id',
        'sku',
        'quantity',
        'unit_price',
    ],
)


# -----------------------------------------------------------------------------
# select()
# -----------------------------------------------------------------------------

selected_df = operations_df.select(
    'order_id',
    'quantity',
    (
        F.col('quantity') * F.col('unit_price')
    ).alias('gross_sales'),
)


# -----------------------------------------------------------------------------
# alias()
# -----------------------------------------------------------------------------

alias_df = operations_df.select(
    F.col('unit_price').alias('price')
)

# A DataFrame itself can also be aliased:
orders_alias = operations_df.alias('o')


# -----------------------------------------------------------------------------
# filter() / where()
# -----------------------------------------------------------------------------

filtered_df = operations_df.filter(
    F.col('quantity') > 0
)

where_df = operations_df.where(
    F.col('quantity') > 0
)


# -----------------------------------------------------------------------------
# withColumn()
# -----------------------------------------------------------------------------

standardized_df = operations_df.withColumn(
    'sku',
    F.upper(F.trim(F.col('sku')))
)


# -----------------------------------------------------------------------------
# drop()
# -----------------------------------------------------------------------------

without_price_df = standardized_df.drop(
    'unit_price'
)


# -----------------------------------------------------------------------------
# distinct()
# -----------------------------------------------------------------------------

# Removes exact duplicate FULL rows.
distinct_df = standardized_df.distinct()


# -----------------------------------------------------------------------------
# dropDuplicates()
# -----------------------------------------------------------------------------

# Defines duplicate identity using selected columns.
deduplicated_df = standardized_df.dropDuplicates(
    ['order_id', 'sku']
)

# Do not assume dropDuplicates() chooses a deterministic preferred row when
# different rows share the same key.


# -----------------------------------------------------------------------------
# orderBy()
# -----------------------------------------------------------------------------

ordered_df = standardized_df.orderBy(
    F.col('order_id').asc()
)

# Global sorting can be expensive on distributed data.
# Use it only when order is actually required.


# -----------------------------------------------------------------------------
# limit()
# -----------------------------------------------------------------------------

preview_df = ordered_df.limit(2)

# A preview is useful for inspection, but does not prove full-data correctness.


# =============================================================================
# 6. EXPRESSIONS AND FUNCTIONS
# =============================================================================

# DataFrame transformations are built from Column expressions.


# -----------------------------------------------------------------------------
# col()
# -----------------------------------------------------------------------------

quantity_column = F.col('quantity')


# -----------------------------------------------------------------------------
# lit()
# -----------------------------------------------------------------------------

literal_column = F.lit('PHASE_1')


# -----------------------------------------------------------------------------
# when() / otherwise()
# -----------------------------------------------------------------------------

classified_df = standardized_df.withColumn(
    'quantity_status',
    F.when(
        F.col('quantity').isNull(),
        F.lit('MISSING'),
    )
    .when(
        F.col('quantity') <= 0,
        F.lit('INVALID'),
    )
    .otherwise(
        F.lit('VALID'),
    ),
)

classified_df.show(truncate=False)

# Prefer built-in expressions over collecting rows into Python and looping.
# Built-in expressions stay inside Spark's structured execution engine and are
# easier for Spark to analyze and optimize.


# =============================================================================
# 7. NULL HANDLING
# =============================================================================

null_df = spark.createDataFrame(
    [
        ('1001', 2, 'ON'),
        ('1002', None, None),
    ],
    ['order_id', 'quantity', 'province'],
)


# -----------------------------------------------------------------------------
# Detecting NULL
# -----------------------------------------------------------------------------

missing_df = null_df.filter(
    F.col('quantity').isNull()
)

present_df = null_df.filter(
    F.col('quantity').isNotNull()
)

# Do not write:
#
# F.col('quantity') == None


# -----------------------------------------------------------------------------
# fillna()
# -----------------------------------------------------------------------------

# Only use a replacement when it has domain meaning.
filled_df = null_df.fillna(
    {'province': 'UNKNOWN'}
)


# -----------------------------------------------------------------------------
# coalesce()
# -----------------------------------------------------------------------------

# Returns the first non-null VALUE expression.
display_df = null_df.select(
    F.coalesce(
        F.col('province'),
        F.lit('UNKNOWN'),
    ).alias('province_display')
)

# This functions.coalesce() is NOT DataFrame.coalesce(),
# which changes execution partition counts.


# -----------------------------------------------------------------------------
# Three-valued logic
# -----------------------------------------------------------------------------

# quantity > 0 evaluates to NULL when quantity is NULL.
# If NULL should be invalid, define the condition explicitly.

valid_quantity = (
    F.col('quantity').isNotNull()
    & (F.col('quantity') > 0)
)

null_df.select(
    'order_id',
    'quantity',
    valid_quantity.alias('valid_quantity'),
).show()


# =============================================================================
# 8. STRING, NUMERIC, DATE, AND TIMESTAMP FUNCTIONS
# =============================================================================

function_df = spark.createDataFrame(
    [
        (
            '1001',
            ' sku-001 ',
            'on',
            ' USER@EXAMPLE.COM ',
            12.995,
            -2,
            '2026-08-18',
            '2026-08-18 09:15:30',
        ),
    ],
    [
        'order_id',
        'sku',
        'province',
        'email',
        'unit_price',
        'variance',
        'order_date_raw',
        'order_ts_raw',
    ],
)


# -----------------------------------------------------------------------------
# String functions
# -----------------------------------------------------------------------------

string_df = (
    function_df
    .withColumn(
        'sku',
        F.upper(F.trim(F.col('sku'))),
    )
    .withColumn(
        'province',
        F.upper(F.trim(F.col('province'))),
    )
    .withColumn(
        'email',
        F.lower(F.trim(F.col('email'))),
    )
    .withColumn(
        'sku_length',
        F.length(F.col('sku')),
    )
    .withColumn(
        'sku_digits',
        F.regexp_replace(
            F.col('sku'),
            r'[^0-9]',
            '',
        ),
    )
    .withColumn(
        'order_sku_key',
        F.concat_ws(
            '-',
            F.col('order_id'),
            F.col('sku'),
        ),
    )
)

# Other useful string functions:
#
# F.split(...)


# -----------------------------------------------------------------------------
# Numeric functions
# -----------------------------------------------------------------------------

numeric_df = (
    string_df
    .withColumn(
        'rounded_price',
        F.round(F.col('unit_price'), 2),
    )
    .withColumn(
        'bankers_round',
        F.bround(F.col('unit_price'), 2),
    )
    .withColumn(
        'variance_abs',
        F.abs(F.col('variance')),
    )
)

# For currency:
# - prefer DecimalType when exact decimal semantics matter;
# - define precision/scale deliberately;
# - do not use abs() to "repair" a negative business value unless the domain
#   actually defines that behavior.


# -----------------------------------------------------------------------------
# Dates
# -----------------------------------------------------------------------------

date_df = numeric_df.withColumn(
    'order_date',
    F.to_date(
        F.col('order_date_raw'),
        'yyyy-MM-dd',
    ),
)

date_df = (
    date_df
    .withColumn(
        'order_year',
        F.year(F.col('order_date')),
    )
    .withColumn(
        'order_month',
        F.month(F.col('order_date')),
    )
    .withColumn(
        'order_day',
        F.dayofmonth(F.col('order_date')),
    )
    .withColumn(
        'next_week',
        F.date_add(F.col('order_date'), 7),
    )
    .withColumn(
        'previous_week',
        F.date_sub(F.col('order_date'), 7),
    )
    .withColumn(
        'month_label',
        F.date_format(
            F.col('order_date'),
            'yyyy-MM',
        ),
    )
)

# date_format() returns a STRING.


# -----------------------------------------------------------------------------
# Timestamps
# -----------------------------------------------------------------------------

timestamp_df = date_df.withColumn(
    'order_ts',
    F.to_timestamp(
        F.col('order_ts_raw'),
        'yyyy-MM-dd HH:mm:ss',
    ),
)

timestamp_df = (
    timestamp_df
    .withColumn(
        'event_year',
        F.year(F.col('order_ts')),
    )
    .withColumn(
        'event_month',
        F.month(F.col('order_ts')),
    )
    .withColumn(
        'event_hour',
        F.hour(F.col('order_ts')),
    )
)

timestamp_df.show(truncate=False)

# Parsing discipline:
# specify expected source patterns when the source contract defines them.
#
# Phase 1 distinction:
# DateType
#     calendar date
#
# Timestamp
#     date + time semantics
#
# Do not treat those as interchangeable.


# =============================================================================
# 9. ARRAYS, STRUCTS, AND NESTED DATA
# =============================================================================

with TemporaryDirectory(
    prefix='pyspark_phase_01_nested_'
) as nested_temp_directory:
    nested_temp = Path(nested_temp_directory)

    item_schema = StructType([
        StructField('sku', StringType(), True),
        StructField('quantity', IntegerType(), True),
    ])

    nested_order_schema = StructType([
        StructField('order_id', StringType(), False),

        # Nested JSON object -> StructType.
        StructField(
            'customer',
            StructType([
                StructField(
                    'customer_id',
                    StringType(),
                    True,
                ),
                StructField(
                    'province',
                    StringType(),
                    True,
                ),
            ]),
            True,
        ),

        # JSON array -> ArrayType.
        StructField(
            'items',
            ArrayType(
                item_schema,
                containsNull=False,
            ),
            True,
        ),
    ])

    nested_json_path = nested_temp / 'orders.json'

    nested_json_path.write_text(
        (
            '{"order_id":"1001",'
            '"customer":{"customer_id":"C001","province":"ON"},'
            '"items":[{"sku":"SKU-001","quantity":2},'
            '{"sku":"SKU-002","quantity":1}]}\n'
            '{"order_id":"1002",'
            '"customer":{"customer_id":"C002","province":"QC"},'
            '"items":[{"sku":"SKU-003","quantity":4}]}\n'
        ),
        encoding='utf-8',
    )

    nested_df = (
        spark.read
        .schema(nested_order_schema)
        .json(str(nested_json_path))
    )

    nested_df.printSchema()


    # -------------------------------------------------------------------------
    # Nested-field access
    # -------------------------------------------------------------------------

    nested_df.select(
        'order_id',
        F.col(
            'customer.customer_id'
        ).alias('customer_id'),
        F.col(
            'customer.province'
        ).alias('province'),
    ).show(truncate=False)


    # -------------------------------------------------------------------------
    # Array functions
    # -------------------------------------------------------------------------

    nested_df.select(
        'order_id',
        F.size(F.col('items')).alias('item_count'),
    ).show()

    # Other useful array functions:
    #
    # F.array_contains(...)
    # F.element_at(...)


    # -------------------------------------------------------------------------
    # struct()
    # -------------------------------------------------------------------------

    structured_df = nested_df.select(
        'order_id',
        F.struct(
            F.col('customer.customer_id').alias(
                'customer_id'
            ),
            F.col('customer.province').alias(
                'province'
            ),
        ).alias('customer_copy'),
    )

    structured_df.show(truncate=False)


    # -------------------------------------------------------------------------
    # Arrays of structs and explode()
    # -------------------------------------------------------------------------

    # ARRAY<STRUCT<...>> is a common nested shape.
    #
    # explode() converts one array element into one output row.
    # Therefore it CHANGES GRAIN.
    #
    # One row per order
    #     ->
    # one row per order item

    item_grain_df = (
        nested_df
        .withColumn(
            'item',
            F.explode(F.col('items')),
        )
        .select(
            'order_id',
            F.col('item.sku').alias('sku'),
            F.col('item.quantity').alias(
                'quantity'
            ),
        )
    )

    item_grain_df.show(truncate=False)


# =============================================================================
# 10. INTRODUCTORY STORAGE CONCEPTS
# =============================================================================

# Row-oriented
# ------------
# Conceptually stores:
#
# row 1: order_id, customer_id, amount, date
# row 2: order_id, customer_id, amount, date
#
# CSV is effectively row-oriented text.
#
# Strengths:
# - interchange;
# - human inspection.
#
# Weaknesses for analytics:
# - no embedded strong schema;
# - text parsing overhead;
# - weaker analytical compression/typing characteristics.


# Columnar
# --------
# Conceptually groups values by column:
#
# order_id values: ...
# customer_id values: ...
# amount values: ...
#
# Parquet is columnar.
#
# Strengths:
# - analytical scans;
# - typed metadata;
# - compression/encoding;
# - reading only needed columns.


# -----------------------------------------------------------------------------
# Compression
# -----------------------------------------------------------------------------

# Compression reduces physical data volume.
#
# Trade-off:
# less storage / I/O
# vs.
# CPU needed to compress/decompress.
#
# Analytical pipelines often benefit because reducing I/O is valuable.


# -----------------------------------------------------------------------------
# Schema preservation
# -----------------------------------------------------------------------------

# CSV:
# schema is not embedded as a Spark data contract.
#
# Parquet:
# stores type information in metadata.


# -----------------------------------------------------------------------------
# Column pruning
# -----------------------------------------------------------------------------

# If a Parquet dataset has many columns but a query needs only:
#
# df.select('order_id', 'order_date')
#
# the columnar reader can avoid reading unnecessary columns when the execution
# plan permits it.
#
# This is column pruning.


# =============================================================================
# 11. DATA-ENGINEERING PRACTICES
# =============================================================================

# 1. Define controlled source contracts explicitly.
#
# ORDER_SCHEMA = StructType([...])

# 2. Keep conceptual pipeline states separate:
#
# raw_df
# standardized_df
# typed_df
# valid_df
# rejected_df

# 3. Preserve raw evidence until rejection is explainable:
#
# unit_price_raw = '12.O9'
# unit_price     = NULL
# rejection_reason = 'INVALID_UNIT_PRICE'

# 4. Keep parse rules separate from business rules:
#
# 'abc' quantity -> parse problem
# -5 quantity    -> parsed integer, business-rule problem

# 5. Standardize only when the domain defines the normalization.
#
# Province code:
# F.upper(F.trim('province'))
#
# Free-text product name:
# do not normalize destructively without a business reason.

# 6. Use exact numeric types for exact values:
#
# DecimalType(12, 2)

# 7. Row counts are useful reconciliation evidence:
#
# candidate rows
# =
# accepted rows + rejected rows

# 8. Rejected records should include a rejection reason.

# 9. Prefer deterministic transformations.

# 10. As code grows, recognize the architectural boundary:
#
# read -> standardize/type -> validate -> write


# =============================================================================
# 12. COMMON PITFALLS
# =============================================================================

# - relying on inferSchema as a production contract;
# - reading everything as strings and never producing typed curated data;
# - assuming explicit schema means the data is business-valid;
# - treating nullable=False as complete validation;
# - using DROPMALFORMED and losing rejected evidence;
# - confusing malformed records with business-invalid records;
# - using dropna() instead of preserving rejects;
# - filling every NULL automatically;
# - casting messy strings without understanding ANSI behavior;
# - using DoubleType for exact monetary values by habit;
# - calling collect() on unknown-size data;
# - assuming show() proves correctness;
# - deduplicating without defining duplicate identity;
# - sorting intermediate data for no business reason;
# - expecting Spark writes to produce one local file;
# - committing generated Parquet outputs to Git.


# =============================================================================
# 13. PHASE 1 KNOWLEDGE SUMMARY
# =============================================================================

# By the end of the lecture, know and be able to explain:
#
# SPARK
# - SparkSession
# - DataFrame vs. RDD
# - transformations vs. actions
# - lazy evaluation
# - DataFrame immutability
#
# SCHEMAS
# - StructType
# - StructField
# - common Spark data types
# - nullability
# - explicit schemas
# - inference vs. enforcement
# - safe type conversion
#
# I/O
# - CSV
# - JSON
# - Parquet
# - read modes
# - write modes
# - malformed records
# - Spark dataset-directory writes
#
# DATAFRAME API
# - select
# - alias
# - filter / where
# - withColumn
# - drop
# - distinct
# - dropDuplicates
# - orderBy
# - limit
#
# EXPRESSIONS / DATA
# - col
# - lit
# - when / otherwise
# - NULL checks
# - coalesce
# - string functions
# - numeric functions
# - date/timestamp functions
# - ArrayType
# - StructType
# - nested-field access
#
# STORAGE
# - row-oriented vs. columnar
# - why Parquet
# - compression
# - schema preservation
# - column pruning


spark.stop()
