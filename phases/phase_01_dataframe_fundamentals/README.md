# Phase 1 — DataFrame Fundamentals, Schemas & I/O

## Objective

Become completely comfortable reading, manipulating, validating, and writing structured data with the PySpark DataFrame API.

Phase 1 establishes the habits that every later phase depends on: make schemas explicit, keep transformations declarative, distinguish parse failures from business-rule failures, preserve rejected data, and write typed analytical outputs rather than allowing raw-file quirks to leak downstream.

Examples in these notes target the repository's current dependency, **PySpark 4.2.0**, and use the repository's retail-data direction.

---

## Table of Contents

- [Phase Mental Model](#phase-mental-model)
- [1. Spark Fundamentals](#1-spark-fundamentals)
- [2. Schemas and Data Types](#2-schemas-and-data-types)
- [3. Reading and Writing Data](#3-reading-and-writing-data)
- [4. Read Modes and Malformed Records](#4-read-modes-and-malformed-records)
- [5. Core DataFrame Operations](#5-core-dataframe-operations)
- [6. Expressions and Functions](#6-expressions-and-functions)
- [7. NULL Handling](#7-null-handling)
- [8. String, Numeric, Date, and Timestamp Functions](#8-string-numeric-date-and-timestamp-functions)
- [9. Arrays, Structs, and Nested Data](#9-arrays-structs-and-nested-data)
- [10. Introductory Storage Concepts](#10-introductory-storage-concepts)
- [11. Data-Engineering Practices](#11-data-engineering-practices)
- [12. Common Pitfalls](#12-common-pitfalls)
- [13. Applied Mastery Target](#13-applied-mastery-target)
- [14. Phase 1 Mastery Checklist](#14-phase-1-mastery-checklist)

---

## Phase Mental Model

A basic production-style batch flow is:

```text
Raw files
   ↓
Read with known options and schema
   ↓
Parse / type safely
   ↓
Standardize values and columns
   ↓
Validate
   ├── valid    → clean typed DataFrame
   └── rejected → rejected DataFrame + reason
   ↓
Write clean data as Parquet
```

The central idea is that a DataFrame is not merely "rows in Python." It is a distributed, schema-aware logical dataset that Spark can analyze and execute across a cluster.

For each dataset, be able to answer:

1. What does one row represent?
2. What schema do I expect?
3. Which fields may legitimately be `NULL`?
4. What makes a record malformed versus business-invalid?
5. What transformations standardize the raw data?
6. What should happen to rejected records?
7. What schema and format should downstream consumers receive?

---

## 1. Spark Fundamentals

### `SparkSession`

`SparkSession` is the main entry point for DataFrame and Spark SQL work.

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName('phase_01_retail')
    .getOrCreate()
)
```

Common uses include:

```python
spark.createDataFrame(...)
spark.read.csv(...)
spark.read.json(...)
spark.read.parquet(...)
spark.range(...)
```

In a normal application, create or receive a `SparkSession` at the application boundary rather than constructing new sessions inside transformation functions.

### Creating DataFrames

From local Python data:

```python
rows = [
    (1001, ' SKU-001 ', '12.99'),
    (1002, 'SKU-002', '8.50'),
]

df = spark.createDataFrame(
    rows,
    ['product_id', 'sku', 'unit_price_raw'],
)
```

With an explicit schema:

```python
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

schema = StructType([
    StructField('product_id', IntegerType(), nullable=False),
    StructField('sku', StringType(), nullable=False),
    StructField('unit_price_raw', StringType(), nullable=True),
])

df = spark.createDataFrame(rows, schema=schema)
```

For ingestion work, constructing a DataFrame in memory is mainly useful for small examples and tests. Production pipelines usually create DataFrames by reading external data.

### DataFrames vs. RDDs

A **DataFrame** is structured data organized into named columns and described by a schema. Spark can reason about its columns, types, expressions, and relational operations.

An **RDD** is a lower-level distributed collection of JVM/Python objects without the same structured schema information available to Spark SQL's optimizer.

For data engineering:

- prefer DataFrames for structured and semi-structured pipelines;
- use built-in DataFrame expressions before considering lower-level RDD logic;
- learn RDDs conceptually now, but do not make them the default API.

### Transformations vs. actions

A **transformation** describes a new DataFrame without immediately computing the result.

Examples:

```python
clean_df = (
    df
    .filter('product_id IS NOT NULL')
    .select('product_id', 'sku')
)
```

Typical transformations include:

- `select`
- `filter` / `where`
- `withColumn`
- `drop`
- `distinct`
- `dropDuplicates`
- `orderBy`
- `limit`

An **action** asks Spark to produce a result or materialize work.

Examples:

```python
clean_df.show()
clean_df.count()
rows = clean_df.collect()
```

Writes also trigger execution:

```python
clean_df.write.mode('overwrite').parquet('data/curated/products')
```

### Lazy evaluation

Spark DataFrame transformations are lazy: Spark builds a logical plan as transformations are chained, then performs the work when an action requires a result.

```python
filtered_df = df.filter('product_id > 1000')   # describes work
selected_df = filtered_df.select('product_id') # extends the plan

selected_df.show()                             # triggers execution
```

Why this matters:

- chained transformations do not imply one immediate pass per line of Python;
- errors involving actual data may surface only when an action executes;
- repeated actions can recompute prior transformations unless Spark can reuse materialized results;
- transformation code should describe the intended dataset clearly rather than relying on row-by-row Python control flow.

Deeper job/stage/task execution comes later in the curriculum.

---

## 2. Schemas and Data Types

A schema is a contract describing column names, data types, nested structure, and nullability.

Inspect a schema with:

```python
df.printSchema()
print(df.schema)
```

### Common Spark data types

| Category | Common PySpark types | Typical retail use |
|---|---|---|
| Text | `StringType` | SKU, status, province |
| Boolean | `BooleanType` | active flag |
| Integers | `IntegerType`, `LongType` | quantities, identifiers |
| Exact numeric | `DecimalType(p, s)` | prices, costs, currency |
| Floating point | `FloatType`, `DoubleType` | measurements where binary floating point is acceptable |
| Date/time | `DateType`, `TimestampType`, `TimestampNTZType` | order date, event timestamp |
| Nested | `ArrayType`, `StructType`, `MapType` | semi-structured JSON attributes |

For monetary values, prefer `DecimalType` when exact decimal arithmetic is required.

```python
from pyspark.sql.types import DecimalType

money_type = DecimalType(12, 2)
```

`DecimalType(12, 2)` allows 12 total digits, 2 of them after the decimal point.

### `StructType` and `StructField`

A row schema is commonly represented as a `StructType` containing `StructField` objects.

```python
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

product_schema = StructType([
    StructField('product_id', IntegerType(), nullable=False),
    StructField('sku', StringType(), nullable=False),
    StructField('product_name', StringType(), nullable=True),
    StructField('unit_price', DecimalType(12, 2), nullable=True),
])
```

Each field expresses:

```text
name + data type + nullable flag
```

### Nullable fields

`nullable=False` documents that the field is not expected to be null in the DataFrame schema.

Treat nullability as part of the data contract, but do not confuse schema metadata with a complete data-quality framework. File-based reads and writes can have format/provider-specific behavior, and production validation should still check required fields explicitly.

```python
from pyspark.sql import functions as F

invalid_required_df = df.filter(
    F.col('product_id').isNull() | F.col('sku').isNull()
)
```

### Explicit schemas

Prefer explicit schemas when the input contract is controlled or understood.

Benefits:

- assumptions are visible in code;
- types do not change because of different input samples;
- malformed input becomes easier to reason about;
- schema behavior is testable;
- inference does not require an extra discovery pass for formats such as CSV;
- downstream contracts become more stable.

### Schema inference vs. schema enforcement

**Inference** asks Spark to inspect the data and guess types.

```python
df = (
    spark.read
    .option('header', True)
    .option('inferSchema', True)
    .csv('data/raw/products.csv')
)
```

This is convenient for exploration but risky as a production contract. A future file can produce a different inferred type because its values differ from the previous sample.

**Explicit schema application** tells Spark what structure to parse into.

```python
df = (
    spark.read
    .option('header', True)
    .schema(product_schema)
    .csv('data/raw/products.csv')
)
```

An explicit schema is not the same thing as "all input is valid." A parser still needs defined behavior for bad tokens, missing values, extra fields, malformed JSON, and other input problems.

### Type casting

Use `cast()` when converting compatible values:

```python
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

typed_df = df.withColumn(
    'unit_price',
    F.col('unit_price_raw').cast(DecimalType(12, 2)),
)
```

PySpark 4.x uses ANSI behavior by default, so invalid casts can raise runtime exceptions instead of silently becoming `NULL`. When malformed values should be retained for rejection rather than aborting the batch, use deliberate safe parsing such as `try_cast` and then validate the resulting `NULL`.

```python
typed_df = df.withColumn(
    'unit_price',
    F.expr('try_cast(unit_price_raw AS DECIMAL(12,2))'),
)
```

Then distinguish:

```text
raw value missing legitimately
vs.
raw value present but unparseable
```

For example:

```python
bad_price = (
    F.col('unit_price_raw').isNotNull()
    & F.col('unit_price').isNull()
)
```

---

## 3. Reading and Writing Data

Spark uses the DataFrame reader/writer interfaces:

```text
spark.read...
df.write...
```

### CSV

CSV is row-oriented text with weak typing. The schema is not embedded in the file.

```python
orders_df = (
    spark.read
    .option('header', True)
    .option('mode', 'PERMISSIVE')
    .option('dateFormat', 'yyyy-MM-dd')
    .schema(order_schema)
    .csv('data/raw/orders.csv')
)
```

Useful CSV options include:

```python
.option('header', True)
.option('sep', ',')
.option('quote', '"')
.option('escape', '"')
.option('nullValue', '')
.option('dateFormat', 'yyyy-MM-dd')
.option('timestampFormat', 'yyyy-MM-dd HH:mm:ss')
```

Do not assume defaults match a source-system contract. Make important parsing options explicit.

### JSON

Spark expects newline-delimited JSON by default: normally one complete JSON object per line.

```python
events_df = (
    spark.read
    .schema(event_schema)
    .json('data/raw/order_events.json')
)
```

Example input:

```json
{"order_id":1001,"event_type":"PLACED","metadata":{"channel":"web"}}
{"order_id":1002,"event_type":"SHIPPED","metadata":{"channel":"store"}}
```

For pretty-printed multi-line JSON documents, use the appropriate `multiLine` option only when that is truly the source format.

JSON is useful for learning nested structures because arrays and structs map naturally into Spark's nested data types.

### Parquet

Parquet is a self-describing columnar format well suited to analytical processing.

Write:

```python
clean_df.write.mode('overwrite').parquet(
    'data/curated/products'
)
```

Read:

```python
clean_df = spark.read.parquet(
    'data/curated/products'
)
```

Parquet preserves schema information in its metadata, unlike CSV. Spark's Parquet reader treats read columns as nullable for compatibility, so downstream validation should not rely solely on a non-nullable source schema surviving every storage boundary.

### Generic load/save style

Equivalent generic syntax is also available:

```python
df = (
    spark.read
    .format('csv')
    .option('header', True)
    .schema(product_schema)
    .load('data/raw/products.csv')
)
```

```python
(
    clean_df.write
    .format('parquet')
    .mode('overwrite')
    .save('data/curated/products')
)
```

Prefer the style that makes the pipeline clearest and stay consistent within a codebase.

### Write modes

Do not confuse **read parse modes** with **write save modes**.

Common write modes:

| Write mode | Meaning |
|---|---|
| `error` / `errorifexists` | fail if output already exists |
| `overwrite` | replace existing output according to writer semantics |
| `append` | add new output |
| `ignore` | do nothing if output exists |

Example:

```python
clean_df.write.mode('overwrite').parquet(output_path)
```

Append/overwrite semantics become more important in later incremental-processing phases. For now, always choose the mode intentionally rather than accepting it accidentally.

### Spark writes datasets, not one normal local file

This:

```python
df.write.parquet('data/curated/orders')
```

typically creates a directory containing one or more part files plus metadata/success markers, not a single `orders.parquet` file.

That behavior reflects distributed execution. Avoid forcing a single file merely to make output look familiar.

---

## 4. Read Modes and Malformed Records

Malformed-record behavior is part of the ingestion contract.

### Parse modes

CSV and JSON readers support these major modes:

| Mode | Behavior | Engineering trade-off |
|---|---|---|
| `PERMISSIVE` | attempts to preserve the row; malformed content can be captured in a corrupt-record field | useful when bad input must be quarantined and investigated |
| `DROPMALFORMED` | drops malformed records | convenient, but destroys evidence and can hide data loss |
| `FAILFAST` | aborts when a malformed record is encountered | appropriate when the contract is strict and partial ingestion is unacceptable |

Example:

```python
raw_schema = StructType([
    StructField('order_id', StringType(), True),
    StructField('customer_id', StringType(), True),
    StructField('order_ts', StringType(), True),
    StructField('_corrupt_record', StringType(), True),
])

raw_df = (
    spark.read
    .option('header', True)
    .option('mode', 'PERMISSIVE')
    .option('columnNameOfCorruptRecord', '_corrupt_record')
    .schema(raw_schema)
    .csv('data/raw/orders.csv')
)
```

### Why `PERMISSIVE` often fits quarantine pipelines

A data engineer frequently needs both:

```text
accepted records
rejected records + evidence
```

Silently dropping malformed rows loses:

- the original bad payload;
- rejection counts;
- debugging evidence;
- auditability.

`PERMISSIVE` plus an explicit corrupt-record field is often a better learning and production pattern when rejects must be preserved.

### Important CSV nuance

For CSV, "wrong number of tokens" is not always classified as a corrupt record in the way newcomers expect:

- fewer tokens can result in missing columns becoming `NULL`;
- extra tokens can be dropped;
- parse behavior can depend on the required columns.

Therefore, malformed-record handling must be tested against representative files rather than inferred from the option name alone.

### Malformed record vs. invalid business record

These are different categories.

**Malformed / parse failure**

```text
"1001,abc,2026-08-19
```

Examples:

- broken quoting;
- invalid JSON syntax;
- an unparseable typed field;
- structurally invalid input.

**Business-invalid**

```text
order_id=1001, quantity=-5, unit_price=12.99
```

The row can be parsed perfectly but violates a business rule.

A strong pipeline keeps those concerns separate:

```text
raw parsing problem          → parsing rejection
parsed but invalid business  → validation rejection
valid                         → accepted
```

Phase 1 only needs a focused reject flow; a reusable data-quality framework is a later curriculum phase.

### Corrupt files vs. corrupt records

Also distinguish:

- **corrupt record**: a row/payload within a supported source format cannot be parsed correctly;
- **corrupt file**: the file itself is unreadable for the selected data source.

Spark has file-source options such as `ignoreCorruptFiles`, but silently ignoring files can cause missing data. Use such options only with explicit operational reasoning.

---

## 5. Core DataFrame Operations

Assume:

```python
from pyspark.sql import functions as F
```

### `select()`

Choose columns and expressions.

```python
selected_df = df.select(
    'order_id',
    'store_id',
    F.col('quantity'),
    (F.col('quantity') * F.col('unit_price')).alias('gross_sales'),
)
```

Prefer projecting only columns that the next step needs.

### `alias()`

Rename an expression:

```python
df.select(
    F.col('unit_price').alias('price')
)
```

A DataFrame can also be aliased, which becomes important for joins later:

```python
orders = orders_df.alias('o')
```

### `filter()` / `where()`

They are aliases for row filtering.

```python
valid_qty_df = df.filter(F.col('quantity') > 0)
```

```python
valid_qty_df = df.where(F.col('quantity') > 0)
```

Prefer Column expressions for composability:

```python
is_valid = (
    F.col('order_id').isNotNull()
    & (F.col('quantity') > 0)
)

valid_df = df.filter(is_valid)
rejected_df = df.filter(~is_valid)
```

Be careful when negating conditions containing `NULL`; three-valued logic can make `~condition` evaluate to `NULL`, not `True`.

### `withColumn()`

Add or replace one column:

```python
standardized_df = df.withColumn(
    'sku',
    F.upper(F.trim(F.col('sku')))
)
```

The original DataFrame is unchanged; a new DataFrame plan is returned.

### `drop()`

Remove columns:

```python
clean_df = typed_df.drop(
    'unit_price_raw',
    '_corrupt_record',
)
```

Do not drop raw evidence before reject routing is complete.

### `distinct()`

Remove duplicate full rows:

```python
unique_rows_df = df.distinct()
```

This answers: "Are these complete rows identical?"

### `dropDuplicates()`

Deduplicate using all columns or a chosen key subset:

```python
deduped_df = df.dropDuplicates(['order_id'])
```

This answers a different question: "Which columns define duplicate identity?"

Do not use deduplication as a substitute for understanding why duplicates exist. If multiple different rows share the same business key, arbitrarily keeping one can hide source-system problems.

### `orderBy()`

Sort rows:

```python
ordered_df = df.orderBy(
    F.col('order_ts').desc(),
    F.col('order_id').asc(),
)
```

Global ordering can be expensive on distributed data. Use it when the output actually requires an order, not merely to make intermediate results visually tidy.

### `limit()`

Return a DataFrame representing at most `n` rows:

```python
sample_df = df.limit(20)
```

Useful for inspection, but a small preview is not proof that the full dataset is correct.

---

## 6. Expressions and Functions

DataFrame transformations are built from **Column expressions**.

### `col()`

Reference a column explicitly:

```python
F.col('quantity')
```

Examples:

```python
F.col('quantity') > 0
F.col('sku').isNull()
F.col('unit_price') * F.col('quantity')
```

### `lit()`

Create a literal Column expression:

```python
F.lit('PHASE_1')
F.lit(0)
F.lit(None)
```

Example:

```python
df.withColumn('pipeline_stage', F.lit('clean'))
```

### `when()` / `otherwise()`

Build conditional expressions.

```python
classified_df = df.withColumn(
    'stock_status',
    F.when(F.col('on_hand_qty') <= 0, F.lit('OUT_OF_STOCK'))
     .when(F.col('on_hand_qty') < 10, F.lit('LOW_STOCK'))
     .otherwise(F.lit('IN_STOCK'))
)
```

Without `otherwise()`, unmatched rows receive `NULL`.

### Prefer built-in expressions over Python row logic

Prefer:

```python
df.withColumn(
    'gross_sales',
    F.col('quantity') * F.col('unit_price'),
)
```

over collecting rows into Python and looping.

Built-in expressions:

- stay inside Spark's structured execution engine;
- preserve schema information;
- are easier to analyze and optimize;
- scale beyond a single Python process.

Custom UDF performance is a later topic.

---

## 7. NULL Handling

`NULL` means the value is unknown or absent. It is not the same as:

```text
0
""
"NULL"
"unknown"
NaN
```

### Detecting nulls

Use:

```python
F.col('customer_id').isNull()
F.col('customer_id').isNotNull()
```

Do not write Python-style comparisons such as:

```python
F.col('customer_id') == None
```

### Filling nulls

`fillna` is useful when a domain-specific default is actually valid:

```python
filled_df = df.fillna(
    {'discount_amount': 0}
)
```

Do not replace every `NULL` with a generic value. A missing customer ID and a missing discount amount have different business meanings.

### `coalesce()`

Return the first non-null expression:

```python
df.select(
    F.coalesce(
        F.col('preferred_name'),
        F.col('legal_name'),
        F.lit('UNKNOWN'),
    ).alias('display_name')
)
```

This `coalesce()` is a SQL function for values. It is different from `DataFrame.coalesce()`, which changes the number of execution partitions and belongs to later storage/performance study.

### Dropping nulls

```python
df.dropna(subset=['order_id', 'store_id'])
```

This is concise, but for an auditable pipeline it is often better to route invalid rows to a rejected dataset rather than silently discard them.

### Three-valued logic

Comparisons involving `NULL` can produce `NULL`, not simply `True` or `False`.

For example:

```python
F.col('quantity') > 0
```

does not evaluate to `False` when `quantity` is `NULL`; it evaluates to unknown/`NULL`.

So define validity explicitly:

```python
valid_quantity = (
    F.col('quantity').isNotNull()
    & (F.col('quantity') > 0)
)
```

This matters when building complementary accepted/rejected filters.

---

## 8. String, Numeric, Date, and Timestamp Functions

Use `pyspark.sql.functions` so transformations remain column expressions.

### String standardization

Common functions:

```python
F.trim('sku')
F.upper('province')
F.lower('email')
F.length('sku')
F.regexp_replace('phone', r'[^0-9]', '')
F.split('tags', r'\|')
F.concat_ws('-', 'store_id', 'sku')
```

Example:

```python
standardized_df = (
    df
    .withColumn('sku', F.upper(F.trim('sku')))
    .withColumn('province', F.upper(F.trim('province')))
    .withColumn('email', F.lower(F.trim('email')))
)
```

Common pattern:

```text
trim → normalize case/format → parse/type → validate
```

Be careful not to normalize away meaningful distinctions. For example, uppercasing a free-text product description may be destructive even if uppercasing a province code is correct.

### Numeric functions

Examples:

```python
F.abs('variance')
F.round('unit_price', 2)
F.bround('unit_price', 2)
F.greatest('quantity', F.lit(0))
F.least('discount_pct', F.lit(1.0))
```

For currency:

- prefer `DecimalType` over `DoubleType` when exact decimal semantics matter;
- define precision and scale intentionally;
- avoid "fixing" invalid negative quantities with `abs()` unless the business rule actually says negative means positive.

### Dates

Parse a string into a date:

```python
df = df.withColumn(
    'order_date',
    F.to_date('order_date_raw', 'yyyy-MM-dd'),
)
```

Common functions:

```python
F.current_date()
F.year('order_date')
F.month('order_date')
F.dayofmonth('order_date')
F.date_add('order_date', 7)
F.date_sub('order_date', 7)
F.datediff('ship_date', 'order_date')
F.date_format('order_date', 'yyyy-MM')
```

### Timestamps

Parse a timestamp:

```python
df = df.withColumn(
    'order_ts',
    F.to_timestamp(
        'order_ts_raw',
        'yyyy-MM-dd HH:mm:ss',
    ),
)
```

Extract or derive components:

```python
F.year('order_ts')
F.month('order_ts')
F.hour('order_ts')
```

### Parsing discipline

Do not allow date/timestamp parsing to depend on accidental source formatting. Specify the expected pattern when the input contract defines one.

Because PySpark 4.x uses ANSI behavior by default, malformed casts/date-time conversions can fail at runtime. For messy input, use explicit validation or safe `try_*` behavior where appropriate so unparseable values can be rejected deliberately instead of crashing or disappearing.

Time zones are a production concern that becomes important when timestamps represent real events. For Phase 1, know whether your field is a calendar date, a timestamp representing an instant, or a timestamp without time-zone semantics; do not treat those concepts interchangeably.

---

## 9. Arrays, Structs, and Nested Data

Semi-structured JSON often contains nested values.

### `ArrayType`

Example schema:

```python
from pyspark.sql.types import ArrayType, StringType

StructField(
    'tags',
    ArrayType(
        StringType(),
        containsNull=False,
    ),
    nullable=True,
)
```

Example value:

```json
"tags": ["clearance", "seasonal"]
```

Useful functions include:

```python
F.size('tags')
F.array_contains('tags', 'clearance')
F.element_at('tags', 1)
```

Spark SQL array positions used by functions can have different indexing rules than Python lists, so verify the specific function rather than assuming Python semantics.

### `StructType` for nested objects

Example JSON:

```json
{
  "order_id": 1001,
  "shipping_address": {
    "city": "Mississauga",
    "province": "ON"
  }
}
```

Schema:

```python
address_schema = StructType([
    StructField('city', StringType(), True),
    StructField('province', StringType(), True),
])

order_schema = StructType([
    StructField('order_id', IntegerType(), False),
    StructField(
        'shipping_address',
        address_schema,
        True,
    ),
])
```

Access nested fields with dot notation:

```python
df.select(
    'order_id',
    F.col('shipping_address.city').alias('city'),
    F.col('shipping_address.province').alias('province'),
)
```

Create a struct:

```python
df.select(
    'order_id',
    F.struct(
        F.col('city'),
        F.col('province'),
    ).alias('shipping_address'),
)
```

### Arrays of structs

A common nested type is:

```text
ARRAY<STRUCT<...>>
```

Example:

```json
{
  "order_id": 1001,
  "items": [
    {"sku": "SKU-1", "quantity": 2},
    {"sku": "SKU-2", "quantity": 1}
  ]
}
```

Understand how to represent and access this structure now. Row-expansion patterns such as `explode()` receive fuller treatment in Phase 2.

### Nested-data engineering principle

Keep nested data when the nested structure is a useful contract. Flatten it when downstream analytical processing benefits from a tabular grain.

Do not flatten automatically without knowing what one resulting row should represent.

---

## 10. Introductory Storage Concepts

Phase 1 introduces storage behavior at a high level. Partitioning, predicate pushdown, file sizing, and shuffle engineering are treated more deeply later.

### Row-oriented vs. columnar

**Row-oriented format**

Conceptually stores values row by row:

```text
row 1: order_id, customer_id, amount, date
row 2: order_id, customer_id, amount, date
...
```

CSV is effectively row-oriented text.

Good fit:

- interchange;
- line-by-line generation;
- simple human inspection.

Weaknesses for analytical processing:

- no embedded strong schema;
- text parsing overhead;
- reading a few columns can still require processing row text broadly;
- weak typing/compression compared with analytical columnar formats.

**Columnar format**

Conceptually groups values by column:

```text
order_id values: ...
customer_id values: ...
amount values: ...
date values: ...
```

Parquet is columnar.

Good fit:

- analytical scans;
- selecting a subset of columns;
- compression;
- typed schema metadata;
- large batch-processing pipelines.

### Why Parquet is common

Parquet is widely used in Spark pipelines because it provides:

- columnar physical layout;
- schema metadata;
- efficient compression/encoding;
- support for reading only needed columns;
- strong integration with Spark's analytical engine.

### Compression

Compression reduces physical storage and I/O by encoding data more compactly.

Columnar data often compresses well because values in one column tend to have similar types and repeated patterns.

Compression is a trade-off:

```text
less data read/written
vs.
CPU work to compress/decompress
```

For analytical pipelines, reduced I/O is often highly valuable.

### Schema preservation

CSV stores text and does not embed Spark's typed schema.

Parquet stores type information in its metadata.

Therefore:

```python
spark.read.parquet(...)
```

can recover a typed schema without scanning raw text to infer it.

### Column pruning

If a Parquet dataset contains:

```text
order_id
customer_id
store_id
product_id
quantity
unit_price
discount_amount
order_ts
...
```

but a query only requires:

```python
df.select('order_id', 'order_ts')
```

a columnar reader can avoid reading unnecessary columns when the execution plan allows it.

This is **column pruning**.

It is one reason to avoid carrying `select("*")` through an entire pipeline when only a few columns are needed.

---

## 11. Data-Engineering Practices

### 1. Define contracts explicitly

For a controlled feed, keep the schema in code:

```python
ORDER_SCHEMA = StructType([...])
```

The schema is executable documentation.

### 2. Separate raw, standardized, valid, and rejected states

Useful conceptual names:

```python
raw_df
standardized_df
typed_df
valid_df
rejected_df
```

This makes pipeline intent easier to inspect than repeatedly overwriting a single variable named `df`.

### 3. Preserve raw evidence long enough to explain rejection

If a price cannot be parsed:

```text
unit_price_raw = "12.O9"
unit_price     = NULL
rejection_reason = "INVALID_UNIT_PRICE"
```

Do not drop `unit_price_raw` before the rejected record is preserved.

### 4. Keep parse rules and business rules distinct

Examples:

```text
"abc" cannot parse as quantity     → type/parse problem
-5 parses as integer               → business-rule problem
```

The distinction matters for ownership and troubleshooting.

### 5. Standardize only according to domain rules

Appropriate:

```python
F.upper(F.trim('province'))
```

if province codes are defined as normalized uppercase codes.

Potentially inappropriate:

```python
F.upper('product_name')
```

if casing is meaningful presentation data.

### 6. Use exact numeric types for exact values

For money:

```python
DecimalType(12, 2)
```

is generally preferable to floating-point representation.

### 7. Treat row counts as reconciliation evidence

For a simple one-input cleaning flow, a useful invariant is:

```text
parsed input rows
=
valid rows
+
rejected rows
```

This assumes every input row is routed exactly once after parsing.

Validate it with actions when appropriate:

```python
input_count = typed_df.count()
valid_count = valid_df.count()
rejected_count = rejected_df.count()

assert input_count == valid_count + rejected_count
```

Later phases will improve testing and observability around such invariants.

### 8. Make rejection reasons explicit

A rejected dataset is much more useful with:

```text
rejection_reason
```

or, when multiple reasons may apply, an array of reasons.

Phase 1 can use a simple priority-based reason:

```python
rejected_df = typed_df.withColumn(
    'rejection_reason',
    F.when(F.col('_corrupt_record').isNotNull(), 'MALFORMED_RECORD')
     .when(F.col('order_id').isNull(), 'MISSING_ORDER_ID')
     .when(F.col('quantity').isNull(), 'INVALID_QUANTITY')
     .when(F.col('quantity') <= 0, 'NON_POSITIVE_QUANTITY')
)
```

Then filter records with a non-null rejection reason.

### 9. Prefer transformations that are deterministic

The same valid input should produce the same standardized values.

Avoid introducing timestamps such as `current_timestamp()` into a business result unless processing time is intentionally part of the dataset.

### 10. Keep I/O separate from business intent when code begins to grow

Phase 1 does not need premature application architecture, but recognize the boundary:

```text
read → standardize/type → validate → write
```

As complexity grows, those concerns should become maintainable functions/modules rather than one monolithic script.

---

## 12. Common Pitfalls

### Relying on `inferSchema` as a production contract

A different sample can infer a different schema. Use inference for exploration; prefer explicit contracts for controlled pipelines.

### Reading everything as strings and never creating a typed curated layer

Raw strings are not an analytical contract. Curated data should have meaningful Spark types.

### Assuming an explicit schema proves the data is valid

A schema describes structure/types. Business validity still requires explicit checks.

### Treating `nullable=False` as a complete guarantee

Nullability is useful schema metadata, but actual enforcement depends on how data enters and leaves Spark. Validate required fields explicitly when correctness matters.

### Using `DROPMALFORMED` and losing rejected data

Dropping bad input may make a row count look clean while hiding data loss.

### Confusing malformed records with business-invalid records

A syntactically valid row can still be unacceptable.

### Using `dropna()` instead of preserving rejects

Dropping is not the same as validating and quarantining.

### Filling every `NULL`

Defaults must have business meaning. `NULL` can be information.

### Casting messy strings without understanding ANSI behavior

In PySpark 4.x, invalid operations/casts can fail at runtime under default ANSI mode. Parse deliberately when bad values are expected.

### Using `DoubleType` for currency by habit

Binary floating point can introduce representation issues. Prefer `DecimalType` for exact monetary values.

### Calling `collect()` on unknown-size data

`collect()` moves all resulting rows to the driver. Use it only when the result is known to be small.

### Assuming `show()` proves correctness

A preview is not validation. Edge cases may exist outside the displayed rows.

### Deduplicating without a defined key

`distinct()` and `dropDuplicates()` can hide source problems. Know whether duplicates are exact duplicates or business-key conflicts.

### Sorting data for no business reason

`orderBy()` is not required for most intermediate transformations and can be expensive.

### Expecting `df.write.parquet(...)` to create one file

Spark writes a distributed dataset directory containing part files.

### Committing generated Parquet outputs

The repository convention is to preserve code, small intentional fixtures, tests, and documentation—not large generated raw/curated/rejected datasets.

---

## 13. Applied Mastery Target

Phase 1 is not complete merely because the syntax in this README is familiar.

The applied target is:

> Take messy raw retail data and produce a clean, typed dataset with explicit schemas, standardized columns, valid records, rejected records, and Parquet output.

A suitable Phase 1 retail input might contain:

```text
order_id_raw
sku_raw
quantity_raw
unit_price_raw
order_date_raw
province_raw
```

with deliberately messy cases such as:

```text
whitespace around SKUs
mixed-case province codes
missing required IDs
non-numeric quantity
zero/negative quantity
malformed price
invalid date
duplicate raw rows
malformed CSV/JSON record
```

A professional Phase 1 result should establish a flow similar to:

```text
data/raw/retail_orders.*
        ↓
explicit raw schema / parse contract
        ↓
standardize strings
        ↓
safe type conversion
        ↓
record validation
       / \
      /   \
 valid     rejected
  |          |
typed      raw evidence
columns    + reason
  |
Parquet
```

### Expected valid output characteristics

Example clean schema:

```text
order_id: long
sku: string
quantity: integer
unit_price: decimal(12,2)
order_date: date
province: string
```

Example standardized values:

```text
sku        = "SKU-001"
province   = "ON"
quantity   = 2
unit_price = 12.99
order_date = 2026-08-19
```

### Expected rejected output characteristics

A rejected row should retain enough information to answer:

```text
What input arrived?
Why was it rejected?
```

Example:

```text
order_id_raw      = "1007"
sku_raw           = " SKU-009 "
quantity_raw      = "abc"
unit_price_raw    = "14.25"
order_date_raw    = "2026-08-19"
province_raw      = "on"
rejection_reason  = "INVALID_QUANTITY"
```

### Minimum reconciliation

For the parsed candidate rows:

```text
candidate row count
=
valid row count
+
rejected row count
```

No row should silently disappear between validation branches.

### Parquet proof

After writing the valid dataset:

```python
valid_df.write.mode('overwrite').parquet(output_path)
```

read it back:

```python
roundtrip_df = spark.read.parquet(output_path)

roundtrip_df.printSchema()
roundtrip_df.show()
```

Confirm:

- expected columns;
- expected data types;
- expected values;
- expected row count.

That read-back check proves more than the absence of a write exception.

---

## 14. Phase 1 Mastery Checklist

Before Phase 1 can be marked complete, I should be able to do and explain the following without relying on a tutorial.

### Spark fundamentals

- Create/use a `SparkSession`.
- Create a DataFrame.
- Explain DataFrames vs. RDDs at a practical level.
- Distinguish transformations from actions.
- Explain lazy evaluation and identify what triggers execution.

### Schemas

- Identify common Spark data types.
- Build a `StructType` using `StructField`.
- Explain field nullability.
- Apply an explicit schema.
- Explain inference vs. enforcement.
- Cast values deliberately and handle malformed casts safely.

### I/O

- Read CSV with explicit options and schema.
- Read JSON, including basic nested structures.
- Read and write Parquet.
- Explain read modes.
- Preserve malformed-record evidence where required.
- Explain common write modes.
- Explain why Spark writes a dataset directory rather than necessarily one file.

### DataFrame API

Use confidently:

- `select()`
- `alias()`
- `filter()` / `where()`
- `withColumn()`
- `drop()`
- `distinct()`
- `dropDuplicates()`
- `orderBy()`
- `limit()`

### Expressions and data handling

Use and explain:

- `col()`
- `lit()`
- `when()` / `otherwise()`
- null checks and `coalesce()`
- string standardization
- numeric transformations
- date/timestamp parsing and functions
- arrays
- structs
- nested-field access

### Storage concepts

Explain at a high level:

- row-oriented vs. columnar formats;
- why Parquet is common for analytical pipelines;
- compression;
- schema preservation;
- column pruning.

### Applied mastery

Produce messy retail input → typed/standardized candidates → valid and rejected branches → Parquet valid output, while preserving rejection reasons and proving basic row-count/schema reconciliation.

Phase 1 remains **in progress** until this applied target and the curriculum mastery requirements are demonstrated.
