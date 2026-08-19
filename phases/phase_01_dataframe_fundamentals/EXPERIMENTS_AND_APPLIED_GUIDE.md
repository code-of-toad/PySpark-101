# Phase 1 — Experiment & Applied Practice Guide

## Purpose

This file consolidates the hands-on instructions for **Phase 1 — DataFrame Fundamentals, Schemas & I/O** of the PySpark 101 curriculum.

Use it as the working guide for experiments and applied practice. The phase README remains the conceptual reference; this file is the execution-oriented companion.

<!-- TOC START -->
## Table of Contents

  - [Purpose](#purpose)
  - [Conventions](#conventions)
- [Experiment 1 — DataFrame Fundamentals](#experiment-1-dataframe-fundamentals)
  - [Goal](#goal)
  - [1.1 Create a DataFrame with inferred types](#11-create-a-dataframe-with-inferred-types)
- [SparkSession is the main entry point for the DataFrame API and Spark SQL.](#sparksession-is-the-main-entry-point-for-the-dataframe-api-and-spark-sql)
- [All values are deliberately Python strings so Spark infers StringType.](#all-values-are-deliberately-python-strings-so-spark-infers-stringtype)
- [No explicit StructType is supplied, so Spark infers the schema.](#no-explicit-structtype-is-supplied-so-spark-infers-the-schema)
- [Inspect the inferred types.](#inspect-the-inferred-types)
- [show() is an action and triggers execution.](#show-is-an-action-and-triggers-execution)
  - [1.2 Transformation vs. action](#12-transformation-vs-action)
- [filter() defines a new DataFrame transformation.](#filter-defines-a-new-dataframe-transformation)
- [show() is an action and triggers Spark execution.](#show-is-an-action-and-triggers-spark-execution)
  - [1.3 DataFrame immutability](#13-dataframe-immutability)
- [drop() returns a new DataFrame; it does not mutate df.](#drop-returns-a-new-dataframe-it-does-not-mutate-df)
- [The original still contains order_date.](#the-original-still-contains-order-date)
- [Capture the derived DataFrame explicitly.](#capture-the-derived-dataframe-explicitly)
  - [Questions](#questions)
- [Experiment 2 — Explicit Schemas](#experiment-2-explicit-schemas)
  - [Goal](#goal)
  - [2.1 Define an explicit schema](#21-define-an-explicit-schema)
- [Define the intended business schema explicitly.](#define-the-intended-business-schema-explicitly)
- [treeString() is easier to read than simpleString() for human inspection.](#treestring-is-easier-to-read-than-simplestring-for-human-inspection)
  - [2.2 Create correctly typed rows](#22-create-correctly-typed-rows)
- [Use Python objects that correspond to the declared Spark types.](#use-python-objects-that-correspond-to-the-declared-spark-types)
  - [2.3 Violate `IntegerType`](#23-violate-integertype)
- ['2' is a Python string, but quantity expects IntegerType.](#2-is-a-python-string-but-quantity-expects-integertype)
  - [2.4 Test nullability](#24-test-nullability)
- [order_id is non-nullable, so None violates the schema contract.](#order-id-is-non-nullable-so-none-violates-the-schema-contract)
- [quantity is nullable=True, so None is structurally permitted.](#quantity-is-nullabletrue-so-none-is-structurally-permitted)
  - [Key distinction](#key-distinction)
- [Experiment 3 — Messy CSV with Explicit Types](#experiment-3-messy-csv-with-explicit-types)
  - [Goal](#goal)
- [Include _corrupt_record so malformed source lines can be preserved.](#include-corrupt-record-so-malformed-source-lines-can-be-preserved)
- [Parser-level failures.](#parser-level-failures)
- [Successfully parsed rows.](#successfully-parsed-rows)
- [This does not imply business validity.](#this-does-not-imply-business-validity)
- [Experiment 4 — Preserve Raw Values and Parse Explicitly](#experiment-4-preserve-raw-values-and-parse-explicitly)
  - [Goal](#goal)
  - [4.1 Read raw fields as strings](#41-read-raw-fields-as-strings)
- [Keep the raw representation intact for diagnostics.](#keep-the-raw-representation-intact-for-diagnostics)
  - [4.2 Create typed columns](#42-create-typed-columns)
  - [4.3 Detect conversion failures](#43-detect-conversion-failures)
- [Raw source value exists but conversion failed.](#raw-source-value-exists-but-conversion-failed)
  - [4.4 Add rejection reasons](#44-add-rejection-reasons)
  - [4.5 Split accepted and rejected rows](#45-split-accepted-and-rejected-rows)
- [Experiment 5 — Core DataFrame Operations](#experiment-5-core-dataframe-operations)
  - [Goal](#goal)
  - [5.1 Standardize strings](#51-standardize-strings)
  - [5.2 `select()` and `alias()`](#52-select-and-alias)
  - [5.3 `filter()` / `where()`](#53-filter-where)
  - [5.4 `drop()`](#54-drop)
  - [5.5 `distinct()` vs. `dropDuplicates()`](#55-distinct-vs-dropduplicates)
- [Remove exact duplicate rows.](#remove-exact-duplicate-rows)
- [Deduplicate according to selected columns.](#deduplicate-according-to-selected-columns)
  - [5.6 `orderBy()` and `limit()`](#56-orderby-and-limit)
- [Experiment 6 — NULL Handling and Conditional Expressions](#experiment-6-null-handling-and-conditional-expressions)
  - [Goal](#goal)
  - [6.1 NULL predicates](#61-null-predicates)
  - [6.2 `coalesce()`](#62-coalesce)
  - [6.3 `when()` / `otherwise()`](#63-when-otherwise)
  - [6.4 NULL propagation](#64-null-propagation)
- [Experiment 7 — String Functions](#experiment-7-string-functions)
  - [Goal](#goal)
  - [7.1 Normalize strings](#71-normalize-strings)
  - [7.2 `regexp_replace()`](#72-regexp-replace)
  - [7.3 `substring()`](#73-substring)
  - [7.4 `length()`](#74-length)
  - [7.5 `concat_ws()`](#75-concat-ws)
- [Experiment 8 — Numeric Functions and Arithmetic](#experiment-8-numeric-functions-and-arithmetic)
  - [Goal](#goal)
  - [8.1 Derived measures](#81-derived-measures)
  - [8.2 `round()` and `bround()`](#82-round-and-bround)
  - [8.3 `abs()`](#83-abs)
- [Experiment 9 — Dates and Timestamps](#experiment-9-dates-and-timestamps)
  - [Goal](#goal)
  - [9.1 Parse safely](#91-parse-safely)
  - [9.2 Extract date components](#92-extract-date-components)
  - [9.3 Date arithmetic](#93-date-arithmetic)
  - [9.4 Date difference](#94-date-difference)
  - [9.5 `date_format()`](#95-date-format)
- [Experiment 10 — Arrays, Structs, and Nested JSON](#experiment-10-arrays-structs-and-nested-json)
  - [Goal](#goal)
  - [10.1 Arrays](#101-arrays)
  - [10.2 Create an array column](#102-create-an-array-column)
  - [10.3 Structs](#103-structs)
  - [10.4 Nested JSON schema](#104-nested-json-schema)
  - [10.5 `explode()`](#105-explode)
  - [10.6 `from_json()`](#106-from-json)
- [Experiment 11 — CSV vs. JSON vs. Parquet](#experiment-11-csv-vs-json-vs-parquet)
  - [Goal](#goal)
  - [11.1 Clean typed DataFrame](#111-clean-typed-dataframe)
  - [11.2 Write CSV](#112-write-csv)
  - [11.3 Write JSON](#113-write-json)
  - [11.4 Write Parquet](#114-write-parquet)
  - [11.5 Validate read-back](#115-validate-read-back)
- [Validate schema equality.](#validate-schema-equality)
- [Validate row-count equality.](#validate-row-count-equality)
  - [11.6 Column pruning](#116-column-pruning)
  - [11.7 Write modes](#117-write-modes)
- [Replace existing output.](#replace-existing-output)
- [Add more records.](#add-more-records)
- [Leave existing output untouched.](#leave-existing-output-untouched)
- [Fail if output already exists.](#fail-if-output-already-exists)
- [Experiment 12 — CSV Read Modes](#experiment-12-csv-read-modes)
  - [Goal](#goal)
  - [12.1 Base schema](#121-base-schema)
  - [12.2 `PERMISSIVE`](#122-permissive)
  - [12.3 `DROPMALFORMED`](#123-dropmalformed)
  - [12.4 `FAILFAST`](#124-failfast)
- [show() forces execution and exposes malformed input.](#show-forces-execution-and-exposes-malformed-input)
  - [Read-mode mental model](#read-mode-mental-model)
- [Applied Phase 1 Project](#applied-phase-1-project)
  - [Goal](#goal)
  - [Repository structure](#repository-structure)
  - [Raw fixture](#raw-fixture)
  - [Accepted output schema](#accepted-output-schema)
  - [Standardization rules](#standardization-rules)
  - [Business rules](#business-rules)
- [Applied Task — Part 1](#applied-task-part-1)
  - [Starter skeleton](#starter-skeleton)
- [Create the Spark entry point for the Phase 1 applied pipeline.](#create-the-spark-entry-point-for-the-phase-1-applied-pipeline)
- [Preserve raw source representations as strings so malformed](#preserve-raw-source-representations-as-strings-so-malformed)
- [values remain available for rejected-record diagnostics.](#values-remain-available-for-rejected-record-diagnostics)
- [TODO: read raw_orders.csv with raw_schema.](#todo-read-raw-orderscsv-with-raw-schema)
- [TODO: standardize sku and province.](#todo-standardize-sku-and-province)
- [TODO: safely parse quantity, unit_price, and order_date.](#todo-safely-parse-quantity-unit-price-and-order-date)
- [TODO: print the resulting schema.](#todo-print-the-resulting-schema)
- [TODO: show the transformed rows.](#todo-show-the-transformed-rows)
  - [Questions to answer before Part 2](#questions-to-answer-before-part-2)
- [After Part 1](#after-part-1)

<!-- TOC END -->

---

## Conventions

- Use **single quotes** for Python strings.
- Include **inline comments** for teaching and engineering reasoning.
- Prefer the evolving **retail-data domain** over unrelated toy examples.
- Preserve raw values when rejected-record diagnostics matter.
- Prefer explicit schemas in controlled pipeline work.
- Treat parsing/type validity separately from business-rule validity.
- Do not mark Phase 1 complete until the applied task and mastery gate are satisfied.

---

# Experiment 1 — DataFrame Fundamentals

## Goal

Understand:

- `SparkSession`;
- DataFrame creation;
- schema inference;
- transformations vs. actions;
- lazy evaluation;
- DataFrame immutability.

## 1.1 Create a DataFrame with inferred types

```python
from pyspark.sql import SparkSession

# SparkSession is the main entry point for the DataFrame API and Spark SQL.
spark = (
    SparkSession.builder
    .appName('phase01-experiment01')
    .master('local[*]')
    .getOrCreate()
)

# All values are deliberately Python strings so Spark infers StringType.
data = [
    ('1001', ' SKU-001 ', '2', '12.99', '2026-08-18'),
    ('1002', 'SKU-002', '3', '8.50', '2026-08-18'),
    ('1003', 'SKU-003', 'abc', '19.99', '2026-08-19'),
]

# No explicit StructType is supplied, so Spark infers the schema.
df = spark.createDataFrame(
    data,
    ['order_id', 'sku', 'quantity', 'unit_price', 'order_date'],
)

# Inspect the inferred types.
df.printSchema()

# show() is an action and triggers execution.
df.show(truncate=False)

spark.stop()
```

## 1.2 Transformation vs. action

```python
from pyspark.sql.functions import col

# filter() defines a new DataFrame transformation.
clean_df = df.filter(
    col('quantity') != 'abc'
)

# show() is an action and triggers Spark execution.
clean_df.show()
```

## 1.3 DataFrame immutability

```python
# drop() returns a new DataFrame; it does not mutate df.
df.drop('order_date')

# The original still contains order_date.
df.show()

# Capture the derived DataFrame explicitly.
smaller_df = df.drop('order_date')

smaller_df.show()
```

## Questions

1. Why did Spark infer `quantity` as a string?
2. What is the execution difference between `filter()` and `show()`?
3. Why does `df` remain unchanged after `df.drop('order_date')`?
4. Why is an all-string schema dangerous in a production pipeline?

---

# Experiment 2 — Explicit Schemas

## Goal

Understand:

- `StructType`;
- `StructField`;
- Spark data types;
- nullability;
- explicit schema enforcement.

## 2.1 Define an explicit schema

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

spark = (
    SparkSession.builder
    .appName('phase01-experiment02')
    .master('local[*]')
    .getOrCreate()
)

# Define the intended business schema explicitly.
schema = StructType([
    StructField('order_id', StringType(), False),
    StructField('sku', StringType(), False),
    StructField('quantity', IntegerType(), True),
    StructField('unit_price', DecimalType(10, 2), True),
    StructField('order_date', DateType(), True),
])

# treeString() is easier to read than simpleString() for human inspection.
print(schema.treeString())

spark.stop()
```

## 2.2 Create correctly typed rows

```python
from datetime import date
from decimal import Decimal

# Use Python objects that correspond to the declared Spark types.
data = [
    ('1001', 'SKU-001', 2, Decimal('12.99'), date(2026, 8, 18)),
    ('1002', 'SKU-002', 3, Decimal('8.50'), date(2026, 8, 18)),
    ('1003', 'SKU-003', 1, Decimal('19.99'), date(2026, 8, 19)),
]

df = spark.createDataFrame(data, schema)

df.printSchema()
df.show(truncate=False)
```

## 2.3 Violate `IntegerType`

```python
from datetime import date
from decimal import Decimal

# '2' is a Python string, but quantity expects IntegerType.
data = [
    ('1001', 'SKU-001', '2', Decimal('12.99'), date(2026, 8, 18)),
]

df = spark.createDataFrame(data, schema)
```

## 2.4 Test nullability

```python
from datetime import date
from decimal import Decimal

# order_id is non-nullable, so None violates the schema contract.
data = [
    (None, 'SKU-001', 2, Decimal('12.99'), date(2026, 8, 18)),
]

df = spark.createDataFrame(data, schema)
```

```python
from datetime import date
from decimal import Decimal

# quantity is nullable=True, so None is structurally permitted.
data = [
    ('1001', 'SKU-001', None, Decimal('12.99'), date(2026, 8, 18)),
]

df = spark.createDataFrame(data, schema)
```

## Key distinction

`nullable=True` means Spark permits NULL structurally. It does **not** mean NULL satisfies business rules.

---

# Experiment 3 — Messy CSV with Explicit Types

## Goal

Observe parser behavior when raw text must become typed Spark values.

Create `messy_orders.csv`:

```csv
order_id,sku,quantity,unit_price,order_date
1001,SKU-001,2,12.99,2026-08-18
1002,SKU-002,3,8.50,2026-08-18
1003,SKU-003,abc,19.99,2026-08-19
1004,SKU-004,-2,4.99,2026-08-19
1005,SKU-005,1,not-a-price,2026-08-20
1006,SKU-006,4,29.99,not-a-date
```

Read it:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

spark = (
    SparkSession.builder
    .appName('phase01-experiment03')
    .master('local[*]')
    .getOrCreate()
)

# Include _corrupt_record so malformed source lines can be preserved.
schema = StructType([
    StructField('order_id', StringType(), True),
    StructField('sku', StringType(), True),
    StructField('quantity', IntegerType(), True),
    StructField('unit_price', DecimalType(10, 2), True),
    StructField('order_date', DateType(), True),
    StructField('_corrupt_record', StringType(), True),
])

df = (
    spark.read
    .option('header', True)
    .option('mode', 'PERMISSIVE')
    .option('columnNameOfCorruptRecord', '_corrupt_record')
    .option('dateFormat', 'yyyy-MM-dd')
    .schema(schema)
    .csv('messy_orders.csv')
)

df.printSchema()
df.show(truncate=False)
```

Inspect parser problems:

```python
from pyspark.sql.functions import col

# Parser-level failures.
corrupt_df = df.filter(
    col('_corrupt_record').isNotNull()
)

# Successfully parsed rows.
# This does not imply business validity.
parsed_df = df.filter(
    col('_corrupt_record').isNull()
)

corrupt_df.show(truncate=False)
parsed_df.show(truncate=False)
```

Key distinction:

- `'abc'` → parsing/type problem;
- `-2` → valid integer, potentially invalid business value.

---

# Experiment 4 — Preserve Raw Values and Parse Explicitly

## Goal

Build a more auditable ingestion pattern.

## 4.1 Read raw fields as strings

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

spark = (
    SparkSession.builder
    .appName('phase01-experiment04')
    .master('local[*]')
    .getOrCreate()
)

# Keep the raw representation intact for diagnostics.
raw_schema = StructType([
    StructField('order_id', StringType(), True),
    StructField('sku', StringType(), True),
    StructField('quantity_raw', StringType(), True),
    StructField('unit_price_raw', StringType(), True),
    StructField('order_date_raw', StringType(), True),
])

raw_df = (
    spark.read
    .option('header', True)
    .schema(raw_schema)
    .csv('messy_orders.csv')
)

raw_df.printSchema()
raw_df.show(truncate=False)
```

## 4.2 Create typed columns

```python
from pyspark.sql.functions import (
    col,
    try_cast,
    try_to_date,
)

typed_df = (
    raw_df
    .withColumn(
        'quantity',
        try_cast(col('quantity_raw'), 'int'),
    )
    .withColumn(
        'unit_price',
        try_cast(col('unit_price_raw'), 'decimal(10,2)'),
    )
    .withColumn(
        'order_date',
        try_to_date(
            col('order_date_raw'),
            'yyyy-MM-dd',
        ),
    )
)

typed_df.printSchema()
typed_df.show(truncate=False)
```

## 4.3 Detect conversion failures

```python
from pyspark.sql.functions import col

# Raw source value exists but conversion failed.
bad_quantity = (
    col('quantity_raw').isNotNull()
    & col('quantity').isNull()
)
```

## 4.4 Add rejection reasons

```python
from pyspark.sql.functions import (
    col,
    lit,
    when,
)

validated_df = (
    typed_df
    .withColumn(
        'rejection_reason',
        when(
            col('order_id').isNull(),
            lit('missing_order_id'),
        )
        .when(
            col('quantity_raw').isNotNull()
            & col('quantity').isNull(),
            lit('invalid_quantity_format'),
        )
        .when(
            col('quantity') <= 0,
            lit('quantity_must_be_positive'),
        )
        .when(
            col('unit_price_raw').isNotNull()
            & col('unit_price').isNull(),
            lit('invalid_unit_price_format'),
        )
        .when(
            col('order_date_raw').isNotNull()
            & col('order_date').isNull(),
            lit('invalid_order_date_format'),
        )
        .otherwise(
            lit(None)
        ),
    )
)
```

## 4.5 Split accepted and rejected rows

```python
from pyspark.sql.functions import col

accepted_df = (
    validated_df
    .filter(
        col('rejection_reason').isNull()
    )
)

rejected_df = (
    validated_df
    .filter(
        col('rejection_reason').isNotNull()
    )
)

accepted_df.show(truncate=False)
rejected_df.show(truncate=False)
```

---

# Experiment 5 — Core DataFrame Operations

## Goal

Practice:

- `select()`;
- `alias()`;
- `filter()` / `where()`;
- `withColumn()`;
- `drop()`;
- `distinct()`;
- `dropDuplicates()`;
- `orderBy()`;
- `limit()`.

## 5.1 Standardize strings

```python
from pyspark.sql.functions import (
    col,
    trim,
    upper,
)

standardized_df = (
    raw_df
    .withColumn(
        'sku',
        upper(trim(col('sku'))),
    )
)

standardized_df.show(truncate=False)
```

## 5.2 `select()` and `alias()`

```python
from pyspark.sql.functions import col

selected_df = (
    standardized_df
    .select(
        col('order_id'),
        col('sku'),
        col('quantity_raw').alias('source_quantity'),
    )
)

selected_df.show(truncate=False)
```

## 5.3 `filter()` / `where()`

```python
from pyspark.sql.functions import col

filtered_df = (
    standardized_df
    .filter(
        col('quantity_raw').isNotNull()
    )
)
```

`where()` is equivalent:

```python
filtered_df = (
    standardized_df
    .where(
        col('quantity_raw').isNotNull()
    )
)
```

## 5.4 `drop()`

```python
business_view_df = (
    standardized_df
    .drop(
        'quantity_raw',
        'unit_price_raw',
        'order_date_raw',
    )
)
```

## 5.5 `distinct()` vs. `dropDuplicates()`

```python
duplicate_data = [
    ('1001', 'SKU-001', '2'),
    ('1001', 'SKU-001', '2'),
    ('1001', 'SKU-001', '3'),
    ('1002', 'SKU-002', '1'),
]

duplicate_df = spark.createDataFrame(
    duplicate_data,
    ['order_id', 'sku', 'quantity_raw'],
)

# Remove exact duplicate rows.
distinct_df = duplicate_df.distinct()

# Deduplicate according to selected columns.
deduplicated_df = (
    duplicate_df
    .dropDuplicates([
        'order_id',
        'sku',
    ])
)
```

Do not assume `dropDuplicates()` deterministically chooses a preferred differing row.

## 5.6 `orderBy()` and `limit()`

```python
from pyspark.sql.functions import col

ordered_df = (
    duplicate_df
    .orderBy(
        col('order_id').asc()
    )
)

sample_df = duplicate_df.limit(2)
```

---

# Experiment 6 — NULL Handling and Conditional Expressions

## Goal

Understand:

- `isNull()`;
- `isNotNull()`;
- `coalesce()`;
- `when()` / `otherwise()`;
- NULL propagation.

```python
from decimal import Decimal

from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

schema = StructType([
    StructField('order_id', StringType(), False),
    StructField('quantity', IntegerType(), True),
    StructField('unit_price', DecimalType(10, 2), True),
    StructField('province', StringType(), True),
])

data = [
    ('1001', 2, Decimal('12.99'), 'ON'),
    ('1002', None, Decimal('8.50'), 'ON'),
    ('1003', 3, None, 'QC'),
    ('1004', 1, Decimal('4.99'), None),
]

df = spark.createDataFrame(data, schema)
```

## 6.1 NULL predicates

```python
from pyspark.sql.functions import col

missing_quantity_df = (
    df
    .filter(
        col('quantity').isNull()
    )
)

present_quantity_df = (
    df
    .filter(
        col('quantity').isNotNull()
    )
)
```

Prefer `isNull()` over `== None`.

## 6.2 `coalesce()`

```python
from pyspark.sql.functions import (
    coalesce,
    col,
    lit,
)

filled_df = (
    df
    .withColumn(
        'province_clean',
        coalesce(
            col('province'),
            lit('UNKNOWN'),
        ),
    )
)
```

## 6.3 `when()` / `otherwise()`

```python
from pyspark.sql.functions import (
    col,
    lit,
    when,
)

classified_df = (
    df
    .withColumn(
        'quantity_status',
        when(
            col('quantity').isNull(),
            lit('MISSING'),
        )
        .when(
            col('quantity') <= 0,
            lit('INVALID'),
        )
        .otherwise(
            lit('VALID')
        ),
    )
)
```

## 6.4 NULL propagation

```python
from pyspark.sql.functions import col

revenue_df = (
    df
    .withColumn(
        'line_revenue',
        col('quantity') * col('unit_price'),
    )
)
```

If either operand is NULL, the result is normally NULL.

---

# Experiment 7 — String Functions

## Goal

Practice string normalization.

```python
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

schema = StructType([
    StructField('order_id', StringType(), False),
    StructField('sku', StringType(), True),
    StructField('province', StringType(), True),
    StructField('customer_email', StringType(), True),
    StructField('product_code', StringType(), True),
])

data = [
    ('1001', ' sku-001 ', 'on', ' DANNY@EXAMPLE.COM ', 'prod_001'),
    ('1002', 'SKU-002', ' ON ', 'user2@example.com', 'PROD-002'),
    ('1003', 'Sku-003', 'qc', None, 'prod 003'),
    ('1004', '  sku-004', 'Bc', ' USER4@EXAMPLE.COM', 'prod__004'),
]

df = spark.createDataFrame(data, schema)
```

## 7.1 Normalize strings

```python
from pyspark.sql.functions import (
    col,
    lower,
    trim,
    upper,
)

standardized_df = (
    df
    .withColumn(
        'sku',
        upper(trim(col('sku'))),
    )
    .withColumn(
        'province',
        upper(trim(col('province'))),
    )
    .withColumn(
        'customer_email',
        lower(trim(col('customer_email'))),
    )
)
```

## 7.2 `regexp_replace()`

```python
from pyspark.sql.functions import (
    col,
    regexp_replace,
    trim,
    upper,
)

cleaned_df = (
    standardized_df
    .withColumn(
        'product_code',
        upper(
            regexp_replace(
                trim(col('product_code')),
                '[_ ]+',
                '-',
            )
        ),
    )
)
```

## 7.3 `substring()`

```python
from pyspark.sql.functions import (
    col,
    substring,
)

sku_parts_df = (
    cleaned_df
    .withColumn(
        'sku_number',
        substring(
            col('sku'),
            5,
            3,
        ),
    )
)
```

Spark SQL-style positions are 1-based.

## 7.4 `length()`

```python
from pyspark.sql.functions import (
    col,
    length,
)

length_df = (
    cleaned_df
    .withColumn(
        'sku_length',
        length(col('sku')),
    )
)
```

## 7.5 `concat_ws()`

```python
from pyspark.sql.functions import (
    col,
    concat_ws,
)

reference_df = (
    cleaned_df
    .withColumn(
        'order_sku_reference',
        concat_ws(
            '-',
            col('order_id'),
            col('sku'),
        ),
    )
)
```

---

# Experiment 8 — Numeric Functions and Arithmetic

## Goal

Practice typed arithmetic and decimal-safe calculations.

```python
from decimal import Decimal

from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

schema = StructType([
    StructField('order_id', StringType(), False),
    StructField('quantity', IntegerType(), False),
    StructField('unit_price', DecimalType(10, 2), False),
    StructField('unit_cost', DecimalType(10, 2), False),
    StructField('discount_amount', DecimalType(10, 2), True),
])

data = [
    ('1001', 2, Decimal('12.99'), Decimal('7.50'), Decimal('2.00')),
    ('1002', 3, Decimal('8.50'), Decimal('5.25'), Decimal('0.00')),
    ('1003', 1, Decimal('19.99'), Decimal('12.40'), None),
    ('1004', -2, Decimal('4.99'), Decimal('2.50'), Decimal('1.00')),
]

df = spark.createDataFrame(data, schema)
```

## 8.1 Derived measures

```python
from pyspark.sql.functions import (
    coalesce,
    col,
    lit,
)

measures_df = (
    df
    .withColumn(
        'gross_sales',
        col('quantity') * col('unit_price'),
    )
    .withColumn(
        'discount_amount_clean',
        coalesce(
            col('discount_amount'),
            lit(0),
        ),
    )
    .withColumn(
        'net_sales',
        col('gross_sales') - col('discount_amount_clean'),
    )
    .withColumn(
        'gross_margin',
        col('net_sales')
        - (
            col('quantity') * col('unit_cost')
        ),
    )
)
```

## 8.2 `round()` and `bround()`

```python
from pyspark.sql.functions import (
    bround,
    col,
    round,
)

rounded_df = (
    measures_df
    .withColumn(
        'gross_margin_rounded',
        round(
            col('gross_margin'),
            2,
        ),
    )
)

rounding_df = (
    spark.range(1)
    .select(
        round(lit(2.5), 0).alias('round_result'),
        bround(lit(2.5), 0).alias('bround_result'),
    )
)
```

## 8.3 `abs()`

```python
from pyspark.sql.functions import (
    abs,
    col,
)

difference_df = (
    measures_df
    .withColumn(
        'margin_magnitude',
        abs(col('gross_margin')),
    )
)
```

Typed arithmetic does not imply business validity.

---

# Experiment 9 — Dates and Timestamps

## Goal

Practice:

- `try_to_date()`;
- `try_to_timestamp()`;
- `year()`;
- `month()`;
- `dayofmonth()`;
- `date_add()`;
- `datediff()`;
- `date_format()`.

```python
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

schema = StructType([
    StructField('order_id', StringType(), False),
    StructField('order_date_raw', StringType(), True),
    StructField('created_at_raw', StringType(), True),
])

data = [
    ('1001', '2026-08-18', '2026-08-18 09:15:30'),
    ('1002', '2026-08-19', '2026-08-19 14:45:10'),
    ('1003', 'bad-date', '2026-08-19 16:30:00'),
    ('1004', '2026-08-20', 'not-a-timestamp'),
]

df = spark.createDataFrame(data, schema)
```

## 9.1 Parse safely

```python
from pyspark.sql.functions import (
    col,
    try_to_date,
    try_to_timestamp,
)

typed_df = (
    df
    .withColumn(
        'order_date',
        try_to_date(
            col('order_date_raw'),
            'yyyy-MM-dd',
        ),
    )
    .withColumn(
        'created_at',
        try_to_timestamp(
            col('created_at_raw'),
            'yyyy-MM-dd HH:mm:ss',
        ),
    )
)
```

## 9.2 Extract date components

```python
from pyspark.sql.functions import (
    col,
    dayofmonth,
    month,
    year,
)

calendar_df = (
    typed_df
    .withColumn(
        'order_year',
        year(col('order_date')),
    )
    .withColumn(
        'order_month',
        month(col('order_date')),
    )
    .withColumn(
        'order_day',
        dayofmonth(col('order_date')),
    )
)
```

## 9.3 Date arithmetic

```python
from pyspark.sql.functions import (
    col,
    date_add,
)

shipping_df = (
    typed_df
    .withColumn(
        'expected_ship_date',
        date_add(
            col('order_date'),
            2,
        ),
    )
)
```

## 9.4 Date difference

```python
from pyspark.sql.functions import (
    col,
    datediff,
    try_to_date,
)

delivery_data = [
    ('1001', '2026-08-18', '2026-08-21'),
    ('1002', '2026-08-19', '2026-08-20'),
]

delivery_df = spark.createDataFrame(
    delivery_data,
    [
        'order_id',
        'order_date_raw',
        'delivery_date_raw',
    ],
)

delivery_df = (
    delivery_df
    .withColumn(
        'order_date',
        try_to_date(
            col('order_date_raw'),
            'yyyy-MM-dd',
        ),
    )
    .withColumn(
        'delivery_date',
        try_to_date(
            col('delivery_date_raw'),
            'yyyy-MM-dd',
        ),
    )
    .withColumn(
        'delivery_days',
        datediff(
            col('delivery_date'),
            col('order_date'),
        ),
    )
)
```

## 9.5 `date_format()`

```python
from pyspark.sql.functions import (
    col,
    date_format,
)

formatted_df = (
    typed_df
    .withColumn(
        'order_month_label',
        date_format(
            col('order_date'),
            'yyyy-MM',
        ),
    )
)
```

Remember: `date_format()` returns a string.

---

# Experiment 10 — Arrays, Structs, and Nested JSON

## Goal

Understand nested Spark SQL types.

## 10.1 Arrays

```python
from pyspark.sql.types import (
    ArrayType,
    StringType,
    StructField,
    StructType,
)

schema = StructType([
    StructField('product_id', StringType(), False),
    StructField(
        'tags',
        ArrayType(
            StringType(),
            containsNull=False,
        ),
        True,
    ),
])

data = [
    ('P001', ['electronics', 'sale']),
    ('P002', ['grocery', 'organic']),
    ('P003', []),
    ('P004', None),
]

df = spark.createDataFrame(data, schema)
```

## 10.2 Create an array column

```python
from pyspark.sql.functions import (
    array,
    col,
)

array_df = (
    product_df
    .withColumn(
        'attributes',
        array(
            col('category'),
            col('promotion_type'),
        ),
    )
)
```

## 10.3 Structs

```python
from pyspark.sql.functions import (
    col,
    struct,
)

nested_df = (
    orders_df
    .withColumn(
        'store',
        struct(
            col('store_id'),
            col('city'),
            col('province'),
        ),
    )
)
```

Access nested fields:

```python
from pyspark.sql.functions import col

store_view_df = (
    nested_df
    .select(
        col('order_id'),
        col('store.store_id').alias('nested_store_id'),
        col('store.city').alias('store_city'),
        col('store.province').alias('store_province'),
    )
)
```

## 10.4 Nested JSON schema

```python
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

customer_schema = StructType([
    StructField('customer_id', StringType(), True),
    StructField('province', StringType(), True),
])

item_schema = StructType([
    StructField('sku', StringType(), True),
    StructField('quantity', IntegerType(), True),
])

order_schema = StructType([
    StructField('order_id', StringType(), False),
    StructField(
        'customer',
        customer_schema,
        True,
    ),
    StructField(
        'items',
        ArrayType(
            item_schema,
            containsNull=False,
        ),
        True,
    ),
])
```

Create `nested_orders.json`:

```json
{"order_id":"1001","customer":{"customer_id":"C001","province":"ON"},"items":[{"sku":"SKU-001","quantity":2},{"sku":"SKU-002","quantity":1}]}
{"order_id":"1002","customer":{"customer_id":"C002","province":"QC"},"items":[{"sku":"SKU-003","quantity":4}]}
```

Read:

```python
orders_df = (
    spark.read
    .schema(order_schema)
    .json('nested_orders.json')
)
```

## 10.5 `explode()`

```python
from pyspark.sql.functions import (
    col,
    explode,
)

items_df = (
    orders_df
    .withColumn(
        'item',
        explode(col('items')),
    )
    .select(
        col('order_id'),
        col('item.sku').alias('sku'),
        col('item.quantity').alias('quantity'),
    )
)
```

`explode()` changes grain from one row per order to one row per order item.

## 10.6 `from_json()`

```python
from pyspark.sql.functions import (
    col,
    from_json,
)

parsed_json_df = (
    raw_json_df
    .withColumn(
        'customer',
        from_json(
            col('customer_json'),
            customer_schema,
        ),
    )
)
```

---

# Experiment 11 — CSV vs. JSON vs. Parquet

## Goal

Compare file formats, writes, read-back validation, and storage behavior.

## 11.1 Clean typed DataFrame

```python
from datetime import date
from decimal import Decimal

from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

schema = StructType([
    StructField('order_id', StringType(), False),
    StructField('sku', StringType(), False),
    StructField('quantity', IntegerType(), False),
    StructField('unit_price', DecimalType(10, 2), False),
    StructField('order_date', DateType(), False),
])

data = [
    ('1001', 'SKU-001', 2, Decimal('12.99'), date(2026, 8, 18)),
    ('1002', 'SKU-002', 3, Decimal('8.50'), date(2026, 8, 18)),
    ('1003', 'SKU-003', 1, Decimal('19.99'), date(2026, 8, 19)),
]

df = spark.createDataFrame(data, schema)
```

## 11.2 Write CSV

```python
(
    df.write
    .mode('overwrite')
    .option('header', True)
    .csv('output/csv/orders')
)
```

Read back without schema:

```python
csv_df = (
    spark.read
    .option('header', True)
    .csv('output/csv/orders')
)

csv_df.printSchema()
```

## 11.3 Write JSON

```python
(
    df.write
    .mode('overwrite')
    .json('output/json/orders')
)
```

## 11.4 Write Parquet

```python
(
    df.write
    .mode('overwrite')
    .parquet('output/parquet/orders')
)
```

Read back:

```python
parquet_df = (
    spark.read
    .parquet('output/parquet/orders')
)

parquet_df.printSchema()
parquet_df.show(truncate=False)
```

## 11.5 Validate read-back

```python
# Validate schema equality.
print(df.schema == parquet_df.schema)

# Validate row-count equality.
source_count = df.count()
parquet_count = parquet_df.count()

print(f'Source rows: {source_count}')
print(f'Parquet rows: {parquet_count}')
print(f'Counts match: {source_count == parquet_count}')
```

## 11.6 Column pruning

```python
from pyspark.sql.functions import col

price_df = (
    spark.read
    .parquet('output/parquet/orders')
    .select(
        col('sku'),
        col('unit_price'),
    )
)
```

## 11.7 Write modes

```python
# Replace existing output.
df.write.mode('overwrite').parquet('output/parquet/orders')

# Add more records.
df.write.mode('append').parquet('output/parquet/orders')

# Leave existing output untouched.
df.write.mode('ignore').parquet('output/parquet/orders')

# Fail if output already exists.
df.write.mode('error').parquet('output/parquet/orders')
```

---

# Experiment 12 — CSV Read Modes

## Goal

Compare:

- `PERMISSIVE`;
- `DROPMALFORMED`;
- `FAILFAST`.

Create `read_modes_orders.csv`:

```csv
order_id,sku,quantity,unit_price,order_date
1001,SKU-001,2,12.99,2026-08-18
1002,SKU-002,3,8.50,2026-08-18
1003,SKU-003,abc,19.99,2026-08-19
1004,SKU-004,1,4.99,not-a-date
1005,SKU-005,2,6.99
1006,SKU-006,1,5.99,2026-08-20,EXTRA
```

## 12.1 Base schema

```python
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

base_schema = StructType([
    StructField('order_id', StringType(), True),
    StructField('sku', StringType(), True),
    StructField('quantity', IntegerType(), True),
    StructField('unit_price', DecimalType(10, 2), True),
    StructField('order_date', DateType(), True),
])
```

## 12.2 `PERMISSIVE`

```python
permissive_schema = StructType([
    StructField('order_id', StringType(), True),
    StructField('sku', StringType(), True),
    StructField('quantity', IntegerType(), True),
    StructField('unit_price', DecimalType(10, 2), True),
    StructField('order_date', DateType(), True),
    StructField('_corrupt_record', StringType(), True),
])

permissive_df = (
    spark.read
    .option('header', True)
    .option('mode', 'PERMISSIVE')
    .option(
        'columnNameOfCorruptRecord',
        '_corrupt_record',
    )
    .option('dateFormat', 'yyyy-MM-dd')
    .schema(permissive_schema)
    .csv('read_modes_orders.csv')
)

permissive_df.show(truncate=False)
```

## 12.3 `DROPMALFORMED`

```python
drop_df = (
    spark.read
    .option('header', True)
    .option('mode', 'DROPMALFORMED')
    .option('dateFormat', 'yyyy-MM-dd')
    .schema(base_schema)
    .csv('read_modes_orders.csv')
)

drop_df.show(truncate=False)
```

## 12.4 `FAILFAST`

```python
failfast_df = (
    spark.read
    .option('header', True)
    .option('mode', 'FAILFAST')
    .option('dateFormat', 'yyyy-MM-dd')
    .schema(base_schema)
    .csv('read_modes_orders.csv')
)

# show() forces execution and exposes malformed input.
failfast_df.show(truncate=False)
```

## Read-mode mental model

| Mode | Behavior |
|---|---|
| `PERMISSIVE` | Preserve as much as possible and continue |
| `DROPMALFORMED` | Drop malformed rows |
| `FAILFAST` | Raise an error and stop |

Parsing modes are separate from business validation.

---

# Applied Phase 1 Project

## Goal

Build:

```text
Messy raw retail CSV
        ↓
Explicit raw schema
        ↓
Standardization
        ↓
Safe type conversion
        ↓
Validation
       / \
      /   \
 accepted rejected
    ↓        ↓
 Parquet   preserved diagnostics
```

## Repository structure

```text
phases/
└── phase_01_dataframe_fundamentals/
    ├── README.md
    ├── data/
    │   └── raw_orders.csv
    └── pipeline.py
```

## Raw fixture

Create `raw_orders.csv`:

```csv
order_id,sku,quantity,unit_price,order_date,province
1001, sku-001 ,2,12.99,2026-08-18,on
1002,SKU-002,3,8.50,2026-08-18, ON
1003,Sku-003,abc,19.99,2026-08-19,qc
1004,sku-004,-2,4.99,2026-08-19,BC
1005,SKU-005,1,not-a-price,2026-08-20,on
1006,SKU-006,4,29.99,not-a-date,QC
1007,,2,15.00,2026-08-20,ab
,SKU-008,3,7.25,2026-08-21,ON
1009, SKU-009 ,1,11.50,2026-08-21,xx
1010,SKU-010,,9.99,2026-08-21,ON
```

## Accepted output schema

```text
order_id     string
sku          string
quantity     integer
unit_price   decimal(10,2)
order_date   date
province     string
```

## Standardization rules

```text
sku       → trim + uppercase
province  → trim + uppercase
```

## Business rules

- `order_id` must not be NULL or blank.
- `sku` must not be NULL or blank.
- `quantity` must parse as integer.
- `quantity` must be greater than zero.
- `unit_price` must parse as `decimal(10,2)`.
- `unit_price` must be greater than or equal to zero.
- `order_date` must parse as `yyyy-MM-dd`.
- `province` must be one of:

```text
AB, BC, MB, NB, NL, NS, NT, NU, ON, PE, QC, SK, YT
```

Do not silently repair invalid business values.

---

# Applied Task — Part 1

Implement only:

1. `SparkSession`;
2. explicit raw string schema;
3. CSV read;
4. SKU and province standardization;
5. typed `quantity`;
6. typed `unit_price`;
7. typed `order_date`;
8. preservation of raw conversion fields;
9. schema inspection;
10. row inspection.

## Starter skeleton

```python
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

# Preserve raw source representations as strings so malformed
# values remain available for rejected-record diagnostics.
raw_schema = StructType([
    StructField('order_id', StringType(), True),
    StructField('sku', StringType(), True),
    StructField('quantity_raw', StringType(), True),
    StructField('unit_price_raw', StringType(), True),
    StructField('order_date_raw', StringType(), True),
    StructField('province', StringType(), True),
])

# TODO: read raw_orders.csv with raw_schema.

# TODO: standardize sku and province.

# TODO: safely parse quantity, unit_price, and order_date.

# TODO: print the resulting schema.

# TODO: show the transformed rows.

spark.stop()
```

## Questions to answer before Part 2

1. Why are raw numeric/date fields initially strings?
2. Why preserve `quantity_raw` after creating `quantity`?
3. Why standardize before validation?
4. Why is `-2` parseable even though it will later be rejected?
5. Why should `'abc'` and NULL be distinguishable?

---

# After Part 1

Next steps:

1. Implement validation rules.
2. Add explicit rejection reasons.
3. Split accepted and rejected DataFrames.
4. Select the final accepted schema.
5. Write accepted output to Parquet.
6. Read Parquet back.
7. Validate schema, row counts, and important values.
8. Complete independent problems.
9. Complete mastery questions.
10. Pass the Phase 1 mastery gate.
11. Update `ROADMAP.md`.
12. Preserve worthwhile repository work.
13. Commit the finalized phase.
14. Only then generate the Phase 2 starter prompt.
