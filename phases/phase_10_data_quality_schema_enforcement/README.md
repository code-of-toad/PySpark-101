# Phase 10 — Data Quality & Schema Enforcement

<a id="toc"></a>
## Table of Contents

- [Objective](#objective)
- [Phase Mental Model](#phase-mental-model)
- [1. Data Quality as an Explicit Contract](#1-data-quality-as-an-explicit-contract)
- [2. Schema Enforcement vs. Schema Validation](#2-schema-enforcement-vs-schema-validation)
- [3. Required Fields](#3-required-fields)
- [4. Domain, Range, and Business-Rule Checks](#4-domain-range-and-business-rule-checks)
- [5. Grain, Primary Keys, and Composite Keys](#5-grain-primary-keys-and-composite-keys)
- [6. Duplicate Detection](#6-duplicate-detection)
- [7. Referential Integrity](#7-referential-integrity)
- [8. Rejection Reasons](#8-rejection-reasons)
- [9. Accepted vs. Rejected Records](#9-accepted-vs-rejected-records)
- [10. Quarantine Patterns](#10-quarantine-patterns)
- [11. Validation Metrics and Reconciliation](#11-validation-metrics-and-reconciliation)
- [12. Reusable Validation Design](#12-reusable-validation-design)
- [13. Validation Order and Pipeline Integration](#13-validation-order-and-pipeline-integration)
- [14. Distributed-Execution and Performance Considerations](#14-distributed-execution-and-performance-considerations)
- [15. End-to-End Retail Validation Example](#15-end-to-end-retail-validation-example)
- [16. Common Data-Quality Failure Modes](#16-common-data-quality-failure-modes)
- [17. Phase 10 Design Review Checklist](#17-phase-10-design-review-checklist)
- [18. Phase 10 Mastery Reference](#18-phase-10-mastery-reference)

---

<a id="objective"></a>
## Objective

Design PySpark pipelines that **protect downstream systems from bad data without silently losing evidence of what went wrong**.

The Phase 10 problem is no longer:

```text
Can I filter out bad rows?
```

It is:

```text
Can I define the data contract explicitly,
identify every important violation,
preserve diagnostic evidence,
separate usable from unusable records,
and make the validation logic reusable and maintainable?
```

Required Phase 10 concerns:

- schema validation;
- required fields;
- business-rule checks;
- domain validation;
- range checks;
- primary-key uniqueness;
- composite-key validation;
- referential integrity;
- duplicate detection;
- accepted vs. rejected records;
- quarantine patterns;
- validation metrics;
- rejection reasons.

Examples target **PySpark 4.2.0**, use the existing retail domain, and prefer:

```python
from pyspark.sql import functions as F
```

The governing engineering principle is:

> **Bad data should become visible, diagnosable, measurable, and intentionally handled — not disappear inside scattered filters.**

This document is lecture/reference material only. It does not mark Phase 10 complete or update `ROADMAP.md`.

---

[Back to Table of Contents](#toc)

---

<a id="phase-mental-model"></a>
## Phase Mental Model

The canonical Phase 10 flow is:

```text
Raw
 ↓
Schema enforcement
 ↓
Data-quality validation
 ├── Accepted → transformations
 └── Rejected → quarantine
```

A useful professional mental model is to separate validation into three scopes:

```text
ROW-LEVEL RULES
required fields
allowed domains
numeric/date ranges
cross-column business rules

DATASET-LEVEL RULES
primary-key uniqueness
composite-key uniqueness
exact duplicate detection
grain validation

CROSS-DATASET RULES
referential integrity
parent/child consistency
```

These are different problems.

For example:

```text
net_sales < 0
→ row-level range violation

order_id appears twice
→ dataset-level uniqueness violation

customer_id does not exist in customers
→ cross-dataset referential-integrity violation
```

The goal is not one giant Boolean expression. The goal is a validation layer whose rules are explicit, reusable, and diagnosable.

A robust pipeline boundary therefore looks like:

```text
source data
    ↓
structural contract
    ↓
annotate validation failures
    ↓
accepted records ──→ business transformations
    │
    └──────────────→ validation metrics

rejected records ──→ quarantine + diagnostics
```

---

[Back to Table of Contents](#toc)

---

<a id="1-data-quality-as-an-explicit-contract"></a>
## 1. Data Quality as an Explicit Contract

Data quality begins by stating what the dataset is supposed to mean.

For an `orders_df` whose grain is:

```text
one row per order_id
```

a contract might say:

```text
STRUCTURE
order_id      string
customer_id   string
order_date    date
order_status  string
net_sales     decimal(12, 2)

REQUIRED VALUES
order_id      not NULL / not blank
customer_id   not NULL / not blank
order_date    not NULL
order_status  not NULL
net_sales     not NULL

DOMAIN
order_status ∈ {COMPLETED, CANCELLED, PENDING}

RANGE
net_sales >= 0.00

GRAIN / KEY
order_id is unique

REFERENTIAL INTEGRITY
customer_id exists in customers_df
```

The contract should exist before the validation code.

Otherwise the pipeline tends to accumulate rules such as:

```python
df.filter(F.col('net_sales') >= 0)
```

without explaining:

```text
why the rule exists
whether rejected rows matter
who owns the allowed values
what grain is expected
whether the batch should fail or quarantine
```

### Validation policy is part of the contract

A useful rule matrix is:

| Violation | Typical handling |
|---|---|
| Missing required column | Fail the dataset/batch contract |
| Incompatible data type | Fail the dataset/batch contract or controlled parse handling |
| Required value is NULL/blank | Reject the affected row |
| Invalid domain/range/business value | Reject the affected row |
| Duplicate primary/composite key | Reject conflicting rows unless explicit survivorship exists |
| Missing foreign-key parent | Reject/quarantine the orphan row |

There is no universal policy for every system. The important point is that the policy is **deliberate rather than accidental**.

---

[Back to Table of Contents](#toc)

---

<a id="2-schema-enforcement-vs-schema-validation"></a>
## 2. Schema Enforcement vs. Schema Validation

These concepts are related but not identical.

### Schema enforcement

Schema enforcement applies an expected structure while reading or constructing data.

```python
ORDERS_SCHEMA = StructType(
    [
        StructField('order_id', StringType(), nullable=False),
        StructField('customer_id', StringType(), nullable=False),
        StructField('order_date', DateType(), nullable=False),
        StructField('order_status', StringType(), nullable=False),
        StructField('net_sales', DecimalType(12, 2), nullable=False),
    ]
)

orders_df = (
    spark.read
    # Apply the expected structural contract at the ingestion boundary.
    .schema(ORDERS_SCHEMA)
    .parquet(orders_path)
)
```

This makes the intended schema explicit.

### Schema validation

Schema validation checks whether an already-created DataFrame satisfies the expected structural contract.

Typical questions:

```text
Are required columns present?
Are their Spark data types correct?
Are unexpected columns allowed?
Must the schema match exactly or only contain a required subset?
```

A lightweight driver-side structural check can inspect schema metadata without launching a Spark job:

```python
def validate_schema(df, expected_schema, allow_extra_columns=False):
    # Compare structural metadata before expensive row-level validation begins.
    expected_fields = {
        field.name: field.dataType
        for field in expected_schema.fields
    }
    actual_fields = {
        field.name: field.dataType
        for field in df.schema.fields
    }

    missing_columns = set(expected_fields) - set(actual_fields)
    unexpected_columns = set(actual_fields) - set(expected_fields)

    type_mismatches = {
        column_name: (expected_fields[column_name], actual_fields[column_name])
        for column_name in expected_fields.keys() & actual_fields.keys()
        if expected_fields[column_name] != actual_fields[column_name]
    }

    if missing_columns:
        raise ValueError(f'Missing required columns: {sorted(missing_columns)}')

    if type_mismatches:
        raise ValueError(f'Column type mismatches: {type_mismatches}')

    if unexpected_columns and not allow_extra_columns:
        raise ValueError(f'Unexpected columns: {sorted(unexpected_columns)}')
```

### Exact vs. compatible schemas

Choose the policy deliberately.

```text
STRICT CONTRACT
actual schema must match the expected schema closely

COMPATIBLE CONTRACT
required columns/types must exist, but extra columns are tolerated
```

A source that adds a harmless new column may be compatible with one pipeline and a breaking change for another.

### Important distinction: nullable metadata vs. actual NULL data

```python
StructField('order_id', StringType(), nullable=False)
```

states schema intent, but it should not replace row-level required-field validation.

Production validation should still check the actual values that reached the DataFrame.

For text sources such as CSV/JSON, parse failures can also produce malformed or NULL values depending on the read configuration. Therefore:

```text
schema enforcement
!=
guarantee that every source value is semantically valid
```

---

[Back to Table of Contents](#toc)

---

<a id="3-required-fields"></a>
## 3. Required Fields

A required field is a business/data contract, not merely a schema property.

For a key column:

```python
missing_order_id = F.col('order_id').isNull()
```

String identifiers often also need blank-value validation:

```python
missing_customer_id = (
    F.col('customer_id').isNull()
    # A blank identifier is present structurally but still unusable as a key.
    | (F.trim(F.col('customer_id')) == '')
)
```

### Required fields should follow grain

If the grain is:

```text
one row per order_id
```

then `order_id` must be usable for every accepted row.

Likewise, if a later join depends on `customer_id`, a missing `customer_id` is not merely cosmetic. It breaks the downstream relationship.

### Do not silently fill required business keys

Avoid inventing key values merely to make validation pass:

```python
# BAD: This hides the source-system defect and changes business identity.
df = df.fillna({'customer_id': 'UNKNOWN'})
```

An `UNKNOWN` member can be a legitimate dimensional-modeling policy, but that is a separate business decision. It should not be an accidental substitute for validation.

---

[Back to Table of Contents](#toc)

---

<a id="4-domain-range-and-business-rule-checks"></a>
## 4. Domain, Range, and Business-Rule Checks

These rule types answer different questions.

### Domain validation

Domain validation checks whether a value belongs to an allowed set.

```python
ALLOWED_ORDER_STATUSES = ('COMPLETED', 'CANCELLED', 'PENDING')

invalid_status = ~F.col('order_status').isin(*ALLOWED_ORDER_STATUSES)
```

Examples:

```text
province ∈ valid Canadian province/territory codes
customer_segment ∈ {CONSUMER, BUSINESS}
order_status ∈ approved lifecycle statuses
```

The allowed set should come from an explicit contract. Do not infer the valid domain from whatever values happen to appear in today's data.

### Range checks

Range validation checks bounds.

```python
negative_net_sales = F.col('net_sales') < F.lit(Decimal('0.00'))
```

Other examples:

```text
quantity > 0
unit_price >= 0
discount_rate between 0 and 1
snapshot_date <= configured processing date
```

### Business-rule checks

Business rules can depend on multiple columns.

Example:

```python
invalid_cancelled_amount = (
    (F.col('order_status') == 'CANCELLED')
    # In this example contract, cancelled orders should not retain sales value.
    & (F.col('net_sales') != F.lit(Decimal('0.00')))
)
```

This is not merely a type, domain, or range rule. It expresses a relationship between fields.

Other examples:

```text
ship_date >= order_date
end_date >= start_date
quantity * unit_price agrees with line amount within the defined rule
COMPLETED orders require a completion timestamp
```

### Keep rules atomic where diagnostics matter

Prefer separate rule identities such as:

```text
MISSING_ORDER_ID
INVALID_ORDER_STATUS
NEGATIVE_NET_SALES
INVALID_CANCELLED_AMOUNT
```

rather than one opaque reason:

```text
INVALID_ROW
```

Atomic rules make quarantine data and metrics useful.

---

[Back to Table of Contents](#toc)

---

<a id="5-grain-primary-keys-and-composite-keys"></a>
## 5. Grain, Primary Keys, and Composite Keys

Uniqueness validation begins with grain.

Before writing code, state:

> **What does one row represent?**

### Primary-key validation

If:

```text
orders_df
= one row per order_id
```

then accepted rows require:

```text
order_id is present
AND
order_id appears once
```

Detect duplicate keys explicitly:

```python
duplicate_order_ids_df = (
    orders_df
    .groupBy('order_id')
    .count()
    # Any key count above one violates the declared order grain.
    .filter(F.col('count') > 1)
    .select('order_id')
)
```

To recover every conflicting source row:

```python
duplicate_order_rows_df = orders_df.join(
    duplicate_order_ids_df,
    on='order_id',
    how='left_semi',
)
```

### Composite-key validation

Some datasets have no single-column key.

Example inventory snapshot grain:

```text
one row per snapshot_date + store_id + product_id
```

```python
inventory_key = ['snapshot_date', 'store_id', 'product_id']

duplicate_inventory_keys_df = (
    inventory_df
    .groupBy(*inventory_key)
    .count()
    # The combination, not each column individually, defines uniqueness.
    .filter(F.col('count') > 1)
)
```

Do **not** incorrectly require every component to be unique by itself.

For example:

```text
store_id repeats across many products
product_id repeats across many stores
snapshot_date repeats across many rows
```

That is expected. The **combination** must be unique.

### Grain validation protects joins

If `customers_df` is expected to be:

```text
one row per customer_id
```

but `customer_id` is duplicated, joining it to orders can multiply order rows.

Therefore uniqueness is not merely a cleanliness preference. It protects downstream grain and measures.

---

[Back to Table of Contents](#toc)

---

<a id="6-duplicate-detection"></a>
## 6. Duplicate Detection

"Duplicate" can mean two different things.

### Exact duplicate row

Every compared column is identical.

```python
exact_duplicate_rows_df = (
    df
    .groupBy(*df.columns)
    .count()
    .filter(F.col('count') > 1)
)
```

### Duplicate business key

The key repeats even if non-key attributes differ.

```text
order_id = O100, status = PENDING
order_id = O100, status = COMPLETED
```

These are not exact duplicate rows, but they violate an order-grain primary-key contract.

### `dropDuplicates()` is not validation

Avoid this pattern:

```python
# BAD: A contract violation disappears without diagnosis.
clean_df = df.dropDuplicates(['order_id'])
```

Questions it leaves unanswered:

```text
How many duplicates existed?
Why did they occur?
Which row survived?
Was the survivor deterministic?
Should both rows have been quarantined?
Did replay create the duplicate?
```

A safer default for an unexplained primary-key collision is:

```text
identify the duplicated key
mark all conflicting rows
quarantine them
measure the violation
investigate the source
```

If the business defines a deterministic survivorship rule — for example latest event by event timestamp plus stable tie-breaker — then deduplication can be an intentional transformation. That rule should be explicit and separately testable.

---

[Back to Table of Contents](#toc)

---

<a id="7-referential-integrity"></a>
## 7. Referential Integrity

Referential integrity asks whether a foreign key has a valid parent.

Example:

```text
orders_df.customer_id
must exist in
customers_df.customer_id
```

A left anti join identifies orphan orders:

```python
orphan_orders_df = orders_df.join(
    customers_df.select('customer_id'),
    on='customer_id',
    how='left_anti',
)
```

An orphan row should usually not continue blindly into a downstream fact/dimension join.

### Validate the parent grain too

Referential integrity answers:

```text
Does a matching parent exist?
```

It does **not** prove:

```text
Is there exactly one parent row?
```

For a dimension expected at one row per `customer_id`, validate parent-key uniqueness separately.

This protects against two different failures:

```text
zero parent matches
→ orphan foreign key

multiple parent matches
→ potential row multiplication
```

### Multi-column referential integrity

Relationships can also be composite.

Example:

```text
inventory_df(store_id, product_id)
```

may need to match a valid store/product relationship table on both columns.

Use the real relationship key rather than checking each component independently when the business relationship is defined by the combination.

---

[Back to Table of Contents](#toc)

---

<a id="8-rejection-reasons"></a>
## 8. Rejection Reasons

A rejected row should explain why it was rejected.

Prefer machine-readable rule identifiers:

```text
MISSING_ORDER_ID
MISSING_CUSTOMER_ID
INVALID_ORDER_STATUS
NEGATIVE_NET_SALES
DUPLICATE_ORDER_ID
ORPHAN_CUSTOMER_ID
```

These are better than free-form prose because they are easier to:

```text
aggregate
alert on
trend over time
test
map to documentation
```

### One row may fail multiple rules

Example:

```text
order_id = NULL
customer_id = C999
order_status = INVALID
net_sales = -10.00
```

A single `rejection_reason` string forces the pipeline to choose only one failure or concatenate text awkwardly.

An array is often more diagnostic:

```text
rejection_reasons = [
    MISSING_ORDER_ID,
    INVALID_ORDER_STATUS,
    NEGATIVE_NET_SALES,
    ORPHAN_CUSTOMER_ID,
]
```

### Reusable row-rule annotation

A simple reusable pattern is a list of `(reason, invalid_condition)` pairs:

```python
from pyspark.sql import Column
from pyspark.sql import DataFrame


def add_record_rejection_reasons(
    df: DataFrame,
    rules: list[tuple[str, Column]],
) -> DataFrame:
    # Each rule contributes its reason only when the row violates that rule.
    reason_columns = [
        F.when(invalid_condition, F.lit(reason))
        for reason, invalid_condition in rules
    ]

    return df.withColumn(
        'rejection_reasons',
        # Remove NULL placeholders so accepted rows receive an empty array.
        F.filter(
            F.array(*reason_columns),
            lambda reason: reason.isNotNull(),
        ),
    )
```

Example rules:

```python
order_rules = [
    ('MISSING_ORDER_ID', F.col('order_id').isNull()),
    (
        'INVALID_ORDER_STATUS',
        ~F.col('order_status').isin(*ALLOWED_ORDER_STATUSES),
    ),
    ('NEGATIVE_NET_SALES', F.col('net_sales') < F.lit(Decimal('0.00'))),
]
```

This pattern handles row-local rules well.

Dataset-level rules such as duplicate keys and cross-dataset rules such as orphans first require distributed operations to identify the violating keys/rows. Their flags or reasons can then be joined/attached to the original record set.

---

[Back to Table of Contents](#toc)

---

<a id="9-accepted-vs-rejected-records"></a>
## 9. Accepted vs. Rejected Records

Once violations are annotated, split the population deliberately.

```python
def split_accepted_rejected(validated_df):
    # Accepted rows have no recorded data-quality violations.
    accepted_df = (
        validated_df
        .filter(F.size('rejection_reasons') == 0)
        .drop('rejection_reasons')
    )

    # Rejected rows retain reasons for diagnosis and quarantine.
    rejected_df = validated_df.filter(
        F.size('rejection_reasons') > 0
    )

    return accepted_df, rejected_df
```

The key invariant is:

```text
Every interpretable input row
must end up intentionally classified as
accepted OR rejected.
```

For a one-to-one validation classification:

```text
input_count = accepted_count + rejected_count
```

### Avoid sequential destructive filtering

Bad pattern:

```python
clean_df = (
    raw_df
    .filter(F.col('order_id').isNotNull())
    .filter(F.col('net_sales') >= 0)
    .filter(F.col('order_status').isin('COMPLETED', 'CANCELLED', 'PENDING'))
)
```

This tells downstream engineers only which rows survived.

It does not preserve:

```text
which rows failed
which rules failed
how many failed each rule
whether one row failed multiple rules
```

Phase 10 validation should make rejected data a first-class output.

---

[Back to Table of Contents](#toc)

---

<a id="10-quarantine-patterns"></a>
## 10. Quarantine Patterns

Quarantine is a controlled destination for rejected records.

Its purpose is not merely storage. It provides evidence for:

```text
triage
source-system debugging
replay/backfill
quality reporting
auditability
```

A useful quarantine record preserves the original business columns plus diagnostic metadata such as:

```text
source_dataset
validation_run_date / validation_run_id
rejection_reasons
optional source-file identifier
optional source-system identifier
```

Example:

```python
quarantine_df = (
    rejected_df
    .withColumn('source_dataset', F.lit('orders'))
    # Inject run metadata from configuration so replay behavior is explicit.
    .withColumn('validation_run_date', F.lit(run_date).cast('date'))
)
```

### Preserve the source evidence

Do not "fix" the rejected row before quarantine in a way that destroys the original failure.

The quarantine output should answer:

```text
What source record arrived?
Which contract did it violate?
When/under which run was it validated?
Can we reproduce or replay the decision?
```

### Quarantine is not a garbage dump

A production quarantine area needs ownership and retention policy.

Questions include:

```text
Who investigates rejected data?
Can corrected records be replayed?
How long are rejects retained?
Can sensitive values be stored there?
What threshold should alert operators?
```

Phase 10 focuses on the data pattern; later phases deepen operational design.

---

[Back to Table of Contents](#toc)

---

<a id="11-validation-metrics-and-reconciliation"></a>
## 11. Validation Metrics and Reconciliation

A validation layer should report what happened.

Core metrics include:

```text
input_count
accepted_count
rejected_count
acceptance_rate
rejection_rate
rejection count by reason
```

The fundamental reconciliation is:

```text
input_count
=
accepted_count + rejected_count
```

when each input row is classified exactly once.

### Per-rule metrics

If rejection reasons are stored as an array:

```python
rejection_reason_counts_df = (
    rejected_df
    .select(F.explode('rejection_reasons').alias('rejection_reason'))
    .groupBy('rejection_reason')
    .count()
)
```

Important:

```text
sum(rejection counts by reason)
may be greater than
rejected_count
```

because one rejected row may fail multiple rules.

That is expected and should be documented.

### Metrics are operational signals

Useful questions include:

```text
Did rejection rate suddenly jump from 0.1% to 12%?
Did one new rejection reason appear after a source release?
Did all records fail the same domain rule?
Did accepted volume unexpectedly collapse?
```

A pipeline can technically finish successfully while producing unacceptable data quality. Metrics make that visible.

### Thresholds vs. hard failures

Not every rejected row should necessarily fail an entire batch.

A policy might say:

```text
0 schema-contract violations tolerated
0 duplicate primary keys tolerated
< 0.5% non-critical row rejections tolerated
```

The thresholds are business/operational policy, not universal Spark rules.

---

[Back to Table of Contents](#toc)

---

<a id="12-reusable-validation-design"></a>
## 12. Reusable Validation Design

The mastery target is a **reusable data-quality layer**, not ad hoc filters scattered across transformation code.

A practical design does not require a large validation framework.

Start with small helpers that represent real validation responsibilities:

```text
validate_schema(...)
add_record_rejection_reasons(...)
find_duplicate_keys(...)
find_orphan_rows(...)
split_accepted_rejected(...)
build_validation_metrics(...)
```

### Generic helper: duplicate keys

```python
def find_duplicate_keys(df, key_columns):
    # Return one row per violating key so the result can be joined back safely.
    return (
        df
        .groupBy(*key_columns)
        .count()
        .filter(F.col('count') > 1)
        .select(*key_columns)
    )
```

### Generic helper: orphans

```python
def find_orphan_rows(child_df, parent_df, key_columns):
    # Left anti keeps child rows for which no parent key exists.
    return child_df.join(
        parent_df.select(*key_columns),
        on=key_columns,
        how='left_anti',
    )
```

### Keep domain rules visible

Generic mechanics can be reusable, while dataset-specific contracts stay explicit.

For example:

```python
ORDER_RULES = [
    ('MISSING_ORDER_ID', F.col('order_id').isNull()),
    ('MISSING_CUSTOMER_ID', F.col('customer_id').isNull()),
    (
        'INVALID_ORDER_STATUS',
        ~F.col('order_status').isin(*ALLOWED_ORDER_STATUSES),
    ),
    ('NEGATIVE_NET_SALES', F.col('net_sales') < F.lit(Decimal('0.00'))),
]
```

Do not hide business meaning inside a highly abstract configuration language unless the scale of the system genuinely requires it.

### Good validation-layer contract

A validation function should make clear:

```text
input DataFrame(s)
expected grain
rules being evaluated
accepted output
rejected output
rejection reasons
metrics / reconciliation behavior
```

### Keep validation separate from business transformations

Prefer:

```text
raw orders
    ↓
validate_orders(...)
    ├── accepted_orders_df
    └── rejected_orders_df

accepted_orders_df
    ↓
transform_orders(...)
```

rather than:

```python
def transform_orders(df):
    # BAD: Invalid data silently disappears inside business logic.
    return (
        df
        .filter(F.col('order_id').isNotNull())
        .filter(F.col('net_sales') >= 0)
        # ...actual business transformation continues...
    )
```

This separation extends the Phase 9 application-architecture principle.

---

[Back to Table of Contents](#toc)

---

<a id="13-validation-order-and-pipeline-integration"></a>
## 13. Validation Order and Pipeline Integration

A useful default sequence is:

```text
1. Read with explicit schema where appropriate
2. Validate structural schema contract
3. Evaluate row-level rules
4. Evaluate primary/composite-key uniqueness
5. Evaluate referential integrity
6. Attach all relevant rejection reasons
7. Split accepted/rejected records
8. Build validation metrics
9. Write rejects to quarantine
10. Send accepted records to transformations
```

### Fail-fast vs. quarantine

Structural failures are often batch-level failures.

Example:

```text
Expected order_id column does not exist at all
```

The pipeline may not have enough information to classify individual rows correctly. Failing fast is usually more appropriate than pretending each row can be meaningfully validated.

Content failures are usually row-level candidates for quarantine:

```text
one order has NULL order_id
one order has invalid status
one order references unknown customer_id
```

### Do not filter too early if you need complete diagnostics

Suppose a row has both:

```text
INVALID_ORDER_STATUS
NEGATIVE_NET_SALES
```

If the first rule filters the row away, the second violation is never measured.

Prefer annotating violations before splitting whenever complete rejection diagnostics are required.

### Accepted data should be the only input to downstream business logic

This keeps downstream transformations simpler:

```text
validation owns data-contract correctness
transformations own business results
```

The transformation layer should not repeatedly re-implement the same source-quality filters.

---

[Back to Table of Contents](#toc)

---

<a id="14-distributed-execution-and-performance-considerations"></a>
## 14. Distributed-Execution and Performance Considerations

Data quality is correctness work, but it is still Spark work.

### Row-level checks are usually narrow expressions

Rules such as:

```text
isNull
isin
numeric comparisons
cross-column conditions
```

can usually be evaluated within existing partitions without a shuffle.

### Uniqueness usually requires redistribution

```python
df.groupBy(*key_columns).count()
```

requires rows with the same key to be colocated, so uniqueness checks commonly introduce a shuffle.

### Referential integrity requires a join

A foreign-key check is a distributed relationship check.

Its physical strategy may be:

```text
broadcast hash join
sort-merge join
another valid Spark join strategy
```

depending on data sizes, statistics, configuration, and AQE.

Do not avoid referential-integrity checks merely because they cost work. Correctness is the requirement. Optimize with evidence if the validation becomes expensive.

### Validation metrics trigger actions

Calls such as:

```python
df.count()
```

are Spark actions.

If the same validated DataFrame is subsequently:

```text
counted for metrics
written as accepted data
written as rejected data
exploded for reason metrics
```

Spark may otherwise recompute lineage multiple times.

Persistence may be useful when repeated materialization is substantial and evidence supports reuse — but do not cache automatically. Apply the Phase 7 principle:

> **Measure and justify reuse rather than treating cache as a default.**

### Keep large rejected populations distributed

Avoid:

```python
# BAD for large datasets: rejected data can overwhelm the driver.
rejected_rows = rejected_df.collect()
```

Write quarantine data and aggregate metrics with distributed DataFrame operations.

---

[Back to Table of Contents](#toc)

---

<a id="15-end-to-end-retail-validation-example"></a>
## 15. End-to-End Retail Validation Example

Reuse the Phase 9 retail relations.

### Input grains

```text
orders_df
= one row per order_id

customers_df
= one row per customer_id
```

### Orders contract

```text
SCHEMA
order_id      string
customer_id   string
order_date    date
order_status  string
net_sales     decimal(12, 2)

REQUIRED
order_id
customer_id
order_date
order_status
net_sales

DOMAIN
order_status ∈ {COMPLETED, CANCELLED, PENDING}

RANGE
net_sales >= 0.00

PRIMARY KEY
order_id

FOREIGN KEY
orders.customer_id → customers.customer_id
```

### Intentionally invalid examples

```text
O001 | C001 | 2026-09-01 | COMPLETED | 125.00
NULL | C002 | 2026-09-01 | COMPLETED |  80.00   ← missing primary key
O003 | C001 | 2026-09-02 | UNKNOWN   |  45.00   ← invalid domain
O004 | C003 | 2026-09-02 | COMPLETED | -20.00   ← invalid range
O005 | C999 | 2026-09-03 | COMPLETED |  30.00   ← orphan customer
O006 | C004 | 2026-09-03 | COMPLETED |  10.00
O006 | C004 | 2026-09-03 | COMPLETED |  15.00   ← duplicate primary key
```

A correct Phase 10 validation layer should identify the violations without hiding them.

### Conceptual output

```text
accepted_orders_df
├── O001
└── other rows satisfying every required contract

rejected_orders_df
├── missing order_id        → [MISSING_ORDER_ID]
├── O003                    → [INVALID_ORDER_STATUS]
├── O004                    → [NEGATIVE_NET_SALES]
├── O005                    → [ORPHAN_CUSTOMER_ID]
├── O006 conflicting row 1  → [DUPLICATE_ORDER_ID]
└── O006 conflicting row 2  → [DUPLICATE_ORDER_ID]
```

The key point is not the exact helper implementation.

It is the architecture:

```text
orders_df
    ↓
orders schema contract
    ↓
row rules
    ↓
key uniqueness
    ↓
customer referential integrity
    ↓
rejection reasons
    ├── accepted_orders_df → Phase 9-style transformations
    └── rejected_orders_df → quarantine
    ↓
validation metrics
```

### Example orchestration boundary

```python
def run_orders_quality_layer(orders_df, customers_df):
    # 1. Structural contract should fail before semantic validation continues.
    validate_schema(orders_df, ORDERS_SCHEMA, allow_extra_columns=False)

    # 2. Annotate record-local rules without dropping evidence.
    validated_df = add_record_rejection_reasons(
        orders_df,
        rules=ORDER_RULES,
    )

    # 3. Dataset-level and cross-dataset checks would attach additional reasons
    #    such as DUPLICATE_ORDER_ID and ORPHAN_CUSTOMER_ID here.
    validated_df = add_key_and_relationship_reasons(
        validated_df,
        customers_df,
    )

    # 4. Only now divide good records from quarantine records.
    accepted_df, rejected_df = split_accepted_rejected(validated_df)

    return accepted_df, rejected_df
```

`add_key_and_relationship_reasons()` is intentionally left conceptual in the phase notes. Its implementation should preserve one output row per input row while attaching dataset-level and cross-dataset violations without accidental row multiplication.

That is a useful applied problem for Phase 10 rather than an abstraction to hide prematurely.

---

[Back to Table of Contents](#toc)

---

<a id="16-common-data-quality-failure-modes"></a>
## 16. Common Data-Quality Failure Modes

### Failure 1 — Scattered silent filters

```python
clean_df = raw_df.filter(...).filter(...).filter(...)
```

Problem:

```text
rejected rows disappear
failure reasons disappear
metrics are unavailable
rules become duplicated across pipelines
```

### Failure 2 — Treating schema application as complete validation

```text
DataFrame has the expected column types
→ therefore data is valid
```

Wrong.

Types do not prove:

```text
required values are present
domains are valid
ranges are valid
keys are unique
relationships are valid
```

### Failure 3 — Using `dropDuplicates()` to hide key collisions

Problem:

```text
contract violation becomes invisible
survivor may be arbitrary without explicit ordering
root cause is not measured
```

### Failure 4 — Checking foreign keys without validating parent grain

A parent key can exist multiple times.

The foreign-key check passes, but the downstream join can still multiply rows.

### Failure 5 — One opaque rejection reason

```text
INVALID_ROW
```

This is difficult to operate.

Prefer atomic machine-readable reasons.

### Failure 6 — Sequential filtering before diagnostics

The first filter removes the row, so later violations are never observed.

### Failure 7 — Collecting rejected data to the driver

Large bad-data populations should remain distributed.

### Failure 8 — Treating every anomaly as a data-quality rule

A rule should come from a real contract.

Do not reject a valid new business case simply because it is statistically unusual.

### Failure 9 — Overengineering a validation framework

A hierarchy of rule classes, registries, factories, and configuration DSLs can create more complexity than the data-quality problem itself.

Start with clear functions and explicit rule definitions. Add abstraction only when repeated real requirements justify it.

### Failure 10 — No reconciliation

If no one checks:

```text
input = accepted + rejected
```

records can still disappear through implementation mistakes even when rejection logic exists.

---

[Back to Table of Contents](#toc)

---

<a id="17-phase-10-design-review-checklist"></a>
## 17. Phase 10 Design Review Checklist

### Contract

- [ ] Input grain is stated explicitly.
- [ ] Required columns and Spark data types are explicit.
- [ ] Required values are defined separately from nullable schema metadata.
- [ ] Domain, range, and business rules have clear ownership.

### Keys and relationships

- [ ] Primary-key uniqueness is validated where applicable.
- [ ] Composite keys are validated as combinations, not as individually unique columns.
- [ ] Duplicate business keys are distinguished from exact duplicate rows.
- [ ] Referential integrity is validated on the correct relationship key.
- [ ] Parent/dimension grain is validated before downstream joins rely on it.

### Rejections

- [ ] Invalid records are not silently discarded.
- [ ] Rejection reasons are machine-readable and diagnostic.
- [ ] One row can retain multiple rejection reasons where useful.
- [ ] Rejected records preserve enough source evidence for investigation/replay.
- [ ] Quarantine metadata is explicit and deterministic where replayability matters.

### Accepted data

- [ ] Only records satisfying the required contract enter downstream transformations.
- [ ] Validation does not accidentally change accepted-record grain.

### Metrics

- [ ] Input, accepted, and rejected counts reconcile.
- [ ] Rejection counts are available by reason.
- [ ] It is understood that per-reason counts can exceed rejected-row count.
- [ ] Quality thresholds/failure policies are explicit rather than implied.

### Architecture

- [ ] Validation logic is reusable rather than scattered through transformations.
- [ ] Generic mechanics are separated from dataset-specific business rules where useful.
- [ ] The design avoids unnecessary framework abstraction.
- [ ] Validation failures are easy for another engineer to locate and understand.

### Spark execution

- [ ] Row-level rules, uniqueness shuffles, and referential joins are understood as different execution costs.
- [ ] Metrics/actions are deliberate.
- [ ] Large reject populations stay distributed.
- [ ] Persistence is used only when repeated computation and evidence justify it.

---

[Back to Table of Contents](#toc)

---

<a id="18-phase-10-mastery-reference"></a>
## 18. Phase 10 Mastery Reference

The curriculum mastery project is:

> **Build a reusable data-quality layer rather than scattering ad hoc filters throughout a pipeline.**

Before Phase 10 can be marked complete, be able to demonstrate a retail-domain validation layer that can:

```text
1. enforce/validate an explicit schema contract;
2. validate required fields;
3. validate domains;
4. validate ranges;
5. validate at least one multi-column business rule;
6. validate a primary key;
7. validate a composite key;
8. detect duplicate records/keys deliberately;
9. validate referential integrity;
10. preserve diagnostic rejection reasons;
11. split accepted and rejected records;
12. produce a quarantine-ready rejected dataset;
13. produce validation metrics;
14. reconcile input = accepted + rejected;
15. keep validation logic reusable and separate from business transformations.
```

Given a pipeline such as:

```text
Raw Orders
Raw Customers
Raw Inventory
      ↓
Schema Enforcement
      ↓
Reusable Data-Quality Layer
      ├── Accepted → transformations
      └── Rejected → quarantine
```

you should be able to explain:

```text
What is the grain of each dataset?
What structural contract is enforced?
Which rules are row-level, dataset-level, or cross-dataset?
Which columns form the primary/composite keys?
How are duplicate keys handled?
How is referential integrity checked?
Can one row fail multiple rules?
What exactly qualifies a row as accepted?
What evidence is retained for rejected rows?
How do validation metrics reconcile?
Where will Spark shuffle or join during validation?
Why is this design safer than scattered filters?
```

Phase 10 is **not complete** when the notes exist.

It is complete only after the reusable data-quality mastery requirement has been demonstrated and the formal phase-completion handoff is performed.

[Back to Table of Contents](#toc)
