# Phase 10 — Experiments & Applied Practice Guide

<a id="toc"></a>
## Table of Contents

- [Purpose](#purpose)
- [Conventions](#conventions)
- [Practice Datasets](#practice-datasets)
- [Data-Quality Exercise Protocol](#data-quality-exercise-protocol)
- [Validation Review Record](#validation-review-record)
- [Experiment 1 — Structural Schema Contract](#experiment-1)
- [Experiment 2 — Required Fields](#experiment-2)
- [Experiment 3 — Domain, Range, and Business Rules](#experiment-3)
- [Experiment 4 — Primary-Key Uniqueness](#experiment-4)
- [Experiment 5 — Composite-Key Validation](#experiment-5)
- [Experiment 6 — Exact Duplicates vs. Duplicate Business Keys](#experiment-6)
- [Experiment 7 — Referential Integrity](#experiment-7)
- [Experiment 8 — Multiple Rejection Reasons](#experiment-8)
- [Experiment 9 — Accepted vs. Rejected Records](#experiment-9)
- [Experiment 10 — Quarantine Design](#experiment-10)
- [Experiment 11 — Validation Metrics and Reconciliation](#experiment-11)
- [Experiment 12 — Distributed Cost of Validation](#experiment-12)
- [Applied Phase 10 Project](#applied-project)
- [Applied Task — Build a Reusable Retail Data-Quality Layer](#applied-task)
- [After the Applied Task](#after-applied-task)

---

<a id="purpose"></a>
## Purpose

This file is the execution-oriented companion for **Phase 10 — Data Quality & Schema Enforcement**.

The Phase 10 `README.md` is the conceptual reference. `phase_10_lecture.py` is the consolidated teaching implementation. This guide turns those ideas into controlled exercises where the repeated habit is:

```text
state the contract
    ↓
state the grain
    ↓
identify rule scope
    ↓
predict invalid rows
    ↓
implement ONE validation concern
    ↓
preserve rejection evidence
    ↓
split accepted/rejected
    ↓
reconcile counts
    ↓
review distributed cost
```

The governing question is:

> **Can this validation layer protect downstream transformations without silently deleting bad data or scattering rules throughout the pipeline?**

For every important exercise, be able to answer:

```text
1. What is the dataset grain?
2. What is the key?
3. Is this a row-level, dataset-level, or cross-dataset rule?
4. What exactly makes a row invalid?
5. Should the violation fail the batch or reject individual rows?
6. What rejection reason should be preserved?
7. Can one row fail multiple rules?
8. Does the validation preserve input grain?
9. How are accepted and rejected records reconciled?
10. Does this rule require a shuffle?
11. Does this rule require a join?
12. Could this rule multiply rows accidentally?
13. Is the rule reusable without hiding business meaning?
14. What evidence would an operator need to diagnose the rejection?
```

The applied section is practice only. It does **not** perform the formal Phase 10 mastery gate, update `ROADMAP.md`, mark Phase 10 complete, or enter Phase 11.

---

[Back to Table of Contents](#toc)

---

<a id="conventions"></a>
## Conventions

- Use **PySpark 4.2.0**.
- Use **single quotes** for Python strings.
- Prefer `from pyspark.sql import functions as F`.
- Include concise inline comments explaining both **what** important code does and **why** the validation matters.
- Reuse deterministic retail-domain data.
- State grain before validating keys or relationships.
- Keep validation separate from downstream business transformations.
- Preserve rejected records rather than silently filtering them away.
- Prefer machine-readable rejection reasons such as `INVALID_ORDER_STATUS`.
- Allow one row to retain multiple rejection reasons where useful.
- Treat exact duplicates and duplicate business keys as different concepts.
- Validate parent/dimension grain before relying on referential integrity.
- Keep large rejected populations distributed.
- Do not use `collect()` as a validation strategy.
- Do not use `dropDuplicates()` to hide unexplained key violations.
- Reconcile `input_count = accepted_count + rejected_count`.
- Remember that per-reason counts may exceed rejected-row count.
- Do not create a validation framework more abstract than the actual problem requires.
- Do not mark Phase 10 complete until the formal mastery gate is explicitly demonstrated.

### Core validation vocabulary

Be able to classify rules as:

```text
structural
required-field
domain
range
cross-column business rule
primary-key uniqueness
composite-key uniqueness
exact duplicate
referential integrity
quarantine
validation metric
reconciliation
```

And classify their scope as:

```text
row-level
dataset-level
cross-dataset
```

---

[Back to Table of Contents](#toc)

---

<a id="practice-datasets"></a>
## Practice Datasets

Reuse the deterministic retail relations from `phase_10_lecture.py`.

### `orders_df`

Declared grain:

```text
one row per order_id
```

Columns:

```text
order_id
customer_id
order_date
order_status
net_sales
```

Intentional defects include:

```text
missing order_id
invalid order_status
negative net_sales
orphan customer_id
duplicate order_id
exact duplicate row
invalid cancelled-order amount
```

### `customers_df`

Declared grain:

```text
one row per customer_id
```

Columns:

```text
customer_id
customer_name
province
customer_segment
```

This relation acts as the parent dataset for:

```text
orders.customer_id
    →
customers.customer_id
```

### `inventory_df`

Declared grain:

```text
one row per snapshot_date + store_id + product_id
```

Columns:

```text
snapshot_date
store_id
product_id
quantity_on_hand
```

Intentional defects include:

```text
duplicate composite key
negative quantity_on_hand
```

---

[Back to Table of Contents](#toc)

---

<a id="data-quality-exercise-protocol"></a>
## Data-Quality Exercise Protocol

For each experiment:

### Step 1 — State the contract

Example:

```text
order_status must be one of:
COMPLETED
CANCELLED
PENDING
```

### Step 2 — State the grain

Example:

```text
orders_df
= one row per order_id
```

### Step 3 — Classify the rule

Example:

```text
INVALID_ORDER_STATUS
→ row-level domain rule
```

### Step 4 — Predict the invalid rows

Do this **before** running Spark code.

### Step 5 — Predict distributed behavior

Ask:

```text
narrow expression?
shuffle?
join?
action?
```

### Step 6 — Implement the rule

Prefer one clear validation concern at a time.

### Step 7 — Preserve diagnostics

Do not immediately discard failing rows.

### Step 8 — Verify accepted/rejected classification

Confirm the resulting grain has not changed accidentally.

### Step 9 — Reconcile

For row classification:

```text
input_count
=
accepted_count + rejected_count
```

### Step 10 — Explain the engineering consequence

State what downstream defect the rule prevents.

---

[Back to Table of Contents](#toc)

---

<a id="validation-review-record"></a>
## Validation Review Record

Use this concise record for each important rule:

```text
Dataset:
Declared grain:
Rule:
Rule scope:
Key columns:
Expected valid behavior:
Predicted invalid rows:
Rejection reason:
Batch failure or row rejection:
Shuffle required?:
Join required?:
Accepted grain after validation:
Diagnostic evidence preserved:
Downstream risk prevented:
```

This forces the validation decision to remain tied to a real data contract.

---

[Back to Table of Contents](#toc)

---

<a id="experiment-1"></a>
## Experiment 1 — Structural Schema Contract

### Objective

Distinguish:

```text
schema enforcement
```

from:

```text
schema validation
```

### Tasks

1. Build the expected `ORDERS_SCHEMA`.
2. Validate the correct `orders_df`.
3. Create one DataFrame missing `net_sales`.
4. Predict whether row-level validation can proceed safely.
5. Create one DataFrame where `net_sales` is a string.
6. Create one DataFrame with an unexpected extra column.
7. Compare strict vs. compatible extra-column policy.

### Questions

```text
Why should a missing required column usually fail the dataset contract?
Why is a wrong Spark data type different from a negative numeric value?
Why can structural validation often run without a Spark job?
Why does nullable schema metadata not replace required-field validation?
```

### Success condition

You can explain why:

```text
schema correct
```

does **not** imply:

```text
data valid
```

---

[Back to Table of Contents](#toc)

---

<a id="experiment-2"></a>
## Experiment 2 — Required Fields

### Objective

Validate required values without silently inventing replacements.

### Tasks

Validate:

```text
order_id
customer_id
order_date
order_status
net_sales
```

For string keys, treat both as invalid:

```text
NULL
''
```

### Predict first

Which rows should receive:

```text
MISSING_ORDER_ID
MISSING_CUSTOMER_ID
MISSING_ORDER_DATE
MISSING_ORDER_STATUS
MISSING_NET_SALES
```

### Question

Why is this dangerous?

```python
df.fillna({'customer_id': 'UNKNOWN'})
```

Answer from the perspective of:

```text
business identity
referential integrity
source-system diagnostics
downstream grain
```

---

[Back to Table of Contents](#toc)

---

<a id="experiment-3"></a>
## Experiment 3 — Domain, Range, and Business Rules

### Objective

Differentiate three common row-level validation types.

### Domain rule

```text
order_status ∈ {
    COMPLETED,
    CANCELLED,
    PENDING,
}
```

Expected rejection reason:

```text
INVALID_ORDER_STATUS
```

### Range rule

```text
net_sales >= 0.00
```

Expected rejection reason:

```text
NEGATIVE_NET_SALES
```

### Cross-column business rule

Use the teaching contract:

```text
if order_status = CANCELLED
then net_sales must equal 0.00
```

Expected rejection reason:

```text
INVALID_CANCELLED_AMOUNT
```

### Questions

```text
Why is INVALID_CANCELLED_AMOUNT not a simple range rule?
Why should allowed domain values come from an explicit contract?
Why should a statistically unusual value not automatically be rejected?
```

---

[Back to Table of Contents](#toc)

---

<a id="experiment-4"></a>
## Experiment 4 — Primary-Key Uniqueness

### Objective

Protect the declared `orders_df` grain.

Declared grain:

```text
one row per order_id
```

### Tasks

1. Find duplicate `order_id` values.
2. Recover every source row participating in a duplicate key.
3. Mark those rows with:

```text
DUPLICATE_ORDER_ID
```

4. Preserve both conflicting `O006` rows.
5. Preserve both exact-duplicate `O007` rows.

### Predict execution

What should you expect from:

```python
orders_df.groupBy('order_id').count()
```

Explain:

```text
why a shuffle is needed
what the grouping proves
what it does NOT prove
```

### Key question

Why is this not acceptable as a generic fix?

```python
orders_df.dropDuplicates(['order_id'])
```

---

[Back to Table of Contents](#toc)

---

<a id="experiment-5"></a>
## Experiment 5 — Composite-Key Validation

### Objective

Validate grain defined by a combination of columns.

Declared inventory grain:

```text
one row per
snapshot_date + store_id + product_id
```

### Tasks

1. Define:

```python
inventory_key = [
    'snapshot_date',
    'store_id',
    'product_id',
]
```

2. Detect duplicate combinations.
3. Mark all conflicting rows with:

```text
DUPLICATE_INVENTORY_KEY
```

4. Confirm that repeated `store_id` alone is valid.
5. Confirm that repeated `product_id` alone is valid.

### Core distinction

```text
each key component unique
```

is **not** the contract.

The contract is:

```text
the combined key is unique
```

---

[Back to Table of Contents](#toc)

---

<a id="experiment-6"></a>
## Experiment 6 — Exact Duplicates vs. Duplicate Business Keys

### Objective

Separate two concepts that are often incorrectly merged.

### Case A — Duplicate business key

```text
O006 | ... | 10.00
O006 | ... | 15.00
```

Same `order_id`, different record values.

### Case B — Exact duplicate row

```text
O007 | C004 | 2026-09-04 | PENDING | 20.00
O007 | C004 | 2026-09-04 | PENDING | 20.00
```

Every compared value matches.

### Tasks

Produce:

```text
duplicate_order_keys_df
exact_duplicate_rows_df
```

### Questions

```text
Why does exact-duplicate detection not replace primary-key validation?
Why does primary-key validation not tell you whether duplicates are identical?
When could deterministic survivorship be legitimate?
What extra business contract would be required?
```

---

[Back to Table of Contents](#toc)

---

<a id="experiment-7"></a>
## Experiment 7 — Referential Integrity

### Objective

Protect the relationship:

```text
orders.customer_id
    →
customers.customer_id
```

### Tasks

1. Confirm `customers_df` is unique on `customer_id`.
2. Identify orphan orders.
3. Confirm `C999` is rejected.
4. Attach:

```text
ORPHAN_CUSTOMER_ID
```

5. Confirm missing `customer_id` remains:

```text
MISSING_CUSTOMER_ID
```

rather than being mislabeled only as an orphan.

### Compare approaches

Understand both:

```python
orders_df.join(
    customers_df.select('customer_id'),
    on='customer_id',
    how='left_anti',
)
```

and:

```text
left join parent existence flag
→ attach rejection reason
```

### Critical question

Why can referential integrity pass while a later join still multiplies rows?

Hint:

```text
parent key exists
!=
parent key is unique
```

---

[Back to Table of Contents](#toc)

---

<a id="experiment-8"></a>
## Experiment 8 — Multiple Rejection Reasons

### Objective

Preserve complete diagnostics for rows violating multiple rules.

Create or modify one row so it violates at least three rules, for example:

```text
order_id = NULL
customer_id = C999
order_status = UNKNOWN
net_sales = -10.00
```

### Expected shape

```text
rejection_reasons = [
    MISSING_ORDER_ID,
    INVALID_ORDER_STATUS,
    NEGATIVE_NET_SALES,
    ORPHAN_CUSTOMER_ID,
]
```

### Tasks

1. Attach all applicable reasons.
2. Confirm the row appears once.
3. Confirm the reason array contains multiple values.
4. Confirm validation did not multiply the record.

### Question

Why is this weaker?

```text
INVALID_ROW
```

Explain from the perspective of:

```text
debugging
metrics
alerting
source-owner communication
testing
```

---

[Back to Table of Contents](#toc)

---

<a id="experiment-9"></a>
## Experiment 9 — Accepted vs. Rejected Records

### Objective

Classify rows only after the required diagnostics have been evaluated.

### Tasks

Create:

```text
validated_orders_df
accepted_orders_df
rejected_orders_df
```

Accepted condition:

```python
F.size('rejection_reasons') == 0
```

Rejected condition:

```python
F.size('rejection_reasons') > 0
```

### Verify

```text
accepted rows contain no failed required rules
rejected rows retain rejection_reasons
accepted order grain remains one row per order_id
```

### Compare with the anti-pattern

```python
clean_df = (
    raw_df
    .filter(...)
    .filter(...)
    .filter(...)
)
```

Explain exactly what diagnostic information disappears.

---

[Back to Table of Contents](#toc)

---

<a id="experiment-10"></a>
## Experiment 10 — Quarantine Design

### Objective

Turn rejected rows into a usable operational dataset.

Add deterministic metadata:

```text
source_dataset
validation_run_date
```

Preserve:

```text
original business columns
rejection_reasons
```

### Tasks

Build:

```text
orders_quarantine_df
```

### Questions

```text
Why should quarantine preserve original source evidence?
Why should validation_run_date be injected for deterministic replay?
What additional metadata could be useful in production?
Why should secrets or unnecessary sensitive data not be copied casually?
```

### Design review

A quarantine dataset should help answer:

```text
What failed?
Why?
From which source?
During which run?
Can it be investigated or replayed?
```

---

[Back to Table of Contents](#toc)

---

<a id="experiment-11"></a>
## Experiment 11 — Validation Metrics and Reconciliation

### Objective

Measure the quality decision rather than merely producing two DataFrames.

Produce:

```text
input_count
accepted_count
rejected_count
acceptance_rate
rejection_rate
```

Then produce counts by:

```text
rejection_reason
```

### Mandatory invariant

```text
input_count
=
accepted_count + rejected_count
```

### Important distinction

If one row fails three rules:

```text
rejected_count += 1
```

but:

```text
per-reason violation counts += 3
```

Therefore:

```text
sum(reason counts)
can exceed
rejected_count
```

### Questions

```text
Why is reconciliation a correctness check?
Why could a pipeline technically succeed while quality metrics show failure?
When might a rejection-rate threshold fail the batch?
```

---

[Back to Table of Contents](#toc)

---

<a id="experiment-12"></a>
## Experiment 12 — Distributed Cost of Validation

### Objective

Connect data-quality correctness to Spark execution.

### Inspect these operations

#### Row-level checks

```text
isNull
trim
isin
numeric comparisons
cross-column expressions
```

Predict:

```text
usually narrow
```

#### Uniqueness

```python
df.groupBy(*key_columns).count()
```

Predict:

```text
shuffle / Exchange
```

#### Referential integrity

```python
child_df.join(parent_df, ...)
```

Predict possible strategies:

```text
BroadcastHashJoin
SortMergeJoin
another valid Spark join strategy
```

#### Metrics

```python
df.count()
```

or aggregations materialized by:

```text
show
collect
write
first
```

Recognize them as actions/materialization.

### Tasks

1. Use `explain('formatted')` on duplicate-key validation.
2. Use `explain('formatted')` on referential-integrity logic.
3. Identify `Exchange` operators.
4. Identify the join strategy.
5. Explain why correctness checks should not be removed merely because they cost distributed work.

### Optional investigation

If one validated DataFrame is reused for:

```text
accepted write
rejected write
summary metrics
reason metrics
```

inspect whether repeated recomputation occurs.

Do not cache automatically.

State what evidence would justify:

```python
validated_df.persist()
```

---

[Back to Table of Contents](#toc)

---

<a id="applied-project"></a>
## Applied Phase 10 Project

The applied task should combine the complete curriculum into one reusable data-quality boundary.

Do not build a separate enterprise-style framework unless the code genuinely needs it.

Reuse the Phase 9 architecture principle:

```text
schemas
validation
transformations
pipeline
```

The new responsibility is:

```text
validation
→ explicit reusable data-quality layer
```

---

[Back to Table of Contents](#toc)

---

<a id="applied-task"></a>
## Applied Task — Build a Reusable Retail Data-Quality Layer

### Requirement

Build a reusable layer for:

```text
orders
customers
inventory
```

with this pipeline shape:

```text
Raw
 ↓
Schema enforcement
 ↓
Data-quality validation
 ├── Accepted → transformations
 └── Rejected → quarantine
```

### Part 1 — Declare contracts

For each dataset, document:

```text
grain
required columns
Spark data types
required values
primary/composite key
domains
ranges
business rules
foreign-key relationships
```

### Part 2 — Structural validation

Implement reusable schema validation that can detect:

```text
missing columns
wrong Spark data types
unexpected columns under strict policy
```

Do not treat structural failures as ordinary row rejections if the dataset can no longer be interpreted safely.

### Part 3 — Orders row-level rules

At minimum validate:

```text
required fields
order_status domain
net_sales range
cancelled-order amount business rule
```

### Part 4 — Orders primary key

Validate:

```text
order_id uniqueness
```

Mark every unexplained conflicting row.

Do not silently choose a survivor.

### Part 5 — Inventory composite key

Validate:

```text
snapshot_date + store_id + product_id
```

as one combined key.

### Part 6 — Duplicate diagnostics

Demonstrate separately:

```text
duplicate business key
exact duplicate row
```

### Part 7 — Referential integrity

Validate:

```text
orders.customer_id
    →
customers.customer_id
```

Before doing so, validate:

```text
customers.customer_id uniqueness
```

### Part 8 — Rejection reasons

Preserve atomic identifiers.

A rejected row should support multiple simultaneous reasons.

### Part 9 — Accepted/rejected split

Produce:

```text
accepted_orders_df
rejected_orders_df

accepted_inventory_df
rejected_inventory_df
```

Only accepted data proceeds to downstream transformations.

### Part 10 — Quarantine

Create quarantine-ready rejected outputs containing:

```text
original business columns
rejection_reasons
source_dataset
validation_run_date
```

### Part 11 — Validation metrics

Produce:

```text
input_count
accepted_count
rejected_count
acceptance_rate
rejection_rate
counts by rejection reason
```

### Part 12 — Reconciliation

Prove:

```text
input_count
=
accepted_count + rejected_count
```

for each validated dataset.

### Part 13 — Architecture review

Explain why the resulting design is better than:

```python
raw_df.filter(...).filter(...).dropDuplicates(...)
```

Your explanation must address:

```text
diagnostics
grain
correctness
reuse
maintainability
downstream reliability
operational visibility
```

### Part 14 — Spark execution review

For at least one:

```text
uniqueness check
referential-integrity check
```

inspect the physical plan and explain:

```text
where redistribution occurs
which join strategy Spark chose
why the distributed cost is justified
```

### Required applied result

The finished work should demonstrate a reusable validation boundary shaped roughly as:

```python
validated_orders_df = validate_orders(
    orders_df,
    customers_df,
)

accepted_orders_df, rejected_orders_df = split_accepted_rejected(
    validated_orders_df,
)
```

The exact function names are not mandatory.

The design principle is.

---

[Back to Table of Contents](#toc)

---

<a id="after-applied-task"></a>
## After the Applied Task

Do **not** mark Phase 10 complete automatically.

Before formal completion, the applied work should be reviewed against the curriculum mastery requirement:

> **Build a reusable data-quality layer rather than scattering ad hoc filters throughout a pipeline.**

The formal mastery gate should verify that you can independently explain and demonstrate:

```text
schema validation
required fields
business-rule checks
domain validation
range checks
primary-key uniqueness
composite-key validation
referential integrity
duplicate detection
accepted vs. rejected records
quarantine patterns
validation metrics
rejection reasons
```

Only after the mastery requirement is demonstrated should Phase 10 be finalized through:

```text
mastery results
phase summary
files added/materially changed
Git commit message
ROADMAP.md update
important decisions/findings
```

Then wait for an explicit request before generating the Phase 11 starter prompt.

[Back to Table of Contents](#toc)
