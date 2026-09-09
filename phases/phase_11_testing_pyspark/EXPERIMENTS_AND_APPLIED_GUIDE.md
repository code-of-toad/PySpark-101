# Phase 11 — Experiments & Applied Practice Guide

<a id="toc"></a>
## Table of Contents

- [Purpose](#purpose)
- [Conventions](#conventions)
- [Practice Subjects Under Test](#practice-subjects-under-test)
- [Testing Exercise Protocol](#testing-exercise-protocol)
- [Test Review Record](#test-review-record)
- [Experiment 1 — Reusable Spark Fixture](#experiment-1)
- [Experiment 2 — Unit Test a Filter Transformation](#experiment-2)
- [Experiment 3 — Deterministic DataFrame Comparison](#experiment-3)
- [Experiment 4 — Schema Testing](#experiment-4)
- [Experiment 5 — Row-Count vs. Identity Assertions](#experiment-5)
- [Experiment 6 — Grain and Primary-Key Testing](#experiment-6)
- [Experiment 7 — Composite-Key Testing](#experiment-7)
- [Experiment 8 — Referential-Integrity Testing](#experiment-8)
- [Experiment 9 — Numerical Reconciliation](#experiment-9)
- [Experiment 10 — Accepted/Rejected Quality Testing](#experiment-10)
- [Experiment 11 — Deterministic Window Testing](#experiment-11)
- [Experiment 12 — Edge Cases](#experiment-12)
- [Experiment 13 — Integration Testing](#experiment-13)
- [Experiment 14 — Distributed Spark Testing Cost](#experiment-14)
- [Experiment 15 — Regression Detection](#experiment-15)
- [Applied Phase 11 Project](#applied-project)
- [Applied Task — Protect the Retail Pipeline with Automated Tests](#applied-task)
- [After the Applied Task](#after-applied-task)

---

<a id="purpose"></a>
## Purpose

This file is the execution-oriented companion for **Phase 11 — Testing PySpark**.

The Phase 11 `README.md` is the conceptual reference. `phase_11_lecture.py` is the consolidated teaching implementation. This guide turns those ideas into controlled testing exercises where the repeated habit is:

```text
state the contract
    ↓
choose the smallest deterministic fixture
    ↓
classify the test boundary
    ↓
predict the correct output
    ↓
call production logic
    ↓
assert schema / identity / grain / values
    ↓
reconcile where applicable
    ↓
introduce edge cases
    ↓
prove a regression is detected
```

The governing question is:

> **Can the test suite detect a meaningful pipeline defect before downstream data is silently corrupted?**

For every important exercise, be able to answer:

```text
1. What production responsibility is under test?
2. Is this a unit test or an integration test?
3. What exact contract is being protected?
4. What is the input grain?
5. What is the expected output grain?
6. What is the smallest fixture that proves the behavior?
7. Which rows should survive?
8. Which rows should fail or be rejected?
9. What schema properties matter?
10. Is row order meaningful?
11. Does the assertion trigger a Spark action?
12. Could this test pass while the business result is still wrong?
13. What defect would make the test fail?
14. Should this become a permanent regression test?
```

The applied section is practice only. It does **not** perform the formal Phase 11 mastery gate, update `ROADMAP.md`, mark Phase 11 complete, or enter Phase 12.

---

[Back to Table of Contents](#toc)

---

<a id="conventions"></a>
## Conventions

- Use **PySpark 4.2.0**.
- Use **single quotes** for Python strings.
- Prefer `from pyspark.sql import functions as F`.
- Use `pytest`.
- Reuse the Phase 9 and Phase 10 retail pipeline logic.
- Import production/reusable logic rather than reimplementing it inside tests.
- Use small deterministic Spark fixtures.
- Reuse production schemas where appropriate.
- Use `Decimal` for exact monetary expectations.
- Control dates explicitly.
- Use stable tie-breakers for deterministic window tests.
- Do not assume DataFrame row order.
- Use exact business keys and values in assertions.
- Do not rely on row-count assertions alone.
- Test grain explicitly.
- Test referential integrity separately from parent-key uniqueness.
- Reconcile measures across meaningful transformation boundaries.
- Test valid and invalid data.
- Test both accepted and rejected populations.
- Do not use `dropDuplicates()` in tests to hide grain defects.
- Use `collect()` only on intentionally tiny fixture outputs.
- Remember that `count()`, `collect()`, and `first()` are Spark actions.
- Do not couple ordinary correctness tests to incidental physical-plan choices.
- Preserve important defects as regression tests.
- Do not mark Phase 11 complete until the formal mastery gate is demonstrated.

### Core testing vocabulary

Be able to distinguish:

```text
unit test
integration test
fixture
assertion
schema assertion
data assertion
grain assertion
uniqueness assertion
referential-integrity assertion
reconciliation assertion
edge-case test
regression test
deterministic comparison
```

---

[Back to Table of Contents](#toc)

---

<a id="practice-subjects-under-test"></a>
## Practice Subjects Under Test

Phase 11 should protect logic that already exists.

### Phase 9 transformation subjects

Use functions such as:

```text
filter_orders()
transform_orders()
build_sales_by_province()
add_processing_date()
select_latest_customer_record()
read_orders()
read_customers()
write_sales_by_province()
run_pipeline()
```

Important Phase 9 contracts include:

```text
business functions receive DataFrames and explicit parameters
filters preserve qualifying order grain
joins should not multiply left-side grain unexpectedly
aggregation deliberately changes grain
run-dependent dates are injected
latest-record selection uses deterministic tie-breaking
I/O remains outside core business logic
```

### Phase 10 validation subjects

Use functions such as:

```text
validate_schema()
find_duplicate_keys()
find_exact_duplicate_rows()
add_rejection_reasons()
append_reason()
split_accepted_rejected()
add_orders_row_level_reasons()
add_duplicate_order_reason()
add_orphan_customer_reason()
validate_orders()
add_inventory_reasons()
build_quarantine()
build_validation_summary()
assert_validation_reconciles()
```

Important Phase 10 contracts include:

```text
required fields are enforced
domains and ranges are enforced
duplicate business keys are diagnosed
composite-key uniqueness is enforced
orphan foreign keys are rejected
multiple rejection reasons can coexist
accepted/rejected populations reconcile to input
quarantine preserves diagnostic evidence
```

### Existing deterministic datasets

Reuse the existing Phase 9/10 retail-domain data where useful.

For focused unit tests, prefer smaller scenario-specific fixtures.

For integration tests, the broader Phase 10 dirty dataset is useful because it exercises several quality rules together.

---

[Back to Table of Contents](#toc)

---

<a id="testing-exercise-protocol"></a>
## Testing Exercise Protocol

For each experiment:

### Step 1 — State the production contract

Example:

```text
filter_orders()
keeps rows when:
order_status ∈ included_statuses
AND
net_sales >= minimum_net_sales
```

### Step 2 — Classify the test

Choose:

```text
unit
integration
edge-case
regression
```

### Step 3 — State input and output grain

Example:

```text
input:
one row per order_id

output:
one row per order_id
```

### Step 4 — Design the smallest fixture

Prefer:

```text
3 rows that prove 3 branches
```

over:

```text
100 rows with irrelevant noise
```

### Step 5 — Predict the output before running

Write down:

```text
surviving keys
rejected keys
expected measures
expected schema
```

### Step 6 — Choose the assertion strategy

Possible choices:

```text
exact business-key set
ordered tiny-row comparison
assertDataFrameEqual()
schema comparison
uniqueness check
left-anti orphan check
measure reconciliation
```

### Step 7 — Run production logic

Call the real transformation/validation responsibility.

### Step 8 — Assert the contract

Do not merely assert that execution succeeded.

### Step 9 — Check for blind spots

Ask:

```text
Could this assertion pass while the business result is wrong?
```

If yes, add the missing assertion.

### Step 10 — Explain Spark execution

Identify whether the assertion uses:

```text
count()
collect()
first()
groupBy()
join()
```

and what distributed work that implies.

### Step 11 — Preserve regressions

If the exercise reveals a realistic defect, turn it into a permanent test.

---

[Back to Table of Contents](#toc)

---

<a id="test-review-record"></a>
## Test Review Record

Use this concise review record for important tests:

```text
Test name:
Production responsibility:
Test type:
Input grain:
Output grain:
Fixture rows:
Contract protected:
Expected business keys:
Expected measures:
Schema properties protected:
Comparison strategy:
Spark actions triggered:
Potential blind spot:
Defect this test should catch:
Should this become a regression test?:
```

This keeps testing tied to engineering meaning rather than test syntax.

---

[Back to Table of Contents](#toc)

---

<a id="experiment-1"></a>
## Experiment 1 — Reusable Spark Fixture

### Objective

Create a reusable session-scoped `SparkSession` fixture.

### Required behavior

Use:

```python
@pytest.fixture(scope='session')
def spark():
    ...
```

Configure:

```text
local[2]
small shuffle partition count
WARN logging
session cleanup
```

### Questions

1. Why is session scope appropriate for the Spark runtime?
2. Why should small mutable DataFrames usually remain function-scoped?
3. What shared Spark state could leak between tests?
4. Why should test configuration stay small and predictable?

### Success criteria

You can explain:

```text
SparkSession fixture
→ expensive shared runtime

DataFrame fixture
→ focused test scenario
```

and why they should not necessarily use the same fixture scope.

---

[Back to Table of Contents](#toc)

---

<a id="experiment-2"></a>
## Experiment 2 — Unit Test a Filter Transformation

### Objective

Unit test Phase 9 `filter_orders()`.

### Fixture

Use exactly three rows:

```text
O001 | COMPLETED | 125.00
O002 | CANCELLED |  80.00
O003 | COMPLETED |  -1.00
```

Configuration:

```text
included_statuses = COMPLETED
minimum_net_sales = 0.00
```

### Predict

Expected surviving key:

```text
O001
```

### Required assertions

Do not stop at:

```python
assert actual_df.count() == 1
```

Also assert:

```text
exact surviving order_id set
order_id remains unique
```

### Explain

Why is this a unit test?

What defect would a count-only assertion miss?

---

[Back to Table of Contents](#toc)

---

<a id="experiment-3"></a>
## Experiment 3 — Deterministic DataFrame Comparison

### Objective

Demonstrate why raw `collect()` ordering is unsafe.

### Task A

Create actual and expected DataFrames containing the same logical rows in different input orders.

Compare using:

```python
actual_df.collect() == expected_df.collect()
```

Observe why this comparison is conceptually fragile.

### Task B

Compare after:

```python
orderBy(...)
```

using a stable key.

### Task C

Use:

```python
from pyspark.testing.utils import assertDataFrameEqual
```

with order-insensitive comparison.

### Questions

1. When is explicit test ordering simplest?
2. When is `assertDataFrameEqual()` clearer?
3. Why should production data not be globally sorted merely for test convenience?
4. What if the chosen sort key is not unique?

### Success criteria

You can state:

```text
DataFrame equality
!=
incidental partition output order equality
```

---

[Back to Table of Contents](#toc)

---

<a id="experiment-4"></a>
## Experiment 4 — Schema Testing

### Objective

Test structure separately from values.

### Subject

Use Phase 9 `add_processing_date()`.

### Required assertions

Protect:

```text
processing_date exists
processing_date Spark type = date
processing_date value = injected run_date
```

### Extension

Test a `net_sales` field expected to remain:

```text
decimal(12,2)
```

### Questions

1. Why is checking the date value insufficient?
2. Why is checking only the schema insufficient?
3. When is nullability worth asserting?
4. When would a strict whole-schema assertion be too brittle?

---

[Back to Table of Contents](#toc)

---

<a id="experiment-5"></a>
## Experiment 5 — Row-Count vs. Identity Assertions

### Objective

Show why row counts are supporting evidence, not complete correctness.

### Setup

Construct two logically different DataFrames with:

```text
count = 2
```

but different business keys.

### Task

Write:

```python
assert actual_df.count() == 2
```

Then add:

```text
exact key-set assertion
```

### Explain

Why can both of these statements be true?

```text
row count is correct
business result is wrong
```

### Success criteria

For any row-count assertion, you can explain what additional identity/value contract supports it.

---

[Back to Table of Contents](#toc)

---

<a id="experiment-6"></a>
## Experiment 6 — Grain and Primary-Key Testing

### Objective

Test declared grain explicitly.

### Subject

Use an order-grain DataFrame:

```text
one row per order_id
```

### Task

Write a reusable helper that detects:

```text
groupBy(order_id)
count > 1
```

### Cases

Test:

```text
all unique keys
one duplicate pair
duplicate identical rows
duplicate conflicting rows
```

### Required understanding

Exact duplicates and duplicate business keys are different concepts.

### Questions

1. Why is `dropDuplicates()` not a grain assertion?
2. Why should a join test protect parent-side uniqueness?
3. What downstream defect can duplicate parent keys create?

---

[Back to Table of Contents](#toc)

---

<a id="experiment-7"></a>
## Experiment 7 — Composite-Key Testing

### Objective

Test inventory grain:

```text
snapshot_date + store_id + product_id
```

### Fixture

Include:

```text
2026-09-05 | S001 | P001 | 10
2026-09-05 | S001 | P001 | 12
2026-09-05 | S001 | P002 |  7
```

### Required assertions

Prove:

```text
P001 composite key is duplicated
both conflicting rows receive DUPLICATE_INVENTORY_KEY
P002 remains valid
```

### Explain

Why is this wrong?

```text
store_id must be unique
product_id must be unique
snapshot_date must be unique
```

### Success criteria

You can distinguish:

```text
individual column repetition
```

from:

```text
composite business-key violation
```

---

[Back to Table of Contents](#toc)

---

<a id="experiment-8"></a>
## Experiment 8 — Referential-Integrity Testing

### Objective

Test child-parent relationships directly.

### Relationship

```text
orders.customer_id
    →
customers.customer_id
```

### Fixture

Use:

```text
O001 | C001
O002 | C999

customers:
C001
```

### Required assertions

Prove:

```text
O001 accepted
O002 rejected
O002 contains ORPHAN_CUSTOMER_ID
accepted orders contain no orphan customer_id
```

Use a left-anti join for the direct accepted-data assertion.

### Extension

Duplicate `C001` in the parent dataset.

Observe that:

```text
parent exists
```

does not imply:

```text
parent grain is valid
```

### Explain

Why should referential integrity and parent uniqueness be separate tests?

---

[Back to Table of Contents](#toc)

---

<a id="experiment-9"></a>
## Experiment 9 — Numerical Reconciliation

### Objective

Protect measures from silent corruption.

### Task A — Aggregate reconciliation

Use:

```text
ON | 125.00
ON |  75.00
BC | 200.00
```

Run `build_sales_by_province()`.

Assert:

```text
sum(input net_sales)
=
sum(output net_sales)
```

### Task B — Per-row formula

For a sales fact containing:

```text
gross_sales
gross_cost
gross_margin
```

assert:

```text
gross_margin = gross_sales - gross_cost
```

for every accepted row.

### Questions

1. What defects can aggregate reconciliation catch?
2. Why can correct row counts coexist with incorrect totals?
3. When is exact Decimal equality preferable?
4. When would an approximate tolerance be legitimate?

---

[Back to Table of Contents](#toc)

---

<a id="experiment-10"></a>
## Experiment 10 — Accepted/Rejected Quality Testing

### Objective

Test the Phase 10 quality layer as a behavioral contract.

### Fixture

Create rows representing:

```text
valid row
missing customer_id
negative net_sales
orphan customer_id
duplicate order_id
```

### Required assertions

Protect:

```text
known-valid rows reach accepted output
known-invalid rows do not
known-invalid rows remain in rejected output
expected rejection reasons exist
input_count = accepted_count + rejected_count
```

### Multiple-reason case

Create one row with:

```text
missing customer_id
negative net_sales
```

Assert the reason set equals:

```text
MISSING_CUSTOMER_ID
NEGATIVE_NET_SALES
```

### Explain

Why should the assertion use a set rather than depending on array order?

---

[Back to Table of Contents](#toc)

---

<a id="experiment-11"></a>
## Experiment 11 — Deterministic Window Testing

### Objective

Prove Phase 9 latest-record selection is deterministic.

### Fixture

Use:

```text
R001 | C001 | 2026-05-01 | ON
R002 | C001 | 2026-05-01 | QC
```

### Contract

Ordering is:

```text
effective_date DESC
customer_record_id DESC
```

### Expected result

```text
R002
```

### Required assertion

Test exact winning record ID.

### Explain

Why does a fixture with different effective dates fail to prove the tie-breaker?

### Extension

Temporarily remove the secondary sort key and reason about why the test should protect against that regression.

---

[Back to Table of Contents](#toc)

---

<a id="experiment-12"></a>
## Experiment 12 — Edge Cases

### Objective

Test boundaries that ordinary fixtures may miss.

### Required scenarios

#### Empty input

Construct with explicit schema:

```python
spark.createDataFrame([], schema=ORDERS_SCHEMA)
```

Assert the quality layer returns:

```text
0 validated
0 accepted
0 rejected
```

#### Numeric boundary

For:

```text
minimum_net_sales = 0.00
```

test:

```text
-0.01 → rejected by filter
 0.00 → accepted
 0.01 → accepted
```

#### All accepted

Every input row should reach accepted output.

#### All rejected

Every input row should reach rejected output.

#### No filter matches

Output should be empty without schema corruption.

### Explain

Why do explicit boundary values matter more than another arbitrary happy-path value?

---

[Back to Table of Contents](#toc)

---

<a id="experiment-13"></a>
## Experiment 13 — Integration Testing

### Objective

Test multiple meaningful responsibilities together.

### Suggested boundary

```text
orders_df + customers_df
        ↓
validate_orders()
        ↓
split accepted / rejected
        ↓
transform_accepted_orders()
        ↓
build_sales_by_province()
        ↓
final assertions
```

### Fixture

Use:

```text
valid ON order
valid BC order
orphan order
```

### Required assertions

Protect:

```text
orphan is rejected
accepted rows enrich correctly
province aggregation is correct
one row per province
input classification reconciles
accepted net_sales reconciles to output net_sales
```

### Explain

Why is this integration rather than unit testing?

Why does this still not require large data?

---

[Back to Table of Contents](#toc)

---

<a id="experiment-14"></a>
## Experiment 14 — Distributed Spark Testing Cost

### Objective

Observe that test assertions can trigger Spark jobs.

### Task

Take one small DataFrame and run:

```text
count()
first()
collect()
```

Observe that these are actions.

### Compare

One test using:

```text
three independent count() calls
```

vs. one test that collects a five-row deterministic fixture once and performs several Python assertions on those rows.

### Questions

1. Which approach is clearer?
2. Which approach launches fewer Spark actions?
3. When is repeated action cost acceptable for clarity?
4. Why should test optimization not lead to unsafe production `collect()` patterns?
5. Why should you avoid caching every fixture automatically?

### Success criteria

You understand:

```text
test assertion
can still be distributed Spark work
```

---

[Back to Table of Contents](#toc)

---

<a id="experiment-15"></a>
## Experiment 15 — Regression Detection

### Objective

Demonstrate the formal Phase 11 idea before the final mastery gate.

### Baseline

Use correct:

```text
build_sales_by_province()
→ sum(net_sales)
```

Confirm tests pass.

### Deliberate mutation

Temporarily replace:

```text
sum(net_sales)
```

with:

```text
avg(net_sales)
```

### Expected failure

The reconciliation test should fail.

### Restore

Return the implementation to:

```text
sum(net_sales)
```

Confirm tests pass again.

### Required explanation

State:

```text
which test failed
which contract it protected
why row count might still have passed
what downstream corruption the failure prevented
```

### Important

This experiment practices the mastery concept.

The phase is not complete until the formal mastery review explicitly confirms that you can do this independently and explain the result.

---

[Back to Table of Contents](#toc)

---

<a id="applied-project"></a>
# Applied Phase 11 Project

The applied task combines the phase into one maintainable automated test suite.

Do not create a separate toy pipeline.

Protect the existing Phase 9/10 retail pipeline behavior.

---

[Back to Table of Contents](#toc)

---

<a id="applied-task"></a>
## Applied Task — Protect the Retail Pipeline with Automated Tests

### Goal

Build a real `pytest` suite capable of detecting silent pipeline corruption.

A sensible target structure is:

```text
tests/
├── conftest.py
├── test_transformations.py
├── test_validation.py
└── test_pipeline_integration.py
```

Use the smallest structure that remains clear and maintainable.

### Part 1 — Shared Spark fixture

Create:

```text
session-scoped SparkSession
local[2]
small shuffle partition count
WARN logging
clean shutdown
```

### Part 2 — Transformation unit tests

Protect at minimum:

```text
filter_orders()
build_sales_by_province()
add_processing_date()
select_latest_customer_record()
```

Assertions should include as appropriate:

```text
exact surviving keys
schema
output grain
exact measures
deterministic tie behavior
```

### Part 3 — Validation unit tests

Protect at minimum:

```text
required-field rejection
domain/range rejection
duplicate order_id
composite inventory key
orphan customer_id
multiple rejection reasons
accepted/rejected classification
```

### Part 4 — Schema tests

Protect important contracts such as:

```text
net_sales DecimalType
processing_date DateType
expected output column set
```

### Part 5 — Grain tests

Protect:

```text
orders
→ one row per order_id

customers
→ one row per customer_id

inventory
→ one row per snapshot_date + store_id + product_id

sales_by_province
→ one row per province
```

### Part 6 — Referential integrity

Prove:

```text
accepted orders.customer_id
→ customers.customer_id
```

contains no orphans.

### Part 7 — Numerical reconciliation

Protect at minimum:

```text
input = accepted + rejected

accepted net_sales
=
sum(final province net_sales)
```

If using a sales fact with margin fields, also protect:

```text
gross_margin
=
gross_sales - gross_cost
```

### Part 8 — Edge cases

Include:

```text
empty input
zero-value boundary
orphan
duplicate primary key
duplicate composite key
tied latest-record dates
all accepted or all rejected
```

### Part 9 — Integration test

Use a small fixture to run:

```text
validation
→ accepted/rejected split
→ enrichment
→ aggregation
→ reconciliation
```

Protect final schema, grain, values, and totals.

### Part 10 — Regression demonstration

Start from a passing suite.

Deliberately break one meaningful pipeline rule.

Recommended mutation:

```text
sum(net_sales)
→
avg(net_sales)
```

Then prove:

```text
test fails
contract violation is identifiable
implementation is restored
suite passes again
```

### Applied review questions

Be prepared to answer:

```text
Why is each unit test a unit test?
Why is the integration test an integration test?
Why is each fixture the minimum useful dataset?
Which tests protect schema?
Which protect grain?
Which protect relationships?
Which protect measures?
Which protect deterministic behavior?
Which assertions trigger Spark jobs?
What defect does each important test prevent?
Which test catches the deliberate regression?
Could the suite still have blind spots?
```

### Success standard

The applied work is successful when:

```text
tests are deterministic
tests call real production/reusable logic
tests are focused
tests protect business/data contracts
tests cover valid and invalid paths
tests protect grain and relationships
tests reconcile important measures
tests include meaningful integration
tests catch a deliberate regression
```

---

[Back to Table of Contents](#toc)

---

<a id="after-applied-task"></a>
## After the Applied Task

Do **not** mark Phase 11 complete automatically.

After the applied test suite exists, the remaining step is the formal mastery review.

The mastery question is not:

```text
Did I write pytest files?
```

It is:

```text
Can I independently explain and demonstrate
that a meaningful bad pipeline change
causes automated tests to fail
before downstream data is silently corrupted?
```

Only after that requirement is demonstrated should Phase 11 be finalized.

[Back to Table of Contents](#toc)
