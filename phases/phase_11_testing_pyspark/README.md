# Phase 11 — Testing PySpark

<a id="toc"></a>
## Table of Contents

- [Objective](#objective)
- [Phase Mental Model](#phase-mental-model)
- [1. Testing as an Executable Data Contract](#1-testing-as-an-executable-data-contract)
- [2. `pytest` Structure and Test Organization](#2-pytest-structure-and-test-organization)
- [3. Reusable Spark Fixtures](#3-reusable-spark-fixtures)
- [4. Deterministic Fixture Design](#4-deterministic-fixture-design)
- [5. Unit Testing Transformation Functions](#5-unit-testing-transformation-functions)
- [6. Deterministic DataFrame Comparison](#6-deterministic-dataframe-comparison)
- [7. Schema and Data Assertions](#7-schema-and-data-assertions)
- [8. Row-Count, Grain, and Uniqueness Tests](#8-row-count-grain-and-uniqueness-tests)
- [9. Referential-Integrity Tests](#9-referential-integrity-tests)
- [10. Numerical Reconciliation](#10-numerical-reconciliation)
- [11. Testing Accepted and Rejected Data](#11-testing-accepted-and-rejected-data)
- [12. Unit Tests vs. Integration Tests](#12-unit-tests-vs-integration-tests)
- [13. Edge-Case Testing](#13-edge-case-testing)
- [14. Distributed Spark Testing Implications](#14-distributed-spark-testing-implications)
- [15. Common Bad Testing Patterns](#15-common-bad-testing-patterns)
- [16. How Phase 11 Builds on Phases 9 and 10](#16-how-phase-11-builds-on-phases-9-and-10)
- [17. Phase 11 Design Review Checklist](#17-phase-11-design-review-checklist)
- [18. Phase 11 Mastery Expectations](#18-phase-11-mastery-expectations)

---

<a id="objective"></a>
## Objective

Treat PySpark transformations and validation logic as **software whose data contracts are protected automatically**.

The Phase 11 problem is no longer:

```text
Did the code run?
```

It is:

```text
If a pipeline change breaks schema, grain, keys, relationships,
measures, validation behavior, or deterministic output,
will an automated test fail before bad data reaches downstream users?
```

Required Phase 11 concerns:

- `pytest`;
- reusable Spark fixtures;
- unit testing transformation functions;
- integration testing;
- deterministic DataFrame comparisons;
- schema testing;
- row-count testing;
- grain testing;
- referential-integrity testing;
- numerical reconciliation;
- edge-case testing.

Examples target **PySpark 4.2.0**, reuse the existing retail domain, and build directly on the Phase 9 transformation architecture and Phase 10 data-quality layer.

The governing engineering principle is:

> **A useful test protects a business or data contract, not merely the existence of a DataFrame.**

This document is lecture/reference material only. It does not mark Phase 11 complete or update `ROADMAP.md`.

---

[Back to Table of Contents](#toc)

---

<a id="phase-mental-model"></a>
## Phase Mental Model

A professional test suite should protect several layers of correctness:

```text
STRUCTURE
schema
column names
data types
nullable expectations where contractually important

ROW SEMANTICS
filters
calculations
business rules
rejection reasons

DATASET SEMANTICS
grain
primary-key uniqueness
composite-key uniqueness
row population

RELATIONSHIPS
referential integrity
parent-key uniqueness
join preservation

RECONCILIATION
input vs. output totals
accepted + rejected = input
measure conservation

DETERMINISM
same logical input
    -> same logical output
```

The core testing distinction is:

```text
unit test
→ one transformation or validation responsibility

integration test
→ multiple meaningful pipeline responsibilities working together
```

Examples:

```text
UNIT
filter_orders()
→ keeps only the configured statuses and minimum sales amount

UNIT
add_duplicate_order_reason()
→ marks every row sharing an unexplained duplicate order_id

INTEGRATION
validate_orders()
→ split accepted/rejected
→ transform accepted orders
→ aggregate sales by province
→ reconcile output totals
```

A test should fail for the **smallest meaningful violated contract**. This makes failures easier to diagnose and prevents one enormous end-to-end test from carrying the entire correctness burden.

---

[Back to Table of Contents](#toc)

---

<a id="1-testing-as-an-executable-data-contract"></a>
## 1. Testing as an Executable Data Contract

A data contract becomes stronger when its expectations are executable.

Suppose a transformation promises:

```text
input grain
one row per order_id

filter rule
order_status must be included
net_sales must meet the configured minimum

output grain after aggregation
one row per province

measure
province net_sales = sum of qualifying order net_sales
```

A weak test checks:

```python
assert result_df is not None
```

A slightly stronger but still incomplete test checks:

```python
assert result_df.count() == 2
```

A useful suite asks whether the business meaning is correct:

```text
Are the correct orders included?
Are excluded orders absent?
Is the schema correct?
Is province unique?
Does each province total equal the expected sum?
Does the total output net_sales reconcile to the qualifying input?
```

### Test the invariant, not the implementation detail

Prefer:

```text
output contains exactly one row per province
```

over:

```text
the function must contain this exact groupBy expression
```

Prefer:

```text
orphan customer rows are rejected with ORPHAN_CUSTOMER_ID
```

over:

```text
the implementation must use one specific join syntax
```

The first style protects behavior while allowing safe refactoring.

### Tests are regression protection

A regression test answers:

> **If someone changes this code later, what incorrect behavior must never return?**

For example, after fixing duplicate-parent row multiplication, preserve a test that creates a duplicated parent key and proves the pipeline rejects or fails that condition according to the contract.

---

[Back to Table of Contents](#toc)

---

<a id="2-pytest-structure-and-test-organization"></a>
## 2. `pytest` Structure and Test Organization

`pytest` discovers tests by convention.

A practical Phase 11 structure may become:

```text
phases/
└── phase_11_testing_pyspark/
    └── README.md

tests/
├── conftest.py
├── test_transformations.py
├── test_validation.py
└── test_pipeline_integration.py
```

Only create files when they support real tests. The exact mature layout can evolve with the implementation.

### Basic test shape

```python
def test_filter_orders_keeps_only_qualifying_rows(spark):
    # Arrange: create the smallest input that distinguishes valid behavior.
    orders_df = ...

    # Act: execute one production transformation responsibility.
    actual_df = filter_orders(
        orders_df,
        included_statuses=('COMPLETED',),
        minimum_net_sales=Decimal('0.00'),
    )

    # Assert: verify the business result, not merely DataFrame existence.
    ...
```

The common mental model is:

```text
ARRANGE
construct deterministic input and expected contract

ACT
call production code

ASSERT
prove the observable result is correct
```

### Useful `pytest` conventions

```text
test_*.py
→ test modules

test_*()
→ test functions

conftest.py
→ shared fixtures available to nearby tests

@pytest.fixture
→ reusable setup with explicit lifecycle

pytest -q
→ concise test run

pytest tests/test_validation.py -q
→ focused module run

pytest tests/test_validation.py::test_name -q
→ focused test run
```

### Tests should import production logic

Prefer:

```python
from retail_pipeline.transformations import filter_orders
```

rather than copying the implementation into the test.

A copied implementation can reproduce the same bug and create a false sense of safety.

---

[Back to Table of Contents](#toc)

---

<a id="3-reusable-spark-fixtures"></a>
## 3. Reusable Spark Fixtures

Creating a new `SparkSession` for every tiny test is unnecessary overhead.

A reusable session-scoped fixture gives tests one controlled local Spark environment:

```python
import pytest

from pyspark.sql import SparkSession


@pytest.fixture(scope='session')
def spark():
    # Reuse one local SparkSession across the test session for efficiency.
    spark_session = (
        SparkSession.builder
        .master('local[2]')
        .appName('phase_11_tests')
        # Keep tiny test shuffles predictable and inexpensive.
        .config('spark.sql.shuffle.partitions', '2')
        .getOrCreate()
    )

    spark_session.sparkContext.setLogLevel('WARN')

    yield spark_session

    # Release the shared Spark runtime after the suite finishes.
    spark_session.stop()
```

### Fixture scopes

The important idea is lifecycle:

```text
session fixture
→ one shared expensive resource for the whole test run

module fixture
→ one resource per test module

function fixture
→ fresh setup for each test
```

For a local `SparkSession`, session scope is often practical. For mutable test data, function-scoped fixtures are safer.

### Data fixtures should stay small

Example:

```python
@pytest.fixture
def customers_df(spark):
    # Keep the parent relation tiny and deterministic.
    rows = [
        ('C001', 'Alice Wong', 'ON', 'CONSUMER'),
        ('C002', 'Ben Tremblay', 'QC', 'CONSUMER'),
    ]

    return spark.createDataFrame(rows, schema=CUSTOMERS_SCHEMA)
```

### Prefer production schemas

When a production schema is part of the contract, tests should normally import it rather than retype a nearly identical version.

```text
production schema
        ↓
fixture construction
        ↓
test
```

This prevents test fixtures from silently drifting away from the schema actually used by the pipeline.

### Keep fixtures focused

Avoid one giant fixture containing every known retail table and defect.

Prefer fixtures that communicate intent:

```text
valid_orders_df
duplicate_orders_df
orphan_orders_df
valid_customers_df
duplicate_customers_df
inventory_with_duplicate_composite_key_df
```

A test should be able to reveal its scenario from the fixture names alone.

---

[Back to Table of Contents](#toc)

---

<a id="4-deterministic-fixture-design"></a>
## 4. Deterministic Fixture Design

A test fixture should contain the **minimum rows necessary to prove one behavior**.

Example filter fixture:

```text
O001 | COMPLETED | 125.00   → keep
O002 | CANCELLED |  80.00   → exclude by status
O003 | COMPLETED |  -1.00   → exclude by minimum amount
```

Three rows prove three branches more clearly than 100 generated rows.

### Deterministic fixtures should avoid hidden runtime state

Prefer explicit values:

```python
run_date = date(2026, 9, 7)
```

rather than production logic that secretly depends on:

```python
F.current_date()
```

Prefer stable window tie-breakers:

```python
.orderBy(
    F.col('effective_date').desc(),
    F.col('customer_record_id').desc(),
)
```

rather than assuming Spark will consistently choose one tied record.

### Use exact numeric types for exact business values

For currency fixtures:

```python
Decimal('125.00')
```

is preferable to:

```python
125.0
```

when the production schema uses `DecimalType` and exact monetary reconciliation matters.

### Separate scenarios instead of creating one all-purpose dirty dataset

The Phase 10 lecture intentionally contains many defects for teaching the complete validation layer. Unit tests should often isolate those defects.

Example:

```text
test_missing_order_id
→ one valid row + one missing-key row

test_duplicate_order_id
→ one valid key + one duplicated key pair

test_orphan_customer_id
→ one matched order + one orphan order
```

The full dirty Phase 10 dataset remains valuable for integration tests because it exercises multiple rules together.

---

[Back to Table of Contents](#toc)

---

<a id="5-unit-testing-transformation-functions"></a>
## 5. Unit Testing Transformation Functions

Phase 9 deliberately shaped business logic as DataFrame-in/DataFrame-out functions. That is the ideal unit-testing boundary.

### Example: filtering

Contract:

```text
filter_orders()
→ keeps rows whose status is configured
→ keeps rows meeting minimum_net_sales
→ preserves order grain
```

Test the exact surviving business keys:

```python
def test_filter_orders_keeps_only_qualifying_orders(spark):
    # Arrange: each row exercises a different filter branch.
    orders_df = spark.createDataFrame(
        [
            ('O001', 'C001', 'COMPLETED', Decimal('125.00')),
            ('O002', 'C002', 'CANCELLED', Decimal('80.00')),
            ('O003', 'C003', 'COMPLETED', Decimal('-1.00')),
        ],
        ['order_id', 'customer_id', 'order_status', 'net_sales'],
    )

    # Act: call the production function directly.
    actual_df = filter_orders(
        orders_df,
        included_statuses=('COMPLETED',),
        minimum_net_sales=Decimal('0.00'),
    )

    # Assert: test the exact business identity of surviving rows.
    actual_order_ids = {
        row['order_id']
        for row in actual_df.select('order_id').collect()
    }

    assert actual_order_ids == {'O001'}
```

### Example: deterministic latest-record selection

Test the tie case deliberately.

```text
C001 | R001 | 2026-05-01
C001 | R002 | 2026-05-01
```

If the production contract says the greater `customer_record_id` wins ties, the expected result must be `R002`.

A test that contains no tie does **not** prove deterministic tie handling.

### Example: aggregation

For `build_sales_by_province()` test:

```text
correct province rows
correct order_count
correct net_sales
one row per province
```

Do not stop after checking the output row count.

### One test, one reason to fail

A unit test can contain several assertions when they all describe one contract. Avoid mixing unrelated responsibilities into one test such that a failure gives no clue what broke.

---

[Back to Table of Contents](#toc)

---

<a id="6-deterministic-dataframe-comparison"></a>
## 6. Deterministic DataFrame Comparison

Spark DataFrames do not guarantee row presentation order unless an ordering operation is explicitly requested.

Therefore this is fragile:

```python
assert actual_df.collect() == expected_df.collect()
```

It can fail because rows arrive in a different order even when the datasets are logically equivalent.

### Strategy 1 — Compare after deterministic ordering

Good for tiny fixtures with a known stable sort key:

```python
actual_rows = actual_df.orderBy('order_id').collect()
expected_rows = expected_df.orderBy('order_id').collect()

assert actual_rows == expected_rows
```

The ordering columns themselves must provide deterministic ordering. If `order_id` can repeat, include a stable secondary key when needed.

### Strategy 2 — Compare order-insensitively

For logically unordered datasets, compare the full expected row population without assuming partition order.

PySpark provides testing helpers such as:

```python
from pyspark.testing.utils import assertDataFrameEqual

# Compare logical DataFrame contents without requiring row order.
assertDataFrameEqual(
    actual_df,
    expected_df,
    checkRowOrder=False,
)
```

Use helper options deliberately. Do not ignore schema, column, or numeric differences merely to make a test pass.

### Strategy 3 — Compare a small selected projection

When a test protects only one responsibility, compare only the columns relevant to that contract.

Example:

```python
actual_df.select('order_id', 'rejection_reasons')
```

This avoids coupling a focused unit test to unrelated output metadata.

### Strategy 4 — Compare business keys and targeted values

For some tests, a full DataFrame equality assertion is unnecessary.

Example:

```text
assert surviving order IDs exactly equal {O001, O004}
assert O003 contains INVALID_ORDER_STATUS
```

This can produce more diagnostic failures than one opaque whole-DataFrame mismatch.

### Never solve comparison instability by sorting production data unnecessarily

`orderBy()` is a wide transformation and can be expensive on large datasets.

Use deterministic ordering in **tiny test assertions** when needed. Do not add a global production sort simply to make tests convenient.

---

[Back to Table of Contents](#toc)

---

<a id="7-schema-and-data-assertions"></a>
## 7. Schema and Data Assertions

Schema correctness and row-value correctness are separate contracts.

A DataFrame can contain the right rows under the wrong types, or the right schema with incorrect values.

### Exact schema testing

When the output contract is strict:

```python
assert actual_df.schema == EXPECTED_SCHEMA
```

PySpark also provides a schema testing helper:

```python
from pyspark.testing.utils import assertSchemaEqual

# Compare the actual structural contract with the expected schema.
assertSchemaEqual(actual_df.schema, EXPECTED_SCHEMA)
```

### Targeted schema testing

Sometimes only specific fields are contractual:

```python
actual_types = {
    field.name: field.dataType.simpleString()
    for field in actual_df.schema.fields
}

assert actual_types['net_sales'] == 'decimal(12,2)'
assert actual_types['processing_date'] == 'date'
```

### Be deliberate about nullability

Ask whether `nullable` is part of the contract being protected.

Do not make tests brittle over nullability metadata when it is not semantically important. Conversely, do not ignore it when downstream systems depend on a strict schema contract.

### Schema tests do not replace data tests

This can pass:

```text
net_sales decimal(12,2)
```

while the data still contains:

```text
-20.00
```

Likewise:

```text
customer_id string
```

does not prove that the identifier exists, is non-blank, is unique where required, or references a valid parent.

---

[Back to Table of Contents](#toc)

---

<a id="8-row-count-grain-and-uniqueness-tests"></a>
## 8. Row-Count, Grain, and Uniqueness Tests

These are related but different assertions.

### Row-count test

A row-count assertion protects an expected population size:

```python
assert accepted_df.count() == 2
assert rejected_df.count() == 9
```

Useful, but incomplete.

The wrong two rows can still produce a count of two.

### Grain test

If the contract says:

```text
sales_by_province_df
= one row per province
```

then test uniqueness at that grain:

```python
def assert_unique_key(df, key_columns):
    # A repeated key means the declared output grain has been violated.
    duplicate_exists = (
        df
        .groupBy(*key_columns)
        .count()
        .filter(F.col('count') > 1)
        .limit(1)
        .count()
        > 0
    )

    assert not duplicate_exists
```

Use:

```python
assert_unique_key(result_df, ['province'])
```

### Primary-key uniqueness

For order grain:

```python
assert_unique_key(accepted_orders_df, ['order_id'])
```

### Composite-key uniqueness

For inventory grain:

```python
assert_unique_key(
    accepted_inventory_df,
    ['snapshot_date', 'store_id', 'product_id'],
)
```

Do not incorrectly test each component for individual uniqueness.

### Grain tests protect join correctness

A left join does not automatically preserve left-side grain.

If the right-side parent contains duplicate keys:

```text
one order
×
two matching customer rows
=
two output rows
```

Therefore tests should protect both:

```text
parent key uniqueness
AND
expected output grain
```

---

[Back to Table of Contents](#toc)

---

<a id="9-referential-integrity-tests"></a>
## 9. Referential-Integrity Tests

A referential-integrity test proves that every required child key has a valid parent.

For:

```text
orders.customer_id
    →
customers.customer_id
```

a direct test is:

```python
orphan_rows_df = (
    accepted_orders_df
    .select('customer_id')
    .join(
        customers_df.select('customer_id'),
        on='customer_id',
        how='left_anti',
    )
)

# Accepted child rows must not contain unmatched customer keys.
assert orphan_rows_df.count() == 0
```

### Test the rejection behavior too

For the Phase 10 quality layer, create an orphan deliberately:

```text
O005 | C999
```

Then assert:

```text
O005 is rejected
O005 contains ORPHAN_CUSTOMER_ID
O005 is not present in accepted_orders_df
```

### Parent uniqueness is a separate test

Referential integrity answers:

```text
Does a parent exist?
```

Parent-grain testing answers:

```text
Is there exactly one parent where the model expects one?
```

Both are required when downstream joins assume a one-to-many relationship.

---

[Back to Table of Contents](#toc)

---

<a id="10-numerical-reconciliation"></a>
## 10. Numerical Reconciliation

A pipeline can preserve row counts and still corrupt measures.

Numerical reconciliation tests protect quantitative invariants.

### Per-row reconciliation

Example business rule:

```text
gross_margin
=
gross_sales - gross_cost
```

Test the invariant directly:

```python
invalid_margin_df = fact_sales_df.filter(
    F.col('gross_margin')
    != (F.col('gross_sales') - F.col('gross_cost'))
)

# Every accepted sales row must satisfy the derived-measure contract.
assert invalid_margin_df.count() == 0
```

### Aggregate reconciliation

For the existing Phase 9 province aggregation:

```python
input_total = (
    enriched_orders_df
    .agg(F.sum('net_sales').alias('net_sales'))
    .first()['net_sales']
)

output_total = (
    sales_by_province_df
    .agg(F.sum('net_sales').alias('net_sales'))
    .first()['net_sales']
)

# Aggregating by province must conserve the qualifying sales amount.
assert output_total == input_total
```

This catches defects such as:

```text
join multiplication
missing groups
incorrect filter placement
wrong aggregation function
unexpected row loss
```

### Classification reconciliation

For the Phase 10 quality layer:

```text
input_count
=
accepted_count + rejected_count
```

That is a numerical reconciliation too.

### Exact vs. approximate numeric assertions

For exact `DecimalType` currency logic, exact equality is often appropriate.

For floating-point scientific values, an explicit tolerance may be appropriate.

Do not introduce broad tolerances merely to hide unexpected numeric drift.

---

[Back to Table of Contents](#toc)

---

<a id="11-testing-accepted-and-rejected-data"></a>
## 11. Testing Accepted and Rejected Data

Phase 10 created a particularly valuable testing surface:

```text
raw
→ validated with rejection_reasons
→ accepted
→ rejected
```

Tests should protect both sides.

### Accepted behavior

Assert that known-valid records:

```text
remain present
retain the intended grain
lose no business columns accidentally
contain no rejection reasons after the accepted projection
can safely enter downstream transformations
```

### Rejected behavior

Assert that known-invalid records:

```text
are absent from accepted data
remain present in rejected data
retain the original source evidence
contain the correct machine-readable reason(s)
```

### Multiple reasons

A row can legitimately fail more than one rule.

For example, a deliberately constructed row may need:

```text
MISSING_CUSTOMER_ID
NEGATIVE_NET_SALES
```

Test the set of reasons without depending on accidental array order unless reason order itself is contractual.

```python
actual_reasons = set(
    rejected_row['rejection_reasons']
)

assert actual_reasons == {
    'MISSING_CUSTOMER_ID',
    'NEGATIVE_NET_SALES',
}
```

### Duplicate-key behavior

For an unexplained duplicate key pair, test that **all conflicting rows** receive the duplicate reason when that is the declared policy.

Do not merely assert that one duplicate remains after deduplication.

### Quarantine metadata

When `build_quarantine()` receives an explicit run date, test that every rejected row receives the configured metadata deterministically.

```text
source_dataset = orders
validation_run_date = configured date
```

---

[Back to Table of Contents](#toc)

---

<a id="12-unit-tests-vs-integration-tests"></a>
## 12. Unit Tests vs. Integration Tests

The distinction is about **responsibility boundaries**, not merely test size.

### Unit test

A unit test isolates one meaningful production responsibility.

Examples:

```text
filter_orders()
→ status/minimum-sales filtering

select_latest_customer_record()
→ deterministic latest-record selection

add_orders_row_level_reasons()
→ row-level validation reasons

find_duplicate_keys()
→ dataset-level duplicate-key detection

build_sales_by_province()
→ province-grain aggregation
```

A unit test normally:

```text
uses tiny in-memory DataFrames
avoids file I/O
avoids unrelated pipeline components
asserts one focused contract
```

### Integration test

An integration test proves multiple real responsibilities compose correctly.

Useful Phase 11 integration boundary:

```text
orders_df + customers_df
        ↓
validate_orders()
        ↓
split accepted / rejected
        ↓
transform accepted orders
        ↓
filter / aggregate
        ↓
reconcile final output
```

Another integration test can exercise real local I/O:

```text
write deterministic input to tmp_path
        ↓
read with production reader
        ↓
run pipeline responsibilities
        ↓
write output
        ↓
read output back
        ↓
assert schema + grain + values
```

### Integration test does not mean production-scale test

The purpose is component interaction, not volume.

A five-row integration fixture can prove:

```text
reader schema
validation behavior
transformation compatibility
writer/read-back schema
final numerical reconciliation
```

Scale and performance testing are different concerns.

### Do not replace unit tests with one end-to-end test

If the only test is:

```text
run_pipeline()
→ final output matches expected
```

then a failure may originate from reading, validation, filtering, joining, aggregation, configuration, or writing.

Use focused unit tests for diagnosis and a smaller number of integration tests for composition.

---

[Back to Table of Contents](#toc)

---

<a id="13-edge-case-testing"></a>
## 13. Edge-Case Testing

Happy-path tests prove ordinary behavior. Edge-case tests protect the boundaries where data pipelines commonly fail.

Important Phase 11 scenarios include:

```text
EMPTY INPUT
no source rows
no qualifying rows after a filter
empty rejected population

REQUIRED VALUES
NULL business key
blank string business key
NULL measure

DOMAINS / RANGES
unknown status
negative currency amount
zero amount at an allowed boundary

KEYS
single valid key
duplicate primary key with identical rows
duplicate primary key with conflicting rows
duplicate composite key

RELATIONSHIPS
valid foreign key
orphan foreign key
duplicate parent key

DETERMINISM
tied effective dates with stable tie-breaker
input rows presented in different orders

QUALITY OUTPUTS
one rejection reason
multiple rejection reasons
all rows accepted
all rows rejected

NUMERICS
zero totals
multiple rows that must aggregate exactly
Decimal values with scale
```

### Boundary values deserve explicit tests

If the rule is:

```text
net_sales >= 0.00
```

then test:

```text
-0.01 → invalid
 0.00 → valid
 0.01 → valid
```

Testing only `125.00` does not prove the boundary.

### Empty DataFrames need explicit schemas

Spark cannot infer a useful schema from an empty Python list.

Use:

```python
empty_orders_df = spark.createDataFrame(
    [],
    schema=ORDERS_SCHEMA,
)
```

This is also a reason production schemas make tests easier.

---

[Back to Table of Contents](#toc)

---

<a id="14-distributed-spark-testing-implications"></a>
## 14. Distributed Spark Testing Implications

PySpark tests are software tests, but Spark's distributed execution model still matters.

### Assertions often trigger Spark jobs

Operations such as:

```text
count()
collect()
first()
```

are actions.

A test that performs five independent actions may run the lineage five times unless Spark can reuse materialized data.

On tiny fixtures this may be acceptable for clarity. Still understand the execution cost.

### Keep test data small instead of optimizing the test suite prematurely

The best first optimization is usually:

```text
5 deterministic rows
```

instead of:

```text
500,000 generated rows
```

Do not cache every fixture by habit.

### `collect()` is acceptable only because fixtures are intentionally tiny

In production code:

```text
collect huge dataset to driver
→ dangerous
```

In a unit test with five known rows:

```text
collect five rows for assertion
→ reasonable
```

The test suite should not normalize unsafe production patterns.

### Row order remains undefined

Partitioning and task scheduling can change result presentation order.

Therefore deterministic comparison must be explicit.

### Local Spark still executes Spark semantics

`local[2]` means Spark runs locally with limited parallelism. It does not turn DataFrames into ordinary Python lists.

Tests can still exercise:

```text
Catalyst planning
shuffles
joins
aggregations
lazy evaluation
Spark SQL types
```

without requiring a cluster.

### Do not assert physical plans unless the test is specifically about execution strategy

A business-correctness test should normally survive a legitimate optimizer change.

Avoid coupling ordinary unit tests to:

```text
BroadcastHashJoin vs. SortMergeJoin
exact Exchange count
exact stage count
```

unless the explicit purpose is a performance/execution regression test.

### Shared Spark state can leak between tests

Be careful with mutable session-wide state such as:

```text
temporary views
SQL configuration changes
cached tables
current database
```

A test that mutates shared state should restore it or isolate the state deliberately.

### Distributed correctness still matters on tiny data

A tiny fixture can expose:

```text
join multiplication
wrong partition-independent ordering assumption
incorrect aggregation grain
nondeterministic window tie
```

You do not need production-scale data to prove these logical defects.

---

[Back to Table of Contents](#toc)

---

<a id="15-common-bad-testing-patterns"></a>
## 15. Common Bad Testing Patterns

### 1. Testing only that a DataFrame exists

```python
assert result_df is not None
```

This proves almost nothing about business correctness.

### 2. Testing only row counts

```python
assert result_df.count() == 10
```

The wrong ten rows can still pass.

### 3. Copying production logic into expected-result code

If production and test both calculate the same formula in the same way, they can share the same defect.

Prefer small fixtures whose expected result can be stated independently.

### 4. Comparing unordered `collect()` results

Spark does not promise row presentation order.

Sort tiny test outputs explicitly or use an order-insensitive comparison.

### 5. Huge generated fixtures

Large data slows feedback and makes failures difficult to understand.

Use the smallest dataset that distinguishes correct from incorrect behavior.

### 6. One giant dirty fixture for every unit test

A test intended to prove one rule becomes dependent on ten unrelated defects.

Use scenario-specific fixtures for unit tests and reserve the comprehensive dirty dataset for integration coverage.

### 7. Mocking Spark DataFrames instead of testing real DataFrame behavior

For transformation logic, real tiny Spark DataFrames usually provide more value than elaborate mocks.

### 8. Testing implementation syntax instead of observable contracts

Refactoring from one equivalent DataFrame expression to another should not break a business-correctness test.

### 9. Ignoring schema because values look correct

`125.00` as a string is not equivalent to `DecimalType(12, 2)` in a production schema contract.

### 10. Ignoring values because schema looks correct

A correct decimal schema does not prevent negative or unreconciled business values.

### 11. Silencing numeric differences with loose tolerances

Use tolerance only when the numeric model justifies approximation.

### 12. Hiding duplicate defects with `dropDuplicates()` inside the test

The test should expose unexplained grain violations, not clean them away.

### 13. Tests that depend on today's date or random values

Inject dates, seed intentional randomness, and create stable tie-breakers.

### 14. Integration tests that duplicate unit-test responsibilities only

An integration test should prove meaningful component interaction.

### 15. No negative test

If a validation function is supposed to reject invalid data, include data that must fail. A suite containing only valid rows cannot prove rejection behavior.

---

[Back to Table of Contents](#toc)

---

<a id="16-how-phase-11-builds-on-phases-9-and-10"></a>
## 16. How Phase 11 Builds on Phases 9 and 10

Phase 11 is the natural consequence of the previous two phases.

### Phase 9 created testable architecture

Existing Phase 9 responsibilities include functions such as:

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

The important design property is:

```text
DataFrame(s) + explicit parameters
        ↓
transformation
        ↓
DataFrame
```

That allows unit tests to call business logic without paths, credentials, or unrelated orchestration.

### Phase 10 created explicit correctness contracts

Existing Phase 10 responsibilities include:

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

Those functions turn previously informal expectations into testable behaviors.

### Phase 11 connects them

```text
PHASE 9
modular, deterministic, testable functions
        ↓
PHASE 10
explicit schema / quality / grain / RI contracts
        ↓
PHASE 11
automated tests that enforce those contracts
```

The preferred progression is therefore not to invent a separate test-only pipeline.

It is to protect the pipeline already built.

### Production logic should not be duplicated in tests

If reusable Phase 9/10 logic is later moved into importable modules, tests should call those modules directly.

The goal is:

```text
one implementation of business logic
+
one independent set of test expectations
```

not:

```text
production implementation
+
copy of production implementation inside tests
```

### Phase 10 rules become Phase 11 regression cases

Examples:

```text
Phase 10 contract:
order_id must be unique

Phase 11 regression test:
a duplicated order_id causes both conflicting rows to receive DUPLICATE_ORDER_ID
```

```text
Phase 10 contract:
accepted/rejected must reconcile to input

Phase 11 regression test:
input_count == accepted_count + rejected_count
```

```text
Phase 9 contract:
latest customer selection has a stable tie-breaker

Phase 11 regression test:
tied effective dates always select the expected customer_record_id
```

---

[Back to Table of Contents](#toc)

---

<a id="17-phase-11-design-review-checklist"></a>
## 17. Phase 11 Design Review Checklist

```text
TEST ORGANIZATION
[ ] pytest discovers the suite cleanly.
[ ] Shared Spark setup is centralized in a reusable fixture.
[ ] Production logic is imported rather than copied into tests.
[ ] Unit and integration tests are separated by responsibility.

FIXTURES
[ ] Fixtures are small and deterministic.
[ ] Production schemas are reused where appropriate.
[ ] Each unit fixture contains only the rows needed to prove the behavior.
[ ] Runtime dates/randomness/ties are controlled explicitly.

TRANSFORMATION CORRECTNESS
[ ] Filters keep and reject the correct business rows.
[ ] Joins preserve the intended grain.
[ ] Aggregations produce the correct groups and measures.
[ ] Window logic includes and tests deterministic tie-breaking.

DATAFRAME COMPARISON
[ ] Tests do not assume undefined row order.
[ ] Whole-DataFrame equality is used only when it matches the contract.
[ ] Targeted projections/keys are used for focused tests where clearer.

SCHEMA
[ ] Required output columns are protected.
[ ] Important Spark data types are protected.
[ ] Nullability is tested only when contractually meaningful.
[ ] Schema assertions do not replace data assertions.

GRAIN AND KEYS
[ ] Primary-key uniqueness is tested.
[ ] Composite-key uniqueness is tested as a combination.
[ ] Parent/dimension grain is protected before joins depend on it.
[ ] Row-count assertions are supported by identity/value assertions.

REFERENTIAL INTEGRITY
[ ] Accepted child rows have valid parents.
[ ] Orphan inputs are tested deliberately.
[ ] Orphan rejection behavior is verified.

DATA QUALITY
[ ] Valid rows reach accepted output.
[ ] Invalid rows reach rejected output.
[ ] Correct rejection reasons are preserved.
[ ] Multiple simultaneous rejection reasons are tested.
[ ] Quarantine metadata is deterministic where applicable.

RECONCILIATION
[ ] input_count = accepted_count + rejected_count.
[ ] Important measures reconcile across aggregation boundaries.
[ ] Derived measures satisfy their row-level formulas.
[ ] Exact Decimal logic is not weakened by unnecessary float tolerance.

EDGE CASES
[ ] Empty input is handled deliberately.
[ ] Boundary numeric values are tested.
[ ] Duplicate and orphan scenarios are covered.
[ ] No-match / all-rejected / all-accepted populations are considered.

SPARK EXECUTION
[ ] collect() is limited to tiny fixtures.
[ ] Test actions are understood as Spark jobs.
[ ] Shared session state does not leak unpredictably between tests.
[ ] Business tests are not coupled to incidental physical-plan choices.

REGRESSION PROTECTION
[ ] Important past defects become permanent regression tests.
[ ] A meaningful bad code change causes the suite to fail.
[ ] Failure messages make the violated contract identifiable.
```

---

[Back to Table of Contents](#toc)

---

<a id="18-phase-11-mastery-expectations"></a>
## 18. Phase 11 Mastery Expectations

The formal Phase 11 mastery requirement is:

> **A pipeline change should be able to fail automated tests before it silently corrupts downstream data.**

To demonstrate that standard, you should be able to build and explain tests that protect:

```text
schema contracts
intended grain
transformation correctness
primary/composite-key uniqueness
referential integrity
numerical reconciliation
deterministic output
edge cases
accepted/rejected quality behavior
meaningful pipeline integration
```

You should also be able to explain:

```text
Why is this a unit test rather than an integration test?
What exact contract does this test protect?
Why is this fixture sufficient?
What Spark action does the assertion trigger?
Why is this DataFrame comparison deterministic?
What defect would make this test fail?
Could this test pass while the business result is still wrong?
```

### Required mastery demonstration

The strongest proof is mutation-style reasoning:

```text
1. Start with a passing test suite.
2. Make a deliberately incorrect pipeline change.
3. Run the suite.
4. Observe the relevant automated test fail.
5. Explain which contract detected the corruption.
6. Restore the correct implementation.
7. Confirm the suite passes again.
```

Examples of deliberate defects:

```text
change sum(net_sales) to avg(net_sales)
remove the deterministic window tie-breaker
allow an orphan customer into accepted data
stop marking duplicate order IDs
change the configured minimum-sales comparison boundary
accidentally join to a duplicated parent relation
```

Phase 11 is **not complete** merely because tests exist.

It is complete only when you demonstrate that the suite can detect a meaningful pipeline regression before that defect would silently reach downstream data.

[Back to Table of Contents](#toc)
