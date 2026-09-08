# Phase 9 — Experiments & Applied Practice Guide

<a id="toc"></a>
## Table of Contents

- [Purpose](#purpose)
- [Conventions](#conventions)
- [Practice Dataset](#practice-dataset)
- [Architecture Exercise Protocol](#architecture-exercise-protocol)
- [Architecture Review Record](#architecture-review-record)
- [Experiment 1 — Decompose a Monolithic Pipeline](#experiment-1-decompose-a-monolithic-pipeline)
- [Experiment 2 — Design Reusable Transformation Functions](#experiment-2-design-reusable-transformation-functions)
- [Experiment 3 — Parameterize Business Logic Without Hidden State](#experiment-3-parameterize-business-logic-without-hidden-state)
- [Experiment 4 — Separate Environment-Specific Configuration](#experiment-4-separate-environment-specific-configuration)
- [Experiment 5 — Establish Reader and Writer Boundaries](#experiment-5-establish-reader-and-writer-boundaries)
- [Experiment 6 — Build Thin Orchestration](#experiment-6-build-thin-orchestration)
- [Experiment 7 — Add Useful Logging Without Extra Spark Work](#experiment-7-add-useful-logging-without-extra-spark-work)
- [Experiment 8 — Handle Exceptions Without Hiding Failures](#experiment-8-handle-exceptions-without-hiding-failures)
- [Experiment 9 — Make Runtime Behavior Deterministic](#experiment-9-make-runtime-behavior-deterministic)
- [Experiment 10 — Design Transformations for Testing](#experiment-10-design-transformations-for-testing)
- [Experiment 11 — Control Dependency Direction and Coupling](#experiment-11-control-dependency-direction-and-coupling)
- [Experiment 12 — Dependency Management and Packaging](#experiment-12-dependency-management-and-packaging)
- [Applied Phase 9 Project](#applied-phase-9-project)
- [Applied Task — Refactor a Retail Pipeline Into an Application](#applied-task-refactor-a-retail-pipeline-into-an-application)
- [After the Applied Task](#after-the-applied-task)

---

<a id="purpose"></a>
## Purpose

This file is the execution-oriented companion for **Phase 9 — PySpark Application Architecture**.

The Phase 9 `README.md` is the conceptual reference. `phase_09_lecture.py` is the consolidated teaching implementation. This guide turns those ideas into controlled architecture exercises where the repeated habit is:

```text
identify responsibility
    ↓
identify current coupling
    ↓
state the desired contract
    ↓
refactor ONE boundary
    ↓
run the same business logic
    ↓
verify correctness
    ↓
review maintainability
```

The governing question is:

> **Can another engineer understand, configure, test, run, change, and troubleshoot this pipeline without reverse-engineering one giant script?**

For every important exercise, be able to answer:

```text
1. What is the input grain?
2. What is the output grain?
3. Which responsibility is being exercised?
4. What does this function/module need to know?
5. What should it deliberately NOT know?
6. What inputs should be passed explicitly?
7. What side effects are allowed here?
8. What business logic can be tested without filesystem setup?
9. What changes between dev/test/prod?
10. Is that difference configuration or business logic?
11. Does logging trigger unnecessary Spark work?
12. Are exceptions adding context or merely hiding the real error?
13. Is behavior deterministic across reruns?
14. Are dependencies declared and importable predictably?
15. Could another engineer locate the correct place to make a change?
```

The applied section is practice only. It does **not** perform the formal Phase 9 mastery gate, update `ROADMAP.md`, mark Phase 9 complete, or enter Phase 10.

---

[Back to Table of Contents](#toc)

---

<a id="conventions"></a>
## Conventions

- Use **PySpark 4.2.0**.
- Use **single quotes** for Python strings.
- Prefer `from pyspark.sql import functions as F`.
- Include concise inline comments explaining both **what** important code does and **why** the boundary matters.
- Reuse deterministic retail-domain data.
- State input and output grain before refactoring business logic.
- Keep business transformations centered on `DataFrame(s) -> DataFrame`.
- Pass paths, dates, thresholds, and environment values explicitly when they are legitimate parameters.
- Keep secrets out of ordinary source-controlled configuration.
- Do not use `df.count()`, `collect()`, or similar Spark actions merely to enrich logs.
- Catch exceptions only where you can recover or add useful application context.
- Preserve original exception causes with `raise ... from error` when translating failures.
- Use stable tie-breakers for window functions when ties are possible.
- Inject run-dependent values when replayability matters.
- Declare third-party dependencies rather than relying on packages installed accidentally on one machine.
- Avoid `sys.path` hacks as a packaging strategy.
- Do not split code into modules merely to create more files; split by real responsibility.
- Do not deepen Phase 10 data-quality rules or Phase 11 pytest mechanics beyond what is needed to prove Phase 9 architecture.
- Do not mark Phase 9 complete until the formal mastery gate is explicitly requested and passed.

### Core architecture vocabulary

Be able to classify code as primarily:

```text
configuration
schema
reader / ingestion
validation
business transformation
writer / persistence
orchestration
logging
exception boundary
test fixture / test support
entry point
packaging / dependency declaration
```

A function can interact with more than one concern, but the goal is to avoid **unnecessary ownership**.

For example:

```text
reader
→ may know file format + schema + path passed in
→ should not define completed-order business rules

transformation
→ may know columns + business rules + grain
→ should not know dev/prod paths

pipeline
→ may know sequence + configuration + dependencies
→ should not contain every transformation expression inline
```

---

[Back to Table of Contents](#toc)

---

<a id="practice-dataset"></a>
## Practice Dataset

Reuse the deterministic retail relations from `phase_09_lecture.py`.

### `orders_df`

Grain:

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

### `customers_df`

Grain:

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

### `customer_history_df`

Grain:

```text
one row per customer_record_id
```

Columns:

```text
customer_record_id
customer_id
effective_date
province
```

The history dataset intentionally contains a tie on `effective_date` so deterministic tie-breaking can be practiced.

### Target analytical output

The core pipeline should eventually produce:

```text
sales_by_province_df
```

Grain:

```text
one row per province
```

Measures/attributes:

```text
province
order_count
net_sales
processing_date
```

The default business rule is:

```text
include COMPLETED orders
minimum net_sales = 0.00
```

Do not let this business rule become entangled with environment paths or output persistence.

---

[Back to Table of Contents](#toc)

---

<a id="architecture-exercise-protocol"></a>
## Architecture Exercise Protocol

Architecture exercises are different from performance experiments.

The main variable is usually **code responsibility**, not runtime speed.

Use this protocol.

### Step 1 — Freeze business correctness

Before refactoring, state:

```text
input grain
output grain
required rows
required measures
join semantics
filter semantics
```

A cleaner architecture that changes business results is still wrong.

### Step 2 — Identify the responsibility being changed

Examples:

```text
move paths out of transformation logic
extract a reusable filter
move schema ownership into one reusable definition
move writes out of business logic
inject run_date
add logging at orchestration boundaries
```

### Step 3 — State the desired contract

Example:

```python
def transform_orders(orders_df, customers_df):
    # Receive only the DataFrames needed for business enrichment.
    ...
    return result_df
```

Contract:

```text
inputs:
orders_df at order grain
customers_df at customer grain

output:
one row per order_id

allowed knowledge:
required business columns
join semantics

forbidden knowledge:
filesystem paths
environment name
credentials
write destination
schedule
```

### Step 4 — Refactor one boundary

Do not simultaneously redesign:

```text
configuration
logging
exceptions
packaging
all transformation functions
```

One controlled change makes the architectural effect easier to understand.

### Step 5 — Run the same business scenario

Use the same deterministic fixture or same input files.

### Step 6 — Reconcile correctness

Confirm the refactor preserved:

```text
schema where expected
grain
row semantics
business totals
```

### Step 7 — Review maintainability

Ask:

```text
Can I now test this function with in-memory DataFrames?
Can I change the path without editing business logic?
Can I change the business rule without editing I/O code?
Is the dependency direction easier to understand?
Did I create a useful boundary or merely another file?
```

---

[Back to Table of Contents](#toc)

---

<a id="architecture-review-record"></a>
## Architecture Review Record

Use this record after each experiment.

```text
EXPERIMENT:

BUSINESS CORRECTNESS
Input grain:
Output grain:
Business rule preserved:
Correctness evidence:

RESPONSIBILITY
Concern being isolated:
Before refactor, this code knew:
After refactor, this code knows:

CONTRACT
Inputs:
Output:
Allowed side effects:
Explicit parameters:

COUPLING
Infrastructure knowledge removed:
Hidden global state removed:
Dependencies made explicit:

TESTABILITY
Can core logic run with in-memory DataFrames only?
Filesystem required?
Credentials required?
Environment required?
Write required?

MAINTAINABILITY
Where would another engineer change the business rule?
Where would another engineer change the path?
Where would another engineer change the write format?
Where would another engineer change run configuration?

FINAL ASSESSMENT
Why is the new boundary better?
What tradeoff or complexity did it add?
```

Do not answer architecture questions with only:

```text
more modular
cleaner
best practice
```

Explain the concrete dependency or responsibility that changed.

---

[Back to Table of Contents](#toc)

---

<a id="experiment-1-decompose-a-monolithic-pipeline"></a>
## Experiment 1 — Decompose a Monolithic Pipeline

### Goal

Learn to identify separate concerns before creating modules.

### Starting point

Use a monolithic function conceptually equivalent to:

```python
def build_sales_report(spark):
    # Hard-coded I/O and business logic are coupled together here.
    orders_df = spark.read.parquet('data/dev/orders')
    customers_df = spark.read.parquet('data/dev/customers')

    result_df = (
        orders_df
        .filter(F.col('order_status') == 'COMPLETED')
        .join(customers_df, on='customer_id', how='left')
        .groupBy('province')
        .agg(F.sum('net_sales').alias('net_sales'))
    )

    result_df.write.mode('overwrite').parquet('data/dev/output')
```

### Before coding

Classify each line as:

```text
configuration
reader
business transformation
writer
orchestration
```

Then answer:

```text
Which lines change if the business rule changes?
Which lines change if dev becomes prod?
Which lines change if Parquet becomes a table write?
Which lines make unit-style testing inconvenient?
```

### Refactor target

Sketch responsibility boundaries first:

```text
build_config(...)
read_orders(...)
read_customers(...)
filter_orders(...)
transform_orders(...)
build_sales_by_province(...)
write_sales_by_province(...)
run_pipeline(...)
```

Do **not** create a separate file for each function yet unless instructed by a later artifact.

### Success criteria

You should be able to point to one owner for each responsibility.

A business rule change such as:

```text
COMPLETED only
→ COMPLETED + SHIPPED
```

should not require changing:

```text
reader path
write path
SparkSession construction
```

### Architecture question

Why is this stronger than saying:

> Split a long file into smaller files.

Your answer should mention **reasons to change**, dependency boundaries, and testability.

---

[Back to Table of Contents](#toc)

---

<a id="experiment-2-design-reusable-transformation-functions"></a>
## Experiment 2 — Design Reusable Transformation Functions

### Goal

Make the business-logic center independent of infrastructure.

### Build

Implement or reuse:

```python
def transform_orders(orders_df, customers_df):
    # Enrich order-grain rows while preserving order grain.
    ...
    return result_df
```

and:

```python
def build_sales_by_province(enriched_orders_df):
    # Deliberately change grain from order to province.
    ...
    return result_df
```

### Required contracts

For `transform_orders(...)`:

```text
input grain:
orders_df -> one row per order_id
customers_df -> one row per customer_id

output grain:
one row per order_id
```

For `build_sales_by_province(...)`:

```text
input grain:
one row per order_id

output grain:
one row per province
```

### Forbidden inside these functions

Do not include:

```text
spark.read
DataFrameWriter
hard-coded paths
environment detection
credentials
CLI parsing
logging setup
job scheduling
```

### Verify

Construct tiny in-memory DataFrames and call the functions directly.

Confirm:

```text
expected province
expected order_count
expected net_sales
```

### Design comparison

Compare:

```python
def transform_orders(orders_df, customers_df):
    ...
```

with:

```python
def transform_orders(application_context):
    ...
```

Explain why the first usually has a clearer dependency contract when the function only needs two DataFrames.

---

[Back to Table of Contents](#toc)

---

<a id="experiment-3-parameterize-business-logic-without-hidden-state"></a>
## Experiment 3 — Parameterize Business Logic Without Hidden State

### Goal

Distinguish legitimate parameters from hidden global configuration.

### Build

Use:

```python
def filter_orders(
    orders_df,
    included_statuses,
    minimum_net_sales,
):
    # Explicit parameters make the same transformation reusable across runs.
    return orders_df.filter(
        F.col('order_status').isin(*included_statuses)
        & (F.col('net_sales') >= F.lit(minimum_net_sales))
    )
```

Run at least two parameter sets:

```text
Case A
included_statuses = ('COMPLETED',)
minimum_net_sales = 0.00

Case B
included_statuses = ('COMPLETED',)
minimum_net_sales = 100.00
```

### Record

For each case:

```text
input grain
output grain
included order_ids
net_sales total
```

### Anti-pattern comparison

Avoid:

```python
INCLUDED_STATUSES = ('COMPLETED',)
MINIMUM_NET_SALES = 100


def filter_orders(orders_df):
    # Hidden module globals make the true function contract less obvious.
    ...
```

Module constants are not universally wrong, but values that legitimately vary per run should not become invisible dependencies.

### Design question

Which values should be parameters, and which values should simply remain implementation details?

Do not make everything configurable merely because configuration exists.

---

[Back to Table of Contents](#toc)

---

<a id="experiment-4-separate-environment-specific-configuration"></a>
## Experiment 4 — Separate Environment-Specific Configuration

### Goal

Keep environment changes from duplicating business logic.

### Build

Use an immutable configuration object such as:

```python
from dataclasses import dataclass
from datetime import date
from decimal import Decimal


@dataclass(frozen=True)
class PipelineConfig:
    environment: str
    orders_path: str
    customers_path: str
    output_path: str
    included_statuses: tuple[str, ...]
    minimum_net_sales: Decimal
    run_date: date
    write_mode: str = 'overwrite'
```

Create at least:

```text
dev config
test config
prod config
```

The exact local practice paths can differ, but the business transformation functions should remain unchanged.

### Verify

Show that:

```text
dev.orders_path != prod.orders_path
```

while:

```text
transform_orders(...)
filter_orders(...)
build_sales_by_province(...)
```

are identical in all environments.

### Failure case

Request an unsupported environment and fail early.

Example:

```python
build_config('stagingg', base_dir)
```

Expected architectural behavior:

```text
configuration error before expensive Spark work begins
```

### Design question

Explain why credentials do **not** belong in ordinary committed config, even though paths and environment names can.

---

[Back to Table of Contents](#toc)

---

<a id="experiment-5-establish-reader-and-writer-boundaries"></a>
## Experiment 5 — Establish Reader and Writer Boundaries

### Goal

Separate persistence mechanics from business semantics.

### Reader contract

Use a reader resembling:

```python
def read_orders(spark_session, path):
    # Reader owns format/schema mechanics; the path is supplied explicitly.
    return (
        spark_session.read
        .schema(ORDERS_SCHEMA)
        .parquet(path)
    )
```

Reader may know:

```text
format
read options
schema
path passed by caller
```

Reader should not decide:

```text
COMPLETED-only business filter
customer join logic
province aggregation
```

### Writer contract

Use a writer resembling:

```python
def write_sales_by_province(result_df, path, mode):
    # Writer owns persistence, not business aggregation semantics.
    (
        result_df.write
        .mode(mode)
        .parquet(path)
    )
```

Writer may know:

```text
format
mode
storage partitioning when appropriate
output destination passed by caller
```

Writer should not silently:

```text
deduplicate rows
change grain
redefine measures
filter business records
```

### Exercise

Seed deterministic Parquet inputs in a temporary directory.

Then run:

```text
reader
→ transformations
→ writer
```

Confirm the same business output can still be produced from in-memory DataFrames **without** calling either I/O function.

That difference is the testability benefit.

---

[Back to Table of Contents](#toc)

---

<a id="experiment-6-build-thin-orchestration"></a>
## Experiment 6 — Build Thin Orchestration

### Goal

Make the pipeline function coordinate layers rather than contain all their implementation.

### Target shape

```python
def run_pipeline(spark_session, config):
    # Coordinate existing responsibilities in a readable sequence.
    orders_df = read_orders(spark_session, config.orders_path)
    customers_df = read_customers(spark_session, config.customers_path)

    filtered_orders_df = filter_orders(
        orders_df,
        config.included_statuses,
        config.minimum_net_sales,
    )

    enriched_orders_df = transform_orders(
        filtered_orders_df,
        customers_df,
    )

    result_df = build_sales_by_province(enriched_orders_df)
    final_df = add_processing_date(result_df, config.run_date)

    write_sales_by_province(
        final_df,
        config.output_path,
        config.write_mode,
    )

    return final_df
```

### Review questions

Can you understand the pipeline sequence without reading every transformation implementation?

Can you answer:

```text
where input is read
where validation occurs
where business rules occur
where output is written
```

from orchestration alone?

### Anti-pattern

If `run_pipeline(...)` grows into hundreds of lines of chained PySpark expressions, the orchestration boundary has collapsed back into the monolith.

### Success criteria

The orchestration function should read approximately like the pipeline architecture diagram.

---

[Back to Table of Contents](#toc)

---

<a id="experiment-7-add-useful-logging-without-extra-spark-work"></a>
## Experiment 7 — Add Useful Logging Without Extra Spark Work

### Goal

Use logs for operational context without accidentally creating extra jobs.

### Add logs for

```text
pipeline start
selected environment
major read boundary
major transformation boundary
write destination
pipeline success
pipeline failure
```

Example:

```python
logger.info('Pipeline started | environment=%s', config.environment)
logger.info('Reading orders | path=%s', config.orders_path)
logger.info('Writing result | path=%s', config.output_path)
```

### Avoid this by default

```python
logger.info('Orders row count=%s', orders_df.count())
```

Why?

```text
count()
→ Spark action
→ distributed execution
→ potentially expensive work
```

The log line is not free.

### Controlled comparison

Version A:

```text
log configuration + boundaries only
```

Version B:

```text
call count() after every stage just for logging
```

Inspect the Spark UI or action behavior and explain why Version B changes application execution rather than merely improving observability.

### Logging rule

Log **known application context** freely.

Trigger new distributed computation only when the metric itself is operationally justified.

---

[Back to Table of Contents](#toc)

---

<a id="experiment-8-handle-exceptions-without-hiding-failures"></a>
## Experiment 8 — Handle Exceptions Without Hiding Failures

### Goal

Use exception boundaries to add context, not to make failures disappear.

### Case A — Fail fast on configuration

Raise a clear configuration error before the pipeline starts if required settings are invalid.

### Case B — Add orchestration context

Use a pattern like:

```python
try:
    final_df = run_business_steps(...)
except Exception as error:
    # Translate only at a boundary where pipeline context is useful.
    raise PipelineExecutionError(
        f'Pipeline failed for environment {config.environment!r}'
    ) from error
```

### Case C — Do not swallow

Reject:

```python
try:
    write_sales_by_province(...)
except Exception:
    print('Something went wrong')
```

Problems:

```text
original error may be hidden
caller may think execution succeeded
automation cannot reliably detect failure
recovery behavior is undefined
```

### Failure exercise

Deliberately provide a missing input path.

Record:

```text
original failure type
application-level context added
whether the original cause remains inspectable
whether the process still fails
```

### Design question

Why should a low-level pure transformation usually **not** catch every possible exception itself?

---

[Back to Table of Contents](#toc)

---

<a id="experiment-9-make-runtime-behavior-deterministic"></a>
## Experiment 9 — Make Runtime Behavior Deterministic

### Goal

Make the same logical input produce predictable business output.

### Part A — Deterministic latest record

Start with history containing tied dates:

```text
customer_id = C002
R003 | 2026-05-01 | AB
R004 | 2026-05-01 | QC
```

Weak ordering:

```python
Window.partitionBy('customer_id').orderBy(
    F.col('effective_date').desc(),
)
```

Stronger ordering:

```python
Window.partitionBy('customer_id').orderBy(
    F.col('effective_date').desc(),
    F.col('customer_record_id').desc(),
)
```

The stable secondary key defines what happens on a tie.

### Part B — Inject run-dependent values

Prefer:

```python
def add_processing_date(result_df, run_date):
    # Caller controls the run date, so replay behavior is explicit.
    return result_df.withColumn(
        'processing_date',
        F.lit(run_date).cast('date'),
    )
```

rather than burying wall-clock behavior inside business logic when replayability matters.

### Part C — Row order

Do not assume a DataFrame has deterministic display/order semantics without `orderBy(...)` when order matters to comparison or presentation.

### Record

Explain the distinction between:

```text
deterministic business result
```

and:

```text
deterministic physical partition/task execution
```

Phase 9 is primarily concerned with the first.

---

[Back to Table of Contents](#toc)

---

<a id="experiment-10-design-transformations-for-testing"></a>
## Experiment 10 — Design Transformations for Testing

### Goal

Prove that architecture determines how easy business logic is to test.

### Testable design

Given:

```python
def transform_orders(orders_df, customers_df):
    ...
    return result_df
```

construct tiny deterministic DataFrames and verify:

```text
schema
order grain
joined province
customer segment
expected row values
```

Then test:

```python
def build_sales_by_province(enriched_orders_df):
    ...
    return result_df
```

Verify:

```text
one row per province
expected order_count
expected net_sales
```

### Architecture contrast

Suppose instead you have:

```python
def build_report(spark):
    # Reads real files and writes output internally.
    ...
```

To test one business rule, you now may need:

```text
temporary filesystem setup
input serialization
path configuration
Spark reads
Spark writes
cleanup
```

That may be appropriate for an integration test, but it is unnecessarily heavy for every transformation check.

### Phase boundary

Do not build the full Phase 11 pytest suite here.

The Phase 9 lesson is simply:

> **Good architecture makes focused tests possible.**

---

[Back to Table of Contents](#toc)

---

<a id="experiment-11-control-dependency-direction-and-coupling"></a>
## Experiment 11 — Control Dependency Direction and Coupling

### Goal

Keep infrastructure concerns pointing toward business logic rather than business logic reaching outward into everything else.

### Preferred dependency direction

```text
pipeline.py
  |-- config.py
  |-- readers.py
  |-- validation.py
  |-- transformations.py
  '-- writers.py

transformations.py
  '-- PySpark DataFrame API
```

### Review exercise

For each proposed import, ask whether it is justified:

```text
transformations.py imports config.py
transformations.py imports pipeline.py
transformations.py imports cloud credential helper
pipeline.py imports transformations.py
reader imports schemas.py
writer imports config values passed directly
```

Do not memorize a rigid rule. Explain the coupling consequence.

### Large-context exercise

Compare:

```python
def transform_orders(orders_df, customers_df):
    ...
```

with:

```python
def transform_orders(application_context):
    ...
```

If `application_context` contains:

```text
SparkSession
paths
credentials
logger
run date
all DataFrames
write mode
```

but the transformation only needs two DataFrames, the function has an unnecessarily broad dependency surface.

### Success criteria

Another engineer should be able to identify a function's real dependencies from its signature and imports without tracing hidden global state.

---

[Back to Table of Contents](#toc)

---

<a id="experiment-12-dependency-management-and-packaging"></a>
## Experiment 12 — Dependency Management and Packaging

### Goal

Understand the difference between declaring dependencies and packaging application code.

### Dependency management

Inspect the repository `requirements.txt`.

At minimum, the Phase 9 environment currently relies on:

```text
pyspark==4.2.0
ipykernel
```

Be able to explain:

```text
requirements.txt
→ declares Python dependencies

virtual environment
→ isolates installed dependencies

package
→ makes your own application code importable/installable coherently
```

### Packaging design

Sketch a future application layout:

```text
phase_09_application/
├── pyproject.toml
├── src/
│   └── retail_pipeline/
│       ├── __init__.py
│       ├── config.py
│       ├── schemas.py
│       ├── readers.py
│       ├── validation.py
│       ├── transformations.py
│       ├── writers.py
│       └── pipeline.py
└── tests/
    ├── test_transformations.py
    └── test_validation.py
```

Do **not** create this package merely because the diagram exists. A later implementation artifact may do so when it serves actual work.

### Import anti-pattern

Avoid relying on:

```python
import sys
sys.path.append('C:/some/specific/local/folder')
```

as the normal application architecture.

### Concept check

Explain why:

```text
pip install -r requirements.txt
```

is not the same thing as:

```text
pip install -e .
```

and why neither command by itself configures Spark cluster resources or cloud IAM.

---

[Back to Table of Contents](#toc)

---

<a id="applied-phase-9-project"></a>
## Applied Phase 9 Project

The applied project combines the architecture boundaries into one maintainability exercise.

Do not begin from an empty architecture diagram.

Begin from a **working but poorly structured** retail pipeline and refactor it without changing business output.

The central requirement is:

> **Preserve correctness while making responsibilities and dependencies obvious.**

Your final design should make it easy to answer:

```text
Where do I change the business rule?
Where do I change the input path?
Where do I change the schema?
Where do I add validation?
Where do I change the output format?
Where do I add another environment?
Where do I add logging?
Where do I handle application-level failure context?
What code can I test without real I/O?
What code is responsible for orchestration only?
```

---

[Back to Table of Contents](#toc)

---

<a id="applied-task-refactor-a-retail-pipeline-into-an-application"></a>
## Applied Task — Refactor a Retail Pipeline Into an Application

### Scenario

You inherit one file containing all of this:

```text
SparkSession creation
hard-coded dev paths
schemas
reads
column checks
completed-order filtering
customer enrichment
province aggregation
processing date
logging
try/except
writes
```

The pipeline is logically correct, but every concern is entangled.

### Required business result

Input:

```text
orders
→ one row per order_id

customers
→ one row per customer_id
```

Rules:

```text
include configured order statuses
apply configured minimum net_sales
left join customer attributes
aggregate to province grain
add injected processing_date
```

Output:

```text
one row per province
```

Required measures:

```text
order_count
net_sales
```

### Part 1 — Freeze expected output

Before refactoring, record the expected deterministic result from the practice data.

At minimum verify:

```text
province
order_count
net_sales
```

Do not proceed if you cannot state the intended output grain.

### Part 2 — Create a responsibility map

Write:

```text
configuration owns:
____________________________

schemas own:
____________________________

readers own:
____________________________

validation owns:
____________________________

transformations own:
____________________________

writers own:
____________________________

pipeline owns:
____________________________
```

### Part 3 — Define function contracts

At minimum design contracts for:

```text
build_config(...)
read_orders(...)
read_customers(...)
require_columns(...)
transform_orders(...)
filter_orders(...)
build_sales_by_province(...)
add_processing_date(...)
write_sales_by_province(...)
run_pipeline(...)
```

For each, state:

```text
inputs
output
side effects
responsibility
what it must NOT know
```

### Part 4 — Refactor business logic first

Extract and run the DataFrame transformations without file I/O.

Verify correctness using in-memory fixtures.

This proves the stable business center exists independently of infrastructure.

### Part 5 — Add configuration

Support at least:

```text
dev
test
prod
```

The transformations must remain identical across environments.

### Part 6 — Add I/O boundaries

Use readers and writers whose paths come from configuration.

Seed local deterministic Parquet data for the runnable exercise.

### Part 7 — Add thin orchestration

The orchestration should read like:

```text
read
→ validate
→ filter
→ enrich
→ aggregate
→ add run metadata
→ write
```

If the orchestration contains all PySpark expressions inline, continue refactoring.

### Part 8 — Add logging

Log useful execution context without adding unjustified actions.

At minimum:

```text
start
environment
input boundaries
write boundary
success/failure
```

### Part 9 — Add failure context

Deliberately trigger one configuration or missing-path failure.

Verify:

```text
failure remains visible
original cause remains inspectable
application context is added where useful
```

### Part 10 — Prove deterministic behavior

Use:

```text
injected run_date
stable window tie-breaker where applicable
explicit orderBy() only when ordered comparison/output is required
```

### Part 11 — Review dependency direction

Draw the actual dependency graph.

A reasonable result may resemble:

```text
entry point
    ↓
pipeline
    ├── config
    ├── readers ──→ schemas
    ├── validation
    ├── transformations
    └── writers
```

Do not force this exact graph if your implementation has a justified alternative.

### Part 12 — Packaging proposal

Without creating unnecessary files yet, state how the refactored code would map into an installable `src/` package.

Explain:

```text
what becomes a module
what becomes the entry point
what remains test-only
what belongs in dependency declaration
```

### Final architecture review

Answer:

```text
1. What does one output row represent?
2. Where is the completed-order business rule implemented?
3. Where are environment paths defined?
4. Which functions can be tested with only in-memory DataFrames?
5. Which functions intentionally perform I/O?
6. Where is logging configured?
7. Which layer adds pipeline-level failure context?
8. How is run_date made deterministic?
9. What dependencies are declared externally?
10. How would this become an installable package?
11. What part of the architecture would change if Parquet output became a warehouse write?
12. What part should NOT change in that migration?
```

### Applied-task success standard

The project is successful when another engineer can inspect the design and quickly identify:

```text
business logic
infrastructure boundaries
configuration
validation
orchestration
side effects
test seams
failure boundaries
```

without reverse-engineering a monolithic script.

---

[Back to Table of Contents](#toc)

---

<a id="after-the-applied-task"></a>
## After the Applied Task

Do not mark Phase 9 complete automatically.

Before the formal mastery gate, preserve the important observations from your implementation:

```text
which responsibilities were hardest to separate
which hidden dependencies were removed
which functions became directly testable
which environment differences became configuration
which side effects remain intentionally at application boundaries
which deterministic choices were necessary
which packaging decisions are justified now vs. premature
```

The expected conceptual handoff is:

```text
Phase 9
→ application structure makes logic understandable and testable

Phase 10
→ deepen reusable data-quality and schema-enforcement behavior

Phase 11
→ formalize automated testing around those architecture seams
```

When explicitly requested, the formal Phase 9 mastery gate should test whether you can **design and defend the architecture**, not merely repeat module names.

---

[Back to Table of Contents](#toc)
