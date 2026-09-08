# Phase 9 — PySpark Application Architecture

<a id="toc"></a>
## Table of Contents

- [Objective](#objective)
- [Phase Mental Model](#phase-mental-model)
- [1. From PySpark Script to PySpark Application](#1-from-pyspark-script-to-pyspark-application)
- [2. Separation of Concerns](#2-separation-of-concerns)
- [3. Maintainable Project Structure](#3-maintainable-project-structure)
- [4. Reusable Transformation Functions](#4-reusable-transformation-functions)
- [5. Parameterized Pipelines](#5-parameterized-pipelines)
- [6. Schemas as an Application Boundary](#6-schemas-as-an-application-boundary)
- [7. Readers and Writers](#7-readers-and-writers)
- [8. Validation as a Separate Concern](#8-validation-as-a-separate-concern)
- [9. Thin Orchestration](#9-thin-orchestration)
- [10. Configuration and Environments](#10-configuration-and-environments)
- [11. Logging](#11-logging)
- [12. Exception Handling](#12-exception-handling)
- [13. Deterministic Behavior](#13-deterministic-behavior)
- [14. Testable Transformation Design](#14-testable-transformation-design)
- [15. Dependency Management](#15-dependency-management)
- [16. Packaging](#16-packaging)
- [17. Dependency Direction and Coupling](#17-dependency-direction-and-coupling)
- [18. End-to-End Retail Application Example](#18-end-to-end-retail-application-example)
- [19. Common Architecture Failure Modes](#19-common-architecture-failure-modes)
- [20. Phase 9 Design Review Checklist](#20-phase-9-design-review-checklist)
- [21. Phase 9 Mastery Reference](#21-phase-9-mastery-reference)

---

<a id="objective"></a>
## Objective

Turn isolated PySpark transformations into **maintainable data-engineering applications**.

The Phase 9 problem is no longer:

```text
Can I make this DataFrame transformation work?
```

It is:

```text
Can another engineer understand, configure, test, run, change, and troubleshoot
this pipeline without reverse-engineering one giant script?
```

Required Phase 9 concerns:

- separate I/O, schemas, business logic, validation, orchestration, and configuration;
- reusable transformation functions;
- parameterized pipelines;
- environment-specific configuration;
- logging;
- exception handling;
- deterministic behavior;
- dependency management;
- packaging;
- maintainable project structure;
- testable transformation design.

Examples target **PySpark 4.2.0**, use deterministic retail-domain data, and prefer:

```python
from pyspark.sql import functions as F
```

The governing engineering principle is:

```python
def transform_orders(orders_df, customers_df):
    # Business logic depends on DataFrames, not infrastructure details.
    ...
    return result_df
```

rather than a transformation that also decides:

```text
where files live
how credentials are obtained
which environment is running
when the job executes
how outputs are persisted
```

Phase 9 focuses on **software structure**. Phase 10 will deepen data-quality design, and Phase 11 will deepen automated testing.

This document is lecture/reference material only. It does not mark Phase 9 complete or update `ROADMAP.md`.

---

[Back to Table of Contents](#toc)

---

<a id="phase-mental-model"></a>
## Phase Mental Model

A production-style PySpark application is a set of cooperating responsibilities.

```text
configuration
      ↓
orchestration
      ↓
read inputs
      ↓
validate boundaries
      ↓
transform business data
      ↓
validate outputs where needed
      ↓
write outputs
```

The important architectural idea is:

> **Separate code by responsibility, then make dependencies explicit.**

A useful mapping is:

```text
config.py
→ What environment/run settings should this application use?

schemas.py
→ What structure do input/output records require?

readers.py
→ How does data enter the application?

validation.py
→ What must be true before data continues?

transformations.py
→ What business result should the data become?

writers.py
→ How does data leave the application?

pipeline.py
→ In what order are those pieces called?
```

The transformation layer should know as little as possible about the outside world.

That gives the application a stable center:

```text
DataFrame(s)
    ↓
transformation function
    ↓
DataFrame
```

Everything else supplies inputs, parameters, execution context, and persistence.

---

[Back to Table of Contents](#toc)

---

<a id="1-from-pyspark-script-to-pyspark-application"></a>
## 1. From PySpark Script to PySpark Application

A learning script often starts like this:

```python
orders_df = spark.read.parquet('data/orders')
customers_df = spark.read.parquet('data/customers')

result_df = (
    orders_df
    .filter(F.col('order_status') == 'COMPLETED')
    .join(customers_df, on='customer_id', how='left')
    .groupBy('province')
    .agg(F.sum('net_sales').alias('net_sales'))
)

result_df.write.mode('overwrite').parquet('data/output/sales_by_province')
```

This can be correct, but it mixes several concerns:

```text
input paths
read format
business rules
join logic
aggregation logic
write mode
output path
```

As the pipeline grows, the file becomes harder to:

```text
understand
test
reuse
configure
change safely
troubleshoot
```

The goal is not to split code into many files for its own sake.

The goal is to separate responsibilities that change for **different reasons**.

For example:

```text
Business rule changes
→ transformations.py

Storage location changes
→ configuration

Parquet read option changes
→ readers.py

Write strategy changes
→ writers.py

Pipeline sequence changes
→ pipeline.py
```

That is the beginning of maintainable application architecture.

---

[Back to Table of Contents](#toc)

---

<a id="2-separation-of-concerns"></a>
## 2. Separation of Concerns

Use one responsibility per architectural layer.

| Concern | Owns | Should generally not own |
|---|---|---|
| Configuration | environment/run settings | transformation implementation |
| Schemas | `StructType` definitions | file paths |
| Readers | loading DataFrames | business aggregations |
| Validation | correctness checks/rules | orchestration |
| Transformations | business DataFrame logic | credentials, paths, writes |
| Writers | persistence mechanics | business joins/filters |
| Orchestration | sequencing components | detailed transformation logic |

### Why this matters

Suppose the output path moves from:

```text
data/curated/sales
```

to:

```text
gs://company-prod-curated/sales
```

If paths are embedded throughout transformation code, infrastructure changes force business-code edits.

If the output path is configuration passed to a writer, the transformation remains unchanged.

The same principle applies to:

```text
credentials
environment names
input locations
write modes
runtime dates
feature flags
```

### Separation does not mean isolation

Layers still cooperate.

For example:

```text
pipeline.py
    ↓
readers.py
    ↓
transformations.py
    ↓
writers.py
```

The design goal is **clear ownership and explicit dependency**, not zero interaction.

---

[Back to Table of Contents](#toc)

---

<a id="3-maintainable-project-structure"></a>
## 3. Maintainable Project Structure

The curriculum gives this conceptual structure:

```text
src/
├── config.py
├── schemas.py
├── readers.py
├── validation.py
├── transformations.py
├── writers.py
└── pipeline.py

tests/
├── test_validation.py
└── test_transformations.py
```

A package-oriented version could become:

```text
src/
└── retail_pipeline/
    ├── __init__.py
    ├── config.py
    ├── schemas.py
    ├── readers.py
    ├── validation.py
    ├── transformations.py
    ├── writers.py
    └── pipeline.py

tests/
├── test_validation.py
└── test_transformations.py
```

The exact file count is not the goal.

Create a module when it has a real responsibility.

Do **not** create structure merely to make the repository look enterprise-sized.

### Good structure test

Ask:

```text
If I need to change a business rule, where do I go?
If I need to change a path, where do I go?
If I need to change how data is written, where do I go?
If I need to test a transformation, can I call it directly?
```

If the answers are obvious, the structure is helping.

If every change requires searching the entire application, the boundaries are weak.

---

[Back to Table of Contents](#toc)

---

<a id="4-reusable-transformation-functions"></a>
## 4. Reusable Transformation Functions

Transformation functions should primarily accept DataFrames and explicit business parameters, then return DataFrames.

### Preferred shape

```python
def transform_orders(orders_df, customers_df):
    # Keep completed orders because downstream sales metrics use completed sales only.
    completed_orders_df = orders_df.filter(
        F.col('order_status') == 'COMPLETED'
    )

    # Enrich orders with customer attributes without performing any I/O here.
    return completed_orders_df.join(
        customers_df,
        on='customer_id',
        how='left',
    )
```

This function is easy to reason about because its contract is visible:

```text
inputs:
orders_df
customers_df

output:
enriched completed orders DataFrame
```

### Avoid infrastructure inside business logic

Avoid:

```python
def transform_orders(spark):
    # BAD: The function chooses infrastructure and business logic together.
    orders_df = spark.read.parquet('C:/prod/orders')
    customers_df = spark.read.parquet('C:/prod/customers')

    result_df = orders_df.join(customers_df, 'customer_id')

    result_df.write.parquet('C:/prod/output')
```

Problems:

```text
cannot test without filesystem setup
cannot reuse with different inputs
paths are hard-coded
read/write behavior is coupled to business logic
function contract is unclear
```

### Prefer small coherent functions

Good:

```python
def filter_completed_orders(orders_df):
    # Isolate one reusable business rule.
    return orders_df.filter(F.col('order_status') == 'COMPLETED')


def enrich_orders_with_customers(orders_df, customers_df):
    # Keep join semantics explicit at the transformation boundary.
    return orders_df.join(
        customers_df,
        on='customer_id',
        how='left',
    )
```

But do not split every single expression into its own function.

A function should represent a meaningful unit of business behavior.

---

[Back to Table of Contents](#toc)

---

<a id="5-parameterized-pipelines"></a>
## 5. Parameterized Pipelines

A maintainable pipeline should receive values that vary between runs rather than burying them in code.

Common parameters include:

```text
processing date
input path
output path
environment
write mode
business cutoff
source system
```

### Parameterize true variability

```python
def filter_orders_for_date(orders_df, processing_date):
    # The processing date changes per run, so pass it explicitly.
    return orders_df.filter(
        F.col('order_date') == F.lit(processing_date).cast('date')
    )
```

Instead of:

```python
def filter_orders_for_date(orders_df):
    # BAD: Hidden run-specific value makes reuse and testing harder.
    return orders_df.filter(F.col('order_date') == F.lit('2026-09-07'))
```

### Do not parameterize everything

A stable domain rule can remain code.

For example, if the business definition of a completed sale is permanently:

```text
order_status = COMPLETED
```

that rule does not automatically need to become a configuration field.

Parameterization is useful when a value legitimately varies by:

```text
run
environment
deployment
business-controlled setting
```

Too much configuration can make application behavior harder to understand.

### Prefer explicit function arguments

Avoid hidden global state:

```python
PROCESSING_DATE = '2026-09-07'
```

Prefer:

```python
def build_daily_sales(sales_df, processing_date):
    # Explicit inputs make the result easier to reproduce.
    ...
```

---

[Back to Table of Contents](#toc)

---

<a id="6-schemas-as-an-application-boundary"></a>
## 6. Schemas as an Application Boundary

Schemas are part of the application's contract with external data.

Keep reusable schema definitions separate from reading and transformation code.

Example:

```python
from pyspark.sql.types import DateType
from pyspark.sql.types import DecimalType
from pyspark.sql.types import LongType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType


orders_schema = StructType([
    StructField('order_id', LongType(), nullable=False),
    StructField('order_date', DateType(), nullable=False),
    StructField('customer_id', LongType(), nullable=False),
    StructField('order_status', StringType(), nullable=False),
    StructField('net_sales', DecimalType(12, 2), nullable=False),
])
```

Then a reader can depend on the schema:

```python
def read_orders(spark, path, schema):
    # Enforce the caller-supplied contract at the I/O boundary.
    return spark.read.schema(schema).parquet(path)
```

### Why separate schemas?

A centralized schema definition makes it easier to:

```text
review expected types
reuse schemas across readers/tests
spot contract changes
test schema expectations
avoid duplicated StructType definitions
```

### Schema ownership

The schema layer should define structure.

It should not decide:

```text
where files live
which environment is active
how the pipeline is orchestrated
```

---

[Back to Table of Contents](#toc)

---

<a id="7-readers-and-writers"></a>
## 7. Readers and Writers

I/O modules isolate storage mechanics from business transformations.

### Reader responsibility

A reader may own:

```text
format
schema application
read options
path received from caller
```

Example:

```python
def read_parquet_dataset(spark, path, schema=None):
    # Build the reader once so optional schema enforcement stays explicit.
    reader = spark.read

    if schema is not None:
        reader = reader.schema(schema)

    return reader.parquet(path)
```

A more domain-specific reader can be clearer:

```python
def read_orders(spark, path, schema):
    # Keep order-specific I/O behavior outside order business logic.
    return spark.read.schema(schema).parquet(path)
```

### Writer responsibility

A writer may own:

```text
output format
write mode
partitioning choice
write options
path received from caller
```

Example:

```python
def write_parquet(df, path, mode='overwrite'):
    # Centralize persistence mechanics so transformations remain storage-agnostic.
    (
        df
        .write
        .mode(mode)
        .parquet(path)
    )
```

### Keep business rules out of writers

Avoid:

```python
def write_completed_sales(df, path):
    # BAD: Persistence function silently changes business data.
    (
        df
        .filter(F.col('order_status') == 'COMPLETED')
        .write
        .parquet(path)
    )
```

The caller should pass the already-correct business result to the writer.

### Performance still matters

Separation of concerns does not remove earlier engineering knowledge.

The writer layer may still need deliberate decisions about:

```text
partitionBy(...)
repartition(...)
coalesce(...)
file count
write mode
```

Those choices should be justified by storage and execution evidence from earlier phases.

---

[Back to Table of Contents](#toc)

---

<a id="8-validation-as-a-separate-concern"></a>
## 8. Validation as a Separate Concern

Validation should be visible rather than scattered across unrelated transformations.

Phase 9 focuses on **where validation belongs architecturally**.

Phase 10 will deepen the validation rules themselves.

### Example structural validation

```python
def require_columns(df, required_columns):
    # Fail early when a transformation's required contract is missing.
    missing_columns = sorted(set(required_columns) - set(df.columns))

    if missing_columns:
        raise ValueError(
            f'Missing required columns: {missing_columns}'
        )

    return df
```

A pipeline can then make the boundary explicit:

```python
orders_df = require_columns(
    orders_df,
    required_columns=[
        'order_id',
        'customer_id',
        'order_status',
    ],
)
```

### Why not scatter checks everywhere?

Bad pattern:

```text
reader filters null IDs
transformation removes duplicate IDs
writer silently removes invalid amounts
pipeline catches everything
```

No one can easily answer:

```text
What makes a row valid?
Where is invalid data handled?
```

A visible validation boundary gives that responsibility a clear home.

### Validation vs. transformation

Some conditions are business transformations.

Some are data-quality rules.

Example:

```text
Keep only COMPLETED orders for a completed-sales metric
→ transformation rule

order_id must never be null
→ validation rule
```

This distinction becomes especially important in Phase 10.

---

[Back to Table of Contents](#toc)

---

<a id="9-thin-orchestration"></a>
## 9. Thin Orchestration

The orchestration layer coordinates components.

It should read like the pipeline's story.

```python
def run_pipeline(spark, config):
    # I/O: load source relations using environment-specific locations.
    orders_df = read_orders(
        spark=spark,
        path=config.orders_path,
        schema=orders_schema,
    )

    customers_df = read_customers(
        spark=spark,
        path=config.customers_path,
        schema=customers_schema,
    )

    # Validation: enforce required input contracts before business logic.
    require_columns(
        orders_df,
        required_columns=['order_id', 'customer_id', 'order_status'],
    )

    # Business logic: DataFrame-in / DataFrame-out transformation.
    result_df = transform_orders(
        orders_df=orders_df,
        customers_df=customers_df,
    )

    # I/O: persist the completed business result.
    write_parquet(
        df=result_df,
        path=config.output_path,
        mode=config.write_mode,
    )
```

This function should not contain the detailed implementation of every join and filter.

### Thin orchestration test

The function should make this sequence obvious:

```text
read
→ validate
→ transform
→ write
```

If `pipeline.py` becomes the largest business-logic file in the application, the architecture has likely collapsed back into a monolith.

### Orchestration is not the same as a scheduler

In Phase 9:

```text
orchestration
→ coordinates functions inside the PySpark application
```

Later, tools such as Airflow can coordinate:

```text
when jobs run
job dependencies
retries
backfills
cross-system workflows
```

Do not confuse application orchestration with platform scheduling.

---

[Back to Table of Contents](#toc)

---

<a id="10-configuration-and-environments"></a>
## 10. Configuration and Environments

Configuration holds values that should vary without editing business logic.

Typical environment-specific values include:

```text
input paths
output paths
catalog/database names
runtime environment name
write mode where appropriate
feature flags
```

### Configuration object

A small immutable object makes required settings explicit.

```python
from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineConfig:
    environment: str
    orders_path: str
    customers_path: str
    output_path: str
    write_mode: str
```

Example environment mapping:

```python
CONFIG_BY_ENVIRONMENT = {
    'local': PipelineConfig(
        environment='local',
        orders_path='data/local/orders',
        customers_path='data/local/customers',
        output_path='data/local/curated/orders',
        write_mode='overwrite',
    ),
    'prod': PipelineConfig(
        environment='prod',
        orders_path='gs://company-prod-raw/orders',
        customers_path='gs://company-prod-raw/customers',
        output_path='gs://company-prod-curated/orders',
        write_mode='overwrite',
    ),
}
```

```python
def get_config(environment):
    # Reject unknown environments instead of silently using the wrong paths.
    try:
        return CONFIG_BY_ENVIRONMENT[environment]
    except KeyError as exc:
        raise ValueError(
            f'Unknown environment: {environment}'
        ) from exc
```

### Do not store secrets in ordinary configuration files

Avoid committing:

```text
passwords
API keys
service-account private keys
access tokens
```

Credentials belong in the runtime's secure identity/secret mechanism, not scattered through Python modules.

Phase 13 will deepen cloud identity and environment configuration.

### Configuration vs. code

Use configuration for values that vary operationally.

Keep code for behavior that should be reviewed as application logic.

Do not hide every business rule inside a configuration file merely to avoid editing Python.

---

[Back to Table of Contents](#toc)

---

<a id="11-logging"></a>
## 11. Logging

A maintainable application should explain what it is doing while it runs.

Prefer structured, intentional logging over scattered `print()` calls.

### Basic logger

```python
import logging


logger = logging.getLogger(__name__)
```

At the application entry point:

```python
def configure_logging():
    # Configure driver-side application logs once at startup.
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s %(message)s',
    )
```

### Useful things to log

Examples:

```text
pipeline start/end
environment
processing date
logical input/output dataset names
important branch decisions
write destination identifier
caught failure context
```

```python
logger.info(
    'Starting orders pipeline: environment=%s processing_date=%s',
    config.environment,
    processing_date,
)
```

### Do not trigger Spark jobs only to create decorative logs

Avoid this casually:

```python
logger.info('Input rows: %s', orders_df.count())
```

`count()` is an action.

Logging it can create an additional Spark job and duplicate expensive lineage.

Only materialize counts when they are genuinely required for:

```text
validation
reconciliation
operational metrics
known debugging work
```

### Do not log sensitive data

Avoid logging:

```text
credentials
raw PII
secret configuration
large record payloads
```

### Logging and Spark

Normal application logging in orchestration code is primarily **driver-side**.

Do not assume that Python logging inside executor-side UDF work behaves like ordinary driver logging.

Keep Phase 9 logging focused on clear application lifecycle and failure context.

---

[Back to Table of Contents](#toc)

---

<a id="12-exception-handling"></a>
## 12. Exception Handling

Exception handling should add useful context without hiding failures.

### Bad pattern

```python
try:
    run_pipeline(spark, config)
except Exception:
    pass
```

This converts a failed pipeline into an apparently successful application.

### Application-boundary handling

A useful pattern is to log the failure once at the top boundary and re-raise it.

```python
def main():
    configure_logging()

    try:
        run_pipeline(spark, config)
    except Exception:
        # Keep the full traceback so the scheduler/runtime sees a real failure.
        logger.exception('Orders pipeline failed')
        raise
```

### Add context when a boundary understands the failure

```python
def get_config(environment):
    try:
        return CONFIG_BY_ENVIRONMENT[environment]
    except KeyError as exc:
        # Convert low-level lookup failure into a domain-specific configuration error.
        raise ValueError(
            f'Unknown environment: {environment}'
        ) from exc
```

`raise ... from exc` preserves the original cause.

### Do not wrap every function in `try/except`

Business transformations often do not need local exception handling.

Let failures propagate unless the function can:

```text
recover safely
add important context
translate to a clearer domain error
perform necessary cleanup
```

### Fail fast on invalid configuration

A pipeline should prefer:

```text
clear startup failure
```

over:

```text
running for 30 minutes before discovering a required output setting is missing
```

Validate configuration before expensive Spark work begins.

---

[Back to Table of Contents](#toc)

---

<a id="13-deterministic-behavior"></a>
## 13. Deterministic Behavior

A maintainable pipeline should produce the same business result for the same inputs and parameters whenever the business requirement itself is deterministic.

Conceptually:

```text
same input data
+ same configuration
+ same business parameters
→ same logical output
```

### Stable tie-breaking

Bad deduplication window:

```python
window_spec = (
    Window
    .partitionBy('order_id')
    .orderBy(F.col('updated_at').desc())
)
```

If two rows share the same `updated_at`, the selected row can be ambiguous.

Prefer a deterministic secondary key:

```python
window_spec = (
    Window
    .partitionBy('order_id')
    .orderBy(
        F.col('updated_at').desc(),
        F.col('source_row_id').asc(),
    )
)
```

### Inject run-dependent timestamps

Avoid burying volatile time inside reusable business logic when reproducibility matters.

Instead of:

```python
def add_load_timestamp(df):
    # Harder to reproduce because evaluation time becomes hidden input.
    return df.withColumn('load_timestamp', F.current_timestamp())
```

prefer:

```python
def add_load_timestamp(df, load_timestamp):
    # Make run time an explicit input so tests and reruns can reproduce it.
    return df.withColumn(
        'load_timestamp',
        F.lit(load_timestamp).cast('timestamp'),
    )
```

### Seed randomness when randomness is intentional

```python
sample_df = source_df.orderBy(F.rand(seed=42))
```

Do not use unseeded randomness in a pipeline that expects reproducible output.

### Do not rely on DataFrame row order

A DataFrame has no meaningful guaranteed global row order unless you explicitly order it for a consuming action.

Correctness should depend on:

```text
keys
grain
values
schema
business semantics
```

not incidental partition or file order.

### Determinism is broader than sorting

It also requires avoiding hidden dependencies on:

```text
current time
unseeded randomness
ambiguous window ordering
mutable global configuration
unstable generated keys
implicit schema inference when contracts should be fixed
```

---

[Back to Table of Contents](#toc)

---

<a id="14-testable-transformation-design"></a>
## 14. Testable Transformation Design

Phase 11 will teach PySpark testing in depth.

Phase 9 establishes the architectural requirement that transformation code must be **easy to test**.

### Testable shape

```python
def calculate_order_margin(order_items_df):
    # Pure DataFrame transformation: no files, credentials, or global SparkSession.
    return order_items_df.withColumn(
        'gross_margin',
        F.col('gross_sales') - F.col('gross_cost'),
    )
```

A test can:

```text
create a tiny input DataFrame
call calculate_order_margin(...)
inspect the returned DataFrame
compare expected schema/rows
```

No filesystem is required.

### Hard-to-test shape

```python
def calculate_and_write_margin(spark):
    # BAD: One function reads, transforms, and writes.
    df = spark.read.parquet('data/input')
    result_df = df.withColumn(
        'gross_margin',
        F.col('gross_sales') - F.col('gross_cost'),
    )
    result_df.write.parquet('data/output')
```

Testing now requires:

```text
Spark
input files
output directory
cleanup
path configuration
```

for a business rule that should have been testable in isolation.

### Good transformation properties

Prefer functions that have:

```text
explicit inputs
explicit parameters
one coherent responsibility
DataFrame return value
no hidden I/O
no hidden global state
no credentials
no scheduler logic
```

### Testing grain and contracts

Good function names and boundaries should make expected grain clear.

Example:

```python
def aggregate_sales_by_store(sales_df):
    # Output grain: one row per store_id.
    return (
        sales_df
        .groupBy('store_id')
        .agg(F.sum('net_sales').alias('net_sales'))
    )
```

That contract can later be tested directly.

---

[Back to Table of Contents](#toc)

---

<a id="15-dependency-management"></a>
## 15. Dependency Management

Dependency management answers:

> **Which external Python packages and versions does this application require?**

The repository currently declares:

```text
pyspark==4.2.0
ipykernel
```

in `requirements.txt`.

### Why dependencies must be declared

Without an explicit dependency file, the application may work only because the developer's machine happens to contain the right packages.

A declared environment makes setup reproducible:

```powershell
python -m pip install -r requirements.txt
```

### Pin runtime-critical dependencies deliberately

For this project:

```text
pyspark==4.2.0
```

makes the Spark/PySpark learning target explicit.

Do not add dependencies casually.

Every package increases:

```text
installation complexity
version compatibility risk
deployment size
security/maintenance surface
```

### Separate the concepts

```text
dependency management
→ what external packages/versions are required

packaging
→ how your own application code is organized/distributed

configuration
→ what values change per environment/run

orchestration
→ how application components execute in sequence
```

These are related, but they solve different problems.

### Environment reproducibility

A useful development flow is:

```text
fresh virtual environment
    ↓
install declared dependencies
    ↓
run tests/application
```

If the application works only in a long-lived developer environment, dependency management is incomplete.

---

[Back to Table of Contents](#toc)

---

<a id="16-packaging"></a>
## 16. Packaging

Packaging turns reusable application modules into an importable/distributable unit.

At minimum, Python package structure usually includes:

```text
src/
└── retail_pipeline/
    ├── __init__.py
    ├── config.py
    ├── schemas.py
    ├── readers.py
    ├── transformations.py
    ├── writers.py
    └── pipeline.py
```

Then application modules can use package imports such as:

```python
from retail_pipeline.readers import read_orders
from retail_pipeline.transformations import transform_orders
```

### Why package the code?

Packaging helps provide:

```text
stable imports
clear module ownership
reusable code
separation from ad hoc scripts
test imports that match production imports
a deployment unit for later cloud execution
```

### Package vs. script

A script is often:

```text
one file executed directly
```

A package is:

```text
multiple cooperating modules with explicit import boundaries
```

A package can still expose a small application entry point.

### Packaging does not mean overengineering

For a small pipeline, one package with a few modules is enough.

Avoid adding:

```text
abstract base classes
factory layers
plugin frameworks
complex dependency injection
```

unless the application actually needs them.

### Spark deployment connection

Later cloud execution may require shipping your Python application code and its dependencies to the Spark runtime.

Phase 9's responsibility is to make the code **packageable**.

Phase 13 will deepen environment-specific deployment and managed Spark execution.

---

[Back to Table of Contents](#toc)

---

<a id="17-dependency-direction-and-coupling"></a>
## 17. Dependency Direction and Coupling

A clean architecture is not only about filenames.

It is also about **who imports whom**.

A useful dependency direction is:

```text
pipeline.py
   ├── config.py
   ├── readers.py
   ├── validation.py
   ├── transformations.py
   └── writers.py

readers.py
   └── schemas.py

transformations.py
   └── PySpark functions/types only where possible
```

The important rule is:

> **Business transformations should not need to import the orchestration layer.**

Avoid circular dependency patterns such as:

```text
pipeline.py imports transformations.py
transformations.py imports pipeline.py
```

### Minimize infrastructure coupling

Prefer:

```python
def transform_orders(orders_df, customers_df):
    ...
```

Over:

```python
def transform_orders(config, spark, logger, filesystem_client, orders_path):
    ...
```

The second function depends on many unrelated services and becomes harder to test and reuse.

### Pass only what a function needs

If a transformation needs one parameter:

```python
def filter_high_value_orders(orders_df, minimum_net_sales):
    # Pass the business parameter directly instead of the entire config object.
    return orders_df.filter(
        F.col('net_sales') >= F.lit(minimum_net_sales)
    )
```

This is usually clearer than:

```python
def filter_high_value_orders(orders_df, config):
    ...
```

when the function needs only one field from `config`.

Explicit narrow dependencies improve maintainability.

---

[Back to Table of Contents](#toc)

---

<a id="18-end-to-end-retail-application-example"></a>
## 18. End-to-End Retail Application Example

Suppose the application must produce completed retail sales enriched with customer province.

### Required business result

```text
input grain:
orders_df = one row per order_id
customers_df = one row per customer_id

output grain:
one row per completed order_id
```

### `schemas.py`

```python
orders_schema = StructType([
    StructField('order_id', LongType(), nullable=False),
    StructField('order_date', DateType(), nullable=False),
    StructField('customer_id', LongType(), nullable=False),
    StructField('order_status', StringType(), nullable=False),
    StructField('net_sales', DecimalType(12, 2), nullable=False),
])
```

### `readers.py`

```python
def read_orders(spark, path, schema):
    # Reader owns Spark I/O mechanics, not business filters.
    return spark.read.schema(schema).parquet(path)
```

### `validation.py`

```python
def require_columns(df, required_columns):
    # Validate the transformation contract before expensive downstream work.
    missing_columns = sorted(set(required_columns) - set(df.columns))

    if missing_columns:
        raise ValueError(
            f'Missing required columns: {missing_columns}'
        )
```

### `transformations.py`

```python
def transform_orders(orders_df, customers_df):
    # Keep completed orders because the output represents completed sales only.
    completed_orders_df = orders_df.filter(
        F.col('order_status') == 'COMPLETED'
    )

    # Preserve one-row-per-order grain because customers must be unique by customer_id.
    return (
        completed_orders_df.alias('orders')
        .join(
            customers_df.alias('customers'),
            on='customer_id',
            how='left',
        )
        .select(
            'order_id',
            'order_date',
            'customer_id',
            F.col('customers.province').alias('province'),
            'net_sales',
        )
    )
```

### `writers.py`

```python
def write_completed_orders(df, path, mode):
    # Writer persists the already-transformed result without changing its semantics.
    (
        df
        .write
        .mode(mode)
        .parquet(path)
    )
```

### `config.py`

```python
@dataclass(frozen=True)
class PipelineConfig:
    environment: str
    orders_path: str
    customers_path: str
    output_path: str
    write_mode: str
```

### `pipeline.py`

```python
def run_pipeline(spark, config):
    # Load environment-specific inputs.
    orders_df = read_orders(
        spark=spark,
        path=config.orders_path,
        schema=orders_schema,
    )

    customers_df = read_customers(
        spark=spark,
        path=config.customers_path,
        schema=customers_schema,
    )

    # Enforce the minimum contracts required by business logic.
    require_columns(
        orders_df,
        ['order_id', 'customer_id', 'order_status', 'net_sales'],
    )

    require_columns(
        customers_df,
        ['customer_id', 'province'],
    )

    # Apply reusable business logic.
    completed_orders_df = transform_orders(
        orders_df=orders_df,
        customers_df=customers_df,
    )

    # Persist the result through the output boundary.
    write_completed_orders(
        df=completed_orders_df,
        path=config.output_path,
        mode=config.write_mode,
    )
```

### Architectural reading

```text
config decides WHERE
readers decide HOW TO READ
schemas define STRUCTURE
validation protects CONTRACTS
transformations decide BUSINESS RESULT
writers decide HOW TO PERSIST
pipeline decides ORDER OF EXECUTION
```

That separation is the central Phase 9 skill.

---

[Back to Table of Contents](#toc)

---

<a id="19-common-architecture-failure-modes"></a>
## 19. Common Architecture Failure Modes

### 1. One giant pipeline script

```text
read
filter
join
validate
log
write
parse arguments
catch errors
all in one file/function
```

Problem:

```text
high coupling
hard-to-test business logic
unclear ownership
```

### 2. Hard-coded paths inside transformations

```python
def transform_orders(spark):
    orders_df = spark.read.parquet('prod/orders')
```

Problem:

```text
business logic now depends on infrastructure
```

### 3. Global configuration

```python
OUTPUT_PATH = 'prod/output'
PROCESSING_DATE = '2026-09-07'
```

Problem:

```text
hidden inputs
harder tests
harder reruns
```

### 4. Business logic inside readers/writers

Problem:

```text
I/O functions silently change data semantics
```

### 5. Catching every exception

```python
try:
    ...
except Exception:
    logger.error('Something failed')
```

Problem:

```text
failure cause may be lost
scheduler may receive false success
```

### 6. Logging actions accidentally

```python
logger.info('Rows=%s', df.count())
```

Problem:

```text
logging triggers additional Spark work
```

### 7. Non-deterministic deduplication

```text
row_number() ordered by a non-unique timestamp only
```

Problem:

```text
same logical input can select different winners
```

### 8. Passing the entire application into every function

```text
spark
config
logger
paths
clients
credentials
```

Problem:

```text
functions depend on far more than they need
```

### 9. Creating too many tiny modules

Problem:

```text
navigation cost exceeds separation benefit
```

Use meaningful responsibility boundaries, not arbitrary file counts.

### 10. Architecture that ignores Spark execution

A clean module structure does not excuse bad Spark design.

Earlier principles still apply:

```text
preserve grain
validate join keys
avoid unnecessary shuffles
use appropriate partitioning
inspect plans
measure runtime evidence
```

Software architecture and distributed execution quality are both required.

---

[Back to Table of Contents](#toc)

---

<a id="20-phase-9-design-review-checklist"></a>
## 20. Phase 9 Design Review Checklist

Before calling a PySpark application maintainable, ask:

### Structure

```text
Can I identify configuration, schemas, I/O, validation, transformations,
and orchestration quickly?
```

### Transformation contracts

```text
Do business functions accept DataFrames/explicit parameters and return DataFrames?
```

### Grain

```text
Is the expected input/output grain clear where joins and aggregations occur?
```

### I/O separation

```text
Can I test core business logic without reading or writing files?
```

### Configuration

```text
Can local/dev/prod locations change without editing transformation code?
```

### Secrets

```text
Are credentials absent from source-controlled configuration?
```

### Logging

```text
Do logs explain lifecycle and failures without causing unnecessary Spark actions?
```

### Exceptions

```text
Do failures propagate clearly instead of being swallowed?
```

### Determinism

```text
Are time, randomness, tie-breaking, and generated keys handled reproducibly?
```

### Dependencies

```text
Can a fresh environment install the declared runtime dependencies?
```

### Packaging

```text
Can application modules be imported consistently by tests and runtime code?
```

### Coupling

```text
Does each function receive only the dependencies it genuinely needs?
```

### Maintainability test

```text
Could a new engineer locate and change one business rule without first learning
every infrastructure detail in the application?
```

---

[Back to Table of Contents](#toc)

---

<a id="21-phase-9-mastery-reference"></a>
## 21. Phase 9 Mastery Reference

The Phase 9 mastery target is:

> **Someone unfamiliar with the original author should be able to understand, test, configure, and maintain the application.**

You should be able to explain these distinctions without relying on memorized filenames.

### Separation of responsibilities

```text
schema
→ record structure

reader
→ external data → DataFrame

validation
→ required correctness contract

transformation
→ DataFrame(s) → business DataFrame

writer
→ DataFrame → external persistence

configuration
→ environment/run-specific values

orchestration
→ sequence of application operations
```

### Core transformation principle

```python
def transform_orders(orders_df, customers_df):
    ...
    return result_df
```

The function should not need to know:

```text
filesystem paths
credentials
cloud project
scheduler
output location
```

unless one of those details is genuinely part of the transformation's explicit contract.

### Core configuration principle

```text
values that vary by environment/run
→ configuration

business behavior
→ code unless deliberately configurable

secrets
→ secure runtime identity/secret mechanism
```

### Core exception principle

```text
catch only when you can recover, add useful context, or translate the error
otherwise let failure propagate
```

### Core determinism principle

```text
same inputs + same parameters
→ same logical business output
```

### Core testability principle

```text
small deterministic DataFrames
    ↓
call transformation directly
    ↓
compare returned result
```

without needing production paths or credentials.

### Core packaging principle

```text
application code is an importable unit
not a collection of unrelated copied scripts
```

### Final Phase 9 reasoning question

Given a monolithic PySpark script, you should be able to answer:

```text
1. Which code is I/O?
2. Which code defines schemas?
3. Which code is validation?
4. Which code is business transformation?
5. Which code is orchestration?
6. Which values belong in configuration?
7. Which functions can become reusable DataFrame-in/DataFrame-out units?
8. Where should logging occur?
9. Where should exceptions be handled?
10. What behavior is currently non-deterministic?
11. What makes the code difficult to test?
12. What dependencies must be declared?
13. What should become an importable package?
14. Which refactorings improve maintainability without adding unnecessary abstraction?
```

If those answers are clear and the resulting application can be understood and tested by someone else, the architecture is serving its purpose.

The formal mastery gate should still be completed through applied implementation and explicit mastery review before Phase 9 is marked complete.

---

[Back to Table of Contents](#toc)
