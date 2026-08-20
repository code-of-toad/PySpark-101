# PySpark for Data Engineering — Final Mastery Curriculum

<a id="toc"></a>
## Table of Contents

- [Goal](#goal)
- [Phase 1 — DataFrame Fundamentals, Schemas & I/O](#phase-1-dataframe-fundamentals-schemas-and-i-o)
- [Phase 2 — Joins, Aggregations, Windows & Data Modeling](#phase-2-joins-aggregations-windows-and-data-modeling)
- [Phase 3 — Spark SQL](#phase-3-spark-sql)
- [Phase 4 — Spark's Execution Model](#phase-4-sparks-execution-model)
- [Phase 5 — Catalyst, Query Plans & AQE](#phase-5-catalyst-query-plans-and-aqe)
- [Phase 6 — Storage, Partitioning & Shuffle Engineering](#phase-6-storage-partitioning-and-shuffle-engineering)
- [Phase 7 — PySpark Performance Engineering](#phase-7-pyspark-performance-engineering)
- [Phase 8 — Spark UI, Monitoring & Debugging](#phase-8-spark-ui-monitoring-and-debugging)
- [Phase 9 — PySpark Application Architecture](#phase-9-pyspark-application-architecture)
- [Phase 10 — Data Quality & Schema Enforcement](#phase-10-data-quality-and-schema-enforcement)
- [Phase 11 — Testing PySpark](#phase-11-testing-pyspark)
- [Phase 12 — Incremental Processing & Idempotency](#phase-12-incremental-processing-and-idempotency)
- [Phase 13 — Cloud Spark](#phase-13-cloud-spark)
- [Phase 14 — Warehouse Integration & Analytical Serving](#phase-14-warehouse-integration-and-analytical-serving)
- [Phase 15 — Batch Pipeline Architecture & Orchestration](#phase-15-batch-pipeline-architecture-and-orchestration)
- [Phase 16 — Structured Streaming Fundamentals](#phase-16-structured-streaming-fundamentals)
- [Phase 17 — Modern Table Formats & Production Engineering](#phase-17-modern-table-formats-and-production-engineering)
- [Phase 18 — Capstone: Production-Style PySpark Data Platform](#phase-18-capstone-production-style-pyspark-data-platform)
- [What to Deprioritize](#what-to-deprioritize)
- [Final Mastery Standard](#final-mastery-standard)
- [Curriculum Progression](#curriculum-progression)

---

<a id="goal"></a>
## Goal

Become capable of **designing, implementing, debugging, testing, explaining, optimizing, and operating production-style PySpark data pipelines**.

The goal is not to memorize every PySpark function or become a Spark internals specialist for its own sake. The goal is to become a **data engineer who can use Spark confidently from raw ingestion through production operation**.

The progression is:

> **Write PySpark → Build correct pipelines → Understand execution → Diagnose performance → Optimize → Productionize**

---

[Back to Table of Contents](#toc)

---

<a id="phase-1-dataframe-fundamentals-schemas-and-i-o"></a>
# Phase 1 — DataFrame Fundamentals, Schemas & I/O

## Objective

Become completely comfortable reading, manipulating, and writing data with the PySpark DataFrame API.

## Learn

### Spark fundamentals
- `SparkSession`
- Creating DataFrames
- DataFrames vs. RDDs
- Transformations vs. actions
- Lazy evaluation

### Schemas
- Spark data types
- `StructType`
- `StructField`
- Explicit schemas
- Nullable fields
- Type casting
- Schema inference vs. schema enforcement

### Reading and writing
- CSV
- JSON
- Parquet
- Read modes
- Malformed records
- Writing datasets

### Core DataFrame operations
- `select()`
- `alias()`
- `filter()` / `where()`
- `withColumn()`
- `drop()`
- `distinct()`
- `dropDuplicates()`
- `orderBy()`
- `limit()`

### Expressions and functions
- `col()`
- `lit()`
- `when()` / `otherwise()`
- NULL handling
- String functions
- Numeric functions
- Date/timestamp functions
- Arrays
- Structs
- Nested data basics

## Storage concepts to introduce immediately

Understand at a high level:

- Row-oriented vs. columnar formats
- Why Parquet is commonly used for analytical pipelines
- Compression
- Schema preservation
- Column pruning

A deeper storage treatment comes later.

## Engineering emphasis

Prefer explicit schemas in controlled production pipelines because they make assumptions about incoming data visible and testable.

## Mastery project

Take messy raw retail data and produce a clean, typed dataset with:

- explicit schemas;
- standardized columns;
- valid records;
- rejected records;
- Parquet output.

---

[Back to Table of Contents](#toc)

---

<a id="phase-2-joins-aggregations-windows-and-data-modeling"></a>
# Phase 2 — Joins, Aggregations, Windows & Data Modeling

## Objective

Be able to express common batch data-engineering transformations while preserving the intended grain of the data.

## Aggregations

Learn:

- `groupBy()`
- `agg()`
- `count`
- `sum`
- `avg`
- `min`
- `max`
- `countDistinct`
- Conditional aggregation

## Joins

Learn:

- Inner joins
- Left joins
- Right joins
- Full joins
- Semi joins
- Anti joins
- Cross joins
- Multi-column joins
- Handling duplicate columns
- Join-key validation

## Window functions

Learn:

- `Window.partitionBy()`
- `orderBy()`
- `row_number`
- `rank`
- `dense_rank`
- `lag`
- `lead`
- Running aggregates
- Rolling windows

## Other transformation patterns

Learn:

- Deduplication
- Latest-record selection
- Conditional transformations
- `union()`
- `unionByName()`
- `explode()`
- Pivot/unpivot concepts

## Data modeling

Understand:

- Grain
- Facts vs. dimensions
- Transaction facts
- Snapshot facts
- Business keys
- Surrogate keys
- Star schemas
- Conformed dimensions
- Slowly changing dimensions conceptually
- Derived measures

## Engineering emphasis — grain preservation

Before every join, be able to answer:

> **What is the grain of each DataFrame, and can this join multiply rows?**

Before writing an output table, be able to answer:

> **What does one row represent?**

## Mastery project

Build analytical facts and dimensions from transactional source data.

Example:

```text
Raw Orders
Raw Order Items
Raw Products
Raw Stores
       ↓
     PySpark
       ↓
dim_product
dim_store
dim_date
fact_sales
fact_inventory_snapshot
```

---

[Back to Table of Contents](#toc)

---

<a id="phase-3-spark-sql"></a>
# Phase 3 — Spark SQL

## Objective

Become fluent moving between SQL and the DataFrame API.

## Learn

- `createOrReplaceTempView()`
- `spark.sql()`
- SQL/DataFrame equivalence
- CTEs
- Joins
- Aggregations
- Window functions
- Validation queries
- Reconciliation queries

## Engineering emphasis

Treat Spark SQL and the PySpark DataFrame API as two interfaces to the same execution engine.

You should be comfortable implementing a transformation either as:

```python
df.groupBy("store_id").agg(...)
```

or:

```sql
SELECT store_id, ...
FROM sales
GROUP BY store_id
```

## Mastery project

Implement the same analytical transformation pipeline twice:

1. DataFrame API
2. Spark SQL

Reconcile the outputs exactly.

---

[Back to Table of Contents](#toc)

---

<a id="phase-4-sparks-execution-model"></a>
# Phase 4 — Spark's Execution Model

## Objective

Understand what Spark actually does after you write PySpark code.

## Architecture

Learn:

- Driver
- Executors
- Cluster manager
- Worker nodes

```text
Spark Application
       │
     Driver
       │
   Executors
       │
     Tasks
```

## Execution hierarchy

```text
Application
   ↓
Jobs
   ↓
Stages
   ↓
Tasks
```

## Core concepts

Learn:

- Lazy evaluation
- Actions
- DAGs
- Lineage
- Partitions
- Parallelism
- Narrow transformations
- Wide transformations
- Shuffle boundaries
- Stage boundaries
- Task execution

## RDDs

Understand:

- What an RDD is
- Why DataFrames are generally preferred
- How RDDs relate conceptually to Spark's distributed execution model

Do **not** spend large amounts of time learning RDD programming.

## Mastery requirement

Given a PySpark pipeline, predict approximately:

- what triggers a Spark job;
- where shuffles occur;
- where stages split;
- which operations can execute in parallel.

---

[Back to Table of Contents](#toc)

---

<a id="phase-5-catalyst-query-plans-and-aqe"></a>
# Phase 5 — Catalyst, Query Plans & AQE

## Objective

Stop treating Spark execution as a black box.

## Learn

- Parsed logical plan
- Analyzed logical plan
- Optimized logical plan
- Physical plan
- Catalyst optimizer
- Adaptive Query Execution

Use:

```python
df.explain()
df.explain("formatted")
```

## Recognize operators such as

- `Scan`
- `Filter`
- `Project`
- `HashAggregate`
- `Exchange`
- `Sort`
- `BroadcastHashJoin`
- `SortMergeJoin`

## Mastery exercise

Take several real pipelines and explain:

> **Why did Spark choose this physical execution strategy?**

---

[Back to Table of Contents](#toc)

---

<a id="phase-6-storage-partitioning-and-shuffle-engineering"></a>
# Phase 6 — Storage, Partitioning & Shuffle Engineering

## Objective

Understand the relationship between Spark execution, physical files, and data distribution.

## File/storage engineering

Deepen your understanding of:

- Parquet
- Columnar storage
- Compression
- Schema preservation
- File statistics
- Column pruning
- Predicate pushdown
- Partition pruning
- File sizing
- Small-file problem

## Spark execution partitions

Learn:

- DataFrame partitions
- Input partitions
- Shuffle partitions
- Output partitions
- `repartition()`
- `coalesce()`
- Repartitioning by key
- Excessive partitioning
- Under-partitioning

## Storage partitioning

Understand directory-based partitioning such as:

```text
fact_sales/
├── year=2025/
└── year=2026/
```

and why:

```python
df.filter(col("year") == 2026)
```

may allow Spark to avoid scanning irrelevant partitions.

## Critical distinction

Understand the difference between:

> **Spark execution partitions**

and:

> **storage partitioning**

They are related concepts, but they are not the same thing.

## Shuffle engineering

Understand why operations such as:

```text
groupBy
join
distinct
orderBy
repartition
```

often require data redistribution.

## Mastery exercise

Inspect partition counts and query plans before and after:

- filters;
- joins;
- aggregations;
- repartitioning;
- coalescing;
- writes.

---

[Back to Table of Contents](#toc)

---

<a id="phase-7-pyspark-performance-engineering"></a>
# Phase 7 — PySpark Performance Engineering

## Objective

Learn to diagnose performance rather than randomly changing Spark configuration.

## I/O optimization

Learn:

- Column pruning
- Predicate pushdown
- Partition pruning
- Avoiding unnecessary reads
- Appropriate file sizing

## Join optimization

Learn:

- Broadcast joins
- Sort-merge joins
- Shuffle joins
- Join-key selection
- Dimension/fact join patterns

## Data distribution

Learn:

- Skew
- Hot keys
- Salting concept
- Repartitioning strategies

## Memory and reuse

Learn:

- Caching
- Persistence
- Cache storage levels conceptually
- When **not** to cache

## Adaptive Query Execution

Learn:

- AQE
- Dynamic partition coalescing
- Runtime join changes
- Skew handling

## Resource concepts

Only after understanding query/data problems, study:

- Executor memory
- Executor cores
- Driver memory
- Garbage collection
- Spill behavior
- Memory pressure

## Principle

> **Fix query and data design before tuning configuration.**

---

[Back to Table of Contents](#toc)

---

<a id="phase-8-spark-ui-monitoring-and-debugging"></a>
# Phase 8 — Spark UI, Monitoring & Debugging

## Objective

Be able to investigate a failed or poorly performing Spark job.

## Learn to inspect

- Jobs
- Stages
- Tasks
- Executors
- SQL/DataFrame queries
- Shuffle read/write
- Task duration
- Failed tasks
- Partition sizes
- Skew
- Spilled data
- Executor utilization

## Diagnostic workflow

```text
Something is slow
        ↓
Identify slow job
        ↓
Identify slow stage
        ↓
Inspect tasks
        ↓
Inspect query plan
        ↓
Identify shuffle / skew / I/O problem
        ↓
Change pipeline
        ↓
Measure again
```

## Mastery requirement

Be able to explain **why** a Spark job is slow rather than merely observing that it is slow.

---

[Back to Table of Contents](#toc)

---

<a id="phase-9-pyspark-application-architecture"></a>
# Phase 9 — PySpark Application Architecture

## Objective

Turn isolated transformations into maintainable data-engineering applications.

## Example structure

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

## Separate

- I/O
- Schemas
- Business logic
- Validation
- Orchestration
- Configuration

## Learn

- Reusable functions
- Parameterized pipelines
- Environment-specific configuration
- Logging
- Exception handling
- Deterministic behavior
- Dependency management
- Packaging

## Engineering principle

Transformation functions should ideally resemble:

```python
def transform_orders(orders_df, customers_df):
    ...
    return result_df
```

rather than embedding paths, credentials, and infrastructure details throughout the business logic.

## Mastery requirement

Someone unfamiliar with the original author should be able to understand, test, and maintain the pipeline.

---

[Back to Table of Contents](#toc)

---

<a id="phase-10-data-quality-and-schema-enforcement"></a>
# Phase 10 — Data Quality & Schema Enforcement

## Objective

Design pipelines that protect downstream systems from bad data.

## Learn

- Schema validation
- Required fields
- Business-rule checks
- Domain validation
- Range checks
- Primary-key uniqueness
- Composite-key validation
- Referential integrity
- Duplicate detection
- Accepted vs. rejected records
- Quarantine patterns
- Validation metrics
- Rejection reasons

## Pipeline pattern

```text
Raw
 ↓
Schema enforcement
 ↓
Data-quality validation
 ├── Accepted → transformations
 └── Rejected → quarantine
```

## Mastery project

Build a reusable data-quality layer rather than scattering ad hoc filters throughout a pipeline.

---

[Back to Table of Contents](#toc)

---

<a id="phase-11-testing-pyspark"></a>
# Phase 11 — Testing PySpark

## Objective

Treat PySpark transformations as software.

## Learn

- `pytest`
- Reusable Spark fixtures
- Unit testing transformation functions
- Integration testing
- Deterministic DataFrame comparisons
- Schema testing
- Row-count testing
- Grain testing
- Referential-integrity testing
- Numerical reconciliation
- Edge-case testing

## Engineering emphasis

Do not merely test:

> "The DataFrame was created."

Test business correctness:

> "Gross margin equals revenue minus cost for every accepted sales row."

## Mastery requirement

A pipeline change should be able to fail automated tests before it silently corrupts downstream data.

---

[Back to Table of Contents](#toc)

---

<a id="phase-12-incremental-processing-and-idempotency"></a>
# Phase 12 — Incremental Processing & Idempotency

## Objective

Move beyond full-refresh pipelines.

## Learn

- Full refresh
- Incremental batches
- Append processing
- High-water marks
- Watermark concepts
- Idempotency
- Deduplication
- Deterministic keys
- Late-arriving records
- Append vs. overwrite
- Partition overwrite
- Upserts
- `MERGE` patterns
- Backfills
- Replay safety

## Core engineering question

For every production pipeline, be able to answer:

> **What happens if this exact batch runs twice?**

## Mastery project

Take a working full-refresh pipeline and convert part of it to incremental processing while proving:

- no duplicate ingestion;
- correct replay behavior;
- correct reconciliation against historical data.

---

[Back to Table of Contents](#toc)

---

<a id="phase-13-cloud-spark"></a>
# Phase 13 — Cloud Spark

## Objective

Understand the transition from a local Spark application to distributed cloud infrastructure.

Choose **one cloud ecosystem** and learn it deeply.

## Learn

- Object storage
- Cloud IAM
- Service accounts / workload identities
- Managed Spark job submission
- Dependency packaging
- Environment configuration
- Logs
- Cluster/serverless execution
- Secrets and configuration principles
- Cost awareness

## Example — GCP

```text
Source
  ↓
GCS Raw
  ↓
Managed Spark
  ↓
GCS Curated
  ↓
BigQuery
```

## Platforms worth understanding conceptually

- Google Cloud Dataproc / Serverless Spark
- Databricks
- AWS EMR
- AWS Glue
- Azure Synapse / Fabric Spark

Do not try to master every platform.

Master Spark itself and one cloud implementation deeply enough to deploy and troubleshoot a real pipeline.

---

[Back to Table of Contents](#toc)

---

<a id="phase-14-warehouse-integration-and-analytical-serving"></a>
# Phase 14 — Warehouse Integration & Analytical Serving

## Objective

Understand where Spark processing ends and downstream analytical systems begin.

## Learn

- Spark → warehouse loads
- Warehouse → Spark reads
- JDBC concepts
- BigQuery / Snowflake / Redshift / similar warehouses
- Staging tables
- Dimensional warehouse models
- Partitioning
- Clustering
- Reconciliation
- Data marts conceptually

## Example

```text
Source
   ↓
Object Storage
   ↓
PySpark Transformation
   ↓
Curated Lake Data
   ↓
Warehouse
   ↓
Analytics / BI
```

## Engineering emphasis

Understand the responsibilities of:

- the data lake;
- Spark;
- the warehouse;
- downstream analytical consumers.

---

[Back to Table of Contents](#toc)

---

<a id="phase-15-batch-pipeline-architecture-and-orchestration"></a>
# Phase 15 — Batch Pipeline Architecture & Orchestration

## Objective

Understand Spark's role inside a complete data platform.

## Example architecture

```text
Sources
   ↓
Ingestion
   ↓
Raw Data Lake
   ↓
Schema Enforcement
   ↓
Data Quality
   ↓
PySpark Transformations
   ↓
Curated Data
   ↓
Warehouse
   ↓
Consumers
```

## Learn

- ETL vs. ELT
- Data lakes
- Data warehouses
- Lakehouse concepts
- Bronze / silver / gold
- Batch pipelines
- Orchestration
- Dependencies
- Scheduling
- Retries
- Backfills
- Lineage
- Observability

## Orchestration

Understand Spark's relationship with tools such as Airflow.

Spark performs distributed data processing.

An orchestrator coordinates **when, in what order, and under what conditions** pipeline jobs execute.

## Mastery requirement

Be able to explain where Spark belongs in a modern data platform — and where it does not.

---

[Back to Table of Contents](#toc)

---

<a id="phase-16-structured-streaming-fundamentals"></a>
# Phase 16 — Structured Streaming Fundamentals

## Objective

Extend batch Spark knowledge to unbounded data without allowing streaming to distract from batch mastery.

## Learn

- Structured Streaming
- Bounded vs. unbounded data
- Micro-batches
- Sources
- Sinks
- Checkpoints
- Triggers
- Output modes
- Event time
- Processing time
- Watermarks
- Late data
- Stateful processing
- Streaming aggregations
- Kafka concepts

## Example

```text
Kafka
  ↓
Spark Structured Streaming
  ↓
Validation / Transformation
  ↓
Lake / Warehouse
```

## Priority

Study this **after becoming strong with batch Spark**.

Most DataFrame knowledge transfers directly.

---

[Back to Table of Contents](#toc)

---

<a id="phase-17-modern-table-formats-and-production-engineering"></a>
# Phase 17 — Modern Table Formats & Production Engineering

## Objective

Understand the technologies and operational concerns that separate a demo pipeline from a production data platform.

## Modern table formats

Study:

- Delta Lake
- Apache Iceberg
- Apache Hudi

Understand the problems they solve:

- ACID transactions
- Schema evolution
- Upserts
- Snapshots
- Time travel
- Concurrent writers
- Metadata management

Do not begin by memorizing APIs. Begin with the engineering problems these technologies address.

## Production engineering

Study:

- Observability
- Metrics
- Logging
- Retries
- Failure recovery
- Backfills
- Replayability
- SLAs
- Lineage
- Schema evolution
- Deployment
- CI/CD
- Secrets
- IAM
- Cost management
- Operational documentation

## Advanced Spark topics to introduce selectively

Once the rest of the curriculum is strong, deepen your knowledge of:

- Executor sizing
- Serialization
- Spill behavior
- Garbage collection
- Advanced skew mitigation
- UDF performance
- Pandas UDFs
- JVM/Python boundary
- Spark Connect
- Deeper Catalyst internals
- Deeper RDD internals

These are **advanced extensions**, not prerequisites for productive data engineering.

---

[Back to Table of Contents](#toc)

---

<a id="phase-18-capstone-production-style-pyspark-data-platform"></a>
# Phase 18 — Capstone: Production-Style PySpark Data Platform

## Objective

Build one serious end-to-end PySpark system without following a tutorial.

## Architecture

```text
Source systems
      ↓
Raw object storage
      ↓
Explicit schemas
      ↓
PySpark validation
      ├── Rejected → quarantine
      │
      └── Accepted
             ↓
       Transformations
             ↓
     Curated Parquet /
       table format
             ↓
         Warehouse
             ↓
    Analytical consumers
```

## Required capabilities

The capstone should include:

### Data ingestion
- Multiple source tables
- Explicit schemas
- Multiple file formats where useful
- Intentionally malformed or invalid records

### Transformation
- Joins
- Aggregations
- Window functions
- Deduplication
- Dimensional modeling
- Facts and dimensions
- Derived measures

### Data quality
- Primary-key checks
- Composite-key checks
- Referential integrity
- Business-rule validation
- Accepted and rejected records
- Rejection reasons

### Storage
- Parquet
- Appropriate partitioning
- File-size awareness
- Partition pruning
- Predicate pushdown

### Spark execution
- Query-plan inspection
- Shuffle analysis
- Broadcast joins where appropriate
- Partition analysis
- Spark UI investigation

### Software engineering
- Modular application structure
- Configuration
- Logging
- Automated tests
- Deterministic behavior
- Documentation

### Production behavior
- Incremental processing
- Idempotency
- Safe retries
- Reconciliation
- Backfills
- Failure handling

### Infrastructure
- Cloud object storage
- Managed Spark execution
- Warehouse integration
- IAM / service accounts
- Environment-specific configuration

## Deliberate performance exercises

Create several problems intentionally:

- unnecessary shuffles;
- badly chosen joins;
- skewed keys;
- excessive partitions;
- too few partitions;
- excessive small files.

Then diagnose and correct them using:

- query plans;
- Spark UI;
- partition inspection;
- measured before/after results.

---

[Back to Table of Contents](#toc)

---

<a id="what-to-deprioritize"></a>
# What to Deprioritize

Until the core curriculum is strong, spend relatively little time on:

- Extensive RDD programming
- Scala
- MLlib
- GraphX
- Obscure Spark configuration flags
- Advanced custom UDF development
- Exotic serialization internals
- Kubernetes-specific Spark administration

These topics can be learned later if a role or project requires them.

---

[Back to Table of Contents](#toc)

---

<a id="final-mastery-standard"></a>
# Final Mastery Standard

At the end of this curriculum, you should be capable of being handed a requirement such as:

```text
Process 500 GB/day of transactional data
```

and reasoning intelligently about:

1. How the data should be ingested.
2. What schemas should be enforced.
3. How invalid data should be handled.
4. What the grain of each dataset should be.
5. How facts and dimensions should be modeled.
6. How transformations should be structured in PySpark.
7. Where joins can multiply rows.
8. Where shuffles will occur.
9. How Spark will divide the work into jobs, stages, and tasks.
10. How the data should be partitioned in memory and in storage.
11. Which file format should be used and why.
12. How to inspect the physical query plan.
13. How to diagnose poor performance using the Spark UI.
14. How to handle skew and expensive joins.
15. How to make the pipeline incremental.
16. How to guarantee reruns are safe.
17. How to test business correctness.
18. How to deploy the application to cloud infrastructure.
19. How to load or expose the result to a warehouse.
20. How to monitor, recover, backfill, and operate the pipeline in production.

If you can reason through those questions confidently and implement the corresponding pipeline, you are no longer merely someone who **knows PySpark syntax**.

You are using Spark as a **data engineer**.

---

[Back to Table of Contents](#toc)

---

<a id="curriculum-progression"></a>
# Curriculum Progression

```text
1. DataFrame fundamentals, schemas & I/O
        ↓
2. Joins, aggregations, windows & data modeling
        ↓
3. Spark SQL
        ↓
4. Spark execution model
        ↓
5. Catalyst, query plans & AQE
        ↓
6. Storage, partitioning & shuffle engineering
        ↓
7. Performance engineering
        ↓
8. Spark UI, monitoring & debugging
        ↓
9. PySpark application architecture
        ↓
10. Data quality & schema enforcement
        ↓
11. Testing PySpark
        ↓
12. Incremental processing & idempotency
        ↓
13. Cloud Spark
        ↓
14. Warehouse integration
        ↓
15. Batch architecture & orchestration
        ↓
16. Structured Streaming
        ↓
17. Modern table formats & production engineering
        ↓
18. Capstone
```

---

## Guiding Principle

The curriculum is organized around becoming a **data engineer who is highly capable with Spark**, not around studying Spark internals in isolation.

The intended learning order is:

1. **Learn to manipulate data correctly.**
2. **Learn to preserve grain and model useful datasets.**
3. **Learn SQL and the DataFrame API as complementary interfaces.**
4. **Understand how Spark executes your work.**
5. **Learn to inspect and optimize distributed execution.**
6. **Build maintainable, tested, reliable applications.**
7. **Move from full refreshes to safe incremental processing.**
8. **Deploy Spark in the cloud and integrate it with warehouses.**
9. **Learn production operations and streaming.**
10. **Deepen advanced internals only when they solve a real engineering problem.**

[Back to Table of Contents](#toc)
