# Phase 4 — Spark's Execution Model

<a id="toc"></a>
## Table of Contents

- [Objective](#objective)
- [Phase Mental Model](#phase-mental-model)
- [1. Spark Application Architecture](#1-spark-application-architecture)
- [2. Application → Jobs → Stages → Tasks](#2-application-jobs-stages-tasks)
- [3. Lazy Evaluation and Actions](#3-lazy-evaluation-and-actions)
- [4. DAGs and Lineage](#4-dags-and-lineage)
- [5. Partitions and Parallelism](#5-partitions-and-parallelism)
- [6. Narrow vs. Wide Transformations](#6-narrow-vs-wide-transformations)
- [7. Shuffles and Shuffle Boundaries](#7-shuffles-and-shuffle-boundaries)
- [8. Stage Boundaries](#8-stage-boundaries)
- [9. Task Execution](#9-task-execution)
- [10. Worked Execution Reasoning](#10-worked-execution-reasoning)
- [11. Conceptual RDD Understanding](#11-conceptual-rdd-understanding)
- [12. Local Mode vs. Cluster Mode](#12-local-mode-vs-cluster-mode)
- [13. Engineering Decision Rules](#13-engineering-decision-rules)
- [14. Common Failure Modes](#14-common-failure-modes)
- [15. Phase 4 Mastery Reference](#15-phase-4-mastery-reference)

---

<a id="objective"></a>
## Objective

Understand what Spark actually does **after PySpark code is written**.

Phase 4 covers:

- driver;
- executors;
- cluster manager;
- worker nodes;
- application → jobs → stages → tasks;
- lazy evaluation;
- actions;
- DAGs;
- lineage;
- partitions;
- parallelism;
- narrow transformations;
- wide transformations;
- shuffles;
- shuffle boundaries;
- stage boundaries;
- task execution;
- conceptual RDD understanding.

Examples target **PySpark 4.2.0**.

The priority is not memorizing Spark terminology independently. It is being able to look at a PySpark pipeline and reason approximately about:

```text
What is lazy?
What triggers execution?
Which dependencies are narrow or wide?
Where should a shuffle occur?
Where should stages split?
How do partitions relate to tasks?
What work can execute in parallel?
```

Phase 4 intentionally stops before detailed Catalyst, logical/physical-plan, operator-selection, and Adaptive Query Execution analysis. Those belong to **Phase 5 — Catalyst, Query Plans & AQE**.

This document is lecture/reference material only. The Phase 4 mastery gate is intentionally not performed here.

---

[Back to Table of Contents](#toc)

---

<a id="phase-mental-model"></a>
## Phase Mental Model

The central model is:

```text
PySpark program
      │
      │ transformations describe work
      ↓
lazy dependency graph
      │
      │ action requires a result
      ↓
Spark job
      │
      │ shuffle dependencies divide the work
      ↓
stages
      │
      │ each stage operates over partitions
      ↓
tasks
      │
      │ scheduled onto executors
      ↓
parallel distributed execution
```

At cluster level:

```text
Spark application
      │
    Driver
      │
      │ requests resources
      ↓
Cluster manager
      │
      │ allocates executor resources
      ↓
Worker nodes
      │
   Executors
      │
     Tasks
```

The most important Phase 4 habit is to stop reading PySpark as if every line executes immediately.

Given:

```python
result_df = (
    sales_df
    .filter(F.col('quantity') > 0)
    .withColumn(
        'gross_sales',
        F.col('quantity') * F.col('unit_price'),
    )
    .groupBy('store_id')
    .agg(F.sum('gross_sales').alias('gross_sales'))
)
```

ask:

```text
Has Spark executed the pipeline yet?
        ↓
No action yet → normally no data-processing job yet.

Which operations can stay partition-local?
        ↓
filter + withColumn

Which operation needs rows with the same key brought together?
        ↓
groupBy('store_id')

What does that imply?
        ↓
wide dependency → shuffle boundary → stage boundary

When does the work actually run?
        ↓
when an action demands the result
```

Phase 4 is about developing that execution reasoning before learning to inspect detailed physical plans in Phase 5.

---

[Back to Table of Contents](#toc)

---

<a id="1-spark-application-architecture"></a>
## 1. Spark Application Architecture

A Spark application is a set of cooperating processes that execute one Spark program.

The four architectural terms required in this phase are:

```text
Driver
Executors
Cluster manager
Worker nodes
```

### Driver

The **driver** is the control center for one Spark application.

It is responsible for work such as:

- running the application's main control flow;
- creating and owning the `SparkSession` / `SparkContext`;
- building the description of the computation;
- coordinating job and stage scheduling;
- sending tasks to executors;
- tracking task completion and failures;
- receiving results that are explicitly returned to the driver.

A useful mental model is:

```text
Driver = decides and coordinates what distributed work must happen.
```

The driver should not be confused with the machine that performs all data processing. Most distributed task work belongs on executors.

### Cluster manager

The **cluster manager** manages compute resources across the cluster and allocates resources to Spark applications.

Spark can work with cluster managers such as:

- Spark Standalone;
- Hadoop YARN;
- Kubernetes.

Conceptually:

```text
Driver:         I need resources for this Spark application.
Cluster manager: Here are resources on worker nodes.
```

The cluster manager is primarily about **resource allocation**, not implementing your `filter()`, `groupBy()`, or join logic.

### Worker nodes

A **worker node** is a machine in the cluster capable of running application code.

Worker nodes provide the CPU and memory resources on which executor processes run.

Do not equate:

```text
worker node = executor
```

A worker node is a machine/node. An executor is an application-specific process running on a node.

### Executors

An **executor** is a process allocated to one Spark application on a worker node.

Executors:

- run tasks;
- process partitions;
- can run multiple tasks concurrently according to available resources;
- store cached data for the application;
- participate in shuffle data exchange;
- report task status and results back to the driver.

Each Spark application normally receives its own executors rather than sharing one executor process across unrelated Spark applications.

### Architecture summary

| Component | Main responsibility | Think of it as |
|---|---|---|
| Driver | Coordinates one Spark application | control plane for the application |
| Cluster manager | Allocates cluster resources | resource allocator |
| Worker node | Provides machine resources | host machine |
| Executor | Runs application tasks | distributed worker process |
| Task | Executes one unit of stage work | partition-level work unit |

### Driver vs. executor responsibilities

A useful distinction is:

```text
Driver
- defines/coordinates work
- schedules tasks
- tracks execution
- can receive collected results

Executors
- run tasks
- process distributed partitions
- produce shuffle/output data
- report results/status
```

This distinction becomes important when deciding whether an operation remains distributed or pulls data back to the driver.

For example:

```python
rows = large_df.collect()
```

asks Spark to process the DataFrame in distributed fashion and then return **all resulting rows to the driver**. On large datasets, that can overwhelm driver memory.

---

[Back to Table of Contents](#toc)

---

<a id="2-application-jobs-stages-tasks"></a>
## 2. Application → Jobs → Stages → Tasks

Spark execution has a hierarchy:

```text
Application
   ↓
Jobs
   ↓
Stages
   ↓
Tasks
```

These terms describe different levels of work. They are not interchangeable.

### Application

A **Spark application** is the entire running Spark program associated with its driver and executors.

One application can execute many actions and therefore many jobs during its lifetime.

```text
one application
    ├── job 0
    ├── job 1
    ├── job 2
    └── ...
```

### Job

A **job** is a parallel computation spawned because Spark must produce a result for an action.

Typical actions include:

```python
df.count()
df.collect()
df.show()
df.write.parquet('...')
```

Conceptually:

```text
lazy transformations
       ↓
action
       ↓
job becomes necessary
```

Do not turn this into the rigid rule:

> one action always equals exactly one visible Spark job.

For Phase 4, the safe model is:

> **Actions trigger execution and spawn Spark jobs; exact job details can depend on Spark's implementation of the query.**

The curriculum asks for approximate prediction, not memorization of every internal job Spark may create.

### Stage

A **stage** is a set of tasks that Spark can execute without crossing another shuffle dependency inside that stage.

A job with no shuffle can often execute as one main stage.

A job containing a shuffle must split so that upstream shuffle output exists before downstream work can consume it.

```text
Stage 0
partition-local work
      │
      │ shuffle
      ↓
Stage 1
post-shuffle work
```

### Task

A **task** is the smallest unit of scheduled work sent to an executor.

Within one stage, Spark creates tasks over that stage's partitions.

A practical approximation is:

```text
one stage
    ↓
N partitions for that stage
    ↓
approximately N tasks for that stage
```

The word **approximately** matters because the relevant partition set can change between stages, especially after shuffles.

### Full hierarchy example

Suppose one application executes two actions:

```python
sales_df.count()
store_totals_df.collect()
```

Conceptually:

```text
Spark application
│
├── Job A: count sales
│   └── one or more stages
│       └── tasks over stage partitions
│
└── Job B: collect store totals
    └── one or more stages
        └── tasks over stage partitions
```

The transformation chain is not itself a sequence of jobs. **Actions create the demand for jobs.**

---

[Back to Table of Contents](#toc)

---

<a id="3-lazy-evaluation-and-actions"></a>
## 3. Lazy Evaluation and Actions

Spark transformations are normally **lazy**.

A transformation describes a new dataset but does not immediately process every input row.

### Transformations describe work

```python
positive_sales_df = sales_df.filter(
    F.col('net_sales') > 0
)
```

```python
store_sales_df = (
    positive_sales_df
    .groupBy('store_id')
    .agg(F.sum('net_sales').alias('net_sales'))
)
```

These statements define relationships between DataFrames.

Conceptually:

```text
sales_df
   ↓ filter
positive_sales_df
   ↓ groupBy + aggregate
store_sales_df
```

Spark remembers how `store_sales_df` can be produced from its ancestors.

### Actions demand a result

Execution becomes necessary when an action requires Spark to materialize a result.

Common actions include:

```python
store_sales_df.show()
store_sales_df.count()
store_sales_df.collect()
store_sales_df.write.parquet('output/path')
```

The exact destination differs:

```text
show / collect
    ↓
some or all result data returns toward the driver

write
    ↓
executors produce distributed output files
```

### Laziness includes expensive transformations

An operation can be logically expensive without executing immediately.

For example:

```python
repartitioned_df = sales_df.repartition('store_id')
```

is associated with redistribution, but the shuffle is not performed merely because the transformation was declared. It occurs only when downstream execution requires that result.

Likewise:

```python
aggregated_df = sales_df.groupBy('store_id').count()
```

defines a wide transformation, but the actual shuffle waits for execution.

### `cache()` / `persist()` do not normally materialize immediately

Calling:

```python
cached_df = sales_df.cache()
```

marks the DataFrame for caching. The cache is populated when an action actually computes the relevant partitions.

So:

```text
cache() declaration ≠ data already cached
```

Detailed caching strategy belongs to later performance phases; the Phase 4 point is about laziness.

### Why laziness matters

Lazy evaluation gives Spark freedom to consider the full requested computation before execution.

For Phase 4, the practical consequence is:

> **Do not reason about execution by assuming every Python line immediately performs distributed work.**

Before predicting jobs or stages, first locate the action.

---

[Back to Table of Contents](#toc)

---

<a id="4-dags-and-lineage"></a>
## 4. DAGs and Lineage

Spark tracks dependencies between datasets as a **directed acyclic graph (DAG)**.

### Directed

Dependencies have a direction:

```text
source
  ↓
filtered
  ↓
enriched
  ↓
aggregated
```

The aggregated result depends on the enriched data, which depends on the filtered data, which depends on the source.

### Acyclic

The dependency graph does not loop back into itself.

You cannot have:

```text
A depends on B
and
B depends on A
```

as one valid Spark lineage chain.

### Lineage

**Lineage** is the ancestry of transformations needed to produce a dataset.

Given:

```python
clean_df = raw_df.filter(F.col('is_valid'))
selected_df = clean_df.select('store_id', 'net_sales')
total_df = selected_df.groupBy('store_id').agg(
    F.sum('net_sales').alias('net_sales')
)
```

conceptually:

```text
raw_df
  ↓ filter
clean_df
  ↓ select
selected_df
  ↓ groupBy / aggregate
total_df
```

`total_df` carries a dependency history back to `raw_df`.

### Why lineage matters

Lineage matters for three major reasons in this phase.

#### 1. Spark knows what must be recomputed

If a result has not been persisted, Spark can follow its dependency chain back to the necessary source data and transformations.

#### 2. Repeated actions may repeat work

Given:

```python
result_df = expensive_source_df.filter(...).groupBy(...).agg(...)

result_df.count()
result_df.collect()
```

those are separate execution demands. Without persistence or another reusable materialization, Spark may recompute shared lineage.

#### 3. Dependencies determine stage structure

Narrow dependencies can often be pipelined together. Wide dependencies require redistribution and create the important boundaries that split execution into stages.

### DAG vs. detailed query plan

For Phase 4, think at the dependency level:

```text
filter → narrow
withColumn → narrow
groupBy → wide / shuffle
```

The detailed parsed, analyzed, optimized, and physical query plans belong to Phase 5.

---

[Back to Table of Contents](#toc)

---

<a id="5-partitions-and-parallelism"></a>
## 5. Partitions and Parallelism

A Spark dataset is divided into **partitions** for distributed execution.

A partition is a logical chunk of data that Spark can assign as a unit of work within a stage.

### Partitions are the basis of task parallelism

For one stage:

```text
Partition 0 → Task 0
Partition 1 → Task 1
Partition 2 → Task 2
Partition 3 → Task 3
```

If the cluster has enough available executor capacity, those tasks can run concurrently.

If there are more runnable tasks than available task slots/cores, tasks run in waves.

### Deliberately establish partition counts

Do not assume a small DataFrame has one partition.

For a deterministic teaching example, create the input with a known partition count:

```python
from pyspark.sql import functions as F

# spark.range() lets us establish four input partitions without first adding
# an unrelated repartition shuffle.
base_df = spark.range(
    start=0,
    end=12,
    step=1,
    numPartitions=4,
)

# This is metadata inspection; it does not require collecting all rows.
input_partition_count = base_df.rdd.getNumPartitions()
```

Here the input is deliberately established as:

```text
12 rows
4 input partitions
```

That makes later reasoning about the first stage concrete.

### More partitions do not automatically mean faster

Parallelism is constrained by resources.

Suppose:

```text
stage partitions = 100
available concurrent task slots = 8
```

Spark cannot run all 100 tasks simultaneously. They execute in waves.

Likewise:

```text
stage partitions = 2
available concurrent task slots = 32
```

only two partition tasks are available, so most capacity cannot help that stage.

A useful approximation is:

```text
actual concurrent work
≈ min(runnable partition tasks, available executor task capacity)
```

This is a reasoning model, not a performance-tuning formula.

### Partition counts can change between stages

A shuffle redistributes data into a new set of partitions.

Therefore:

```text
input stage has 4 partitions
```

 does **not** prove:

```text
all later stages also have 4 partitions
```

Never infer downstream task counts from an earlier partition count unless the downstream partitioning has also been established.

### Execution partitions vs. storage partitions

Phase 4 focuses on **Spark execution partitions**.

Do not confuse them with directory-based storage partitioning such as:

```text
fact_sales/
├── year=2025/
└── year=2026/
```

That distinction is treated deeply in Phase 6.

For now:

```text
execution partition
= unit of distributed Spark work

storage partition
= physical/data-layout organization
```

They can influence one another, but they are not the same concept.

---

[Back to Table of Contents](#toc)

---

<a id="6-narrow-vs-wide-transformations"></a>
## 6. Narrow vs. Wide Transformations

The narrow/wide distinction explains whether Spark can continue processing partitions locally or must redistribute data.

### Narrow dependency

A dependency is **narrow** when each child partition depends on only a limited number of parent partitions and Spark can process the dependency without globally redistributing records among partitions.

Typical partition-local DataFrame operations are conceptually narrow:

```python
filtered_df = sales_df.filter(
    F.col('quantity') > 0
)

selected_df = filtered_df.select(
    'store_id',
    'product_id',
    'quantity',
)

enriched_df = selected_df.withColumn(
    'is_large_order',
    F.col('quantity') >= 10,
)
```

Conceptually:

```text
Parent partition 0 → child partition 0
Parent partition 1 → child partition 1
Parent partition 2 → child partition 2
```

Each partition can do its own filtering/projection/expression work without needing rows from every other partition.

### Why narrow operations can be pipelined

Suppose one partition contains 1,000 sales rows.

Spark can conceptually execute:

```text
read partition
    ↓
filter rows
    ↓
compute derived column
    ↓
project columns
```

inside one stage because no cross-partition redistribution is required between those operations.

A chain of three transformations does **not** imply three stages.

### Wide dependency

A dependency is **wide** when a child partition depends on data coming from many parent partitions.

This usually means records must be redistributed.

Common conceptual examples include:

```text
groupBy / aggregation by key
distinct
global orderBy
repartition
many ordinary joins
```

For example:

```python
store_sales_df = sales_df.groupBy('store_id').agg(
    F.sum('net_sales').alias('net_sales')
)
```

If rows for `S01` exist in several input partitions, the final `S01` aggregate cannot be computed correctly from just one original partition.

Spark must bring matching keys together.

### Narrow vs. wide summary

| Dependency | Requires cross-partition redistribution? | Can often stay in same stage? | Typical examples |
|---|---:|---:|---|
| Narrow | No global redistribution | Yes | `filter`, `select`, many `withColumn` expressions |
| Wide | Usually yes | No across the shuffle boundary | `groupBy`, `distinct`, `repartition`, global sort |

### Important join caveat

A join is **not** safely classified by saying:

> all joins always shuffle both sides.

Many joins require redistribution, but Spark can use different execution strategies depending on data distribution and query planning.

For Phase 4, the right question is:

> **Would matching rows need to be brought together if the inputs are not already suitably distributed?**

Detailed join strategy selection belongs to later query-plan and performance phases.

---

[Back to Table of Contents](#toc)

---

<a id="7-shuffles-and-shuffle-boundaries"></a>
## 7. Shuffles and Shuffle Boundaries

A **shuffle** redistributes records across partitions so that downstream work has the data grouping or ordering it requires.

### Why shuffles exist

Suppose the input is deliberately distributed like this:

```text
Partition 0: S01, S02, S01
Partition 1: S02, S01, S02
Partition 2: S01, S02, S01
Partition 3: S02, S01, S02
```

Now calculate:

```python
sales_df.groupBy('store_id').agg(
    F.sum('net_sales').alias('net_sales')
)
```

No single input partition contains all `S01` rows or all `S02` rows.

Spark must redistribute records by the grouping key:

```text
Before shuffle
P0: S01 S02 S01
P1: S02 S01 S02
P2: S01 S02 S01
P3: S02 S01 S02

          ↓ redistribute by key

After shuffle
Q0: rows needed for one key range/bucket
Q1: rows needed for another key range/bucket
...
```

Only after compatible keys are brought together can the downstream aggregate finish correctly.

### Shuffle boundary

A **shuffle boundary** is the point where upstream partition-local processing must stop and redistributed data must be made available to downstream processing.

Conceptually:

```text
filter
  ↓ narrow
withColumn
  ↓ narrow
groupBy
  ↓
========== SHUFFLE BOUNDARY ==========
  ↓
finish grouped aggregation
```

### Shuffle work is materially different from narrow work

A shuffle can involve:

- partitioning records for downstream destinations;
- writing intermediate shuffle data;
- transferring data between executors/nodes;
- reading shuffle data downstream;
- waiting for required upstream shuffle outputs.

This is why shuffles are central to distributed Spark performance.

Phase 4 focuses on recognizing **where and why** they happen. Detailed shuffle tuning belongs to later phases.

### Operations that commonly imply shuffle reasoning

When you see these, ask whether data must move:

```text
groupBy
join
distinct
orderBy
repartition
```

Do not memorize the list mechanically. Ask:

> **Can each output partition be produced correctly from only its local parent partition data?**

If not, a wide dependency and shuffle are likely involved.

---

[Back to Table of Contents](#toc)

---

<a id="8-stage-boundaries"></a>
## 8. Stage Boundaries

A Spark job is divided into stages around shuffle dependencies.

The core Phase 4 rule is:

> **Narrow transformations can usually be pipelined within a stage; a shuffle dependency creates a stage boundary.**

### No shuffle: one conceptual stage

```text
read
 ↓
filter
 ↓
select
 ↓
withColumn
 ↓
action
```

All transformation dependencies are narrow.

Conceptually, Spark can pipeline the partition-local work inside one main stage.

### One shuffle: two conceptual stages

```text
read
 ↓
filter
 ↓
withColumn
 ↓
groupBy
 ↓
========== shuffle ==========
 ↓
finish aggregate
 ↓
action
```

Conceptually:

```text
Stage 0
- read
- filter
- derived column
- produce shuffle output

Stage 1
- read shuffled data
- finish aggregate
- produce action result
```

### Two shuffle boundaries: roughly three stage regions

```text
read
 ↓
filter
 ↓
groupBy
 ↓
========== shuffle 1 ==========
 ↓
aggregate result
 ↓
orderBy
 ↓
========== shuffle 2 ==========
 ↓
ordered result
 ↓
action
```

The important skill is to identify the boundaries before worrying about exact Spark UI stage IDs.

### Stage count is not transformation count

This pipeline has several transformations:

```python
result_df = (
    sales_df
    .filter(F.col('quantity') > 0)
    .select('store_id', 'quantity', 'unit_price')
    .withColumn(
        'gross_sales',
        F.col('quantity') * F.col('unit_price'),
    )
)
```

but no obvious wide dependency.

Do not predict:

```text
filter = stage 1
select = stage 2
withColumn = stage 3
```

That is the wrong model.

### Why stage prediction is approximate for DataFrames

DataFrame code describes relational work at a high level. Spark can rewrite and choose how to execute that work.

Therefore Phase 4 predictions should be stated like:

```text
Expected shuffle here.
Expected stage split here.
Expected partition-local pipeline here.
```

not:

```text
This must always be exactly Stage ID 4 with exactly 17 tasks.
```

Detailed physical-plan verification is deliberately deferred to Phase 5.

---

[Back to Table of Contents](#toc)

---

<a id="9-task-execution"></a>
## 9. Task Execution

A stage becomes real distributed work through **tasks**.

### One task handles one stage partition

For a stage with four partitions:

```text
Stage
├── Task 0 → partition 0
├── Task 1 → partition 1
├── Task 2 → partition 2
└── Task 3 → partition 3
```

The tasks perform equivalent stage logic on different partitions.

### Executors run tasks

Suppose an application has two executors and enough cores for four concurrent tasks:

```text
Executor A
├── Task 0
└── Task 1

Executor B
├── Task 2
└── Task 3
```

Those tasks can run in parallel because their stage dependencies are satisfied and there is executor capacity available.

### More partitions than available capacity

Suppose a stage has eight partitions but capacity for four concurrent tasks.

Conceptually:

```text
Wave 1: tasks 0–3
Wave 2: tasks 4–7
```

The exact scheduling order is not important here. The important point is that partitions expose potential parallel work, while executor resources limit how much of that work can run simultaneously.

### Stage dependencies constrain parallelism

Tasks in a downstream post-shuffle stage cannot simply run as though upstream shuffle output already exists.

Conceptually:

```text
upstream stage tasks
        ↓
shuffle outputs become available
        ↓
downstream stage tasks
```

So Spark parallelism exists **within dependency constraints**.

### Tasks can be retried

Tasks can fail and be retried.

This has an important engineering consequence:

> Code executed as distributed task work should not rely on unsafe one-time side effects.

A task may execute more than once under failure/retry conditions. Production-safe side-effect and idempotency design is treated more deeply in later phases.

### Driver-side vs. task-side work

Keep the conceptual separation clear:

```text
Driver
- coordinates
- schedules
- receives requested results

Tasks on executors
- process partitions
- perform distributed computation
```

If you use `collect()`, distributed tasks still perform the computation, but the final rows are returned to driver memory.

---

[Back to Table of Contents](#toc)

---

<a id="10-worked-execution-reasoning"></a>
## 10. Worked Execution Reasoning

Use deliberately constructed data so the execution concepts are observable rather than accidental.

### Establish four input partitions without an unrelated shuffle

```python
from pyspark.sql import functions as F

# Create 12 deterministic rows across exactly four source partitions.
# Using spark.range(..., numPartitions=4) establishes the input partition count
# directly instead of calling repartition() and accidentally introducing the
# very shuffle we want to study later.
sales_df = (
    spark.range(
        start=0,
        end=12,
        step=1,
        numPartitions=4,
    )
    .select(
        F.col('id').alias('sale_id'),
        # Alternating store IDs ensure each store appears across multiple input
        # partitions, so grouping by store genuinely requires redistribution.
        F.when(
            (F.col('id') % 2) == 0,
            F.lit('S01'),
        )
        .otherwise(F.lit('S02'))
        .alias('store_id'),
        (F.col('id') + F.lit(1)).alias('quantity'),
    )
)
```

Known input property:

```text
input partitions = 4
```

The grouping key is deliberately spread across those partitions:

```text
S01 appears in multiple partitions
S02 appears in multiple partitions
```

### Define the pipeline

```python
result_df = (
    sales_df
    .filter(
        F.col('quantity') >= 3
    )
    .withColumn(
        'line_value',
        F.col('quantity') * F.lit(10),
    )
    .groupBy('store_id')
    .agg(
        F.sum('line_value').alias('store_value')
    )
)
```

No action has occurred yet.

Now trigger execution:

```python
result_rows = result_df.collect()
```

### Question 1 — What is lazy?

These operations describe work but do not by themselves require the full computation to run:

```text
spark.range relation
select
a column expression
filter
withColumn
groupBy / agg declaration
```

The exact query representation is an internal Spark concern; the important Phase 4 point is that the DataFrame transformation chain remains lazy until execution is demanded.

### Question 2 — What triggers execution?

```python
result_df.collect()
```

is the action that requires the result.

Therefore it triggers distributed execution.

### Question 3 — Which dependencies are narrow or wide?

Conceptually:

```text
select        → narrow
filter        → narrow
withColumn    → narrow
groupBy       → wide
```

The first three operations can work partition-by-partition.

The grouped result cannot, because all rows for each `store_id` must contribute to the same logical group even though those rows began in different partitions.

### Question 4 — Where should a shuffle occur?

At the grouping dependency:

```text
4 input partitions
      ↓
select
      ↓
filter
      ↓
withColumn
      ↓
groupBy('store_id')
      ↓
========== SHUFFLE ==========
      ↓
finish grouped sums
```

### Question 5 — Where should stages split?

The shuffle creates the important stage boundary.

Conceptually:

```text
Stage region 1
- process the four input partitions
- execute narrow transformations
- prepare shuffle output

========== shuffle boundary ==========

Stage region 2
- read redistributed rows
- finish grouped aggregation
- return the action result
```

Do not claim an exact Spark UI stage ID from this reasoning alone.

### Question 6 — How do partitions relate to tasks?

The first stage starts from a deliberately established four-partition input, so approximately four partition tasks are available for that stage's input work.

Do **not** assume the post-shuffle stage also has four tasks unless its shuffle partition count is separately established or observed.

This is the correct discipline:

```text
known partition count → reason about task count
unknown partition count → do not invent task count
```

### Question 7 — What work can execute in parallel?

The four first-stage partition tasks are independent enough to run concurrently once scheduled, subject to available executor capacity.

If this were run with four available task slots, Spark could potentially process all four first-stage partitions at once.

The downstream aggregation stage cannot simply finish before the required upstream shuffle data exists.

### Compare with a narrow-only pipeline

```python
narrow_only_df = (
    sales_df
    .filter(
        F.col('quantity') >= 3
    )
    .withColumn(
        'line_value',
        F.col('quantity') * F.lit(10),
    )
    .select(
        'sale_id',
        'store_id',
        'line_value',
    )
)

narrow_only_count = narrow_only_df.count()
```

Reasoning:

```text
lazy until count()
        ↓
count() triggers execution
        ↓
filter / withColumn / select are partition-local
        ↓
no grouping redistribution is required by that chain
        ↓
expect no shuffle boundary from those transformations
        ↓
expect them to pipeline within the same stage region
```

This contrast—**same input, one version narrow-only, one version wide**—is the foundation for the later experiment notebook.

---

[Back to Table of Contents](#toc)

---

<a id="11-conceptual-rdd-understanding"></a>
## 11. Conceptual RDD Understanding

An **RDD** is a **Resilient Distributed Dataset**.

At a high level, it is:

```text
an immutable distributed collection
        +
partitioned across Spark execution
        +
transformed through dependency relationships
        +
recoverable through lineage
```

### Why RDDs matter conceptually in Phase 4

RDD terminology exposes Spark's distributed execution foundations very directly:

```text
RDD partitions
RDD dependencies
RDD lineage
narrow dependencies
wide dependencies
```

Those ideas help explain why DataFrame work becomes partitioned tasks separated by shuffle boundaries.

### Why DataFrames are generally preferred

For data engineering, use the DataFrame API by default because DataFrames provide:

- named columns;
- schemas and Spark SQL types;
- relational transformations;
- integration with Spark SQL;
- substantially more opportunity for Spark to optimize execution than arbitrary low-level RDD code.

Detailed query optimization belongs to Phase 5. The Phase 4 takeaway is simply:

> **RDDs are useful for understanding Spark's distributed dependency model; DataFrames are usually the better production-facing abstraction for structured data engineering.**

### Do not use the false shortcut

Avoid saying:

> A DataFrame is just an RDD with column names.

That hides important structured-query semantics and optimizer behavior.

A better model is:

```text
RDD concepts
help explain
partitions + dependencies + lineage

DataFrame API
is the preferred structured interface
for most data-engineering work
```

### Minimal RDD exposure

You may occasionally inspect partition metadata through the underlying classic Spark interface:

```python
partition_count = df.rdd.getNumPartitions()
```

The goal is **not** to rewrite Phase 4 exercises using `map()`, `flatMap()`, `reduceByKey()`, and large amounts of RDD code.

Use RDDs only where they clarify the execution model.

---

[Back to Table of Contents](#toc)

---

<a id="12-local-mode-vs-cluster-mode"></a>
## 12. Local Mode vs. Cluster Mode

Most learning experiments can run locally while still demonstrating the core scheduling model.

### Local mode

A session configured with:

```python
spark = (
    SparkSession.builder
    .appName('phase04-execution-model')
    .master('local[4]')
    .getOrCreate()
)
```

asks Spark to run locally with four worker threads available for task execution.

This is useful because:

- partitions still exist;
- transformations are still lazy;
- actions still trigger jobs;
- shuffles still create execution boundaries;
- stages and tasks are still visible;
- multiple tasks can execute concurrently.

But local mode does **not** create a real multi-machine cluster.

### Cluster mode

On a cluster, the conceptual architecture is physically separated across machines/processes:

```text
Driver
   ↓
cluster manager
   ↓
worker nodes
   ↓
executors
   ↓
tasks over distributed partitions
```

Now shuffle data can genuinely move across executors on different nodes, network distance matters, and executor resources are distributed across machines.

### What local experiments can prove

Local experiments can demonstrate:

```text
lazy evaluation
jobs
stages
tasks
partitions
narrow dependencies
wide dependencies
shuffles
```

They cannot fully reproduce:

```text
real network behavior across worker machines
cluster-manager resource contention
multi-node executor failure patterns
production cluster resource allocation
```

That distinction keeps Phase 4 experiments useful without pretending a laptop is a production cluster.

---

[Back to Table of Contents](#toc)

---

<a id="13-engineering-decision-rules"></a>
## 13. Engineering Decision Rules

1. **Find the action first.** Before discussing jobs, ask what actually demands a result.
2. **Do not equate Python lines with execution steps.** Transformations can accumulate lazily before any distributed job runs.
3. **Classify dependencies, not method names alone.** Ask whether downstream work can use partition-local data or needs redistribution.
4. **Expect stage boundaries at shuffle dependencies.** Narrow operations can usually pipeline together.
5. **Establish partition counts before claiming task counts.** Never invent a number because the dataset looks small.
6. **Treat partitions as potential parallel work, not guaranteed simultaneous work.** Executor capacity limits concurrency.
7. **Remember that partition counts can change after shuffles.** One stage's partition count does not automatically carry into the next.
8. **Separate the driver from executors.** The driver coordinates; executors perform distributed task work.
9. **Separate worker nodes from executors.** A node is a machine; an executor is an application process hosted on a node.
10. **Do not assume every join shuffles in the same way.** Predict the redistribution requirement conceptually; inspect the actual strategy later.
11. **Do not use `collect()` casually on large data.** Distributed execution can still end by overwhelming driver memory.
12. **Treat shuffle prediction as an engineering skill.** Expensive distributed behavior is often determined by where data must move.
13. **Keep Phase 4 predictions approximate.** Detailed physical-plan certainty belongs to Phase 5.
14. **Use RDDs to clarify fundamentals, not as the default structured-data API.**
15. **Predict before observing.** Later, compare your prediction to Spark UI and query-plan evidence rather than reasoning backward from the answer.

A repeatable execution-prediction checklist is:

```text
1. What action triggers execution?
2. What lineage must be computed for that action?
3. How many input partitions are actually established?
4. Which dependencies are narrow?
5. Which dependencies are wide?
6. Where must data shuffle?
7. Where should stages split?
8. What partition set feeds each stage?
9. Approximately how many tasks does that imply?
10. Which tasks can run concurrently given available resources?
```

---

[Back to Table of Contents](#toc)

---

<a id="14-common-failure-modes"></a>
## 14. Common Failure Modes

### Assuming transformations execute immediately

```python
filtered_df = df.filter(...)
```

normally defines new lazy work. It does not mean every row has already been filtered.

### Assuming every DataFrame method creates a job

Jobs are driven by execution demands such as actions, not by counting transformation calls.

### Assuming every transformation creates a stage

Several narrow transformations can pipeline inside one stage.

### Treating actions and stages as synonyms

An action triggers a job. That job can contain one stage or multiple stages.

### Treating shuffle and stage boundary as unrelated

Shuffle dependencies are the key reason Spark must separate upstream and downstream work into different stages.

### Assuming a shuffle happens when the transformation line is reached

Even a wide transformation remains lazy until an action requires its output.

### Assuming all joins always shuffle both inputs

Many joins require redistribution, but the actual strategy depends on how Spark can execute the query. Detailed join plans come later.

### Assuming a partition is a CPU core

A partition is a unit of data/work. A core/task slot is execution capacity. They are related through scheduling, but they are not the same thing.

### Assuming a task is permanently tied to one dataset partition

Tasks are created **per stage** over that stage's partitions. A shuffle can create a new partition set for the next stage.

### Assuming four input partitions means four tasks everywhere

Four input partitions justify approximately four first-stage input tasks. They do not prove the task count of a downstream post-shuffle stage.

### Assuming more partitions always means more speed

Too few partitions can limit parallelism, while excessive partitions can create scheduling and overhead costs. Partition engineering is treated later.

### Confusing execution partitions with storage partitions

Spark execution partitions are units of distributed work. Directory partitions such as `year=2026/` are storage layout. Phase 6 treats the distinction deeply.

### Confusing a worker node with an executor

A worker node is a machine capable of hosting application code. An executor is a process allocated to one Spark application.

### Assuming the driver performs all computation

The driver coordinates work. Executors run distributed tasks over partitions.

### Treating `local[*]` as a real cluster

Local mode can demonstrate jobs, stages, tasks, partitions, shuffles, and concurrency, but it does not reproduce multi-machine network and cluster-manager behavior.

### Assuming `cache()` has already materialized the data

Caching is still lazy until an action computes the relevant data.

### Thinking RDD mastery requires extensive RDD programming

Phase 4 requires conceptual RDD understanding, not a detour into low-level RDD application development.

### Drifting into Phase 5 too early

Phase 4 should answer:

```text
Where should execution boundaries exist?
```

Phase 5 will answer questions such as:

```text
What logical and physical plan did Spark actually choose?
Why did it choose particular operators?
How did AQE alter execution?
```

Keep those layers distinct.

---

[Back to Table of Contents](#toc)

---

<a id="15-phase-4-mastery-reference"></a>
## 15. Phase 4 Mastery Reference

The curriculum mastery requirement is eventually to take a PySpark pipeline and predict approximately:

- what triggers a Spark job;
- where shuffles occur;
- where stages split;
- which operations can execute in parallel.

That mastery gate is intentionally deferred until explicitly requested.

Before beginning it, Phase 4 fluency should include being able to explain:

### Architecture

- what the driver does;
- what executors do;
- what the cluster manager does;
- what worker nodes are;
- why worker nodes and executors are not synonyms;
- why the driver and executors have different responsibilities.

### Execution hierarchy

- what one Spark application represents;
- why actions trigger jobs;
- how jobs divide into stages;
- how stages divide into tasks;
- why one action should not be simplistically equated with exactly one visible job;
- why one transformation should not be equated with one stage.

### Laziness, DAGs, and lineage

- which code is lazy;
- which operations are actions;
- what a DAG represents;
- what lineage represents;
- why repeated actions can recompute lineage when results are not persisted;
- why dependency structure matters to execution.

### Partitions and parallelism

- what an execution partition is;
- why partitions expose task-level parallel work;
- how stage partition counts relate approximately to stage task counts;
- why available executor capacity limits concurrent task execution;
- why partition counts can change between stages;
- why execution partitioning is not the same as storage partitioning.

### Narrow and wide dependencies

- why `filter`, projection, and ordinary derived-column expressions are conceptually narrow;
- why grouping by a key spread across partitions is wide;
- why wide dependencies require redistribution;
- why several narrow transformations can pipeline inside one stage.

### Shuffles and stages

- what a shuffle accomplishes;
- why a shuffle boundary creates a stage boundary;
- why grouped keys may need to move across partitions;
- why shuffle work is materially more expensive than partition-local work;
- why detailed shuffle tuning is intentionally deferred.

### Task execution

- what one task represents;
- where tasks run;
- how tasks map to stage partitions;
- what can run in parallel;
- why downstream stages depend on upstream stage outputs;
- why retries make task-side side effects dangerous.

### RDDs

- what an RDD is;
- why RDD concepts help explain partitions, dependencies, and lineage;
- why DataFrames are generally preferred for structured data engineering;
- why Phase 4 does not require extensive RDD programming.

### Final reasoning standard

Given a pipeline such as:

```python
result_df = (
    sales_df
    .filter(F.col('order_status') == 'COMPLETED')
    .withColumn(
        'gross_sales',
        F.col('quantity') * F.col('unit_price'),
    )
    .groupBy('store_id')
    .agg(F.sum('gross_sales').alias('gross_sales'))
)

result_df.show()
```

you should be able to say, before running it:

```text
1. The transformation chain is lazy.
2. show() demands a result and triggers execution.
3. filter and withColumn are conceptually narrow.
4. groupBy is wide when matching store keys span partitions.
5. That grouping requires a shuffle.
6. The shuffle creates the important stage boundary.
7. Tasks execute over each stage's partitions.
8. Independent tasks in the same ready stage can run in parallel,
   limited by available executor capacity.
9. Exact downstream partition/task counts should not be invented unless
   those counts have been established.
10. Exact physical operators and Catalyst/AQE behavior belong to Phase 5.
```

The final Phase 4 standard is not:

> I know the definitions of driver, stage, task, and shuffle.

It is:

> **I can read a PySpark pipeline and form a defensible mental prediction of how Spark will turn lazy transformations into distributed jobs, stages, shuffles, and partition-level tasks.**

---

[Back to Table of Contents](#toc)
