# Phase 4 — Experiment & Applied Practice Guide

<a id="toc"></a>
## Table of Contents

- [Purpose](#purpose)
- [Conventions](#conventions)
- [Practice Dataset](#practice-dataset)
- [Experiment 1 — Architecture and Local-Mode Mental Mapping](#experiment-1-architecture-and-local-mode-mental-mapping)
- [Experiment 2 — Lazy Evaluation and Actions](#experiment-2-lazy-evaluation-and-actions)
- [Experiment 3 — Lineage and Repeated Execution Demands](#experiment-3-lineage-and-repeated-execution-demands)
- [Experiment 4 — Partitions as Units of Distributed Work](#experiment-4-partitions-as-units-of-distributed-work)
- [Experiment 5 — Narrow Transformations and Partition-Local Work](#experiment-5-narrow-transformations-and-partition-local-work)
- [Experiment 6 — Wide Transformation and a Genuine Shuffle](#experiment-6-wide-transformation-and-a-genuine-shuffle)
- [Experiment 7 — Shuffle Boundaries and Stage Boundaries](#experiment-7-shuffle-boundaries-and-stage-boundaries)
- [Experiment 8 — Multiple Wide Boundaries](#experiment-8-multiple-wide-boundaries)
- [Experiment 9 — Tasks, Partitions, and Parallelism](#experiment-9-tasks-partitions-and-parallelism)
- [Experiment 10 — Multiple Actions, Reuse, and Cache Laziness](#experiment-10-multiple-actions-reuse-and-cache-laziness)
- [Experiment 11 — Join Redistribution Reasoning](#experiment-11-join-redistribution-reasoning)
- [Experiment 12 — Minimal RDD Bridge](#experiment-12-minimal-rdd-bridge)
- [Experiment 13 — Execution Prediction Drills](#experiment-13-execution-prediction-drills)
- [Applied Phase 4 Project](#applied-phase-4-project)
- [Applied Task — Part 1](#applied-task-part-1)
- [After Part 1](#after-part-1)

---

<a id="purpose"></a>
## Purpose

This file is the execution-oriented companion for **Phase 4 — Spark's Execution Model**.

The Phase 4 README is the conceptual reference. `phase_04_lecture.py` is the consolidated lecture implementation. This guide turns those ideas into small experiments where Spark's distributed execution model is deliberately made observable.

The central skill is **prediction before observation**.

For every important pipeline, ask:

```text
What is lazy?
What triggers execution?
Which dependencies are narrow or wide?
Where should a shuffle occur?
Where should stages split?
How do partitions relate to tasks?
What work can execute in parallel?
```

The working model is:

```text
PySpark transformations
        │
        │ describe work lazily
        ↓
lineage / dependency graph
        │
        │ action demands a result
        ↓
job
        │
        │ shuffle dependencies divide work
        ↓
stages
        │
        │ one task per stage partition
        ↓
tasks scheduled on executors
```

This guide deliberately does **not** teach detailed Catalyst internals, physical-plan operators, Adaptive Query Execution decisions, or join-strategy selection. Those belong to Phase 5 and later performance phases.

The applied section also does **not** perform the Phase 4 mastery gate. It gives a fresh pipeline to reason about independently and stops before phase completion.

---

[Back to Table of Contents](#toc)

---

<a id="conventions"></a>
## Conventions

- Use **PySpark 4.2.0**.
- Use **single quotes** for Python strings.
- Use `from pyspark.sql import functions as F`.
- Include inline comments explaining both **what** the code does and **why** the observation matters.
- Use `master('local[4]')` so local experiments have four task-execution threads available.
- Establish partition counts deliberately rather than assuming Spark chose a particular count.
- Prefer `spark.range(..., numPartitions=...)` when an experiment needs a known source partition count without first creating a repartition shuffle.
- Keep seed data small, deterministic, and manually understandable.
- Make grouping keys span multiple source partitions before claiming that a grouped operation must redistribute data.
- Treat `filter()`, `select()`, and derived-column expressions as conceptually narrow unless another operation in the lineage changes that reasoning.
- Treat `groupBy()` / grouped aggregation as wide when rows with the same grouping key originate from multiple source partitions.
- Treat a shuffle as the important conceptual stage boundary.
- Use **approximate** stage reasoning for DataFrames in Phase 4. Exact physical plans and runtime adaptations belong to Phase 5.
- Do not equate a partition with a CPU core.
- Do not equate a worker node with an executor.
- Do not equate one transformation with one stage.
- Do not equate one DataFrame method call with one Spark job.
- Keep RDD usage minimal and conceptual.
- Do not mark Phase 3 complete merely because Phase 4 work has begun.
- Do not mark Phase 4 complete until its mastery requirement is explicitly performed and passed.

---

[Back to Table of Contents](#toc)

---

<a id="practice-dataset"></a>
# Practice Dataset

Use one deliberately partitioned retail-like DataFrame throughout most experiments.

The dataset is designed so that:

- there are exactly **four input partitions**;
- both `S01` and `S02` appear in every source partition;
- grouping by `store_id` therefore genuinely requires data from multiple source partitions to be brought together;
- the row count is small enough to inspect manually;
- narrow transformations can be observed without an unrelated repartition step;
- a small dimension DataFrame is available for join reasoning without assuming one fixed join strategy.

## Start one controlled Spark session

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# local[4] gives the local Spark application four task-execution threads.
# This does NOT create four worker machines; it creates a local environment that
# can still demonstrate partitions, tasks, stages, shuffles, and concurrency.
spark = (
    SparkSession.builder
    .appName('phase04-execution-model-practice')
    .master('local[4]')
    .config('spark.sql.shuffle.partitions', '4')
    .getOrCreate()
)
```

The `spark.sql.shuffle.partitions` setting establishes a **configured shuffle-partition target** for SQL/DataFrame shuffle operations. Do not turn this into a Phase 5 AQE lesson: Phase 4 reasoning only needs to know that a wide dependency creates redistribution and that post-shuffle partitioning can differ from source partitioning.

## Create four source partitions without an earlier shuffle

```python
# spark.range(..., numPartitions=4) directly establishes four source partitions.
# We deliberately avoid repartition() here because repartition() itself is a
# shuffle-producing operation that would contaminate the experiment setup.
source_df = spark.range(
    start=0,
    end=16,
    step=1,
    numPartitions=4,
)


# Derive a deterministic retail-like schema through narrow expressions only.
# Alternating store IDs ensure both store keys occur throughout the source.
source_df = (
    source_df
    .withColumn(
        'store_id',
        F.when(
            F.col('id') % 2 == 0,
            F.lit('S01'),
        ).otherwise(F.lit('S02')),
    )
    .withColumn(
        'product_id',
        F.concat(
            F.lit('P'),
            F.lpad(
                ((F.col('id') % 4) + 1).cast('string'),
                3,
                '0',
            ),
        ),
    )
    .withColumn(
        'quantity',
        ((F.col('id') % 3) + 1).cast('int'),
    )
    .withColumn(
        'unit_price',
        ((F.col('id') % 5) + F.lit(10)).cast('decimal(12,2)'),
    )
)
```

## Verify the source partition contract

```python
# getNumPartitions() lets us confirm the partition count we intentionally set.
source_partition_count = source_df.rdd.getNumPartitions()

assert source_partition_count == 4

print(f'Source partition count: {source_partition_count}')
```

## Make source partition membership visible

```python
# spark_partition_id() exposes the partition processing each row at this point
# in the lineage. collect() is intentionally used because the dataset is tiny.
source_partition_rows = (
    source_df
    .select(
        'id',
        'store_id',
        'product_id',
        F.spark_partition_id().alias('source_partition_id'),
    )
    .collect()
)


# Sort only in ordinary Python after collection so the Spark pipeline does not
# gain a global orderBy() shuffle merely for display.
for row in sorted(
    source_partition_rows,
    key=lambda item: (item['source_partition_id'], item['id']),
):
    print(row)
```

Expected source layout from `spark.range()`:

```text
partition 0 → ids 0-3
partition 1 → ids 4-7
partition 2 → ids 8-11
partition 3 → ids 12-15
```

Because `store_id` alternates by `id`, each partition contains both store keys:

```text
partition 0 → S01, S02
partition 1 → S01, S02
partition 2 → S01, S02
partition 3 → S01, S02
```

That deliberate placement is critical later. A claim that `groupBy('store_id')` needs redistribution is now based on the actual seed layout rather than on a vague rule.

## Create a small dimension for join reasoning

```python
# This relation is intentionally tiny. Later we will use it to explain why the
# shortcut 'every join shuffles both sides' is unsafe.
stores_df = spark.createDataFrame(
    [
        ('S01', 'Toronto Central'),
        ('S02', 'Mississauga West'),
        ('S03', 'Vancouver Downtown'),
    ],
    ['store_id', 'store_name'],
)
```

Keep `source_df`, `stores_df`, and the Spark session available for the remaining experiments.

---

[Back to Table of Contents](#toc)

---

<a id="experiment-1-architecture-and-local-mode-mental-mapping"></a>
# Experiment 1 — Architecture and Local-Mode Mental Mapping

## Goal

Understand the roles of:

- driver;
- executors;
- cluster manager;
- worker nodes;
- local mode vs. real cluster mode.

## 1.1 Inspect the local Spark context

```python
# The master string confirms that this practice application runs in local mode
# with four local task-execution threads available.
print(f'Spark master: {spark.sparkContext.master}')
print(f'Default parallelism: {spark.sparkContext.defaultParallelism}')
```

Expected master:

```text
local[4]
```

`defaultParallelism` is useful context, but do not use it as a substitute for explicitly checking the partition count of the DataFrame you are reasoning about.

## 1.2 Map local observations to cluster concepts

Use this mental map:

```text
THIS LOCAL PRACTICE PROCESS

Python program
    │
    └── Driver control flow
            │
            └── local Spark execution with four task threads

REAL CLUSTER CONCEPT

Spark application
    │
  Driver
    │
    ├── communicates with cluster manager
    │
    └── schedules tasks to executors
              │
          worker nodes
```

Important limitation:

```text
local[4] != four worker nodes
local[4] != four executors
```

Local mode lets you study the **execution model**, but not the real network, resource-allocation, and failure behavior of a multi-machine cluster.

## 1.3 Place jobs, stages, and tasks in the application hierarchy

Use this hierarchy throughout the phase:

```text
Spark application
    ↓
Jobs
    ↓
Stages
    ↓
Tasks
```

Read it as:

- the **application** is the running Spark program;
- an **action** creates an execution demand that Spark fulfills as job work;
- a **job** is divided into **stages** around shuffle dependencies;
- a **stage** runs one **task** per partition of that stage's data.

This is a reasoning model, not a promise that every action always maps to one uniquely visible job entry in every Spark circumstance. Phase 4 focuses on the stable hierarchy and dependency logic rather than exact UI counters.

## Questions

1. Which component runs your application's main control flow?
2. Which component allocates cluster resources?
3. What is the difference between a worker node and an executor?
4. Which component actually runs partition-level tasks?
5. What does `local[4]` give you, and what does it not give you?

---

[Back to Table of Contents](#toc)

---

<a id="experiment-2-lazy-evaluation-and-actions"></a>
# Experiment 2 — Lazy Evaluation and Actions

## Goal

Observe the difference between:

- transformations that describe work;
- actions that demand a result;
- expensive-looking transformations and actual execution.

## 2.1 Define a narrow transformation chain

```python
# These operations describe a new DataFrame but do not yet require Spark to
# produce the final rows.
lazy_sales_df = (
    source_df
    .filter(F.col('id') != 15)
    .withColumn(
        'gross_sales',
        F.col('quantity') * F.col('unit_price'),
    )
    .select(
        'id',
        'store_id',
        'product_id',
        'quantity',
        'gross_sales',
    )
)
```

At this point:

```text
filter()      → lazy transformation
withColumn()  → lazy transformation
select()      → lazy transformation
```

The important idea is not that those methods are cheap. The important idea is that their work can remain described in lineage until an action requires a concrete result.

## 2.2 Trigger execution explicitly

```python
# count() is an action because Spark must produce enough of the result to know
# the final number of rows.
lazy_sales_count = lazy_sales_df.count()

assert lazy_sales_count == 15

print(f'Row count after action: {lazy_sales_count}')
```

## 2.3 Use another action

```python
# collect() is also an action. Distributed tasks produce rows, then the final
# result is returned to the driver as Python Row objects.
lazy_sales_rows = lazy_sales_df.collect()

assert len(lazy_sales_rows) == 15
```

Do not interpret `collect()` as:

```text
The driver performs all computation.
```

The distributed work is executed as Spark tasks. `collect()` changes where the **final result** goes: back into driver memory.

## Predict before running

For this code:

```python
result_df = (
    source_df
    .filter(F.col('quantity') >= 2)
    .withColumn('double_quantity', F.col('quantity') * 2)
)
```

answer before adding an action:

```text
Has a result-producing Spark action been requested?  No.
What is lazy?                                      Both transformations.
What would trigger execution?                     count/show/collect/write/etc.
```

## Questions

1. Why does assigning a transformed DataFrame to a variable not imply that all rows were processed immediately?
2. Is `groupBy()` itself an action merely because grouping is expensive?
3. Why does `count()` trigger execution?
4. Why is `collect()` potentially dangerous on large data even though the distributed computation still runs on Spark executors?

---

[Back to Table of Contents](#toc)

---

<a id="experiment-3-lineage-and-repeated-execution-demands"></a>
# Experiment 3 — Lineage and Repeated Execution Demands

## Goal

Understand lineage as the dependency history Spark can use to determine how a result is produced.

## 3.1 Build a reusable lineage branch

```python
# This branch describes how clean sales rows are derived from the source.
# It remains a DataFrame lineage description until an action needs it.
clean_sales_df = (
    source_df
    .filter(F.col('id') != 15)
    .withColumn(
        'gross_sales',
        F.col('quantity') * F.col('unit_price'),
    )
)


# Two downstream DataFrames now depend on the same upstream lineage.
store_sales_df = (
    clean_sales_df
    .groupBy('store_id')
    .agg(F.sum('gross_sales').alias('gross_sales'))
)

product_sales_df = (
    clean_sales_df
    .groupBy('product_id')
    .agg(F.sum('gross_sales').alias('gross_sales'))
)
```

Conceptually:

```text
source_df
   │
   ├── filter
   │
   └── gross_sales expression
          │
       clean_sales_df
          │
          ├───────────────┐
          ↓               ↓
    group by store   group by product
          ↓               ↓
    store_sales_df   product_sales_df
```

The branch structure is a useful lineage/DAG mental model.

## 3.2 Trigger two separate result demands

```python
# First action: Spark computes the lineage needed for the store result.
store_sales_rows = store_sales_df.collect()


# Second action: this is a new result demand. Without deliberate persistence,
# Spark may need to compute the shared upstream lineage again for this action.
product_sales_rows = product_sales_df.collect()

assert len(store_sales_rows) == 2
assert len(product_sales_rows) == 4
```

Phase 4 lesson:

```text
DataFrame variable reuse != automatically materialized intermediate data
```

Lineage tells Spark **how to derive** a result. It does not mean every intermediate DataFrame has already been stored somewhere.

## 3.3 Why lineage matters for recoverability

Conceptually, if Spark loses a partition that can be recomputed from known deterministic upstream dependencies, lineage gives Spark the recipe for rebuilding that work.

Do not overextend this into production fault-tolerance guarantees for arbitrary external side effects. The Phase 4 point is simply:

```text
lineage = dependency history Spark can use to derive/recompute data
```

## Questions

1. What does `clean_sales_df` represent before an action?
2. Why can two downstream DataFrames share the same upstream lineage?
3. Does assigning the shared branch to a Python variable automatically cache it?
4. Why can a second action create additional work even when the transformation code was already defined earlier?
5. How does lineage support Spark's recoverability model conceptually?

---

[Back to Table of Contents](#toc)

---

<a id="experiment-4-partitions-as-units-of-distributed-work"></a>
# Experiment 4 — Partitions as Units of Distributed Work

## Goal

Make execution partitions concrete and connect them to task-level work.

## 4.1 Reconfirm the established source partition count

```python
source_partition_count = source_df.rdd.getNumPartitions()

assert source_partition_count == 4
```

This is stronger than saying:

```text
Spark probably used four partitions.
```

The count is an explicit property of this experiment setup.

## 4.2 Inspect which rows belong to which current partition

```python
partition_observation_rows = (
    source_df
    .select(
        'id',
        'store_id',
        F.spark_partition_id().alias('partition_id'),
    )
    .collect()
)


for row in sorted(
    partition_observation_rows,
    key=lambda item: (item['partition_id'], item['id']),
):
    print(row)
```

Expected observation:

```text
four distinct partition IDs: 0, 1, 2, 3
```

## 4.3 Partition is not CPU core

Keep these concepts separate:

```text
Partition
    = a logical chunk of data/work for a stage

Task
    = one stage's computation for one partition

Execution slot / core capacity
    = ability to run a task at a given time
```

With:

```text
4 ready partitions
local[4]
```

Spark can potentially run four ready tasks concurrently for a stage.

If the same stage had:

```text
8 ready partitions
local[4]
```

there would still be eight tasks for that stage, but only about four could run at once in this local setup. The others would wait for capacity.

## 4.4 Empty partitions still exist

```python
# This narrow filter removes every row from the final source partition because
# partition 3 originally contains ids 12-15.
mostly_empty_tail_df = source_df.filter(F.col('id') < 12)


# Narrow filtering does not itself repartition the DataFrame.
remaining_partition_count = mostly_empty_tail_df.rdd.getNumPartitions()

assert remaining_partition_count == 4
```

The DataFrame still has four partitions even though one partition now contains no surviving rows.

That distinction matters:

```text
partition count != number of non-empty partitions
```

## Questions

1. What does one partition represent in the execution model?
2. What is the task relationship to a stage partition?
3. Why can eight partitions produce eight tasks even with only four execution slots?
4. Why does `filter()` not automatically shrink the partition count just because some partitions become empty?
5. Why should you inspect partition counts instead of inferring them from row count?

---

[Back to Table of Contents](#toc)

---

<a id="experiment-5-narrow-transformations-and-partition-local-work"></a>
# Experiment 5 — Narrow Transformations and Partition-Local Work

## Goal

Observe a pipeline whose transformations can remain partition-local.

## 5.1 Build the narrow pipeline

```python
# Every surviving output row depends on one input row from the same source
# partition. No key-based redistribution is required for these operations.
narrow_df = (
    source_df
    .filter(F.col('id') != 15)
    .withColumn(
        'gross_sales',
        F.col('quantity') * F.col('unit_price'),
    )
    .select(
        'id',
        'store_id',
        'product_id',
        'gross_sales',
    )
)
```

Prediction:

```text
filter      → narrow
withColumn  → narrow
select      → narrow
shuffle?    → no shuffle required by these transformations
stage split → no shuffle boundary introduced by this chain
```

## 5.2 Verify the partition count did not change

```python
narrow_partition_count = narrow_df.rdd.getNumPartitions()

assert narrow_partition_count == 4
```

## 5.3 Observe partition membership after the narrow chain

```python
narrow_partition_rows = (
    narrow_df
    .select(
        'id',
        'store_id',
        F.spark_partition_id().alias('partition_id'),
    )
    .collect()
)

for row in sorted(
    narrow_partition_rows,
    key=lambda item: (item['partition_id'], item['id']),
):
    print(row)
```

The surviving rows remain associated with their existing source partitions because the chain never required rows from different source partitions to be brought together.

## 5.4 Why narrow work can pipeline

Conceptually:

```text
partition 0: filter → derive column → project
partition 1: filter → derive column → project
partition 2: filter → derive column → project
partition 3: filter → derive column → project
```

Each partition can flow through the chain independently.

That is why several narrow transformations do **not** imply several stages.

## Questions

1. Why is `filter()` narrow?
2. Why is adding `gross_sales` narrow?
3. Why can these transformations run as a pipeline inside the same conceptual stage region?
4. Does three transformations mean three tasks per partition?
5. What event would force a new stage boundary later?

---

[Back to Table of Contents](#toc)

---

<a id="experiment-6-wide-transformation-and-a-genuine-shuffle"></a>
# Experiment 6 — Wide Transformation and a Genuine Shuffle

## Goal

Create a wide dependency whose need for redistribution is proven by the seed layout.

## 6.1 Prove grouping keys span source partitions

```python
# Collect only the tiny source partition/key pairs so we can prove that each
# store key appears in multiple input partitions.
store_partition_pairs = {
    (row['store_id'], row['source_partition_id'])
    for row in source_partition_rows
}

print(sorted(store_partition_pairs))
```

Expected set:

```text
('S01', 0), ('S01', 1), ('S01', 2), ('S01', 3)
('S02', 0), ('S02', 1), ('S02', 2), ('S02', 3)
```

Therefore a complete `S01` aggregate cannot be produced from only one original source partition. The same is true for `S02`.

## 6.2 Define grouped aggregation

```python
# The narrow derived measure can happen locally first.
# groupBy('store_id') is wide here because matching store keys are spread across
# the four source partitions and must be brought together for final aggregation.
grouped_sales_df = (
    source_df
    .withColumn(
        'gross_sales',
        F.col('quantity') * F.col('unit_price'),
    )
    .groupBy('store_id')
    .agg(
        F.sum('gross_sales').alias('gross_sales'),
        F.sum('quantity').alias('units_sold'),
    )
)
```

At definition time the chain is still lazy.

Prediction:

```text
withColumn()        → narrow
GROUP BY dependency → wide
why wide?           → same keys exist in multiple source partitions
shuffle?            → yes, redistribution by grouping key is required
```

## 6.3 Trigger the grouped result

```python
# collect() is the action that now demands the grouped output.
grouped_sales_rows = sorted(
    grouped_sales_df.collect(),
    key=lambda row: row['store_id'],
)

for row in grouped_sales_rows:
    print(row)
```

The correctness result has only two store rows, but the execution lesson is more important:

```text
source partition 0 ─┐
source partition 1 ─┼─ shuffle rows by store_id ─→ grouped result
source partition 2 ─┼─
source partition 3 ─┘
```

## 6.4 Distinguish wide dependency from action

`groupBy()` and `agg()` describe a wide dependency but are not themselves the result-demanding action.

```text
groupBy/agg → describes shuffle-producing work
collect      → triggers execution of that work
```

## Questions

1. What fact about the seed data proves this grouping cannot finish entirely inside each original partition?
2. Why is the grouped dependency wide?
3. Does the shuffle occur immediately when the Python interpreter reaches `.groupBy()`?
4. Which call actually demands execution?
5. Why is it unsafe to call every expensive transformation an action?

---

[Back to Table of Contents](#toc)

---

<a id="experiment-7-shuffle-boundaries-and-stage-boundaries"></a>
# Experiment 7 — Shuffle Boundaries and Stage Boundaries

## Goal

Connect a wide dependency to the conceptual split between stages.

Use this pipeline:

```python
stage_reasoning_df = (
    source_df
    .filter(F.col('id') != 15)
    .withColumn(
        'gross_sales',
        F.col('quantity') * F.col('unit_price'),
    )
    .groupBy('store_id')
    .agg(F.sum('gross_sales').alias('gross_sales'))
)
```

## 7.1 Predict the execution regions before the action

```text
REGION 1
source partitions
    ↓
filter
    ↓
withColumn
    ↓
prepare rows for grouping

========== SHUFFLE BOUNDARY ==========

REGION 2
grouped partitions
    ↓
finish aggregation
    ↓
produce result rows
```

The useful Phase 4 approximation is:

```text
one shuffle boundary
        ↓
approximately two stage regions
```

Do not use this false rule:

```text
three transformations = three stages
```

The narrow `filter()` and `withColumn()` can pipeline together before the shuffle.

## 7.2 Trigger execution

```python
stage_reasoning_rows = stage_reasoning_df.collect()

assert len(stage_reasoning_rows) == 2
```

## 7.3 Explain why the stage split exists

Downstream grouping work cannot complete until the required shuffle data from upstream partitions has been produced and made available to the downstream side of the boundary.

That dependency constraint is why the shuffle is the important stage divider.

## Questions

1. Which transformations belong conceptually before the shuffle?
2. Which work belongs after the shuffle?
3. Why can the downstream stage not simply finish at the same time as an arbitrary upstream partition?
4. Why is stage count not equal to transformation count?
5. Why does Phase 4 say "approximately" rather than asserting exact Spark UI stage IDs?

---

[Back to Table of Contents](#toc)

---

<a id="experiment-8-multiple-wide-boundaries"></a>
# Experiment 8 — Multiple Wide Boundaries

## Goal

Reason about a pipeline that contains more than one redistribution requirement.

## 8.1 Add a global ordering after grouping

```python
# First wide dependency: group rows by store_id.
# Second wide dependency: globally order the grouped result by gross_sales.
ordered_store_sales_df = (
    source_df
    .withColumn(
        'gross_sales',
        F.col('quantity') * F.col('unit_price'),
    )
    .groupBy('store_id')
    .agg(F.sum('gross_sales').alias('gross_sales'))
    .orderBy(F.col('gross_sales').desc())
)
```

Prediction before execution:

```text
source + derived measure
        │
        │ narrow
        ↓
GROUP BY store_id
        │
===== shuffle boundary 1 =====
        ↓
finish grouped aggregation
        │
        ↓
global ORDER BY
        │
===== shuffle boundary 2 =====
        ↓
produce globally ordered result
```

Phase 4 conceptual estimate:

```text
two wide boundaries
        ↓
roughly three stage regions
```

Again, do not convert this into exact plan/operator claims.

## 8.2 Trigger execution

```python
ordered_store_sales_rows = ordered_store_sales_df.collect()

for row in ordered_store_sales_rows:
    print(row)
```

## 8.3 Compare against a narrow-only chain

```python
narrow_only_df = (
    source_df
    .filter(F.col('quantity') >= 1)
    .withColumn('quantity_plus_one', F.col('quantity') + 1)
    .select('id', 'store_id', 'quantity_plus_one')
)

narrow_only_rows = narrow_only_df.collect()

assert len(narrow_only_rows) == 16
```

Conceptual contrast:

```text
NARROW-ONLY
filter → withColumn → select → action
no redistribution boundary introduced by the transformations

MULTI-WIDE
withColumn → groupBy → shuffle → orderBy → shuffle → action
multiple stage regions
```

## Questions

1. Why does a global order need a different distribution requirement from a partition-local projection?
2. How many conceptual wide boundaries are present?
3. Why are there not five stages merely because five DataFrame operations appear in the chain?
4. Which parts can pipeline locally before the first shuffle?

---

[Back to Table of Contents](#toc)

---

<a id="experiment-9-tasks-partitions-and-parallelism"></a>
# Experiment 9 — Tasks, Partitions, and Parallelism

## Goal

Connect established partition counts to approximate task counts and available execution capacity.

## 9.1 Start with the known four-partition source

```python
assert source_df.rdd.getNumPartitions() == 4
assert spark.sparkContext.master == 'local[4]'
```

For a stage that reads these four source partitions directly:

```text
4 stage partitions
        ↓
approximately 4 tasks for that stage
```

The tasks are independent at the partition level and can be scheduled onto available executor capacity.

With `local[4]`:

```text
up to roughly four ready tasks can run concurrently
```

This is **execution capacity**, not a statement that every Spark stage always has exactly four tasks.

## 9.2 Create a source with more partitions than local task capacity

```python
# This second source deliberately has eight partitions while local mode still
# provides only four task-execution threads.
eight_partition_df = spark.range(
    start=0,
    end=32,
    step=1,
    numPartitions=8,
)

assert eight_partition_df.rdd.getNumPartitions() == 8
```

Conceptual result for a narrow stage over this source:

```text
8 partitions → about 8 tasks
local[4]     → only about 4 ready tasks can execute at once

wave 1: tasks for some partitions
wave 2: remaining tasks after capacity frees
```

Do not infer that two waves will have identical duration. Partitions can contain different amounts of work, and real executors can have different resource conditions.

## 9.3 Distinguish task identity across stages

Suppose a shuffle produces four downstream partitions.

Do **not** say:

```text
The same original four tasks continue into stage 2.
```

Instead:

```text
Stage 1 has tasks for Stage 1 partitions.
Shuffle boundary.
Stage 2 has a new task set for Stage 2 partitions.
```

A task is stage-specific.

## Questions

1. Why does an eight-partition stage imply about eight tasks even with only four local execution threads?
2. What limits how many ready tasks can run simultaneously?
3. Why is a partition not the same thing as a core?
4. Why is a task not permanently attached to one original source partition across the whole job?
5. What dependency can prevent downstream tasks from completing even when executor capacity is available?

---

[Back to Table of Contents](#toc)

---

<a id="experiment-10-multiple-actions-reuse-and-cache-laziness"></a>
# Experiment 10 — Multiple Actions, Reuse, and Cache Laziness

## Goal

Understand that:

- each action is a new execution demand;
- naming a DataFrame does not materialize it;
- `cache()` / `persist()` express reuse intent but are not normally materializing actions themselves.

This experiment is about **execution semantics**, not Phase 7 cache tuning.

## 10.1 Reuse an unpersisted DataFrame

```python
reusable_df = (
    source_df
    .filter(F.col('id') != 15)
    .withColumn(
        'gross_sales',
        F.col('quantity') * F.col('unit_price'),
    )
)


# First action demands one result.
reusable_count = reusable_df.count()


# Second action demands another result from the same lineage.
reusable_total = reusable_df.agg(
    F.sum('gross_sales').alias('gross_sales')
).collect()[0]['gross_sales']

assert reusable_count == 15
assert reusable_total is not None
```

The Python variable `reusable_df` made the lineage reusable in code. It did not by itself create a stored materialized dataset.

## 10.2 Mark the DataFrame for caching

```python
# cache() requests persistence for reuse, but this call does not by itself need
# to materialize every row immediately.
cached_df = reusable_df.cache()

assert cached_df.is_cached
```

## 10.3 Materialize through an action

```python
# This action computes the DataFrame and allows the resulting partitions to be
# stored according to the cache policy for later reuse.
cached_count = cached_df.count()

assert cached_count == 15
```

## 10.4 Reuse, then clean up

```python
cached_total = cached_df.agg(
    F.sum('gross_sales').alias('gross_sales')
).collect()[0]['gross_sales']

assert cached_total == reusable_total


# Release the cached data because this tiny experiment is finished.
cached_df.unpersist()
```

Phase 4 lesson:

```text
cache()     → marks reuse intent
count()     → action that materializes needed data
second use  → can reuse persisted partitions
```

Do not turn this into a rule that caching is always beneficial. Cache-selection and performance tradeoffs belong later.

## Questions

1. Why can two actions on the same unpersisted DataFrame cause repeated upstream work?
2. Does a Python variable materialize its DataFrame?
3. Is `cache()` itself normally the action that computes every row?
4. Which call in this experiment first materializes the cached lineage?
5. Why is "cache everything" not a valid engineering rule?

---

[Back to Table of Contents](#toc)

---

<a id="experiment-11-join-redistribution-reasoning"></a>
# Experiment 11 — Join Redistribution Reasoning

## Goal

Learn the correct Phase 4 join question without drifting into Phase 5 operator selection.

The unsafe shortcut is:

```text
every join always shuffles both inputs
```

The better question is:

```text
Do matching rows need to be brought together across partitions, and could Spark
use a strategy that avoids redistributing one side?
```

## 11.1 Define a small dimension join

```python
joined_df = source_df.join(
    stores_df,
    on='store_id',
    how='left',
)
```

This DataFrame remains lazy until an action.

## 11.2 Trigger the result without asserting a physical join strategy

```python
joined_count = joined_df.count()

assert joined_count == 16
```

What Phase 4 can say confidently:

- a join requires related rows to be matched by key;
- depending on how data is distributed and which execution strategy Spark chooses, redistribution may be required;
- a tiny relation may permit a strategy that does **not** shuffle both sides;
- therefore `join == shuffle both inputs` is not a safe universal rule.

What Phase 4 intentionally does **not** assert:

- exact physical join operator;
- exact exchange operator placement;
- Catalyst's detailed decision process;
- AQE runtime strategy changes.

Those are Phase 5+ questions.

## 11.3 Compare with a grouping case where redistribution is logically necessary

For the earlier `groupBy('store_id')`, the seed data proves each store key exists in every source partition. A complete per-store result therefore must combine contributions from multiple source partitions.

That is stronger Phase 4 evidence than memorizing:

```text
groupBy always bad
join always shuffle
```

## Questions

1. Why is "every join shuffles both sides" false as a universal rule?
2. What can you reason about a join at Phase 4 without inspecting its physical plan?
3. Why is the earlier grouped example a cleaner guaranteed-shuffle experiment than this tiny join?
4. Which later phase will inspect exact join strategies and physical operators?

---

[Back to Table of Contents](#toc)

---

<a id="experiment-12-minimal-rdd-bridge"></a>
# Experiment 12 — Minimal RDD Bridge

## Goal

Understand why RDD vocabulary appears in Spark execution discussions without turning Phase 4 into an RDD-programming course.

RDD stands for:

```text
Resilient Distributed Dataset
```

A useful conceptual model is:

```text
RDD
  = immutable distributed collection
  + partitions
  + dependencies
  + lineage-based recomputation model
```

## 12.1 Access the underlying RDD view minimally

```python
# DataFrames remain the primary API. We touch .rdd only to connect the familiar
# DataFrame to Spark's partitioned distributed-execution foundations.
source_rdd = source_df.select(
    'id',
    'store_id',
).rdd

print(f'RDD class: {type(source_rdd).__name__}')
print(f'RDD partitions: {source_rdd.getNumPartitions()}')

assert source_rdd.getNumPartitions() == 4
```

## 12.2 Stop before RDD programming takes over

Do **not** rewrite the phase using:

```text
map
flatMap
reduceByKey
combineByKey
custom partitioners
```

Those APIs are not required for the Phase 4 learning objective.

The useful bridge is:

```text
DataFrame
    ↓
structured abstraction used for data engineering
    ↓
still executed as distributed partitioned work
    ↓
RDD concepts help explain partitions, dependencies, lineage, and task execution
```

## 12.3 Why DataFrames remain preferred

For structured data engineering, DataFrames provide:

- named columns;
- schemas;
- relational transformations;
- Spark SQL integration;
- higher-level expressions;
- opportunities for Spark's structured query engine to optimize execution.

The details of those optimizer opportunities are intentionally deferred to Phase 5.

## Questions

1. What does RDD stand for?
2. Which execution-model concepts become intuitive through RDD vocabulary?
3. Why are DataFrames still preferred for most structured data-engineering work?
4. Why is extensive RDD programming explicitly deprioritized in this curriculum?

---

[Back to Table of Contents](#toc)

---

<a id="experiment-13-execution-prediction-drills"></a>
# Experiment 13 — Execution Prediction Drills

## Goal

Practice the Phase 4 mastery-style reasoning process without performing the formal mastery gate.

For every pipeline, answer these seven questions **before execution**:

```text
1. What is lazy?
2. What triggers execution?
3. Which dependencies are narrow or wide?
4. Where should a shuffle occur?
5. Where should stages split?
6. How do partitions relate to tasks?
7. What work can execute in parallel?
```

## Drill A — Narrow-only pipeline

```python
prediction_a_df = (
    source_df
    .filter(F.col('quantity') >= 2)
    .withColumn('quantity_x2', F.col('quantity') * 2)
    .select('id', 'store_id', 'quantity_x2')
)

prediction_a_rows = prediction_a_df.collect()
```

### Expected reasoning

```text
Lazy:
    filter, withColumn, select

Action:
    collect

Dependencies:
    narrow only

Shuffle:
    none required by these transformations

Stage split:
    no shuffle boundary introduced by the chain

Tasks:
    source has four established partitions, so the narrow stage has about four
    partition tasks, even if some partitions contain different numbers of rows

Parallelism:
    ready partition tasks can run concurrently up to available local capacity
```

## Drill B — One wide boundary

```python
prediction_b_df = (
    source_df
    .filter(F.col('id') != 15)
    .groupBy('store_id')
    .agg(F.sum('quantity').alias('units_sold'))
)

prediction_b_rows = prediction_b_df.collect()
```

### Expected reasoning

```text
Lazy:
    filter, groupBy, agg

Action:
    collect

Dependencies:
    filter = narrow
    grouped dependency = wide

Shuffle:
    grouping by store_id requires redistribution because both store keys span
    multiple source partitions

Stage split:
    at the grouping shuffle boundary

Tasks:
    stage-specific task sets correspond to that stage's partitions

Parallelism:
    upstream partition tasks can run independently; downstream work waits for
    required shuffle outputs
```

## Drill C — Two wide boundaries

```python
prediction_c_df = (
    source_df
    .groupBy('product_id')
    .agg(F.sum('quantity').alias('units_sold'))
    .orderBy(F.col('units_sold').desc())
)

prediction_c_rows = prediction_c_df.collect()
```

### Expected reasoning

```text
Wide boundary 1:
    product grouping

Wide boundary 2:
    global ordering

Approximate stage model:
    region 1 → shuffle → region 2 → shuffle → region 3
```

## Drill D — Two actions on one lineage

```python
prediction_d_df = source_df.filter(F.col('quantity') >= 2)

prediction_d_count = prediction_d_df.count()
prediction_d_rows = prediction_d_df.collect()
```

### Expected reasoning

```text
Two actions create two execution demands.
The DataFrame variable alone does not mean the first result was automatically
stored for the second action.
```

## Drill E — Join without overclaiming

```python
prediction_e_df = source_df.join(
    stores_df,
    on='store_id',
    how='left',
)

prediction_e_count = prediction_e_df.count()
```

### Expected reasoning

```text
Action:
    count

Join reasoning:
    related rows must be matched by key, but Phase 4 should not assert the exact
    physical join strategy or claim that both inputs necessarily shuffle
```

## Final drill questions

1. Which drill is guaranteed by the seed design to require key redistribution?
2. Which drill has two conceptual wide boundaries?
3. Which drill demonstrates that multiple transformations do not imply multiple stages?
4. Which drill demonstrates that multiple actions are separate result demands?
5. Which drill intentionally refuses to predict an exact physical join strategy?

---

[Back to Table of Contents](#toc)

---

<a id="applied-phase-4-project"></a>
# Applied Phase 4 Project

## Objective

Apply the execution model to a fresh retail pipeline without using detailed query-plan inspection as the primary reasoning tool.

The applied task is intentionally about **predicting Spark's execution behavior from the transformation structure and known partition layout**.

You are not being asked to optimize the pipeline yet.

You are not being asked to inspect Catalyst plans yet.

You are not being asked to perform the formal Phase 4 mastery gate yet.

The workflow is:

```text
establish source partitions
        ↓
build a realistic transformation pipeline
        ↓
predict laziness / action
        ↓
classify narrow vs. wide dependencies
        ↓
predict shuffle boundaries
        ↓
predict stage regions
        ↓
reason about tasks and parallelism
        ↓
run only enough to validate observable assumptions
```

---

[Back to Table of Contents](#toc)

---

<a id="applied-task-part-1"></a>
# Applied Task — Part 1

Build and reason about this pipeline.

## Source contract

Use the existing `source_df` with:

```text
16 rows
4 deliberately established source partitions
store_id alternates between S01 and S02
both store keys span every source partition
```

## Pipeline

```python
applied_df = (
    source_df
    .filter(F.col('quantity') >= 2)
    .withColumn(
        'gross_sales',
        F.col('quantity') * F.col('unit_price'),
    )
    .groupBy(
        'store_id',
        'product_id',
    )
    .agg(
        F.sum('quantity').alias('units_sold'),
        F.sum('gross_sales').alias('gross_sales'),
    )
    .filter(F.col('units_sold') >= 2)
    .orderBy(
        F.col('gross_sales').desc(),
        F.col('store_id').asc(),
        F.col('product_id').asc(),
    )
)
```

Do **not** run the action immediately.

First write down answers to all seven required questions:

```text
1. What is lazy?
2. What triggers execution?
3. Which dependencies are narrow or wide?
4. Where should a shuffle occur?
5. Where should stages split?
6. How do partitions relate to tasks?
7. What work can execute in parallel?
```

Then answer these more specific questions:

1. Why can the first `filter()` execute partition-locally?
2. Why can `gross_sales` be derived partition-locally?
3. Why does grouping by `(store_id, product_id)` require redistribution for this seed?
4. Is the post-aggregation `filter()` narrow relative to the grouped rows?
5. Why does the global `orderBy()` introduce another wide dependency?
6. How many conceptual wide boundaries are present?
7. Approximately how many stage regions would you predict from those boundaries?
8. Why can the first stage's source partitions be processed independently?
9. Why can the downstream grouped stage not finish before the required upstream shuffle output exists?
10. Why should you avoid claiming an exact physical operator or exact Spark UI stage numbering at this point?

## Validate only the observable source assumptions

Before running the final result, you may verify:

```python
# The input partition count was deliberately established.
assert source_df.rdd.getNumPartitions() == 4


# Confirm each grouping key combination that exists in multiple source
# partitions really does span partitions. We collect only tiny metadata rows.
applied_partition_rows = (
    source_df
    .select(
        'store_id',
        'product_id',
        F.spark_partition_id().alias('partition_id'),
    )
    .collect()
)

key_to_partitions = {}

for row in applied_partition_rows:
    key = (row['store_id'], row['product_id'])
    key_to_partitions.setdefault(key, set()).add(row['partition_id'])

for key, partition_ids in sorted(key_to_partitions.items()):
    print(key, sorted(partition_ids))
```

Then trigger the result:

```python
applied_rows = applied_df.collect()

for row in applied_rows:
    print(row)
```

After execution, compare what happened to your **conceptual prediction**. Do not use detailed physical-plan inspection to replace the reasoning exercise.

---

[Back to Table of Contents](#toc)

---

<a id="after-part-1"></a>
# After Part 1

If you can explain the applied pipeline clearly, preserve these conclusions:

```text
Transformations are lazy until an action demands a result.

Narrow transformations can pipeline over existing partitions.

Wide dependencies require data redistribution.

Shuffle boundaries are the important conceptual stage boundaries.

A stage has approximately one task per partition of that stage's data.

Independent ready tasks can execute in parallel subject to available executor
capacity and dependency constraints.

Partitions are data/work units; cores or task slots are execution capacity.

A task belongs to one stage and one partition of that stage.

Lineage describes how Spark can derive data; a Python DataFrame variable is not
an automatically materialized intermediate dataset.

RDD concepts help explain Spark execution, but DataFrames remain the primary
structured-data API.

Exact physical operators, Catalyst planning, and AQE behavior belong to Phase 5.
```

Do **not** mark Phase 4 complete from these experiments alone.

The formal curriculum mastery requirement still requires an explicit mastery check in which, given a PySpark pipeline, you independently predict approximately:

- what triggers a Spark job;
- where shuffles occur;
- where stages split;
- which operations can execute in parallel.

That mastery gate should be performed only when explicitly requested.

---

[Back to Table of Contents](#toc)
