# Phase 8 — Experiments & Applied Practice Guide

<a id="toc"></a>
## Table of Contents

- [Purpose](#purpose)
- [Conventions](#conventions)
- [Practice Dataset](#practice-dataset)
- [Spark UI Experiment Protocol](#spark-ui-experiment-protocol)
- [Runtime Evidence Record](#runtime-evidence-record)
- [Experiment 1 — Multiple Actions and Jobs](#experiment-1-multiple-actions-and-jobs)
- [Experiment 2 — Stage Boundaries and Shuffle Evidence](#experiment-2-stage-boundaries-and-shuffle-evidence)
- [Experiment 3 — Balanced Tasks vs. Skewed Stragglers](#experiment-3-balanced-tasks-vs-skewed-stragglers)
- [Experiment 4 — Too Few vs. Too Many Tasks](#experiment-4-too-few-vs-too-many-tasks)
- [Experiment 5 — Sort-Merge vs. Broadcast Join](#experiment-5-sort-merge-vs-broadcast-join)
- [Experiment 6 — Spill and Memory Pressure](#experiment-6-spill-and-memory-pressure)
- [Experiment 7 — Failed Task Localization](#experiment-7-failed-task-localization)
- [Experiment 8 — Executor Utilization and Driver Misuse](#experiment-8-executor-utilization-and-driver-misuse)
- [Experiment 9 — AQE Runtime Behavior](#experiment-9-aqe-runtime-behavior)
- [Experiment 10 — Recomputed Lineage vs. Cache Reuse](#experiment-10-recomputed-lineage-vs-cache-reuse)
- [Applied Phase 8 Project](#applied-phase-8-project)
- [Applied Task — Diagnose and Improve One Slow Pipeline](#applied-task-diagnose-and-improve-one-slow-pipeline)
- [After the Applied Task](#after-the-applied-task)

---

<a id="purpose"></a>
## Purpose

This file is the execution-oriented companion for **Phase 8 — Spark UI, Monitoring & Debugging**.

The Phase 8 `README.md` is the conceptual reference. `phase_08_lecture.py` is the consolidated teaching implementation. This guide converts those ideas into controlled experiments where the repeated habit is:

```text
predict
    ↓
run one known action
    ↓
identify the relevant job
    ↓
identify the slow / failing stage
    ↓
inspect tasks
    ↓
record actual runtime evidence
    ↓
connect evidence to df.explain('formatted')
    ↓
state one root-cause hypothesis
    ↓
change ONE thing
    ↓
rerun the same action
    ↓
compare
    ↓
reconcile correctness
```

The governing question is:

> **Where is the time or failure occurring, and what physical behavior caused it?**

For every important experiment, be able to answer:

```text
1. Which action triggered execution?
2. Which job corresponds to that action?
3. Which stage is slow or failing?
4. What do the tasks reveal?
5. Is the stage scan-heavy, shuffle-heavy, join-heavy, aggregate-heavy, or sort-heavy?
6. Are partition sizes balanced?
7. Are there stragglers?
8. Is spill present?
9. Are executors being used effectively?
10. Is the problem actually on the driver?
11. What does df.explain('formatted') show?
12. Which physical operator explains the runtime behavior?
13. What ONE change most directly targets the evidence?
14. What changed on rerun?
15. Did schema, grain, row semantics, and business totals remain correct?
```

This guide deliberately reuses Phase 4–7 concepts rather than reteaching them.

Expected prior vocabulary includes:

```text
jobs
stages
tasks
narrow vs. wide transformations
Exchange
BroadcastHashJoin
SortMergeJoin
HashAggregate
execution partitions
shuffle partitions
repartition()
coalesce()
skew
hot keys
caching
AQE
```

The applied section is practice only. It does **not** perform the formal Phase 8 mastery gate, update `ROADMAP.md`, or mark the phase complete.

---

[Back to Table of Contents](#toc)

---

<a id="conventions"></a>
## Conventions

- Use **PySpark 4.2.0**.
- Use **single quotes** for Python strings.
- Prefer `from pyspark.sql import functions as F`.
- Include inline comments explaining both **what** important code does and **why** the observation matters.
- Reuse deterministic retail-style data across experiments.
- State the intended grain before changing joins, aggregations, or deduplication logic.
- Label materializing actions with `spark.sparkContext.setJobDescription(...)` when useful.
- Use the live URL Spark reports through `spark.sparkContext.uiWebUrl`; do not assume a fixed port.
- Keep the Spark application alive while inspecting the live UI.
- Use `df.explain('formatted')` beside runtime evidence.
- Treat Spark UI metrics as observations to record, not values to predict precisely.
- Never invent stage IDs, job IDs, task durations, shuffle sizes, spill values, or AQE changes.
- Use `df.rdd.getNumPartitions()` only for the current execution partition count.
- Use `F.spark_partition_id()` only when row-to-partition evidence is useful.
- Aggregate diagnostic evidence before collecting it to the driver.
- Use the same materializing action for before/after comparisons.
- State whether a run is cold, warm, cache-materializing, or cache-reusing.
- Change exactly **one variable** per optimization experiment.
- Restore temporary Spark configuration changes after experiments.
- Reconcile correctness after every meaningful optimization.
- Do not turn local teaching configuration values into universal production defaults.
- Do not mark Phase 8 complete until the formal mastery gate is explicitly requested and passed.

### Core diagnostic vocabulary

Be able to classify the observed bottleneck as primarily:

```text
I/O
shuffle
join strategy
skew / hot key
partition count
partition imbalance
spill / memory pressure
cache pressure
executor utilization
driver misuse
failure / retry
AQE runtime behavior
```

A real workload can have several issues. The experiment discipline is still:

> **Diagnose and change one thing at a time.**

---

[Back to Table of Contents](#toc)

---

<a id="practice-dataset"></a>
## Practice Dataset

Reuse the deterministic retail relations from `phase_08_lecture.py` and the notebooks.

### `fact_sales_df`

Grain:

```text
one row per sale_id
```

Useful columns:

```text
sale_id
order_date
store_id
product_id
customer_id
order_status
quantity
unit_price
gross_sales
year
month
```

### `dim_store_df`

Grain:

```text
one row per store_id
```

Useful columns:

```text
store_id
store_name
province
store_format
```

### `dim_product_df`

Grain:

```text
one row per product_id
```

Useful columns:

```text
product_id
product_name
category
```

### Controlled skew relation

The skew experiment deliberately creates a hot `store_id` such as:

```text
S001
```

Do not assume the runtime consequence before execution.

The deterministic key distribution supports a hypothesis; the Spark UI must confirm whether the workload actually creates measurable stragglers, disproportionate shuffle reads, or spill in your environment.

### Why reuse one domain

Using the same data model lets you focus on runtime reasoning instead of learning a new business domain for every experiment.

The recurring correctness rules are:

```text
fact_sales_df
→ one row per sale_id

dim_store_df
→ store_id unique

dim_product_df
→ product_id unique

province aggregation
→ one row per province

province x category aggregation
→ one row per province x category
```

---

[Back to Table of Contents](#toc)

---

<a id="spark-ui-experiment-protocol"></a>
## Spark UI Experiment Protocol

Use this protocol for every experiment.

### 1. State the business requirement and grain

Example:

```text
Requirement:
completed gross sales by province

Output grain:
one row per province
```

### 2. Predict the physical behavior

Before execution, write a prediction such as:

```text
The groupBy should require redistribution by the grouping key.
I therefore expect an Exchange in the physical plan and shuffle-related runtime work.
```

Do **not** predict exact runtime metrics.

Bad:

```text
Stage 3 will have one 5.2-second straggler.
```

Good:

```text
If the hot key materially skews the shuffle, I expect one or a few reduce tasks
may process substantially more data than peers.
```

### 3. Inspect the formatted physical plan

Use:

```python
result_df.explain('formatted')
```

Identify relevant operators:

```text
Scan
Filter
Exchange
Sort
HashAggregate
BroadcastExchange
BroadcastHashJoin
SortMergeJoin
AdaptiveSparkPlan
```

### 4. Run one labelled materializing action

Example:

```python
spark.sparkContext.setJobDescription(
    'PHASE 8 - skewed aggregation',
)

result_count = result_df.count()
```

### 5. Locate the corresponding runtime work

Do not start with random tabs.

Use:

```text
action
→ job / SQL query
→ stage
→ tasks
```

### 6. Record actual evidence

Record values from your runtime rather than replacing them with assumptions.

### 7. Explain the causal chain

Use:

```text
runtime symptom
+
physical plan
+
data evidence where relevant
→ root-cause hypothesis
```

### 8. Change one thing

Example:

```text
SortMergeJoin baseline
→ broadcast validated-small store dimension
```

Do not simultaneously change:

```text
join strategy
shuffle partitions
AQE
cache state
executor settings
```

### 9. Rerun the same action

Compare equivalent work.

### 10. Reconcile correctness

At minimum, confirm the invariants relevant to the experiment:

```text
schema
grain
row count where meaningful
key uniqueness
business totals
```

---

[Back to Table of Contents](#toc)

---

<a id="runtime-evidence-record"></a>
## Runtime Evidence Record

Use this template for each significant run.

```text
RUN LABEL:

BUSINESS REQUIREMENT:

OUTPUT GRAIN:

MATERIALIZING ACTION:

PREDICTION BEFORE EXECUTION:

JOB / QUERY
- job ID(s):
- SQL/DataFrame query ID if applicable:
- status:
- elapsed time:

STAGE
- stage ID:
- role in physical plan:
- task count:
- stage duration:
- input:
- shuffle write:
- shuffle read:
- memory spill:
- disk spill:
- failed / retried tasks:

TASK DISTRIBUTION
- typical duration:
- maximum duration:
- typical input / shuffle read:
- maximum input / shuffle read:
- stragglers present?:
- spill concentrated?:

EXECUTOR EVIDENCE
- useful parallelism observed?:
- repeated slow/failing executor?:
- notable GC / spill pattern?:

PHYSICAL PLAN EVIDENCE
- relevant operators:
- relevant Exchange(s):
- join strategy:
- AQE evidence:

ROOT-CAUSE HYPOTHESIS:

FIRST CHANGE:

WHY THIS CHANGE FIRST:

RERUN DIFFERENCE:

CORRECTNESS RECONCILIATION:
```

You do not need every field for every experiment.

Use the smallest evidence set that can answer the engineering question.

---

[Back to Table of Contents](#toc)

---

<a id="experiment-1-multiple-actions-and-jobs"></a>
## Experiment 1 — Multiple Actions and Jobs

### Goal

Connect a **materializing action** to the runtime work it triggers.

### Build

Create:

```python
completed_store_sales_df = (
    fact_sales_df
    .filter(F.col('order_status') == 'COMPLETED')
    .groupBy('store_id')
    .agg(
        F.sum('gross_sales').alias('gross_sales'),
        F.count('*').alias('sale_line_count'),
    )
)
```

Run two separate labelled actions:

```python
completed_store_sales_df.count()
```

and:

```python
(
    completed_store_sales_df
    .agg(F.sum('gross_sales').alias('gross_sales'))
    .first()
)
```

### Predict before execution

Answer:

```text
Which parts of the lineage are likely to be reused logically?
Without persistence, should the second action be assumed to reuse computed data?
Where is a shuffle expected?
```

### Inspect

For each action, record:

```text
job / query entry
stage count
slowest stage
task count
shuffle write/read
```

### Explain

Answer:

> **Why can two actions over the same DataFrame produce separate runtime work?**

Your answer should include lazy evaluation and recomputation of uncached lineage, but the actual job structure must come from your run.

### Do not optimize yet

This experiment is about identification, not caching.

Do not add `.cache()` until Experiment 10.

---

[Back to Table of Contents](#toc)

---

<a id="experiment-2-stage-boundaries-and-shuffle-evidence"></a>
## Experiment 2 — Stage Boundaries and Shuffle Evidence

### Goal

Connect a stage boundary to the physical operator that requires redistributed data.

### Build

Use completed sales joined to stores and aggregated to province:

```python
province_sales_df = (
    fact_sales_df
    .filter(F.col('order_status') == 'COMPLETED')
    .select(
        'store_id',
        'gross_sales',
    )
    .join(
        dim_store_df.select(
            'store_id',
            'province',
        ),
        on='store_id',
        how='inner',
    )
    .groupBy('province')
    .agg(F.sum('gross_sales').alias('gross_sales'))
)
```

### Before execution

Inspect:

```python
province_sales_df.explain('formatted')
```

Identify:

```text
join operator
Exchange(s)
aggregation operator
```

### Run

Use one labelled action such as:

```python
province_sales_df.orderBy('province').collect()
```

The result is tiny, so collecting the final province-level output is acceptable.

### Inspect

Find the relevant job/query and answer:

```text
Which stage reads source data?
Which stage writes shuffle data?
Which stage reads shuffle data?
Which stage contains the final aggregation?
Which stage dominates runtime?
```

### Explain

Complete:

```text
Stage ___ exists because the physical plan contains ______.
The runtime evidence shows ______.
Therefore the stage is performing ______ physical behavior.
```

The goal is to stop saying only:

```text
Stage 4 is slow.
```

and instead say:

```text
Stage 4 is the reduce-side stage created by ______, and its tasks show ______.
```

---

[Back to Table of Contents](#toc)

---

<a id="experiment-3-balanced-tasks-vs-skewed-stragglers"></a>
## Experiment 3 — Balanced Tasks vs. Skewed Stragglers

### Goal

Use task-level evidence to distinguish balanced shuffle work from hot-key skew.

### Part A — Balanced keys

Create a relation where rows are distributed roughly evenly across 40 `store_id` values.

Then aggregate:

```python
balanced_store_units_df = (
    balanced_sales_df
    .groupBy('store_id')
    .agg(F.sum('units').alias('units'))
)
```

Before execution:

1. inspect key frequencies;
2. inspect `explain('formatted')`;
3. predict what a reasonably balanced reduce stage would look like.

Run one labelled action.

Record:

```text
task count
typical task duration
maximum task duration
typical shuffle read
maximum shuffle read
spill
```

### Part B — Hot key

Create the deterministic skew relation where `S001` owns approximately 80% of rows.

Verify key frequencies first:

```python
(
    skewed_sales_df
    .groupBy('store_id')
    .count()
    .orderBy(
        F.col('count').desc(),
        F.col('store_id').asc(),
    )
    .show()
)
```

Then aggregate by `store_id` and run the same type of materializing action.

### Compare

Do **not** start with runtime.

Build the causal chain:

```text
verified hot key
    ↓
Exchange hashpartitioning(store_id, ...)
    ↓
actual task-size distribution
    ↓
actual task-duration distribution
    ↓
skew conclusion or no-skew conclusion
```

### Required explanation

Answer:

> **What evidence would make you confident that the problem is skew rather than simply a slow executor?**

Strong evidence can include:

```text
slow task also has disproportionately large shuffle/input volume
same logical partition remains expensive independent of executor
hot-key frequency supports the distribution problem
spill concentrates in the oversized task
```

Do not claim evidence that your run does not show.

---

[Back to Table of Contents](#toc)

---

<a id="experiment-4-too-few-vs-too-many-tasks"></a>
## Experiment 4 — Too Few vs. Too Many Tasks

### Goal

Separate **partition count** from **partition balance** and reason about useful parallelism.

### Part A — Too few partitions

Starting from the same deterministic input:

```python
few_partitions_df = base_task_df.repartition(2)
```

Record:

```text
current execution partition count
relevant stage task count
task duration
available worker capacity vs. runnable task count
```

### Question

If only two tasks are runnable on a runtime capable of four concurrent tasks, is adding more executor capacity the first fix?

Explain why or why not.

### Part B — Many small partitions

Create:

```python
many_partitions_df = base_task_df.repartition(240)
```

Run an equivalent action.

Record:

```text
task count
typical task duration
data per task
stage runtime
```

### Compare

Do not conclude:

```text
2 partitions = bad
240 partitions = bad
```

Instead answer:

```text
At what point did work become too coarse for available parallelism?
At what point did tasks become so small that coordination overhead appeared disproportionate?
```

### Required distinction

Explain:

```text
partition count
!=
partition balance
```

A workload can have many partitions and still have one hot partition.

---

[Back to Table of Contents](#toc)

---

<a id="experiment-5-sort-merge-vs-broadcast-join"></a>
## Experiment 5 — Sort-Merge vs. Broadcast Join

### Goal

Connect join strategy to shuffle behavior using both the physical plan and Spark UI.

### Correctness precondition

Verify:

```text
dim_store_df
→ one row per store_id
```

Do not optimize a dimension join whose key is not unique when uniqueness is required.

### Baseline — forced sort-merge join

Temporarily disable automatic broadcasting:

```python
spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '-1')
```

Use merge hints to make the teaching intent explicit.

Inspect:

```python
baseline_df.explain('formatted')
```

Confirm the actual plan before execution.

Run one labelled action and record:

```text
join strategy
Exchange(s)
shuffle write
shuffle read
join-stage task count
task-duration distribution
spill
```

### Optimization — change one thing

Broadcast the validated-small dimension:

```python
F.broadcast(dim_store_df)
```

Inspect the new physical plan.

Run the same materializing action.

### Compare

Build a table from actual observations:

| Evidence | Sort-merge baseline | Broadcast version |
|---|---:|---:|
| Join operator |  |  |
| Fact-side join Exchange |  |  |
| Dimension-side join Exchange |  |  |
| Shuffle write |  |  |
| Shuffle read |  |  |
| Slowest relevant stage |  |  |
| Typical task duration |  |  |
| Max task duration |  |  |
| Spill |  |  |

### Reconcile correctness

Confirm identical province-level business totals.

### Required explanation

Answer:

> **Why did broadcasting help or fail to help in your actual run?**

The answer must cite both:

```text
physical-plan change
+
runtime evidence
```

not wall-clock time alone.

Restore the broadcast threshold afterward.

---

[Back to Table of Contents](#toc)

---

<a id="experiment-6-spill-and-memory-pressure"></a>
## Experiment 6 — Spill and Memory Pressure

### Goal

Learn to treat spill as a runtime symptom whose cause must be localized.

### Build

Use the deterministic aggregation candidate from the lecture:

```text
many grouping keys
few input partitions
aggregation state
shuffle
```

Inspect the plan and run one labelled action.

### Record actual evidence

```text
memory spill
disk spill
which tasks spill
task durations
input / shuffle sizes
```

### Valid outcome A — no spill

If your run shows:

```text
memory spill = 0
disk spill = 0
```

record exactly that.

The lesson is still valid:

```text
this local workload did not exceed the runtime's useful in-memory execution capacity
```

Do not manufacture an unrealistic low-memory environment solely to force non-zero spill.

### Valid outcome B — spill occurs

If spill appears, distinguish:

```text
spill concentrated in one/few oversized tasks
```

from:

```text
spill broadly across similarly sized tasks
```

The first points more strongly toward skew / oversized partitions.

The second can justify investigating:

```text
general partition size
operator memory demand
cache pressure
concurrency
legitimate resource sizing
```

### Required question

> **Why is “increase executor memory” not the first conclusion from a non-zero spill metric?**

---

[Back to Table of Contents](#toc)

---

<a id="experiment-7-failed-task-localization"></a>
## Experiment 7 — Failed Task Localization

### Goal

Use the UI to narrow a failure to the job, stage, task/partition, and operation that caused it.

### Setup

Use the lecture's deterministic failure relation with one known bad row.

Example behavior:

```python
F.when(
    F.col('id') == 7777,
    F.raise_error('intentional Phase 8 failure for sale_id 7777'),
)
```

### Important

Keep the Spark application alive after catching the exception so the failed runtime evidence remains inspectable.

### Run

Enable the failure experiment intentionally.

Wrap it in `try` / `except` so the notebook/script can continue after recording the failure.

### Record

```text
job ID
stage ID
failing task / partition
task attempt
executor
exception text
whether the same logical task retried
```

### Explain

Answer:

```text
Is this failure deterministic and data-specific?
Is it resource-related?
Does the same partition repeatedly fail?
Do failures move across executors?
```

### Required principle

> **Preserve failure evidence before repeatedly rerunning or changing configuration.**

A useful failure explanation is:

```text
The same logical partition repeatedly failed with the same deterministic
expression error. That points to data/logic in that partition rather than a
random executor-capacity problem.
```

Use your actual evidence.

---

[Back to Table of Contents](#toc)

---

<a id="experiment-8-executor-utilization-and-driver-misuse"></a>
## Experiment 8 — Executor Utilization and Driver Misuse

### Goal

Distinguish distributed executor-side work from driver-side pressure or misuse.

### Part A — Executor utilization

Run a relation with enough partitions to expose several concurrent tasks.

Record:

```text
active tasks
completed tasks
task distribution across executors
GC time if relevant
spill if relevant
```

Then compare with the two-partition experiment from Experiment 4.

### Required explanation

Answer:

> **Why can many idle executors be a partitioning or straggler symptom rather than evidence that the cluster needs more executors?**

### Part B — Safe driver diagnostic

Use:

```python
driver_safe_diagnostic_df = (
    fact_sales_df
    .groupBy('store_id')
    .agg(
        F.count('*').alias('row_count'),
        F.sum('gross_sales').alias('gross_sales'),
    )
)
```

Collect only the small aggregated result.

### Compare conceptually

```text
SAFE
huge distributed fact
    → aggregate in Spark
    → tiny result
    → collect

RISKY
huge distributed fact
    → collect entire fact
    → driver memory / local Python pressure
```

Do **not** intentionally collect an unnecessarily huge dataset just to trigger a driver OOM.

### Required distinction

Explain:

```text
driver memory
!=
executor memory
```

and why increasing executor memory does not repair `collect()` misuse.

---

[Back to Table of Contents](#toc)

---

<a id="experiment-9-aqe-runtime-behavior"></a>
## Experiment 9 — AQE Runtime Behavior

### Goal

Observe AQE as a runtime behavior instead of assuming that enabling it changed execution.

### Baseline — AQE disabled

Use a deliberately high shuffle-partition target such as:

```python
spark.conf.set('spark.sql.shuffle.partitions', '96')
spark.conf.set('spark.sql.adaptive.enabled', 'false')
```

Run a small-key aggregation.

Record:

```text
configured shuffle partition target
actual runtime task count
physical plan
stage runtime
```

### Change one thing — enable AQE

Use:

```python
spark.conf.set('spark.sql.adaptive.enabled', 'true')
spark.conf.set('spark.sql.adaptive.coalescePartitions.enabled', 'true')
```

Run the same logical workload.

After materialization, inspect:

```python
result_df.explain('formatted')
```

### Record

```text
runtime plan final/adaptive evidence
post-shuffle task count
coalescing evidence
join-strategy changes if any
skew handling if any
```

### Valid conclusion

This is acceptable:

```text
AQE was enabled, but this particular run showed no material runtime adaptation.
```

### Invalid conclusion

Do not write:

```text
AQE coalesced the partitions
```

solely because AQE was enabled.

### Required explanation

Answer:

> **What runtime evidence proves that AQE actually changed execution?**

Restore the original AQE and shuffle-partition settings afterward.

---

[Back to Table of Contents](#toc)

---

<a id="experiment-10-recomputed-lineage-vs-cache-reuse"></a>
## Experiment 10 — Recomputed Lineage vs. Cache Reuse

### Goal

Use repeated jobs and the Storage view to decide whether persistence is justified.

### Baseline

Create one expensive reusable intermediate involving:

```text
filter
join
aggregation
```

Run two separate actions without caching.

Record:

```text
which upstream stages repeat
join/aggregation work repeated
runtime of each action
```

### Hypothesis

Only if meaningful upstream work repeats should you consider persistence.

### Change one thing

Use:

```python
cached_df = expensive_reused_df.cache()
```

Then distinguish:

```text
cache materialization action
```

from:

```text
cache reuse action
```

### Inspect

Use both runtime evidence and the Storage view.

Record:

```text
cached partition count
materialization cost
reuse job/stage structure
whether expensive upstream work disappears on reuse
```

### Required explanation

Answer:

> **Why is comparing an uncached first run directly with a cached reuse run incomplete?**

Your answer must account for the cost of materializing the cache.

Finally:

```python
cached_df.unpersist()
```

---

[Back to Table of Contents](#toc)

---

<a id="applied-phase-8-project"></a>
## Applied Phase 8 Project

The applied project combines the phase into one diagnosis.

It is intentionally not a sequence of isolated metric questions.

You are given one slow retail pipeline:

```text
partitioned Parquet fact_sales
        ↓
Q1 2026 + COMPLETED filter
        ↓
store dimension join
        ↓
product dimension join
        ↓
province x category aggregation
        ↓
Parquet write
```

The business requirement is:

```text
completed Q1 2026 gross sales by province and category
```

The required output grain is:

```text
one row per province x category
```

The baseline intentionally contains at least one physical design choice worth investigating.

Your task is **not** to optimize everything you know how to optimize.

Your task is:

> **Use runtime evidence to find the most important first change.**

The applied project should be solved using the same hierarchy:

```text
correctness
    ↓
action
    ↓
job
    ↓
stage
    ↓
tasks
    ↓
runtime evidence
    ↓
physical plan
    ↓
root cause
    ↓
ONE change
    ↓
rerun
    ↓
correctness reconciliation
```

---

[Back to Table of Contents](#toc)

---

<a id="applied-task-diagnose-and-improve-one-slow-pipeline"></a>
## Applied Task — Diagnose and Improve One Slow Pipeline

### Step 1 — Freeze correctness

State before execution:

```text
source grain:

store dimension grain:

product dimension grain:

output grain:

required business measure:

join keys:
```

Validate expected dimension-key uniqueness.

### Step 2 — Inspect the baseline physical plan

Use:

```python
baseline_pipeline_df.explain('formatted')
```

Record:

```text
Scan:
PartitionFilters:
PushedFilters:
Exchange(s):
Join strategy 1:
Join strategy 2:
Aggregation operator:
AdaptiveSparkPlan present?:
```

Do not diagnose from the plan alone.

This is only the prediction layer.

### Step 3 — Run one known materializing action

Prefer the actual output write:

```python
(
    baseline_pipeline_df
    .write
    .mode('overwrite')
    .parquet(output_path)
)
```

Label the action clearly.

### Step 4 — Find the relevant job/query

Record:

```text
job ID(s):
SQL/DataFrame query ID:
wall-clock runtime:
```

### Step 5 — Find the dominant stage

For the stage that matters most, record:

```text
stage ID:
role:
task count:
stage duration:
input:
shuffle write:
shuffle read:
memory spill:
disk spill:
failed/retried tasks:
```

### Step 6 — Inspect task distribution

Record:

```text
typical task duration:
maximum task duration:
typical input/shuffle read:
maximum input/shuffle read:
stragglers?:
spill concentrated?:
```

### Step 7 — Inspect executor behavior

Answer:

```text
Are executors receiving useful work?
Are many idle because only a few tasks remain?
Does one executor repeatedly host slow or failed work?
```

### Step 8 — Connect runtime evidence to the plan

Write one causal paragraph.

Template:

```text
The slowest stage is ______.
Its tasks show ______.
The formatted physical plan shows ______.
That operator requires ______ physical behavior.
The runtime evidence therefore supports ______ as the primary bottleneck.
```

### Step 9 — Rank possible causes

Classify the evidence:

```text
[ ] unnecessary I/O
[ ] large required I/O
[ ] unnecessary shuffle
[ ] required shuffle
[ ] bad join strategy
[ ] skew / hot key
[ ] too few partitions
[ ] too many tiny partitions
[ ] spill / memory pressure
[ ] cache pressure
[ ] executor-specific issue
[ ] driver misuse
[ ] AQE opportunity
[ ] other
```

Pick **one primary cause**.

### Step 10 — Choose the first change

Complete:

```text
FIRST CHANGE:

EVIDENCE THAT JUSTIFIES IT:

WHAT I AM DELIBERATELY NOT CHANGING YET:
```

Possible first changes include:

```text
broadcast a validated-small dimension
remove unnecessary repartition
reduce rows before shuffle
reduce row width before shuffle
address a verified hot key
change partition count
remove unjustified caching
replace driver-side collection
```

Do not simultaneously change several variables.

### Step 11 — Rerun the same action

Record the same runtime evidence for the optimized version.

Create a comparison table:

| Evidence | Baseline | Optimized |
|---|---:|---:|
| Materializing action |  |  |
| Physical join strategy |  |  |
| Relevant stage runtime |  |  |
| Task count |  |  |
| Typical task duration |  |  |
| Maximum task duration |  |  |
| Shuffle write |  |  |
| Shuffle read |  |  |
| Memory spill |  |  |
| Disk spill |  |  |
| Failed/retried tasks |  |  |
| Wall-clock runtime |  |  |

### Step 12 — Reconcile correctness

Validate at least:

```text
schema unchanged
one row per province x category
same number of output groups
same gross_sales totals
same join semantics
```

For small deterministic outputs, exact ordered row comparison is acceptable.

For larger workloads, reconcile with distributed comparisons rather than collecting the entire dataset.

### Step 13 — Final diagnosis

Write the conclusion in this format:

```text
Symptom:

Action:

Slow / failing location:

Task evidence:

Physical-plan evidence:

Root cause:

First change:

Why this change first:

Rerun evidence:

Correctness reconciliation:

What I would investigate next if more optimization were required:
```

### Standard of a strong answer

Strong:

```text
The write action triggered the workload. The join-related reduce stage dominated
runtime. Most tasks were similar, but one task read much more shuffle data and
ran substantially longer. The formatted plan showed hash repartitioning by
store_id feeding SortMergeJoin, and key-frequency evidence confirmed a hot
store_id. Therefore the first problem is skewed join-key distribution, not an
arbitrary lack of executors. I changed only the skew strategy, reran the same
write, and verified that the task tail improved while province-category totals
remained identical.
```

Weak:

```text
The Spark UI looked slow, so I increased shuffle partitions and memory.
```

The core Phase 8 standard is causal explanation.

---

[Back to Table of Contents](#toc)

---

<a id="after-the-applied-task"></a>
## After the Applied Task

Do not mark Phase 8 complete automatically.

Before the formal mastery gate, make sure you can independently answer:

```text
Which action triggered the job?
Which job matters?
Which stage is slow?
What do the tasks reveal?
Is the problem I/O, shuffle, skew, partitioning, join strategy,
spill, cache pressure, executor pressure, or driver misuse?
What does the physical plan show?
What should I change FIRST?
Did the rerun improve runtime behavior without breaking correctness?
```

The formal mastery requirement remains:

> **Explain why a Spark job is slow rather than merely observe that it is slow.**

A final professional diagnostic workflow should feel natural:

```text
symptom
→ action
→ job
→ stage
→ tasks
→ evidence
→ plan
→ root cause
→ one change
→ rerun
→ validate
```

Do not update `ROADMAP.md`, mark Phase 8 complete, or generate Phase 9 until the mastery gate is explicitly requested and passed.

---

[Back to Table of Contents](#toc)
