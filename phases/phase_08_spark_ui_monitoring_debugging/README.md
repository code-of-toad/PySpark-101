# Phase 8 — Spark UI, Monitoring & Debugging

<a id="toc"></a>
## Table of Contents

- [Objective](#objective)
- [Phase Mental Model](#phase-mental-model)
- [1. Spark UI as Runtime Evidence](#1-spark-ui-as-runtime-evidence)
- [2. Diagnostic Workflow: Symptom to Root Cause](#2-diagnostic-workflow-symptom-to-root-cause)
- [3. Actions, Jobs, and Finding the Work That Matters](#3-actions-jobs-and-finding-the-work-that-matters)
- [4. Stages and Shuffle Boundaries](#4-stages-and-shuffle-boundaries)
- [5. Tasks: The Most Important Unit of Runtime Evidence](#5-tasks-the-most-important-unit-of-runtime-evidence)
- [6. Shuffle Read and Write](#6-shuffle-read-and-write)
- [7. Partition Sizes, Skew, and Stragglers](#7-partition-sizes-skew-and-stragglers)
- [8. Spill and Memory Pressure](#8-spill-and-memory-pressure)
- [9. Executors and Utilization](#9-executors-and-utilization)
- [10. Driver vs. Executor Problems](#10-driver-vs-executor-problems)
- [11. Failed Tasks and Failure Localization](#11-failed-tasks-and-failure-localization)
- [12. SQL/DataFrame Queries and the Physical Plan](#12-sqldataframe-queries-and-the-physical-plan)
- [13. AQE Runtime Behavior](#13-aqe-runtime-behavior)
- [14. Common Runtime Evidence Patterns](#14-common-runtime-evidence-patterns)
- [15. Measuring a Change Correctly](#15-measuring-a-change-correctly)
- [16. Integrated Phase 8 Diagnostic Workflow](#16-integrated-phase-8-diagnostic-workflow)
- [17. Common Failure Modes](#17-common-failure-modes)
- [18. Phase 8 Mastery Reference](#18-phase-8-mastery-reference)

---

<a id="objective"></a>
## Objective

Learn to diagnose slow or failing Spark workloads using **runtime evidence** from the Spark UI.

Phase 8 turns the execution, physical-plan, partitioning, shuffle, skew, join, AQE, and performance concepts from Phases 4–7 into an operational debugging workflow.

The required topics are:

- jobs;
- stages;
- tasks;
- executors;
- SQL/DataFrame queries;
- shuffle read/write;
- task duration;
- failed tasks;
- partition sizes;
- skew and stragglers;
- spill;
- executor utilization;
- driver vs. executor problems;
- AQE runtime behavior;
- connecting Spark UI evidence to `df.explain('formatted')`.

Examples and experiments target **PySpark 4.2.0** and deterministic retail/data-engineering workloads.

The central Phase 8 question is:

> **Where is the time or failure occurring, and what physical behavior caused it?**

The central workflow is:

```text
symptom
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
change ONE thing
    ↓
rerun
    ↓
compare
    ↓
validate correctness
```

Phase 4–7 concepts are assumed knowledge. This phase does **not** substantially reteach:

```text
lazy evaluation
jobs / stages / tasks
narrow vs. wide transformations
Exchange
BroadcastHashJoin
SortMergeJoin
execution partitions
storage partitions
repartition()
coalesce()
skew
caching
AQE
```

Instead, Phase 8 asks:

> **What evidence does the running application provide that one of those mechanisms is actually the problem?**

Do not invent observations. Whenever behavior depends on the runtime, record what the Spark UI and physical plan actually show.

This document is lecture/reference material only. The formal Phase 8 mastery gate is intentionally deferred until explicitly requested.

---

[Back to Table of Contents](#toc)

---

<a id="phase-mental-model"></a>
## Phase Mental Model

A slow Spark job is not one problem.

It is a symptom produced by physical work occurring somewhere in:

```text
action
  ↓
job
  ↓
stage
  ↓
tasks
  ↓
executors
```

The Spark UI helps answer **where** the problem appears.

The physical plan helps answer **why that work exists**.

Use both:

```text
Spark UI
→ where runtime cost / failure appears

df.explain('formatted')
→ which physical operators created that work
```

The key distinction is:

```text
runtime symptom
!=
physical root cause
```

Examples:

```text
one task takes much longer than the rest
→ symptom: straggler
→ possible cause: skewed partition, spill, oversized input, slow executor

large shuffle read
→ symptom: heavy network/disk redistribution
→ possible cause: required aggregation, sort-merge join, distinct, repartition,
  or an avoidable upstream design choice

many tiny fast tasks
→ symptom: scheduling overhead
→ possible cause: excessive partition count or small-file fragmentation

low executor activity while the application appears stuck
→ possible cause: driver-side work, insufficient runnable tasks,
  serialized stage dependency, or application logic outside Spark execution

out-of-memory failure
→ possible cause: oversized partition, broadcast, cache pressure,
  driver collection, or genuinely insufficient memory
```

A useful mental model is:

```text
WHERE?
job → stage → task → executor

WHAT PHYSICAL BEHAVIOR?
scan → exchange → join → aggregate → sort → cache → collect

WHY?
data volume → distribution → partitioning → strategy → memory → misuse
```

The goal is not to memorize every metric.

The goal is to reduce uncertainty until one change is justified.

---

[Back to Table of Contents](#toc)

---

<a id="1-spark-ui-as-runtime-evidence"></a>
## 1. Spark UI as Runtime Evidence

The Spark UI is a runtime inspection surface for a Spark application.

It should be treated as **evidence**, not as a dashboard to stare at.

For a local Spark application, the UI is commonly available while the application is running. The exact address or port may vary by runtime, so use the URL Spark reports rather than assuming a fixed value.

### What the UI can help answer

| Question | Evidence surface |
|---|---|
| Which action caused work? | Jobs / SQL query relationship |
| Which job is slow or failed? | Jobs |
| Which stage dominates runtime? | Stages |
| Are tasks balanced? | Task duration and task metrics |
| Is a shuffle expensive? | Shuffle read/write |
| Is one partition much larger? | Per-task input/shuffle metrics |
| Is data spilling? | Memory/disk spill |
| Are executors busy? | Executor task/activity metrics |
| Did tasks fail or retry? | Failed tasks / stage details |
| Did AQE modify execution? | SQL query/runtime plan evidence |
| Does the physical strategy match the runtime symptom? | SQL query + `explain('formatted')` |

### Do not diagnose from one number

For example:

```text
high shuffle read
```

does not automatically mean:

```text
reduce shuffle partitions
```

You still need to ask:

```text
Why did this shuffle exist?
Was its volume expected?
Were tasks balanced?
Did it spill?
Was the join strategy appropriate?
Did AQE alter it?
```

Metrics become useful when connected to a causal story.

---

[Back to Table of Contents](#toc)

---

<a id="2-diagnostic-workflow-symptom-to-root-cause"></a>
## 2. Diagnostic Workflow: Symptom to Root Cause

Use the same workflow for both performance and failure diagnosis.

### Step 1 — Freeze correctness

Before optimization, state:

```text
required output grain
required schema
required row semantics
required business totals
```

A faster wrong pipeline is still wrong.

### Step 2 — Identify the materializing action

Transformations are lazy.

Ask:

```text
Which action actually triggered execution?
```

Examples:

```python
result_df.count()
result_df.collect()
result_df.write.mode('overwrite').parquet(output_path)
result_df.show()
```

One application can contain multiple actions and therefore multiple jobs.

### Step 3 — Find the relevant job

Do not assume the latest job is the important job.

Match the action you care about to the corresponding runtime work.

### Step 4 — Find the expensive or failing stage

Ask:

```text
Which stage consumed most of the relevant job's runtime?
Which stage failed?
Which stage contains the suspicious shuffle boundary?
```

### Step 5 — Inspect task distribution

Ask:

```text
Are most tasks similar?
Are a few tasks much slower?
Are input or shuffle sizes balanced?
Are tasks spilling?
Are failures concentrated?
```

### Step 6 — Connect the stage to the physical plan

Use:

```python
result_df.explain('formatted')
```

Look for operators associated with the runtime behavior:

```text
Scan
Filter
Exchange
HashAggregate
Sort
BroadcastExchange
BroadcastHashJoin
SortMergeJoin
AdaptiveSparkPlan
```

### Step 7 — Form one root-cause hypothesis

Example:

```text
UI evidence:
one reduce task reads far more shuffle data than peers

Plan evidence:
Exchange hashpartitioning(store_id, ...)

Data evidence:
one store_id is disproportionately frequent

Hypothesis:
hot store_id creates skew after the shuffle
```

### Step 8 — Change one thing

Examples:

```text
filter earlier
remove unnecessary repartition
broadcast genuinely small dimension
change partition count
fix hot-key strategy
remove unjustified cache
replace driver collect
```

### Step 9 — Rerun the same workload

Compare the same action under comparable conditions.

### Step 10 — Reconcile correctness

Validate:

```text
schema
grain
counts where meaningful
key uniqueness where required
business totals
```

The final answer should be:

```text
The job was slow because ______.
The evidence was ______.
I changed ______ first because ______.
After rerun, ______ improved while correctness remained ______.
```

---

[Back to Table of Contents](#toc)

---

<a id="3-actions-jobs-and-finding-the-work-that-matters"></a>
## 3. Actions, Jobs, and Finding the Work That Matters

A debugging session should begin with the action.

Suppose:

```python
sales_by_store_df = (
    fact_sales_df
    .filter(F.col('order_status') == 'COMPLETED')
    .groupBy('store_id')
    .agg(F.sum('net_sales').alias('net_sales'))
)

row_count = sales_by_store_df.count()

sales_by_store_df.write.mode('overwrite').parquet(output_path)
```

There are two materializing actions:

```text
count()
write(...)
```

Do not reason about the application as if it were one indivisible job.

### Multiple actions can repeat lineage

Without persistence, separate actions may recompute the same upstream lineage.

Runtime investigation should therefore ask:

```text
Did the application perform the same expensive scan/join/aggregation more than once?
Was that expected?
Would persistence be justified because the lineage is genuinely reused?
```

Do not cache merely because multiple jobs appear. First prove that the repeated lineage is expensive enough and reused enough to justify storage.

### Job-level questions

For the relevant job, record:

```text
action / operation that triggered it
job runtime
number of stages
successful / failed status
which stage dominates runtime
```

The job view narrows the investigation.

It rarely provides the complete root cause by itself.

---

[Back to Table of Contents](#toc)

---

<a id="4-stages-and-shuffle-boundaries"></a>
## 4. Stages and Shuffle Boundaries

Stages divide a job around boundaries where downstream work cannot proceed until required upstream output exists.

The most important Phase 8 use of a stage is:

> **Locate the physical region where runtime becomes expensive or fails.**

### Typical relationship

```text
upstream stage
    ↓
shuffle write
    ↓
Exchange boundary
    ↓
downstream stage
    ↓
shuffle read
```

Examples that often introduce shuffle boundaries include:

```text
groupBy
join
distinct
orderBy
repartition
```

Whether a particular operation shuffles depends on the actual plan and existing partitioning.

### Stage-level questions

Ask:

```text
How many tasks are in this stage?
How long does the stage take?
Does it read input data?
Does it write shuffle data?
Does it read shuffle data?
Are task durations tightly grouped or widely spread?
Do a few tasks dominate completion time?
Is spill present?
Are failures/retries concentrated here?
```

### Slow stage vs. necessary stage

A stage can be expensive because it performs legitimately large work.

Diagnosis is not:

```text
large stage
→ remove stage
```

Instead:

```text
Is the work required?
Is the data volume unnecessarily large?
Is distribution unhealthy?
Is the physical strategy appropriate?
Is the task count appropriate?
```

The goal is to eliminate unnecessary work and badly distributed work, not all expensive work.

---

[Back to Table of Contents](#toc)

---

<a id="5-tasks-the-most-important-unit-of-runtime-evidence"></a>
## 5. Tasks: The Most Important Unit of Runtime Evidence

A stage may look slow only because a few tasks are slow.

That is why task-level evidence is often where the root cause becomes visible.

### Healthy-looking task distribution

Conceptually:

```text
Task 1  ███████
Task 2  ████████
Task 3  ███████
Task 4  ████████
Task 5  ███████
```

Similar durations do not prove perfect execution, but they suggest reasonably balanced work.

### Straggler pattern

```text
Task 1  ███
Task 2  ███
Task 3  ███
Task 4  █████████████████████████
```

The stage finishes when the slowest required tasks finish.

Therefore:

> **Median task performance can look healthy while stage latency remains poor.**

### What to compare across tasks

Record the runtime metrics actually shown in your environment, especially:

```text
duration
input size / records
shuffle read
shuffle write
memory spill
disk spill
failed / retried status
executor placement
```

### Interpretation discipline

Do not jump from:

```text
one task is slow
```

to:

```text
data skew
```

Check whether the slow task also has:

```text
much larger input
much larger shuffle read
spill
retries
different executor behavior
```

A slow task with similar data volume may point somewhere else.

---

[Back to Table of Contents](#toc)

---

<a id="6-shuffle-read-and-write"></a>
## 6. Shuffle Read and Write

Shuffle metrics quantify data redistribution between stages.

The important distinction is:

```text
shuffle write
→ data produced for downstream repartitioned consumption

shuffle read
→ redistributed data consumed by downstream tasks
```

### Why shuffle matters

Shuffle can involve:

```text
serialization
network transfer
disk I/O
sorting
hash partitioning
memory pressure
spill
coordination between stages
```

Therefore, large shuffle volume deserves investigation.

But shuffle is often logically required.

### Example: aggregation

```python
store_sales_df = (
    fact_sales_df
    .groupBy('store_id')
    .agg(F.sum('net_sales').alias('net_sales'))
)
```

Rows sharing the same grouping key must be brought together appropriately.

The correct question is not:

```text
How do I eliminate every shuffle?
```

It is:

```text
Is this shuffle required by the result?
Is too much data reaching it?
Are rows unnecessarily wide?
Is an extra repartition creating another Exchange?
Are partitions balanced after the shuffle?
```

### Large shuffle write

Investigate:

```text
Was too much source data read?
Could filtering happen earlier?
Could columns be projected earlier?
Is an unnecessary wide operation present?
Is the join multiplying rows?
```

### Large shuffle read

Investigate:

```text
How many downstream tasks consume it?
Are partition sizes balanced?
Are tasks spilling?
Is a sort-merge join handling two large inputs as expected?
Would a genuinely small side support broadcasting?
```

### Runtime + plan connection

UI:

```text
large shuffle read in slow stage
```

Plan:

```text
Exchange
Sort
SortMergeJoin
```

Possible conclusion:

```text
The runtime cost is associated with the shuffle/sort requirements of the
sort-merge join. The next question is whether that join strategy is necessary
for the actual input sizes and semantics.
```

That is stronger than merely saying:

```text
The shuffle is large.
```

---

[Back to Table of Contents](#toc)

---

<a id="7-partition-sizes-skew-and-stragglers"></a>
## 7. Partition Sizes, Skew, and Stragglers

Partition **count** and partition **distribution** are different problems.

You can have:

```text
200 partitions
```

and still have:

```text
199 small partitions
1 enormous partition
```

### Runtime signature of skew

Common evidence can include:

```text
most tasks finish quickly
a small number of tasks run much longer
straggler tasks read much more shuffle data
straggler tasks process many more records
spill concentrates in those tasks
stage completion waits on a small tail
```

Record what your run actually shows.

### Confirm with data evidence

For a suspected hot key:

```python
key_frequency_df = (
    fact_sales_df
    .groupBy('store_id')
    .agg(F.count('*').alias('row_count'))
    .orderBy(
        F.col('row_count').desc(),
        F.col('store_id').asc(),
    )
)
```

This distinguishes:

```text
runtime symptom: uneven tasks
```

from:

```text
data cause: uneven key frequency
```

### Skew is not simply "one key is popular"

Skew becomes an execution problem when data distribution causes materially uneven work.

The same key distribution can behave differently depending on:

```text
operation
partitioner
partition count
AQE
input size
join strategy
```

### Too few tasks

Possible evidence:

```text
very small number of long tasks
large data volume per task
limited parallelism despite available executor capacity
spill due to oversized partitions
```

Potential cause:

```text
under-partitioned execution
```

### Too many tasks

Possible evidence:

```text
very large task count
most tasks process tiny amounts of data
tasks finish quickly but stage overhead remains meaningful
```

Potential cause:

```text
excessive partitioning / small-file driven scan fragmentation
```

Do not tune task count from a rule of thumb alone.

Use the workload's actual task sizes and durations.

---

[Back to Table of Contents](#toc)

---

<a id="8-spill-and-memory-pressure"></a>
## 8. Spill and Memory Pressure

Spill occurs when Spark cannot keep required execution data entirely in memory and moves some work to disk.

Spill is a **symptom**, not automatically a configuration problem.

### Operators that may create memory pressure

Examples include:

```text
aggregation
sorting
shuffle processing
join processing
caching
large partitions
```

### Runtime evidence

Record:

```text
memory spill
disk spill
task duration
partition/input size
shuffle read
which tasks spill
```

### Diagnostic distinction

Pattern A:

```text
most tasks do not spill
one huge task spills heavily
```

First suspect:

```text
skew / oversized partition
```

before increasing executor memory.

Pattern B:

```text
many similarly sized tasks spill
```

Possible questions:

```text
Are partitions generally too large?
Is the operator intrinsically memory-intensive?
Is caching competing for memory?
Is executor concurrency creating aggregate pressure?
Are resources genuinely undersized for the workload?
```

### Spill decision rule

Use:

```text
spill
    ↓
which tasks?
    ↓
how large are their inputs/shuffles?
    ↓
is distribution skewed?
    ↓
is the operator necessary?
    ↓
is cache consuming useful memory?
    ↓
only then consider resource/config changes
```

This preserves the Phase 7 rule:

> **Fix query and data design before tuning configuration.**

---

[Back to Table of Contents](#toc)

---

<a id="9-executors-and-utilization"></a>
## 9. Executors and Utilization

Executor evidence helps determine whether distributed workers are being used effectively.

Do not equate:

```text
more executor activity
```

with:

```text
better job
```

The goal is appropriate utilization for required work.

### Questions to ask

```text
Are executors receiving tasks?
Are tasks distributed across executors?
Is one executor repeatedly hosting slow or failing tasks?
Are many executors idle while only a few tasks remain?
Is one stage incapable of using available parallelism because it has too few tasks?
Are executors showing signs of memory pressure?
```

### Low utilization can be legitimate

For example:

```text
final stage has only two partitions
→ only two tasks are runnable
→ many executors remain idle
```

Adding executors would not fix the partitioning constraint.

### One slow executor

If slow tasks consistently appear on the same executor while comparable tasks elsewhere are healthy, investigate executor-specific issues.

Do not assume skew until you compare:

```text
task input/shuffle sizes
task durations
executor placement
failure/retry patterns
```

### Tail utilization

Near stage completion, many executors may naturally become idle while a few stragglers remain.

This is often the visible consequence of skew:

```text
many executors idle
+
one or two long-running tasks
```

The problem is not necessarily insufficient executor count.

The problem may be uneven work.

---

[Back to Table of Contents](#toc)

---

<a id="10-driver-vs-executor-problems"></a>
## 10. Driver vs. Executor Problems

The driver and executors perform different roles.

Phase 8 requires recognizing when the bottleneck is not distributed task execution at all.

### Executor-side work

Typical distributed work includes:

```text
scans
filters
projections
joins
aggregations
sorts
shuffle processing
partition-level transformations
```

Executor problems are often visible through:

```text
slow tasks
failed tasks
spill
executor loss
uneven task distribution
```

### Driver-side work

The driver is responsible for application coordination and hosts Python code outside distributed task execution.

Common driver misuse includes:

```python
rows = huge_df.collect()
```

or:

```python
rows = huge_df.toPandas()
```

The distributed part may finish successfully, followed by driver-side memory or processing pressure.

### Diagnostic clue

If Spark tasks complete but the application is still slow or fails around result collection, ask:

```text
Is the driver receiving too much data?
Is Python code doing heavy local work after Spark finishes?
Is the application creating an enormous query/plan?
Is the driver coordinating an excessive number of tiny tasks?
```

### Better pattern

Aggregate distributed data before collecting small diagnostics:

```python
diagnostic_df = (
    fact_sales_df
    .groupBy('store_id')
    .agg(F.count('*').alias('row_count'))
)

diagnostic_rows = diagnostic_df.collect()
```

This is fundamentally different from collecting the entire fact table.

### Driver memory is not executor memory

Increasing executor memory does not fix a driver-side `collect()` misuse.

Always identify **where** the memory pressure exists before tuning resources.

---

[Back to Table of Contents](#toc)

---

<a id="11-failed-tasks-and-failure-localization"></a>
## 11. Failed Tasks and Failure Localization

A failed Spark application should be debugged with the same narrowing process as a slow one.

```text
application failed
    ↓
which job?
    ↓
which stage?
    ↓
which task(s)?
    ↓
same failure or different failures?
    ↓
what operation was executing?
    ↓
what data / partition reached it?
```

### Failed task vs. failed stage

Spark may retry failed tasks.

Therefore, investigate:

```text
Did one task fail once and recover?
Did the same logical task fail repeatedly?
Did many tasks fail similarly?
Did the executor disappear?
```

### Useful failure classes

| Runtime evidence | Investigation direction |
|---|---|
| Same partition repeatedly fails | Data-specific or deterministic partition-level failure |
| Failures move across tasks/executors | Broader resource/infrastructure problem may exist |
| Failure after `collect()` | Driver-side pressure or local processing |
| Heavy spill before failure | Memory pressure; inspect partition sizes and skew |
| Join stage explodes in records/work | Check grain and join-key uniqueness |
| One executor repeatedly loses tasks | Executor-specific issue may exist |

### Preserve the failing evidence

When feasible, record:

```text
job ID
stage ID
task/partition ID
exception
executor
input/shuffle size
physical operator
relevant key/data characteristics
```

Do not immediately rerun repeatedly and lose the causal trail.

---

[Back to Table of Contents](#toc)

---

<a id="12-sqldataframe-queries-and-the-physical-plan"></a>
## 12. SQL/DataFrame Queries and the Physical Plan

The SQL/DataFrame query view and `df.explain('formatted')` connect source-level transformations to runtime execution.

### Core workflow

```text
slow stage
    ↓
identify related SQL/DataFrame query
    ↓
inspect runtime query evidence
    ↓
inspect formatted physical plan
    ↓
map slow stage to Exchange / join / aggregate / scan / sort
```

### Common operators and runtime questions

#### Scan

Plan:

```text
Scan
```

Ask:

```text
Is the slow work I/O-heavy?
Are required partitions being pruned?
Are only required columns read?
Are many tiny input tasks being created?
```

#### Exchange

Plan:

```text
Exchange
```

Ask:

```text
How much shuffle data is written/read?
Why is redistribution required?
Are downstream partitions balanced?
```

#### HashAggregate

Plan:

```text
HashAggregate
```

Ask:

```text
Is there a large shuffle into the aggregate?
Are grouping keys skewed?
Is spill present?
```

#### SortMergeJoin

Plan:

```text
Exchange
Sort
SortMergeJoin
Sort
Exchange
```

Ask:

```text
Are both sides large?
How much data is shuffled?
Are join-key partitions balanced?
Would broadcasting one genuinely small side be appropriate?
```

#### BroadcastHashJoin

Plan:

```text
BroadcastExchange
BroadcastHashJoin
```

Ask:

```text
Is the broadcast side actually small enough?
Did runtime show broadcast-related pressure or failure?
Did the broadcast avoid a large-side shuffle as intended?
```

### Plan tells you why; UI tells you how it behaved

Example:

```text
Plan:
SortMergeJoin on store_id

UI:
join-related stage has 12 tasks
11 tasks finish quickly
1 task reads dramatically more shuffle data and runs much longer

Data:
one store_id dominates row count

Diagnosis:
the sort-merge join's shuffled join key is skewed; the stage tail is caused by
one oversized partition.
```

That is a complete causal explanation.

---

[Back to Table of Contents](#toc)

---

<a id="13-aqe-runtime-behavior"></a>
## 13. AQE Runtime Behavior

Adaptive Query Execution can modify parts of the physical strategy using runtime statistics.

Phase 8 focuses on **observing the runtime consequences**.

Relevant behaviors include:

```text
post-shuffle partition coalescing
runtime join strategy changes
skew-related partition handling
```

### Do not infer AQE behavior from configuration alone

This:

```python
spark.conf.set('spark.sql.adaptive.enabled', 'true')
```

means AQE is enabled.

It does **not** prove that AQE changed a particular query.

You must inspect the actual runtime plan/evidence.

### Partition coalescing

Possible before/after reasoning:

```text
configured shuffle target:
many partitions

runtime shuffle data:
small

AQE:
coalesces post-shuffle work into fewer partitions

UI evidence:
fewer runtime tasks than the static target would suggest
```

Record your actual task counts rather than inventing expected values.

### Runtime join changes

A query may begin with one strategy and adapt when runtime statistics justify another.

The correct Phase 8 question is:

```text
What did the runtime plan actually execute?
```

### AQE and skew

AQE may mitigate certain skewed shuffle partitions.

Still inspect:

```text
task duration distribution
shuffle sizes
remaining stragglers
runtime plan
```

AQE is not permission to ignore poor data design.

### AQE experiment discipline

Compare:

```text
same data
same action
same business result
AQE off vs. on
```

Record:

```text
physical/runtime plan
stage/task counts
shuffle behavior
task distribution
elapsed time as supporting evidence
correctness
```

---

[Back to Table of Contents](#toc)

---

<a id="14-common-runtime-evidence-patterns"></a>
## 14. Common Runtime Evidence Patterns

Use these as hypotheses, not automatic diagnoses.

| Runtime evidence | Likely investigation |
|---|---|
| One/few tasks much slower than peers | Skew, oversized partition, spill, executor-specific issue |
| One/few tasks read much more shuffle data | Skewed shuffle partition / hot key |
| Many long tasks with similar sizes | Workload may simply be large; inspect strategy, I/O, partition sizing |
| Many tiny short tasks | Excessive partitioning or small-file overhead |
| Large shuffle on both join sides | Sort-merge/shuffle join; verify whether both sides truly need shuffling |
| Heavy spill concentrated in stragglers | Skew/oversized partitions before adding memory |
| Heavy spill across many balanced tasks | General partition sizing/resource pressure |
| Executors mostly idle during long stage | Too few runnable tasks or stragglers |
| Same task repeatedly fails | Deterministic data/partition problem |
| Application fails after distributed work finishes | Driver collection/local processing |
| Several actions repeat similar expensive jobs | Recomputed lineage; evaluate justified persistence |
| Runtime task count differs from static shuffle target | AQE may have coalesced/adapted; inspect runtime plan |
| Large scan work before selective result | Check pruning and source filters |
| Job becomes slower after `.repartition(...)` | Verify whether added Exchange was necessary |
| Join output unexpectedly explodes | Correctness/grain problem before performance tuning |

### Evidence hierarchy

Prefer a combined explanation:

```text
job evidence
+
stage evidence
+
task evidence
+
physical plan
+
data distribution / correctness evidence
```

over a one-metric conclusion.

---

[Back to Table of Contents](#toc)

---

<a id="15-measuring-a-change-correctly"></a>
## 15. Measuring a Change Correctly

A before/after comparison is meaningful only when the experiment is controlled.

### Freeze the workload

Keep constant:

```text
input data
business logic except the one intended change
materializing action
runtime environment where practical
correctness checks
```

### Change one variable

Bad experiment:

```text
enable AQE
change shuffle partitions
broadcast join
cache input
increase memory
rerun once
```

If performance improves, you do not know why.

Better:

```text
baseline
↓
one hypothesis
↓
one change
↓
rerun
↓
compare
```

### Compare physical evidence, not only wall-clock time

Record:

```text
job/stage runtime
task-duration distribution
task count
shuffle read/write
spill
failed/retried tasks
executor behavior
physical/runtime plan
```

Local elapsed time can still be useful:

```python
from time import perf_counter

started_at = perf_counter()
result_count = result_df.count()
elapsed_seconds = perf_counter() - started_at
```

But wall-clock timing is affected by:

```text
JVM warm-up
filesystem cache
Spark cache
background machine load
small teaching datasets
```

### Warm-up deliberately

For controlled comparisons, decide whether you are measuring:

```text
cold execution
warm execution
cache materialization
cache reuse
```

Do not compare unlike runs without saying so.

### Correctness gate

After each change, reconcile the result.

Examples:

```python
assert baseline_df.schema == optimized_df.schema
```

and for deterministic small results:

```python
baseline_rows = baseline_df.orderBy('store_id').collect()
optimized_rows = optimized_df.orderBy('store_id').collect()

assert baseline_rows == optimized_rows
```

For larger workloads, use distributed reconciliation rather than collecting entire datasets.

---

[Back to Table of Contents](#toc)

---

<a id="16-integrated-phase-8-diagnostic-workflow"></a>
## 16. Integrated Phase 8 Diagnostic Workflow

Use this as the default investigation template.

### Scenario

A retail pipeline is slow:

```text
partitioned Parquet fact_sales
    ↓
date/status filter
    ↓
join dim_store
    ↓
groupBy province
    ↓
write result
```

### Step 1 — State correctness

```text
output grain:
one row per province

required measure:
completed net_sales

required dimension:
province from dim_store
```

### Step 2 — Trigger one known action

For example:

```python
result_df.write.mode('overwrite').parquet(output_path)
```

### Step 3 — Find the relevant job

Record:

```text
job ID:
action:
runtime:
stage count:
```

### Step 4 — Find the dominant stage

Record:

```text
stage ID:
runtime:
task count:
input:
shuffle write:
shuffle read:
spill:
failures:
```

### Step 5 — Inspect tasks

Record actual observations:

```text
min/typical/max task duration:
smallest/typical/largest shuffle read:
straggler count:
spill concentration:
```

### Step 6 — Inspect the physical plan

```python
result_df.explain('formatted')
```

Possible operators:

```text
Scan parquet
Filter
Exchange
Sort
SortMergeJoin
HashAggregate
```

### Step 7 — Build the causal story

Example only if runtime evidence supports it:

```text
The slowest stage is the join reduce stage.
Most tasks finish quickly, while one task reads far more shuffle data and spills.
The physical plan repartitions both sides by store_id for SortMergeJoin.
Key-frequency evidence shows one store_id dominates the fact rows.
Therefore the primary problem is join-key skew, not an arbitrary lack of
shuffle partitions.
```

### Step 8 — Choose the first change

Possible options depend on evidence:

```text
reduce data before shuffle
fix unnecessary repartition
broadcast small dimension
address hot-key skew
change partition count
remove cache pressure
replace driver misuse
```

Choose the most direct causal intervention.

### Step 9 — Rerun and compare

Record:

```text
same action:
same input:
new stage runtime:
new task distribution:
new shuffle:
new spill:
new runtime plan:
```

### Step 10 — Validate correctness

Confirm:

```text
same schema
same province grain
same completed net_sales totals
```

### Final explanation template

```text
Symptom:
The pipeline was slow.

Location:
Job ___, stage ___ dominated runtime.

Task evidence:
________________________________.

Plan evidence:
________________________________.

Root cause:
________________________________.

First change:
________________________________.

Why this change first:
________________________________.

Rerun evidence:
________________________________.

Correctness:
________________________________.
```

This is the core Phase 8 skill.

---

[Back to Table of Contents](#toc)

---

<a id="17-common-failure-modes"></a>
## 17. Common Failure Modes

### Failure mode 1 — Reading every Spark UI tab without a question

Bad:

```text
open UI
→ inspect everything
→ drown in metrics
```

Better:

```text
symptom
→ relevant job
→ slow/failing stage
→ suspicious tasks
→ plan
```

### Failure mode 2 — Treating stage duration as the root cause

A stage is a location.

You still need to explain the physical behavior within it.

### Failure mode 3 — Calling every straggler "skew"

Confirm uneven data volume, spill, executor placement, or other supporting evidence.

### Failure mode 4 — Changing shuffle partitions immediately

Partition count is only one possibility.

Check:

```text
data distribution
join strategy
unnecessary Exchange
input volume
spill
```

first.

### Failure mode 5 — Increasing memory because spill exists

Spill may be caused by one skewed partition.

Fixing distribution can be better than increasing resources.

### Failure mode 6 — Using `collect()` for diagnostics

Never move a huge dataset to the driver just to inspect it.

Aggregate diagnostics in Spark first.

### Failure mode 7 — Optimizing a broken join

If join grain is wrong and rows multiply unexpectedly, fix correctness first.

Do not hide the error with:

```python
result_df.distinct()
```

### Failure mode 8 — Assuming broadcast from source-code intent

This:

```python
fact_df.join(F.broadcast(dim_df), ...)
```

expresses an intended strategy.

Inspect the actual plan/runtime evidence to confirm what executed.

### Failure mode 9 — Assuming AQE changed something because it is enabled

AQE enabled does not mean AQE modified every query.

Inspect the runtime plan and task behavior.

### Failure mode 10 — Comparing different workloads

Do not compare:

```text
count() baseline
```

to:

```text
write() optimized
```

and call the timing difference an optimization result.

### Failure mode 11 — Ignoring cache state

A cached second run and uncached first run are not equivalent.

State cache materialization/reuse explicitly.

### Failure mode 12 — Inventing Spark UI observations

Teaching code can predict what is likely.

The learner must record what the runtime actually shows.

The rule is:

> **Prediction before execution; observation after execution.**

---

[Back to Table of Contents](#toc)

---

<a id="18-phase-8-mastery-reference"></a>
## 18. Phase 8 Mastery Reference

By the end of Phase 8, you should be able to answer the following for a real PySpark workload.

### Action and job

```text
Which action triggered the job?
Which job corresponds to the workload I care about?
Are multiple actions recomputing the same lineage?
```

### Stage

```text
Which stage dominates runtime or fails?
What physical boundary created that stage?
Is the stage scan-heavy, shuffle-heavy, join-heavy, aggregate-heavy, or sort-heavy?
```

### Tasks

```text
Are tasks balanced?
Are there stragglers?
Do slow tasks process more input or shuffle data?
Do they spill?
Are failures concentrated in particular partitions or executors?
```

### Data movement

```text
How much shuffle data is written and read?
Why does the shuffle exist?
Is the shuffle required by the business result?
Can upstream data volume or row width be reduced?
```

### Partitioning and skew

```text
Are there too few tasks?
Too many tiny tasks?
One/few oversized partitions?
Hot keys?
```

### Memory

```text
Is spill isolated or widespread?
Is memory pressure caused by skew, partition size, cache pressure,
broadcast size, or genuinely insufficient resources?
```

### Executors

```text
Are executors receiving useful work?
Are many idle because parallelism is limited?
Is one executor repeatedly slow or failing?
```

### Driver

```text
Is the problem actually outside distributed task execution?
Am I collecting too much data?
Is driver memory/configuration being confused with executor memory?
```

### Plan

```text
What does df.explain('formatted') show?
Which operator corresponds to the slow/failing stage?
Why did Spark choose this strategy?
```

### AQE

```text
Did AQE actually change the runtime plan?
Did it coalesce post-shuffle partitions?
Change join strategy?
Mitigate skew?
What changed in the tasks?
```

### Optimization

```text
What should I change FIRST?
What evidence justifies that specific change?
Did I change only one variable?
```

### Validation

```text
Did the rerun improve the relevant runtime evidence?
Did schema, grain, row semantics, and business totals remain correct?
```

The core mastery requirement is:

> **Explain why a Spark job is slow rather than merely observe that it is slow.**

A strong Phase 8 diagnosis should sound like:

```text
The write action triggered the job.

Stage 4 dominated runtime. Most tasks completed in roughly the same range, but
one task ran much longer and consumed substantially more shuffle data.

The formatted physical plan showed a hash-partitioning Exchange on store_id
feeding a SortMergeJoin. Key-frequency evidence confirmed a hot store_id.

The primary problem was therefore skewed shuffle distribution, not simply an
insufficient number of executors.

I changed the skew-handling strategy first, reran the same write action, and
compared stage runtime, task-duration distribution, shuffle sizes, and spill.

The optimized result preserved the same schema, grain, and business totals.
```

That is the Phase 8 standard:

```text
observe
→ localize
→ explain
→ intervene
→ measure
→ validate
```

---

[Back to Table of Contents](#toc)
