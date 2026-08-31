# Phase 7 — Experiments & Applied Practice Guide

<a id="toc"></a>
## Table of Contents

- [Purpose](#purpose)
- [Conventions](#conventions)
- [Practice Dataset](#practice-dataset)
- [Performance Experiment Protocol](#performance-experiment-protocol)
- [Experiment 1 — Diagnose Unnecessary I/O](#experiment-1-diagnose-unnecessary-io)
- [Experiment 2 — File Sizing: Fix the Writer, Then Re-read](#experiment-2-file-sizing-fix-the-writer-then-re-read)
- [Experiment 3 — Broadcast vs. Shuffle Join](#experiment-3-broadcast-vs-shuffle-join)
- [Experiment 4 — Remove an Unnecessary Shuffle](#experiment-4-remove-an-unnecessary-shuffle)
- [Experiment 5 — Diagnose Skew and Apply Targeted Salting](#experiment-5-diagnose-skew-and-apply-targeted-salting)
- [Experiment 6 — Justified vs. Unnecessary Caching](#experiment-6-justified-vs-unnecessary-caching)
- [Experiment 7 — AQE Runtime Improvements](#experiment-7-aqe-runtime-improvements)
- [Applied Phase 7 Project](#applied-phase-7-project)
- [Applied Task — Diagnose and Optimize One Pipeline](#applied-task-diagnose-and-optimize-one-pipeline)
- [After the Applied Task](#after-the-applied-task)

---

<a id="purpose"></a>
## Purpose

This file is the execution-oriented companion for **Phase 7 — PySpark Performance Engineering**.

The Phase 7 `README.md` is the conceptual reference. `phase_07_lecture.py` is the consolidated teaching implementation. This guide turns those ideas into controlled experiments where the main habit is:

```text
baseline
    ↓
predict bottleneck
    ↓
inspect evidence
    ↓
execute / measure
    ↓
change ONE thing
    ↓
inspect again
    ↓
execute / measure again
    ↓
compare
    ↓
reconcile correctness
    ↓
explain WHY
```

The governing engineering rule is:

> **Fix query and data design before tuning configuration.**

For every important experiment, answer:

```text
1. What is the required output grain?
2. What business result must remain unchanged?
3. What is the suspected bottleneck?
4. What evidence would support or reject that hypothesis?
5. Which files, rows, and columns are actually read?
6. Which Exchanges are present, and why?
7. What join strategy is actually used?
8. How many execution partitions exist at the relevant boundary?
9. Are those partitions balanced?
10. Is expensive lineage being recomputed?
11. What is AQE changing at runtime, if anything?
12. What ONE change most directly targets the evidence?
13. What changed physically after the optimization?
14. Did schema, grain, row semantics, and business totals remain correct?
```

This guide deliberately uses Phase 4–6 concepts rather than reteaching them. In particular, you are expected to already recognize:

```text
jobs / stages / tasks
narrow vs. wide transformations
Exchange
BroadcastHashJoin
SortMergeJoin
Parquet pruning
execution partitions
repartition()
coalesce()
AQE
skew
```

This guide also intentionally avoids deep Phase 8 Spark UI work. Plans, partition counts/distribution, filesystem evidence, cache state, and controlled elapsed time are sufficient for these experiments.

The applied section is practice only. It does **not** perform the formal Phase 7 mastery gate, update `ROADMAP.md`, or mark the phase complete.

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
- State the grain before changing a join, aggregation, or deduplication strategy.
- Use `df.explain('formatted')` as the primary physical-plan inspection tool.
- Use `df.rdd.getNumPartitions()` only for current Spark execution partitions.
- Use `F.spark_partition_id()` when you need row-distribution evidence.
- Aggregate diagnostic evidence before collecting it to the driver.
- Use filesystem inspection for physical Parquet file count and file-size evidence.
- Treat local wall-clock timing as **supporting evidence**, not proof by itself.
- Compare the same materializing action before and after.
- State whether a run is cold, cache-materializing, or cache-reusing.
- Restore temporary Spark configuration changes after controlled experiments.
- Change exactly **one optimization variable** at a time.
- Reconcile correctness after every meaningful optimization.
- Do not turn teaching configuration values into universal production defaults.
- Do not mark Phase 7 complete until the formal mastery gate is explicitly requested and passed.

### Required diagnosis vocabulary

Be able to classify a bottleneck as primarily:

```text
I/O
join strategy
shuffle
skew / hot key
partition count
data distribution
reuse / recomputation
AQE opportunity
resource pressure
```

A real workload can have several problems. The experiment discipline is still:

```text
identify the highest-value bottleneck
→ change one thing
→ compare
```

---

[Back to Table of Contents](#toc)

---

<a id="practice-dataset"></a>
# Practice Dataset

The paired Phase 7 notebooks should create the deterministic data and helpers directly. The experiment guide assumes the following core DataFrames.

## Main grains

```text
fact_sales_df
= one row per sale_id

dim_store_df
= one row per store_id

dim_product_df
= one row per product_id

skewed_sales_df
= one row per sale_id, with one deliberately dominant store_id
```

## Main fact columns

```text
sale_id
order_date
store_id
product_id
customer_id
order_status
quantity
unit_price
promotion_code
sales_channel
device_type
year
month
gross_sales
```

The intentionally wide fact schema lets the exercises demonstrate both:

```text
unnecessary read width
unnecessary shuffle width
```

## Start one controlled Spark session

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


spark = (
    SparkSession.builder
    .appName('phase_07_performance_experiments')
    .master('local[4]')
    # Keep the baseline static until the AQE experiment deliberately enables it.
    .config('spark.sql.shuffle.partitions', '12')
    .config('spark.sql.adaptive.enabled', 'false')
    .getOrCreate()
)
```

## Required inspection helpers

The notebook should provide helpers equivalent to:

```python
def show_partition_summary(df, label):
    '''Print execution-partition count and non-empty row distribution.'''

    rows_by_partition = (
        df
        .select(F.spark_partition_id().alias('partition_id'))
        .groupBy('partition_id')
        .agg(F.count('*').alias('row_count'))
        .orderBy('partition_id')
        .collect()
    )

    print(f'\n{label}')
    print('execution partitions:', df.rdd.getNumPartitions())

    for row in rows_by_partition:
        print(
            f'partition {row.partition_id}: '
            f'{row.row_count} rows'
        )


def time_count(df, label):
    '''Materialize the same count workload and report elapsed local time.'''

    started_at = perf_counter()
    row_count = df.count()
    elapsed_seconds = perf_counter() - started_at

    print(f'{label}: {row_count} rows in {elapsed_seconds:.3f} s')
    return row_count, elapsed_seconds


def parquet_data_files(path):
    '''Return only physical Parquet data files below a local path.'''

    return sorted(Path(path).rglob('*.parquet'))
```

The notebook should also include a small correctness helper or explicit reconciliation cells for:

```text
row count where meaningful
business totals
grain/key uniqueness
schema where relevant
```

## Temporary storage root

```python
# Keep this object alive for the entire notebook session.
temp_directory = TemporaryDirectory(prefix='pyspark_phase_07_')
temp = Path(temp_directory.name)
```

Do not treat the exact local timings as production conclusions. The primary purpose is to connect a deliberate physical change to visible Spark evidence.

---

[Back to Table of Contents](#toc)

---

<a id="performance-experiment-protocol"></a>
# Performance Experiment Protocol

Every experiment should record the following before the change.

## Baseline record

```text
Business requirement:
Input grain(s):
Output grain:
Correctness invariants:
Suspected bottleneck:
Plan evidence:
Partition/distribution evidence:
File evidence, if relevant:
Cache state:
Materializing action:
Baseline elapsed time:
```

Then write a one-sentence hypothesis:

```text
I expect <one change> to improve performance because <one physical cause>.
```

After the change, record:

```text
Changed variable:
Plan change:
Partition/distribution change:
File-layout change, if relevant:
Cache/AQE change, if relevant:
Optimized elapsed time:
Correctness reconciliation:
Causal explanation:
```

### Strong explanation pattern

```text
Bottleneck:
The large fact/dimension join performs a two-sided shuffle.

Evidence:
The baseline physical plan contains Exchanges on both sides followed by
SortMergeJoin. The dimension is small and unique on the join key.

Change:
Broadcast only the dimension.

Before → after:
SortMergeJoin + two-sided redistribution
→ BroadcastHashJoin + BroadcastExchange of the small side.

Why:
The large fact side no longer needs ordinary join-key redistribution.

Correctness:
The join keys, output grain, row count, and business totals remain unchanged.
```

### Weak explanation pattern

```text
Broadcast was faster because broadcast joins are faster.
```

The weak explanation names an API but does not explain the physical change or validate the result.

---

[Back to Table of Contents](#toc)

---

<a id="experiment-1-diagnose-unnecessary-io"></a>
# Experiment 1 — Diagnose Unnecessary I/O

## Goal

Use Parquet scan evidence to distinguish:

```text
column pruning
predicate pushdown
partition pruning
unnecessary historical reads
```

and identify the most relevant first optimization.

## Setup

Write the fact dataset as a year/month-partitioned Parquet dataset:

```python
partitioned_sales_path = temp / 'fact_sales_partitioned'

(
    fact_sales_df
    .write
    .mode('overwrite')
    .partitionBy('year', 'month')
    .parquet(str(partitioned_sales_path))
)
```

The business request is:

> Compute completed June 2026 sales by store using only `store_id`, `quantity`, and `gross_sales`.

Required output grain:

```text
one row per store_id
```

## Baseline

Build an intentionally broad query that reads the partitioned dataset and carries more columns than necessary.

Your baseline should still produce the **correct June 2026 result**, but it should demonstrate unnecessary scan or intermediate width that can be improved.

## Predict the bottleneck

Before `explain('formatted')`, write predictions for:

```text
ReadSchema
PartitionFilters
PushedFilters
number of unrelated columns carried toward the aggregation
```

Answer:

1. Which columns are truly required to compute the result?
2. Which storage partition columns should restrict the physical read?
3. Which ordinary predicate may appear in `PushedFilters`?
4. What evidence would show that Spark is reading irrelevant history?

## Inspect baseline

```python
baseline_df.explain('formatted')
```

Record:

```text
ReadSchema:
PartitionFilters:
PushedFilters:
Relevant Exchange(s):
```

Then execute the same materializing action you will use after optimization.

## Change ONE thing

Choose the single highest-value I/O improvement supported by the baseline evidence.

Possible single changes include:

```text
project only required columns
add/repair the storage-partition predicate
replace a full-history requirement with the actual June-only business filter
```

Do not change all three at once.

## Inspect again

```python
optimized_df.explain('formatted')
```

Compare only the evidence affected by your chosen change.

## Correctness reconciliation

Confirm:

```text
same output grain
same set of store_ids
same quantity totals
same gross_sales totals
```

## Required explanation

Complete:

```text
The baseline bottleneck was ____________________.
The plan proved this through ____________________.
I changed only ____________________.
After the change, ____________________ changed physically.
Performance should improve because Spark now ____________________.
The business result remained correct because ____________________.
```

## Important trap

Do not claim that writing `.filter()` earlier in Python source automatically proves earlier physical execution.

Use the optimized physical scan as evidence.

---

[Back to Table of Contents](#toc)

---

<a id="experiment-2-file-sizing-fix-the-writer-then-re-read"></a>
# Experiment 2 — File Sizing: Fix the Writer, Then Re-read

## Goal

Show that pathological output partitioning can create a poor physical file layout that becomes a future read-performance problem.

## Baseline writer

Take the same small/medium fact dataset and deliberately create excessive writer parallelism:

```python
small_file_source_df = fact_sales_df.repartition(96)
```

Write it to a new unpartitioned Parquet path.

## Predict before writing

State:

```text
current execution partitions
expected writer parallelism
expected rough file-count behavior
why the file sizes are likely to be small for this teaching dataset
```

## Inspect physical layout

Record:

```text
number of Parquet data files
minimum file size
maximum file size
average file size
```

A helper can summarize sizes from `parquet_data_files(path)`.

## Re-read the fragmented dataset

```python
fragmented_read_df = spark.read.parquet(str(fragmented_path))
```

Record:

```text
input/current execution partition count
same materializing action time
```

The exact local timing may be noisy. The file count and file-size evidence are the stronger causal observations.

## Change ONE thing

Change only the writer distribution, for example:

```python
compacted_source_df = fact_sales_df.repartition(8)
```

Write the same logical data to a second path.

Do **not** simultaneously change:

```text
compression codec
schema
partitionBy columns
maxRecordsPerFile
read configuration
```

## Compare

Record:

| Evidence | Fragmented baseline | Improved writer |
|---|---:|---:|
| Writer execution partitions |  |  |
| Parquet data files |  |  |
| Average file size |  |  |
| Re-read execution partitions |  |  |
| Re-read elapsed time |  |  |

## Correctness reconciliation

Confirm that both outputs contain the same:

```text
row count
schema
sale_id set or deterministic aggregate checksum
gross_sales total
```

## Required explanation

Explain why:

```text
fewer files
!=
automatically better
```

Your answer must mention the tradeoff between:

```text
metadata/file-open overhead
and
useful scan/write parallelism
```

Then explain why fixing the writer is preferable to permanently compensating for a bad layout with arbitrary reader-side tuning.

---

[Back to Table of Contents](#toc)

---

<a id="experiment-3-broadcast-vs-shuffle-join"></a>
# Experiment 3 — Broadcast vs. Shuffle Join

## Goal

Compare a controlled two-sided shuffle join with a broadcast join while preserving join semantics and grain.

## Business requirement

Enrich every sale with:

```text
store_name
province
store_format
```

Expected relationship:

```text
fact_sales_df
many rows per store_id

+

dim_store_df
one row per store_id

→ many-to-one enrichment
```

Expected output grain:

```text
one row per sale_id
```

## Correctness checks before performance work

Verify that `dim_store_df` is unique on `store_id`.

Answer:

```text
What would happen to row count and gross_sales totals if dim_store_df contained
duplicate store_id values?
```

## Baseline: controlled shuffle join

Temporarily disable automatic broadcast and use a merge hint only to create an interpretable baseline:

```python
spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '-1')

baseline_join_df = (
    fact_sales_df
    .join(
        dim_store_df,
        on='store_id',
        how='left',
    )
)
```

Inspect:

```python
baseline_join_df.explain('formatted')
```

Look for evidence such as:

```text
Exchange on both join inputs
Sort
SortMergeJoin
```

## Hypothesis

Write a one-sentence hypothesis explaining why broadcasting the dimension should reduce data movement.

## Change ONE thing

Keep all join keys, selected columns, and business logic the same. Change only the physical join hint:

```python
optimized_join_df = fact_sales_df.join(
    F.broadcast(dim_store_df),
    on='store_id',
    how='left',
)
```

## Inspect again

Look for:

```text
BroadcastExchange
BroadcastHashJoin
absence of ordinary fact-side join-key Exchange
```

## Measure

Use the same materializing action for both plans under comparable cache conditions.

## Reconcile correctness

Confirm:

```text
same schema except harmless physical metadata differences
same row count
sale_id remains unique
same gross_sales total
same unmatched store count
```

## Required explanation

Explain:

1. What physical work the baseline sort-merge join required.
2. What data is replicated by the broadcast join.
3. Why the large fact side can avoid ordinary join-key redistribution.
4. Why a table being called a **dimension** is not sufficient evidence that it should be broadcast.
5. Why the broadcast version must not be accepted if the dimension violates expected uniqueness.

## Restore configuration

Restore the normal broadcast threshold after the controlled comparison.

---

[Back to Table of Contents](#toc)

---

<a id="experiment-4-remove-an-unnecessary-shuffle"></a>
# Experiment 4 — Remove an Unnecessary Shuffle

## Goal

Identify an explicit repartition that does not satisfy the next wide operator and therefore adds avoidable redistribution.

## Business requirement

Compute completed sales totals by `product_id`.

Required output grain:

```text
one row per product_id
```

## Baseline smell

Create an unnecessary distribution first:

```python
baseline_df = (
    fact_sales_df
    .filter(F.col('order_status') == 'COMPLETED')
    .repartition(12, 'store_id')
    .groupBy('product_id')
    .agg(
        F.sum('gross_sales').alias('gross_sales'),
        F.sum('quantity').alias('units'),
    )
)
```

## Predict

Answer before inspection:

```text
What distribution does repartition(12, 'store_id') create?
What distribution does groupBy('product_id') require?
Can the first distribution satisfy the second requirement?
How many Exchanges do you expect to see?
```

## Inspect baseline

```python
baseline_df.explain('formatted')
```

For each `Exchange`, write:

```text
Exchange 1 creates ____________________ because ____________________.
Exchange 2 creates ____________________ because ____________________.
```

## Change ONE thing

Remove only the explicit `repartition(12, 'store_id')`.

Keep:

```text
same filter
same aggregation
same shuffle-partition configuration
same action
```

## Inspect again

Compare:

```text
Exchange count
Exchange partitioning expressions
output partition count
```

## Correctness reconciliation

Confirm that product-level:

```text
row count
gross_sales total
units total
```

are unchanged.

## Required explanation

Your explanation must distinguish:

```text
necessary shuffle
vs.
unnecessary shuffle
```

Do not claim that the optimized query has "no shuffle" if `groupBy('product_id')` still requires one.

The expected engineering conclusion is closer to:

> The product aggregation still requires redistribution by `product_id`, but the prior redistribution by `store_id` had no downstream value and could be removed.

---

[Back to Table of Contents](#toc)

---

<a id="experiment-5-diagnose-skew-and-apply-targeted-salting"></a>
# Experiment 5 — Diagnose Skew and Apply Targeted Salting

## Goal

Prove that one hot key creates badly distributed work, show why ordinary key repartitioning does not solve it, and then apply salting only to a controlled aggregation case.

## Baseline key-frequency profile

Start with:

```python
key_frequency_df = (
    skewed_sales_df
    .groupBy('store_id')
    .agg(F.count('*').alias('row_count'))
    .orderBy(
        F.col('row_count').desc(),
        F.col('store_id').asc(),
    )
)

key_frequency_df.show(truncate=False)
```

Answer:

```text
What percentage of rows belongs to HOT_STORE?
How does the largest key compare with a typical cold key?
```

## Baseline physical distribution

```python
skewed_by_key_df = skewed_sales_df.repartition(
    12,
    'store_id',
)
```

Inspect the partition row distribution.

## Required diagnosis

Explain why:

```text
12 partitions
```

can still be badly distributed.

Then explain why changing only:

```text
12 → 100 hash partitions by store_id
```

still does not split all `HOT_STORE` rows across many buckets.

## Baseline aggregation

Compute:

```text
gross_sales by store_id
```

using ordinary `groupBy('store_id')`.

Record the correct baseline result because salting must reproduce it exactly.

## Change ONE thing: introduce deterministic salt

Use `sale_id` to create a salt that varies **within** the hot key:

```python
salted_df = skewed_sales_df.withColumn(
    'salt',
    F.pmod(
        F.xxhash64('sale_id'),
        F.lit(8),
    ),
)
```

Then implement a two-stage aggregation:

```text
stage 1 grain
= one row per (store_id, salt)

stage 2 grain
= one row per store_id
```

Do not copy a salted join pattern here. This experiment is intentionally limited to aggregation because the correctness reasoning is easier to inspect directly.

## Inspect distribution

Inspect how `HOT_STORE` is spread across `(store_id, salt)` values.

Answer:

```text
Why does hashing only store_id fail as salt?
Why does a row-varying value such as sale_id help spread the hot key?
```

## Reconcile exactly

Compare ordinary vs. salted final results for every `store_id`.

Required invariants:

```text
same store_id set
same gross_sales per store
same total gross_sales
same final output grain
```

## Required explanation

Your answer must explicitly state:

1. The problem was **distribution**, not merely partition count.
2. Normal hash repartitioning keeps equal keys together.
3. Salt is a physical distribution aid, not part of the business key.
4. The second aggregation restores the original `store_id` grain.
5. Salting is not the first fix if the hot key is an invalid/null/sentinel data-quality issue.
6. AQE may sometimes reduce the need for manual salting in real workloads.

---

[Back to Table of Contents](#toc)

---

<a id="experiment-6-justified-vs-unnecessary-caching"></a>
# Experiment 6 — Justified vs. Unnecessary Caching

## Goal

Distinguish a reusable expensive intermediate from a DataFrame that should not be cached.

## Build one expensive reusable intermediate

Use a pipeline containing several operations, for example:

```text
filter completed sales
→ project required columns
→ join store dimension
→ derive a business column
```

Call the result:

```text
reused_sales_df
```

Use it for at least three independent downstream actions, such as:

```text
sales by province
sales by store_format
sales by product_id
```

## Baseline: no cache

Run the three downstream workloads without persisting `reused_sales_df`.

Inspect the plans and record whether the expensive upstream lineage appears repeatedly.

Record total elapsed time for the same three-action sequence.

## Hypothesis

Write:

```text
Caching reused_sales_df should help because ____________________ is currently
recomputed ____________________ times.
```

## Change ONE thing

Persist only the reusable intermediate:

```python
reused_sales_df.persist(StorageLevel.MEMORY_AND_DISK)
```

Materialize it intentionally before timing the **reuse** sequence:

```python
reused_sales_df.count()
```

The materialization action has a cost. Do not pretend the cache appears for free.

## Re-run the same downstream workload

Record:

```text
cache storage level
cache-materialization time
three-action reuse time
plans showing in-memory reuse where visible
```

## Unpersist

```python
reused_sales_df.unpersist()
```

## Part B — prove when not to cache

Take a cheap DataFrame that is used exactly once.

Predict whether caching it can reduce any repeated computation.

Explain why:

```text
used once
→ no recomputation to avoid
→ cache population adds work/storage pressure without reuse benefit
```

You do not need to force a dramatic timing regression on a small local machine. The physical reasoning is the key evidence.

## Required explanation

Compare these two choices:

```text
cache the raw wide fact table
vs.
cache the filtered/projected reusable intermediate
```

Explain why the second is usually the stronger candidate when both would support the same downstream reuse.

Your answer must also mention:

```text
memory pressure
eviction / disk fallback conceptually
unpersisting after the reuse window
benchmark contamination from warm cache state
```

---

[Back to Table of Contents](#toc)

---

<a id="experiment-7-aqe-runtime-improvements"></a>
# Experiment 7 — AQE Runtime Improvements

## Goal

Use AQE as a runtime optimizer after the static query/data design is already reasonable.

This experiment should examine at least:

```text
post-shuffle partition coalescing
runtime join adaptation if reproducible locally
skew handling concept/evidence where reproducible
```

The notebook does not need to force every AQE feature to trigger on every machine. The requirement is to inspect what actually happened and avoid claiming behavior that the plan does not show.

## Part A — partition coalescing

Create a small-result wide operation with an intentionally generous static shuffle target:

```python
spark.conf.set('spark.sql.shuffle.partitions', '64')
spark.conf.set('spark.sql.adaptive.enabled', 'false')
```

Build a `groupBy()` whose final result is small.

### Baseline

Inspect:

```text
planned Exchange partition count
result execution partition count
```

Then enable AQE while changing nothing else:

```python
spark.conf.set('spark.sql.adaptive.enabled', 'true')
```

Rebuild the DataFrame after changing the configuration so the experiment does not accidentally reuse an old planned query.

Execute it and inspect the adaptive/final plan.

Look for evidence that AQE coalesced small shuffle partitions.

### Required explanation

Explain why:

```text
planned shuffle partitions
!=
necessarily final runtime partitions
```

when AQE is active.

## Part B — runtime join changes

Construct a join where one side becomes small after filtering but avoid explicitly broadcasting it.

Inspect the initial/adaptive plan before claiming that Spark changed join strategy.

If the local environment shows a runtime switch, document:

```text
initial join strategy
final join strategy
runtime evidence that made the smaller build side apparent
```

If it does **not** switch, record that fact and explain what conditions would have to differ for runtime broadcast adaptation to become relevant.

## Part C — skew handling

Use the deliberately skewed dataset in a shuffle/join workload with AQE skew handling enabled.

Inspect the final adaptive plan.

If Spark shows skew splitting/handling, document it.

If not, do not fabricate a result. Record:

```text
key-frequency evidence proving skew exists
AQE settings used
plan evidence showing whether adaptive skew handling triggered
```

and explain that small local data may not cross the thresholds required to activate the runtime mechanism.

## Restore baseline configuration

Restore the normal Phase 7 teaching configuration after the experiment.

## Required conclusion

Finish with:

> AQE can improve uncertain runtime physical decisions, but it does not replace correct join keys, selective reads, reasonable file layout, or deliberate data-distribution design.

---

[Back to Table of Contents](#toc)

---

<a id="applied-phase-7-project"></a>
# Applied Phase 7 Project

The applied task combines the experiments into one engineering diagnosis.

You are given a retail pipeline that is **correct but intentionally inefficient**.

The goal is not to maximize the number of optimizations.

The goal is to prove that you can identify and fix the **most relevant first bottleneck** with evidence.

The workflow is mandatory:

```text
observe
→ hypothesize
→ inspect
→ change ONE thing
→ execute / measure
→ compare
→ explain
```

You may reuse:

```text
fact_sales_df
dim_store_df
dim_product_df
partitioned Parquet output
skewed_sales_df
inspection helpers
```

---

[Back to Table of Contents](#toc)

---

<a id="applied-task-diagnose-and-optimize-one-pipeline"></a>
# Applied Task — Diagnose and Optimize One Pipeline

## Business requirement

Produce June 2026 completed sales by:

```text
province
category
```

with:

```text
units
gross_sales
```

using:

```text
partitioned fact_sales Parquet
+
dim_store_df
+
dim_product_df
```

Expected output grain:

```text
one row per (province, category)
```

## Intentionally inefficient baseline

Build a baseline containing several realistic smells, but keep the result logically correct. Examples may include:

```text
reading unnecessary columns
an unnecessary explicit repartition
forcing a shuffle join against one small dimension
carrying wide rows into the final aggregation
caching an intermediate used only once
```

Do **not** deliberately break join keys or grain. This task is performance engineering, not a hidden correctness puzzle.

## Part 1 — Freeze correctness

Before any optimization, record:

```text
fact grain
dim_store grain
dim_product grain
join-key uniqueness expectations
output grain
expected cardinality behavior
```

Create a baseline correctness snapshot containing at least:

```text
output row count
total units
total gross_sales
sorted deterministic output for this teaching dataset or equivalent reconciliation
```

## Part 2 — Diagnose

Inspect:

```python
baseline_df.explain('formatted')
```

Also inspect only the additional evidence necessary to evaluate the likely bottlenecks:

```text
input/current partition counts
key-frequency distribution if skew is suspected
file count/size if I/O layout is suspected
cache state if reuse is suspected
```

Fill out:

```text
Candidate bottleneck 1:
Evidence:
Estimated importance:

Candidate bottleneck 2:
Evidence:
Estimated importance:

Candidate bottleneck 3:
Evidence:
Estimated importance:
```

Then choose **one** first optimization.

## Part 3 — Defend the first optimization

Write:

```text
My first optimization is ____________________.

I chose it before the other candidates because ____________________.

The physical behavior I expect to change is ____________________.

The physical behavior I expect to remain necessary is ____________________.
```

A strong answer may deliberately leave a required shuffle in place.

## Part 4 — Change ONE thing

Implement only your chosen first optimization.

Do not sneak in cleanup changes at the same time.

Examples of valid one-change interventions:

```text
broadcast one proven-small dimension
remove one redundant repartition
project before a wide operator
repair one missing partition-pruning predicate
remove one unnecessary cache
cache one genuinely reused expensive intermediate
```

## Part 5 — Re-inspect and measure

Use the same evidence surfaces as the baseline.

Record:

| Evidence | Baseline | After first optimization |
|---|---|---|
| Relevant scan behavior |  |  |
| Join strategy |  |  |
| Relevant Exchanges |  |  |
| Execution partitions |  |  |
| Distribution evidence |  |  |
| Cache/AQE state |  |  |
| Elapsed time |  |  |

## Part 6 — Reconcile correctness

Prove that the optimized pipeline still has:

```text
same output grain
same output row count
same units total
same gross_sales total
same province/category result rows
```

If any correctness invariant changes unexpectedly, the optimization is rejected until the cause is understood.

## Part 7 — Explain the result

Write the final diagnosis using this exact structure:

```text
Bottleneck:

Evidence:

First optimization:

Before → after physical behavior:

Runtime result:

Why performance changed:

Correctness validation:

What I would investigate next:
```

The final line is important. Performance engineering is iterative; the first optimization can expose the next bottleneck.

## Resource concepts checkpoint

Only after completing the query/data diagnosis, answer conceptually:

1. Which part of this pipeline could create executor-memory pressure?
2. Which mistake could create driver-memory pressure?
3. How could skew cause spill even if average partition size looks reasonable?
4. Why can increasing executor cores increase simultaneous memory demand?
5. Why would increasing executor memory be a weak first response if the plan still contains an unnecessary large shuffle?

Do not tune resource configuration in this applied task unless the evidence clearly proves that query/data-design issues have already been addressed and legitimate resource pressure remains.

---

[Back to Table of Contents](#toc)

---

<a id="after-the-applied-task"></a>
# After the Applied Task

Do not update `ROADMAP.md` or mark Phase 7 complete yet.

Before the formal mastery gate, you should now be able to look at an inefficient PySpark pipeline and say something substantially more precise than:

> It is slow because Spark is distributed and shuffles are expensive.

You should be able to say:

```text
The current bottleneck is most likely <specific physical problem>.

I believe that because <specific plan / partition / file / reuse evidence>.

The first optimization should be <one targeted change>.

I expect that to change <specific physical behavior> while leaving
<required physical behavior> intact.

After execution, I will compare <specific evidence> and reconcile
<specific correctness invariants> before accepting the optimization.
```

That is the Phase 7 engineering standard.

The next artifact is:

```text
exercises/phase_07_experiments_SOLUTION.ipynb
```

The solution notebook should implement the experiments in this guide directly. The starter notebook must later be derived from that solution notebook while preserving setup, deterministic data, helpers, prompts, and inspection scaffolding and removing worked solution code.

---

[Back to Table of Contents](#toc)
