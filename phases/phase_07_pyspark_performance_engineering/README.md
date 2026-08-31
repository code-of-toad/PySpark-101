# Phase 7 — PySpark Performance Engineering

<a id="toc"></a>
## Table of Contents

- [Objective](#objective)
- [Phase Mental Model](#phase-mental-model)
- [1. Performance Engineering as Controlled Diagnosis](#1-performance-engineering-as-controlled-diagnosis)
- [2. Evidence Before Optimization](#2-evidence-before-optimization)
- [3. I/O Optimization](#3-io-optimization)
- [4. File Sizing and Scan Parallelism](#4-file-sizing-and-scan-parallelism)
- [5. Join Strategy Selection](#5-join-strategy-selection)
- [6. Join Keys, Fact/Dimension Patterns, and Correctness](#6-join-keys-fact-dimension-patterns-and-correctness)
- [7. Unnecessary Shuffles and Repartitioning Strategy](#7-unnecessary-shuffles-and-repartitioning-strategy)
- [8. Skew and Hot Keys](#8-skew-and-hot-keys)
- [9. Salting as a Targeted Skew Technique](#9-salting-as-a-targeted-skew-technique)
- [10. Partition Count vs. Partition Distribution](#10-partition-count-vs-partition-distribution)
- [11. Caching and Persistence](#11-caching-and-persistence)
- [12. Adaptive Query Execution](#12-adaptive-query-execution)
- [13. Resource Concepts After Query and Data Design](#13-resource-concepts-after-query-and-data-design)
- [14. Measuring Before and After](#14-measuring-before-and-after)
- [15. End-to-End Diagnostic Workflow](#15-end-to-end-diagnostic-workflow)
- [16. Engineering Decision Rules](#16-engineering-decision-rules)
- [17. Common Failure Modes](#17-common-failure-modes)
- [18. Phase 7 Mastery Reference](#18-phase-7-mastery-reference)

---

<a id="objective"></a>
## Objective

Learn to diagnose PySpark performance problems instead of randomly changing Spark configuration.

Phase 7 turns the execution, plan, storage, partitioning, and shuffle concepts from Phases 4–6 into a repeatable optimization process.

The required topics are:

- column pruning;
- predicate pushdown;
- partition pruning;
- unnecessary reads;
- file sizing;
- broadcast joins;
- sort-merge joins;
- shuffle joins;
- join-key selection;
- fact/dimension join patterns;
- unnecessary shuffle;
- join grain and correctness risks;
- skew;
- hot keys;
- salting conceptually;
- repartitioning strategies;
- too few, too many, and poorly distributed partitions;
- caching;
- persistence;
- storage levels conceptually;
- when not to cache;
- AQE partition coalescing;
- AQE runtime join changes;
- AQE skew handling;
- executor memory;
- executor cores;
- driver memory;
- garbage collection;
- spill;
- memory pressure.

Examples target **PySpark 4.2.0** and use deterministic retail/data-engineering workloads.

The central Phase 7 rule is:

> **Fix query and data design before tuning configuration.**

The central Phase 7 workflow is:

```text
observe
    ↓
hypothesize
    ↓
inspect
    ↓
change ONE thing
    ↓
execute / measure
    ↓
compare
    ↓
explain
```

Phase 4–6 concepts such as jobs/stages/tasks, narrow vs. wide transformations, physical plans, `Exchange`, `BroadcastHashJoin`, `SortMergeJoin`, AQE, Parquet pruning, execution partitions, `repartition()`, `coalesce()`, and skew are assumed knowledge and used as evidence rather than substantially retaught.

Phase 7 intentionally does **not** expand deeply into Phase 8 material such as:

- Spark UI navigation;
- detailed stage/task metric interpretation;
- executor dashboards;
- event logs;
- failure forensics;
- deep runtime monitoring infrastructure.

Those become the main diagnostic surface in Phase 8.

This document is lecture/reference material only. The formal Phase 7 mastery gate is intentionally deferred until explicitly requested.

---

[Back to Table of Contents](#toc)

---

<a id="phase-mental-model"></a>
## Phase Mental Model

A slow Spark pipeline is not a request to start changing configuration.

It is a request to identify **where unnecessary work or badly distributed work exists**.

Use this hierarchy:

```text
1. correctness / grain
        ↓
2. unnecessary data read
        ↓
3. unnecessary data movement
        ↓
4. bad join strategy
        ↓
5. bad partition distribution / skew
        ↓
6. unnecessary recomputation
        ↓
7. AQE opportunities
        ↓
8. resource pressure
        ↓
9. configuration tuning only when evidence justifies it
```

The most important distinction is:

```text
symptom
!=
root cause
```

Examples:

```text
slow task
→ could be skew, oversized input, spill, expensive computation, or bad join strategy

large shuffle
→ could be a required aggregation, an unnecessary repartition, a wrong join, or avoidable pre-join data volume

out-of-memory failure
→ could be insufficient memory, but could also be a hot partition, oversized broadcast, cache pressure, collect(), or a broken join grain

many tasks
→ could be healthy parallelism or excessive tiny partitions
```

The Phase 7 habit is therefore:

> **Do not optimize the symptom. Explain the physical cause first.**

A useful end-to-end model is:

```text
business requirement + required grain
        ↓
physical data read
        ↓
pruning / scan work
        ↓
current execution partitions
        ↓
wide requirements / Exchanges
        ↓
join + aggregation strategy
        ↓
partition-size distribution
        ↓
reuse / recomputation
        ↓
AQE runtime adjustments
        ↓
resource demand
        ↓
measured result
        ↓
correctness reconciliation
```

Every performance improvement must preserve the intended business result.

---

[Back to Table of Contents](#toc)

---

<a id="1-performance-engineering-as-controlled-diagnosis"></a>
## 1. Performance Engineering as Controlled Diagnosis

Performance engineering is a **causal investigation**.

Bad optimization looks like:

```text
job is slow
    ↓
set random Spark configs
    ↓
run again
    ↓
maybe faster, maybe slower
    ↓
no explanation
```

Professional optimization looks like:

```text
job is slow
    ↓
state the expected grain and business result
    ↓
identify the expensive physical behavior
    ↓
form one concrete hypothesis
    ↓
change one relevant variable
    ↓
run the same workload
    ↓
compare physical behavior + result
    ↓
explain why the change helped or did not help
```

### Common bottleneck classes

Use the smallest useful classification first.

| Bottleneck class | Typical physical problem |
|---|---|
| I/O | Spark reads more files, rows, or columns than the result requires |
| Join | Large sides are shuffled unnecessarily, the wrong join strategy is chosen, or the join keys create excess work |
| Shuffle | Records are redistributed more often or at greater volume than necessary |
| Skew | A small number of partitions receive disproportionately large amounts of data |
| Partitioning | Work is divided into too few, too many, or badly balanced partitions |
| Reuse | The same expensive lineage is recomputed multiple times |
| AQE opportunity | Runtime statistics could reduce post-shuffle work or adapt strategy |
| Memory/resource pressure | Partitions, broadcasts, caches, or concurrency exceed useful memory capacity |

Do not force every slow workload into one category. A pipeline can have several problems, but fix and measure them one at a time.

### Optimization order matters

Suppose a fact table scan reads 300 columns and five years of history, but the query needs six columns from one day.

Increasing executor memory is not the first optimization.

The first question is:

> **Why is Spark doing so much work in the first place?**

Similarly, if a join multiplies rows because the dimension is not unique at the join key, faster execution would only produce the wrong answer more quickly.

Correctness comes first.

---

[Back to Table of Contents](#toc)

---

<a id="2-evidence-before-optimization"></a>
## 2. Evidence Before Optimization

A hypothesis needs evidence.

Phase 7 should use the lightest evidence that can answer the question without jumping ahead into full Spark UI analysis.

### Physical-plan evidence

Use:

```python
result_df.explain('formatted')
```

Look for evidence such as:

```text
Scan
ReadSchema
PartitionFilters
PushedFilters
Exchange
Sort
BroadcastExchange
BroadcastHashJoin
SortMergeJoin
HashAggregate
AdaptiveSparkPlan
```

The physical plan answers questions such as:

```text
Which columns are actually scanned?
Which filters reach the data source?
Which storage partitions can be pruned?
Where does Spark redistribute data?
Which join algorithm is planned?
Is a broadcast actually present?
Is AQE active?
```

### Partition-count evidence

Use:

```python
print(df.rdd.getNumPartitions())
```

This answers only:

> **How many current Spark execution partitions does this DataFrame expose?**

It does not tell you whether those partitions are balanced.

### Distribution evidence

For controlled experiments, summarize rows by current partition:

```python
partition_distribution_df = (
    df
    .select(
        '*',
        F.spark_partition_id().alias('partition_id'),
    )
    .groupBy('partition_id')
    .agg(
        F.count('*').alias('row_count')
    )
    .orderBy('partition_id')
)
```

For key skew, summarize key frequencies:

```python
key_frequency_df = (
    df
    .groupBy('store_id')
    .agg(F.count('*').alias('row_count'))
    .orderBy(
        F.col('row_count').desc(),
        F.col('store_id').asc(),
    )
)
```

Do not collect raw large datasets merely to inspect distribution. Aggregate first.

### File evidence

For local experiments, inspect:

```text
number of files
file sizes
storage partition directories
```

This is often more useful than changing a read configuration when the true problem is a poor physical layout.

### Reuse evidence

Ask:

```text
How many actions depend on this lineage?
Does the same expensive scan/join/aggregation execute repeatedly?
Is the DataFrame cached or persisted?
Was the cache materialized before timing the reused workload?
```

Inspect:

```python
print(df.storageLevel)
```

and physical plans where cached data may appear through in-memory scan operators.

### Runtime evidence without Phase 8 depth

Controlled elapsed time can support a conclusion:

```python
from time import perf_counter

started_at = perf_counter()
result_count = result_df.count()
elapsed_seconds = perf_counter() - started_at
```

But local wall-clock timing is noisy and can be distorted by:

```text
warm filesystem caches
JVM warm-up
cached Spark data
background machine load
small teaching datasets
```

Use runtime as **supporting evidence**, not the only evidence.

### Correctness evidence

Before/after performance comparisons should also reconcile:

```text
schema
grain
row counts where meaningful
key uniqueness where required
business aggregates
referential integrity
exact outputs for small deterministic exercises
```

A useful Phase 7 evidence pattern is:

```text
plan evidence
+
partition/distribution evidence
+
physical file evidence where relevant
+
controlled runtime
+
correctness reconciliation
```

---

[Back to Table of Contents](#toc)

---

<a id="3-io-optimization"></a>
## 3. I/O Optimization

The cheapest byte is often the byte Spark never reads.

Phase 6 established the mechanisms. Phase 7 asks whether the pipeline is **actually exploiting them**.

### Column pruning

If a 60-column fact table is used only for:

```text
store_id
order_date
order_status
net_sales
```

then carrying every other column through the scan and wide operators increases unnecessary I/O and row width.

Example:

```python
sales_required_df = (
    spark.read.parquet(sales_path)
    .select(
        'store_id',
        'order_date',
        'order_status',
        'net_sales',
    )
)
```

Inspect the scan's `ReadSchema`.

Remember that Spark may still need columns used by filters even if they are not in the final projection.

### Predicate pushdown

Use selective predicates where the data source can exploit them.

Example:

```python
completed_sales_df = (
    spark.read.parquet(sales_path)
    .filter(F.col('order_status') == 'COMPLETED')
)
```

Inspect `PushedFilters` rather than assuming syntactic placement means the predicate was pushed.

Avoid turning a simple pushdown-compatible predicate into an unnecessarily opaque expression without a reason.

The engineering question is:

> **Can the source prove that some physical data cannot match before Spark decodes it?**

### Partition pruning

If storage is partitioned by:

```text
year
month
```

prefer filters that directly constrain those stored partition columns when the business requirement allows it.

Example:

```python
february_sales_df = (
    spark.read.parquet(sales_path)
    .filter(
        (F.col('year') == 2026)
        & (F.col('month') == 2)
    )
)
```

Inspect `PartitionFilters`.

A query that logically needs February 2026 but fails to prune other years/months has an I/O design problem before it has an executor-sizing problem.

### Avoid unnecessary reads

Common unnecessary-read patterns include:

```text
select('*') when only a few columns are needed
full-history scans for incremental/date-bounded work
reading datasets that are later discarded by logic
re-reading the same expensive source lineage for several actions
poor storage partitioning relative to common filters
many tiny files creating high metadata/open overhead
```

### Source-code order is not the final authority

Do not memorize rules such as:

> Always write `filter()` before `select()` because Spark executes Python lines in that order.

Catalyst can reorder and push compatible operations.

The better rule is:

> **Express the required result clearly, then inspect the optimized physical scan to verify that unnecessary I/O disappeared.**

### Reduce row width before expensive movement

Even when a shuffle is logically necessary, moving narrower rows can reduce the amount of data redistributed.

Example:

```python
join_input_df = fact_sales_df.select(
    'store_id',
    'product_id',
    'net_sales',
)
```

If 40 unrelated descriptive columns are not required downstream, do not send them through a large shuffle merely because they were present upstream.

### I/O optimization test

Before/after, compare:

```text
ReadSchema
PartitionFilters
PushedFilters
selected file layout
input partition count
physical file count/size where relevant
same business result
```

---

[Back to Table of Contents](#toc)

---

<a id="4-file-sizing-and-scan-parallelism"></a>
## 4. File Sizing and Scan Parallelism

File sizing is both a storage problem and a future execution problem.

### Too many small files

A fragmented dataset can create:

```text
high listing/metadata overhead
many file opens
many tiny scan units
task scheduling overhead
poor object-storage request efficiency
```

Example problem:

```text
200 GB dataset
2,000,000 tiny Parquet files
```

The total byte volume may be reasonable while the file layout is operationally expensive.

### Files that are too large

The opposite extreme can reduce useful read parallelism and create heavy individual tasks.

The goal is not:

```text
minimum number of files
```

It is:

```text
files large enough to amortize metadata/open overhead
+
files small enough to preserve useful parallel scan work
```

There is no universal ideal file size.

### Fix the writer before tuning the reader

If a pipeline creates pathological files every day, the long-term fix is usually in the write/layout design.

Possible tools include:

```text
intentional repartitioning before write
coalescing a small result before write
appropriate storage partition columns
writer file-splitting controls when justified
compaction in systems that support it
```

Do not begin by changing file-scan configuration merely to compensate for a permanently broken writer layout.

### File count is not execution partition count

Carry forward the Phase 6 distinction:

```text
physical files
!=
input partitions
!=
current DataFrame partitions
```

Spark can split large files and combine small files into scan partitions depending on source planning.

### Output optimization must preserve future access patterns

Reducing file count is not automatically a win if it destroys useful storage partitioning or creates giant files that future jobs cannot scan efficiently.

The correct question is:

> **What output layout makes the next important workload cheaper without making this write unnecessarily expensive?**

### Evidence

For controlled experiments, compare:

```text
output execution partition count
number of Parquet data files
file-size distribution
next-read input partition count
next-read elapsed time
same logical rows and schema
```

Treat simple size heuristics as starting points for experiments, not universal production laws.

---

[Back to Table of Contents](#toc)

---

<a id="5-join-strategy-selection"></a>
## 5. Join Strategy Selection

Join optimization begins with two questions:

```text
1. Is the join logically correct?
2. What physical movement does the correct join require?
```

Only then should you choose or influence a join strategy.

### Broadcast hash join

A broadcast join is useful when one side is small enough to replicate to the executors safely.

Typical fact/dimension pattern:

```text
large fact_sales
        +
small unique dim_store
        ↓
BroadcastHashJoin
```

Conceptually:

```text
small dimension
    ↓ broadcast to executors
large fact partitions stay distributed
    ↓
local hash lookup against broadcast side
```

The major benefit is avoiding an ordinary two-sided join shuffle for the large fact side.

Inspect for:

```text
BroadcastExchange
BroadcastHashJoin
```

You can explicitly hint a known-small side in controlled code:

```python
joined_df = fact_sales_df.join(
    F.broadcast(dim_store_df),
    on='store_id',
    how='left',
)
```

Do not broadcast simply because a table is called a dimension.

The actual relation must be small enough after required filtering/projection, and the replicated build-side memory cost must be reasonable.

### Sort-merge join

For large equi-join inputs, Spark commonly uses a sort-merge strategy.

Conceptually:

```text
left side  → repartition by join key → sort
right side → repartition by join key → sort
                         ↓
                    merge matches
```

Physical evidence commonly includes:

```text
Exchange
Sort
SortMergeJoin
```

A sort-merge join is not inherently bad.

If both relations are large and neither safely broadcasts, redistributing by the join key may be the correct scalable strategy.

The optimization target is then often to reduce **how much data** reaches the join or to improve distribution, not to eliminate a necessary shuffle at all costs.

### Shuffle joins generally

A shuffle join is any join strategy that needs data redistributed so matching keys can meet in compatible partitions.

The useful question is not:

> How do I avoid every shuffle join?

It is:

> **Is this redistribution required by the correct query, and is Spark moving the minimum reasonable amount of data?**

### Reduce join inputs before the join when semantics permit

Useful patterns include:

```text
filter irrelevant rows
project only required columns
pre-aggregate to the required grain
remove logically invalid duplicates from a dimension
```

Example:

```python
active_stores_df = (
    dim_store_df
    .filter(F.col('is_active'))
    .select(
        'store_id',
        'region',
    )
)
```

A smaller build side can change both I/O and join strategy.

### Do not force a join strategy without evidence

Hints can be useful, but the engineering standard is:

```text
observe current strategy
        ↓
explain why it is expensive
        ↓
prove an alternative is appropriate
        ↓
change one thing
        ↓
inspect actual resulting strategy
        ↓
measure + reconcile
```

AQE may also change join strategy at runtime when better statistics become available.

---

[Back to Table of Contents](#toc)

---

<a id="6-join-keys-fact-dimension-patterns-and-correctness"></a>
## 6. Join Keys, Fact/Dimension Patterns, and Correctness

The fastest join is useless if it joins the wrong rows.

### Grain before strategy

Before every join, state:

```text
left grain
right grain
join key
expected output grain
expected cardinality
```

Example:

```text
fact_sales
= one row per sale_id

dim_store
= one row per store_id

join key
= store_id

expected relationship
= many fact rows to one dimension row

expected output grain
= one row per sale_id
```

If `dim_store` contains duplicate `store_id` rows, the join can multiply fact rows.

That is first a correctness bug and second a performance problem.

### Join-key selection

Choose keys from business semantics, not from whichever key happens to make a plan cheaper.

A valid join key should be evaluated for:

```text
business meaning
uniqueness on the side expected to be unique
null behavior
cardinality
frequency distribution
composite-key requirements
```

A missing component of a composite key can create both wrong matches and much larger join output.

### Fact/dimension joins

A common high-value pattern is:

```text
large fact
+
small, unique, narrow dimension
```

Optimization opportunities often include:

```text
project dimension columns before join
filter dimension when business semantics allow
validate dimension uniqueness
broadcast when genuinely small enough
```

### Null or sentinel keys can become hot keys

Values such as:

```text
NULL
'UNKNOWN'
'UNMAPPED'
```

can accumulate huge row counts.

Before treating the resulting skew as purely physical, ask whether those records should:

```text
join at all
be rejected/quarantined
use a different business rule
remain unmatched in a left join
```

Correct data-quality handling can remove the physical hot-key problem without any salting.

### Pre-aggregation can reduce join volume

If the downstream output only needs store-level sales totals, joining raw sale lines to another store-grain dataset may be unnecessary.

When semantics permit:

```python
store_sales_df = (
    fact_sales_df
    .groupBy('store_id')
    .agg(F.sum('net_sales').alias('net_sales'))
)
```

can reduce rows before a later store-grain join.

But pre-aggregation changes grain.

Use it only when the requested result remains correct.

### Correctness validation after join optimization

At minimum, compare relevant invariants such as:

```text
expected row count behavior
fact primary-key uniqueness
joined dimension match rate
business totals before/after
null/unmatched key counts
```

Do not accept a faster join until those invariants still hold.

---

[Back to Table of Contents](#toc)

---

<a id="7-unnecessary-shuffles-and-repartitioning-strategy"></a>
## 7. Unnecessary Shuffles and Repartitioning Strategy

A shuffle is expensive because records cross partition boundaries and intermediate data must be coordinated between stages.

But a shuffle is not automatically a mistake.

The goal is:

> **Remove unnecessary redistribution and reduce the volume of necessary redistribution.**

### Common necessary shuffles

Operations such as these often require global/key-based redistribution:

```text
groupBy
large non-broadcast join
distinct / dropDuplicates
global orderBy
repartition
```

Phase 6 already established why.

Phase 7 asks whether the pipeline is paying for more redistribution than the business result requires.

### Common unnecessary-shuffle patterns

#### Repartitioning before an operation that will redistribute anyway

Example smell:

```python
prepared_df = sales_df.repartition(200, 'store_id')

result_df = (
    prepared_df
    .groupBy('product_id')
    .agg(F.sum('net_sales').alias('net_sales'))
)
```

The explicit `store_id` distribution does not satisfy the later `product_id` grouping requirement.

You may pay for two distributions instead of one.

#### Repartitioning both join sides by habit

```python
left_df = left_df.repartition(200, 'store_id')
right_df = right_df.repartition(200, 'store_id')
joined_df = left_df.join(right_df, 'store_id')
```

This is not automatically an optimization.

Spark's physical join operator already requests the distribution it requires. Inspect the plan to determine whether explicit repartitioning removes, preserves, or adds exchanges.

#### Global sorting when only partition-local order is needed

```python
df.orderBy('event_ts')
```

can require global redistribution/order.

If the actual requirement is only local ordering within existing partitions, a different operation may be appropriate.

Do not weaken the business requirement merely to avoid a shuffle.

#### Global deduplication as a cleanup habit

```python
joined_df.distinct()
```

can hide an upstream grain bug while adding another shuffle.

Fix the duplicate-producing logic instead.

### Reduce shuffle width and row count

Before a required wide operator, ask:

```text
Can I prune columns?
Can I filter rows?
Can I pre-aggregate safely?
Can a small dimension be broadcast?
Can I eliminate invalid duplicate dimension rows?
```

Reducing input volume often matters more than trying to tune the shuffle itself.

### Use `repartition()` intentionally

Useful reasons include:

```text
increase parallelism after a severe collapse in partitions
rebalance highly uneven non-key partitions
distribute by a key that a reused downstream workload benefits from
prepare a deliberate writer distribution
create a controlled performance experiment
```

Each reason pays for redistribution and therefore needs evidence.

### Use `coalesce()` intentionally

Use it mainly when:

```text
you need fewer partitions
current distribution is acceptable
a full rebalance is unnecessary
```

Do not use it to fix skew.

### Repartitioning is not a substitute for join/skew reasoning

```python
df.repartition(500, 'customer_id')
```

can still produce a giant partition if one `customer_id` dominates the data.

Partition count and key distribution are separate dimensions.

---

[Back to Table of Contents](#toc)

---

<a id="8-skew-and-hot-keys"></a>
## 8. Skew and Hot Keys

**Skew** means distributed work is materially uneven.

A **hot key** is a key value whose frequency is so large that key-based redistribution sends disproportionate work to one or a few partitions.

Example:

```text
store_id = S01 → 70% of all rows
remaining 99 stores → 30% of all rows
```

Hash partitioning by `store_id` cannot evenly spread the `S01` rows because equal keys must normally meet for the keyed operation.

### Why skew hurts

One large partition can create:

```text
one long-running straggler task
large memory working set
spill to disk
heavy garbage collection
poor executor utilization after other tasks finish
slow join/aggregation completion
```

The stage often finishes at the speed of its slowest required partition.

### Diagnose the key distribution first

Use a frequency profile:

```python
store_frequency_df = (
    sales_df
    .groupBy('store_id')
    .agg(F.count('*').alias('row_count'))
    .orderBy(
        F.col('row_count').desc(),
        F.col('store_id').asc(),
    )
)
```

Useful questions:

```text
What share of rows belongs to the largest key?
How large is the largest key relative to the median key?
Are NULL/sentinel values dominating?
Is skew present on one side or both sides of a join?
```

### Inspect execution-partition distribution

For a controlled experiment:

```python
repartitioned_df = sales_df.repartition(
    8,
    'store_id',
)

partition_sizes_df = (
    repartitioned_df
    .select(
        F.spark_partition_id().alias('partition_id')
    )
    .groupBy('partition_id')
    .agg(F.count('*').alias('row_count'))
    .orderBy('partition_id')
)
```

A healthy partition count can still hide unhealthy partition sizes.

### Fix skew at the highest-value layer first

Use this order of thought:

```text
1. Is the hot key logically valid?
2. Can invalid/sentinel/null records be handled separately?
3. Can the data volume be reduced before the wide operation?
4. Can a small side be broadcast so the large side avoids a join shuffle?
5. Can AQE handle the skewed shuffle partition adequately?
6. Is targeted salting justified?
```

### Skew is not always a partition-count problem

Increasing:

```text
8 partitions → 200 partitions
```

does not split one indivisible hot key across 200 buckets for a normal key-based hash distribution.

More partitions may help the non-hot keys while the hot-key partition remains the bottleneck.

That is why skew requires distribution reasoning, not number tuning.

---

[Back to Table of Contents](#toc)

---

<a id="9-salting-as-a-targeted-skew-technique"></a>
## 9. Salting as a Targeted Skew Technique

**Salting** deliberately adds an additional distribution key so rows belonging to a hot business key can be spread across several physical buckets.

It is an advanced targeted technique, not the first response to every skewed join or aggregation.

### Aggregation concept

Suppose `store_id = 'S01'` dominates sales.

A two-stage salted aggregation can conceptually do:

```text
hot store rows
    ↓ add salt based on row-level information
(store_id, salt)
    ↓ first partial aggregation
smaller salted partial states
    ↓ aggregate again by store_id
final correct store total
```

Important:

> **The salt must vary across rows of the hot key. Hashing only `store_id` would send every `S01` row to the same salt and solve nothing.**

A deterministic teaching pattern might use a stable row identifier:

```python
salted_df = sales_df.withColumn(
    'salt',
    F.pmod(
        F.xxhash64('sale_id'),
        F.lit(8),
    ),
)
```

Then:

```text
groupBy('store_id', 'salt')
        ↓
partial aggregate
        ↓
groupBy('store_id')
        ↓
final aggregate
```

### Join concept

For a hot-key join, salting is more complex.

The hot rows on the large side can be spread across salt values, while the matching rows on the other side must be expanded or otherwise made compatible with those salt values.

That increases implementation complexity and can increase data volume on the replicated side.

### Correctness risks

Salting can break results if you:

```text
forget the second aggregation
replicate dimension rows incorrectly
salt both sides inconsistently
change the logical business key
apply salt to non-hot keys unnecessarily
```

Always reconcile against an unsalted correct baseline on deterministic data.

### When salting is justified

Use it when evidence shows:

```text
one/few keys dominate
the wide operation is truly bottlenecked by those keys
simpler fixes are insufficient
AQE does not adequately resolve the problem
correctness can be preserved and tested
```

The goal is not to make the code look clever.

The goal is to split a proven hot physical workload while preserving the original logical result.

---

[Back to Table of Contents](#toc)

---

<a id="10-partition-count-vs-partition-distribution"></a>
## 10. Partition Count vs. Partition Distribution

A partitioning problem can take at least three forms:

```text
too few partitions
too many partitions
poorly distributed partitions
```

These require different fixes.

### Too few partitions

Symptoms can include:

```text
available cores sitting idle
very large individual tasks
large per-task memory demand
long task duration
large output per writer task
```

A justified `repartition()` can create more parallel units of work, but only after you identify where extra parallelism is actually useful.

### Too many partitions

Symptoms can include:

```text
many tiny tasks
scheduler overhead
many tiny shuffle blocks
metadata overhead
small output files
very little useful work per task
```

Reducing partitions can help, but do not collapse so aggressively that you create a new under-parallelized bottleneck.

### Poor distribution

A DataFrame can have a seemingly reasonable number of partitions while one is enormous.

Example:

```text
64 partitions
63 partitions ≈ 100 MB
1 partition ≈ 20 GB
```

The average partition size is misleading.

The problem is distribution, not count.

### Repartition by count vs. by key

```python
balanced_df = df.repartition(32)
```

requests a redistributed partition count without preserving a business-key grouping requirement.

```python
keyed_df = df.repartition(
    32,
    'store_id',
)
```

creates hash distribution by `store_id`.

The second form is useful only if the downstream work benefits from that key distribution and the key frequencies are acceptable.

### AQE changes the static-number mindset

A configured/planned shuffle partition count is not necessarily the final runtime partition count when AQE is active.

That is useful: a deliberately high enough initial parallelism target can sometimes be coalesced adaptively after Spark observes actual shuffle sizes.

Do not turn this into a rule that "more initial partitions is always better."

The correct structure remains workload-dependent.

### No magic partition number

A partition count only becomes meaningful with context:

```text
data bytes
row width
key distribution
available task slots
downstream operator
file layout
AQE behavior
```

Phase 7 should train the question:

> **What evidence says the current partitioning is the bottleneck?**

not:

> What number should `spark.sql.shuffle.partitions` always be?

---

[Back to Table of Contents](#toc)

---

<a id="11-caching-and-persistence"></a>
## 11. Caching and Persistence

Caching is a trade:

```text
spend storage memory/disk
        ↓
avoid recomputing an expensive lineage
```

It is beneficial only when the avoided recomputation is worth the storage and memory pressure.

### `cache()`

A common pattern is:

```python
reused_df = (
    fact_sales_df
    .filter(F.col('order_status') == 'COMPLETED')
    .join(
        F.broadcast(dim_store_df),
        on='store_id',
        how='left',
    )
)

reused_df.cache()
```

Caching is lazy. The DataFrame becomes populated in cache when an action executes it.

A controlled experiment can intentionally materialize it:

```python
reused_df.count()
```

Then several downstream actions can reuse the cached result.

### `persist()`

`persist()` allows an explicit storage-level choice.

Conceptually, storage levels decide combinations of:

```text
memory retention
possible disk fallback
serialized/deserialized representation
replication behavior depending on level
```

Phase 7 needs the engineering tradeoff, not memorization of every storage-level constant.

### Good cache candidates

A DataFrame is a stronger cache candidate when it is:

```text
expensive to compute
reused multiple times
stable across those uses
small enough to cache without damaging the rest of the workload
selective enough that caching meaningfully reduces future work
```

Examples:

```text
expensive cleaned fact subset reused by several reports
expensive join reused by several independent aggregations
iterative exploratory workload over one stable intermediate
```

### When not to cache

Avoid caching by habit when the DataFrame is:

```text
used once
cheap to recompute
so large it will cause eviction/thrashing
an unfiltered/unprojected raw source when only a small subset is reused
part of a workload where cache pressure harms execution memory
```

Caching the largest DataFrame in the job simply because it is large is often the wrong instinct.

### Cache placement matters

Compare:

```text
cache raw 200-column fact table
        ↓
filter/project repeatedly
```

with:

```text
filter/project once
        ↓
cache the smaller reusable intermediate
```

The second can require much less cache space while avoiding the same repeated upstream work.

### Unpersist when the reuse window ends

```python
reused_df.unpersist()
```

Long-lived unnecessary caches compete with shuffle, aggregation, join, and other working memory.

### Caching can contaminate benchmarks

If you compare a cold first run with a cached second run without acknowledging the difference, you have not isolated your optimization.

For controlled experiments:

```text
state whether each run is cold or cache-reusing
materialize intentionally
use the same downstream action
unpersist between unrelated experiments
```

### Cache evidence

Useful evidence includes:

```text
df.storageLevel
in-memory scan operators in the plan
fewer repeated source/join computations
controlled first-use vs. reuse timings
```

Do not cache merely because one action was slow.

Cache because the same expensive result is **reused**.

---

[Back to Table of Contents](#toc)

---

<a id="12-adaptive-query-execution"></a>
## 12. Adaptive Query Execution

AQE allows Spark to modify parts of the physical execution strategy using statistics observed during runtime.

Phase 5 introduced AQE. Phase 7 asks how it can reduce real performance waste.

### 1. Dynamic shuffle-partition coalescing

A static shuffle target can create many tiny post-shuffle partitions when the actual output is smaller than expected.

AQE can combine adjacent small shuffle partitions into fewer runtime partitions.

Conceptually:

```text
planned shuffle
Q0 Q1 Q2 Q3 Q4 Q5 Q6 Q7 ...
        ↓ runtime sizes are small
AQE
        ↓
coalesced downstream partitions
R0 R1 R2 ...
```

Benefit:

```text
less tiny-task overhead
more useful work per downstream task
```

### 2. Runtime join-strategy changes

A join side may turn out to be much smaller at runtime than static planning information suggested.

AQE can sometimes change the join strategy, for example from a shuffle-heavy plan toward a broadcast-based plan when runtime evidence makes broadcasting appropriate.

The important habit is:

> **Inspect the adaptive/final physical behavior rather than assuming the initial plan is the whole story.**

### 3. Skew handling

AQE can detect oversized shuffle partitions and split skewed work so one giant reducer partition does not dominate the whole stage.

Conceptually:

```text
normal partitions
Q0 Q1 Q2

skewed partition
Q3 = huge
        ↓
AQE skew handling
        ↓
Q3a Q3b Q3c ...
```

For a skewed join, Spark may also need to replicate compatible data from the other side so the split pieces can still join correctly.

### AQE is not a substitute for data design

AQE does not excuse:

```text
wrong join keys
broken grain
scanning years of unnecessary data
millions of tiny source files
broadcasting a relation that is fundamentally too large
caching everything
```

Think of AQE as:

```text
runtime correction of uncertain physical details
```

not:

```text
a replacement for engineering the query correctly
```

### Controlled AQE experiment

A useful Phase 7 experiment can compare the same workload with one AQE behavior changed while keeping business logic constant.

Inspect:

```text
initial vs. adaptive/final plan
post-shuffle partition count
join operator if it changes
partition skew behavior
timing
correctness
```

Do not change five AQE-related settings at once.

### AQE decision rule

Use AQE where runtime information can improve decisions that were uncertain before execution.

Still fix obvious static design problems first.

---

[Back to Table of Contents](#toc)

---

<a id="13-resource-concepts-after-query-and-data-design"></a>
## 13. Resource Concepts After Query and Data Design

Resource tuning matters, but it comes **after** the query and data layout are reasonably engineered.

The order is deliberate.

### Executor memory

Executor memory supports the JVM-side work performed by executor processes, including task execution and cached/persisted data.

Memory demand rises with factors such as:

```text
large partitions
wide rows
large aggregation state
large sort/shuffle working sets
large broadcast relations
multiple concurrent tasks
cached datasets
```

Adding memory can help a workload that genuinely has insufficient capacity.

It does not fix an unnecessary 5 TB shuffle that should have been 100 GB.

### Executor cores

Executor cores influence how many tasks an executor can run concurrently.

More cores can increase parallel work, but they can also increase simultaneous memory demand inside one executor.

Therefore:

```text
more cores per executor
!=
automatically faster
```

The useful balance depends on task memory, CPU cost, I/O behavior, and cluster layout.

### Driver memory

The driver coordinates the Spark application and holds driver-side state/results.

Driver memory becomes especially relevant when code pulls distributed data back to the driver through operations such as:

```python
df.collect()
df.toPandas()
```

on results that are not actually small.

Increasing driver memory is not a solution to executor-side skew or oversized shuffle partitions.

### Garbage collection

The JVM reclaims unreachable objects through garbage collection.

Excessive garbage-collection time can indicate that an executor is spending too much time managing memory rather than doing useful computation.

Possible contributors include:

```text
high memory pressure
large/complex task working sets
heavy object allocation
aggressive caching
many concurrent memory-heavy tasks
```

Do not respond by changing GC settings first.

First ask why the workload is creating that pressure.

### Spill

When execution cannot keep all required intermediate state in memory, Spark operators such as sorts, shuffles, or aggregations may spill data to disk.

Spill is a legitimate safety mechanism.

The problem is **excessive spill** caused by work units that are much larger than the available execution memory can handle efficiently.

Likely upstream causes include:

```text
skewed partition
under-partitioning
large sort
large aggregation state
shuffle-heavy join
cache pressure
```

### Memory pressure

Treat memory pressure as a physical consequence to explain.

Ask:

```text
Which operator needs the memory?
Which partition is too large?
Is a broadcast too large?
Is cache consuming memory needed by execution?
Are too many heavy tasks concurrent?
Can data volume be reduced before the operator?
```

Only after those questions are answered should resource sizing become the primary lever.

### Resource-first anti-pattern

Bad:

```text
OOM
→ double executor memory
```

Better:

```text
OOM
→ identify executor vs. driver
→ identify operator/partition pattern
→ inspect skew / broadcast / cache / shuffle volume
→ reduce unnecessary work if possible
→ then size resources if the remaining legitimate workload requires it
```

Phase 8 will provide richer runtime evidence for these symptoms.

---

[Back to Table of Contents](#toc)

---

<a id="14-measuring-before-and-after"></a>
## 14. Measuring Before and After

An optimization claim needs a comparable baseline.

### Define the baseline

Record the business result and relevant physical evidence before changing anything.

Example:

```text
output grain = one row per store_id
output rows = 120 stores
net_sales total = X
join strategy = SortMergeJoin
shuffle/output partitions = N
elapsed time = T
```

### Change one thing

Examples:

```text
project fewer columns
add a partition-pruning predicate
broadcast one proven-small dimension
remove one redundant repartition
salt one proven hot key
cache one reused expensive intermediate
enable one AQE behavior for comparison
```

Do not simultaneously:

```text
change join hint
change shuffle partition count
change cache placement
change executor memory
change file layout
```

and then claim to know why runtime improved.

### Execute the same work

The baseline and optimized versions must perform the same business work.

Be careful with actions.

A `count()` can be useful for controlled exercises, but an optimizer may avoid computing columns that a count does not need.

If the real workload writes a full result, the strongest comparison may be the same full write or another action that materializes the same required columns/logic.

### Control cache state

State explicitly whether each run is:

```text
cold
cache-materializing
cache-reusing
```

Unpersist between experiments when cached state would invalidate the comparison.

### Use multiple evidence types

A strong before/after explanation might say:

```text
Before:
- 40-column Parquet ReadSchema
- two Exchanges around a large join
- 128 partitions, one holding 55% of rows
- runtime 18.4 s

Change:
- projected to 6 required columns before the join

After:
- 6-column scan/join payload
- same required Exchanges
- same partition count/distribution
- runtime 12.1 s
- identical reconciled output

Explanation:
- the join still required redistribution, but each shuffled row was narrower,
  so Spark moved and processed fewer bytes without changing grain.
```

The numbers are meaningful because the physical explanation matches the change.

### Local timing is evidence, not truth

Small local experiments can have noisy timings.

If the plan/distribution clearly improves but one tiny run is slower by a fraction of a second, do not overfit to that single observation.

Repeat controlled runs and prioritize causal physical evidence.

### Correctness is part of measurement

Every optimization comparison should end with:

```text
same schema where required
same grain
same business totals
same expected matches/unmatched rows
same deterministic result for controlled exercises
```

A performance regression can be acceptable if it is required for correctness.

A correctness regression is not an optimization.

---

[Back to Table of Contents](#toc)

---

<a id="15-end-to-end-diagnostic-workflow"></a>
## 15. End-to-End Diagnostic Workflow

Use this workflow for every important Phase 7 experiment.

```text
observe
    ↓
hypothesize
    ↓
inspect
    ↓
change ONE thing
    ↓
execute / measure
    ↓
compare
    ↓
explain
```

### Step 0 — Freeze the intended result

State:

```text
input grain(s)
output grain
join cardinality expectations
required columns
business invariants
```

This prevents a faster but incorrect rewrite from being accepted.

### Step 1 — Observe the symptom

Examples:

```text
scan seems disproportionate to result
join dominates the plan
many Exchanges appear
one partition is much larger than others
same expensive lineage runs repeatedly
post-shuffle work is fragmented into tiny partitions
memory pressure appears during one wide operator
```

Do not diagnose yet.

### Step 2 — Form one hypothesis

Good hypothesis:

> The fact/dimension join is shuffling both sides even though the projected dimension is small enough to broadcast.

Weak hypothesis:

> Spark needs tuning.

### Step 3 — Inspect evidence

Choose evidence that can falsify the hypothesis.

Examples:

```text
ReadSchema / PartitionFilters / PushedFilters
join operator + Exchanges
partition count
key-frequency distribution
partition-size distribution
cache storage level
adaptive/final plan
file count/size
```

### Step 4 — Choose the smallest relevant change

Examples:

```text
select required columns
add the missing storage-partition filter
broadcast the proven-small dimension
remove one redundant repartition
pre-aggregate at a valid grain
separate invalid hot-key rows
apply targeted salting
cache one reused expensive intermediate
unpersist an unnecessary cache
allow AQE to coalesce runtime shuffle partitions
```

### Step 5 — Execute the same workload

Use the same action/sink and comparable cache conditions.

### Step 6 — Compare physical behavior

Ask:

```text
Did the scan read less?
Did the join strategy change?
Did an Exchange disappear?
Did shuffled row width/volume reduce conceptually?
Did partition distribution improve?
Did repeated lineage disappear?
Did AQE alter runtime partitioning?
Did memory pressure move or disappear?
```

### Step 7 — Reconcile correctness

Prove that the optimization preserved the result.

### Step 8 — Explain the causal chain

Use this template:

```text
Bottleneck:
<physical problem>

Evidence:
<plan / partition / distribution / file / runtime evidence>

Change:
<one optimization>

Before → after:
<what physically changed>

Why:
<causal explanation>

Correctness:
<how equivalence was validated>
```

### Step 9 — Only then consider another change

Performance engineering is iterative.

The next most important bottleneck may become visible only after the first one is removed.

---

[Back to Table of Contents](#toc)

---

<a id="16-engineering-decision-rules"></a>
## 16. Engineering Decision Rules

1. **Preserve correctness and grain before optimizing anything.** A faster wrong pipeline is worse engineering.
2. **Fix query and data design before tuning configuration.** Configuration should solve a demonstrated resource/execution need, not uncertainty.
3. **State one bottleneck hypothesis at a time.** Then gather evidence that could prove it wrong.
4. **Use the physical plan as evidence.** Do not assume pruning, broadcast, or shuffle behavior from source code alone.
5. **Read only the columns the result needs.** Narrow rows before expensive joins/shuffles when semantics permit.
6. **Use partition pruning for selective storage access.** If common filters cannot exclude irrelevant storage partitions, reconsider the data/layout design.
7. **Treat predicate pushdown as an observed source capability.** Inspect pushed filters rather than assuming every filter is delegated.
8. **Fix pathological file layouts at the writer when possible.** Reader configuration should not permanently compensate for bad output design.
9. **Do not optimize for the fewest files.** Optimize for useful scan/write parallelism with manageable metadata overhead.
10. **Broadcast only relations that are genuinely small enough.** A dimension label does not guarantee broadcast safety.
11. **Validate uniqueness on the dimension side of a many-to-one join.** Row multiplication is a correctness and performance failure.
12. **Choose join keys from business semantics first.** Performance never justifies an incorrect key.
13. **Reduce join inputs before the join when semantics permit.** Filter, project, and pre-aggregate at a valid grain.
14. **Do not treat `SortMergeJoin` as a failure.** It is often the correct scalable strategy for two large equi-join inputs.
15. **Remove unnecessary shuffles, not necessary ones.** Explain what distribution each `Exchange` creates and why it exists.
16. **Do not repartition before every join/aggregation by habit.** The explicit repartition is itself a shuffle.
17. **Use `repartition()` when you need a new distribution.** Pay for it only when downstream work benefits.
18. **Use `coalesce()` mainly to reduce partitions without requiring a rebalance.** It is not a skew fix.
19. **Distinguish partition count from partition balance.** A reasonable count can still contain one catastrophic partition.
20. **Profile key frequencies before repartitioning by key.** A hot key survives ordinary hash repartitioning.
21. **Treat null/sentinel hot keys as a data-quality/business question first.** They may not belong in the same join path.
22. **Use salting only for proven hot-key bottlenecks.** Preserve the original logical key and reconcile the final result.
23. **Cache only reused expensive intermediates.** One-use DataFrames normally should not be cached.
24. **Place caches after useful reduction when possible.** Caching unnecessary rows/columns wastes memory.
25. **Unpersist when reuse ends.** Cache capacity is a shared performance resource.
26. **Do not compare cold and warm/cache-reused runs as though they are equivalent.** Control experiment state.
27. **Let AQE adapt uncertain runtime details.** Still fix obvious static design problems yourself.
28. **Inspect adaptive/final behavior when AQE is enabled.** Initial shuffle counts and join choices may not be the final execution.
29. **Treat spill and heavy GC as symptoms.** Find the operator/partition/cache causing memory pressure before changing memory settings.
30. **Do not confuse driver memory with executor memory.** Driver-side collection problems and executor-side task problems have different causes.
31. **More executor cores can increase concurrent memory pressure.** Resource settings interact; larger numbers are not inherently faster.
32. **Change one thing per experiment.** Otherwise causal attribution becomes weak.
33. **Use the same materializing action/sink before and after.** Comparing different work is not benchmarking.
34. **Reconcile outputs after every meaningful optimization.** Performance evidence is incomplete without correctness evidence.
35. **Prefer the optimization with the largest justified reduction in work.** Micro-tuning should come after high-level data/query fixes.

A repeatable Phase 7 checklist is:

```text
1. What is the required output grain?
2. What correctness invariants must not change?
3. Which files/columns/rows does Spark actually read?
4. Are partition and predicate pruning occurring?
5. Is the file layout causing avoidable overhead?
6. Where are the Exchanges?
7. Why is each Exchange required?
8. What join strategy is used?
9. Are join keys semantically correct and dimension keys unique?
10. Can join inputs be reduced safely?
11. Could one side broadcast safely?
12. How many execution partitions exist at important boundaries?
13. Are those partitions balanced?
14. Which keys are hottest?
15. Is repartitioning helping or merely adding another shuffle?
16. Is the same expensive lineage reused?
17. Would caching save more work than it costs?
18. What is AQE changing at runtime?
19. Is spill/GC/memory pressure caused by a data/query problem?
20. What ONE change has the strongest evidence behind it?
21. Did the same workload become physically cheaper?
22. Did the business result remain correct?
```

---

[Back to Table of Contents](#toc)

---

<a id="17-common-failure-modes"></a>
## 17. Common Failure Modes

### Random configuration tuning

Weak:

> The job is slow, so increase `spark.sql.shuffle.partitions` and executor memory.

Better:

> The plan shows a two-sided shuffle join, but the projected dimension is small. Test broadcast first because it directly targets the observed data movement.

### Treating runtime alone as diagnosis

A faster run does not prove why it was faster.

Use physical evidence and control cache/warm-state differences.

### Reading everything and hoping Catalyst fixes everything

Catalyst can prune and push many operations, but it cannot invent a better physical storage layout or business filter that the query never expressed.

Inspect the scan.

### Assuming source-code order equals physical execution order

DataFrame transformations are declarative and optimized.

Use the physical plan as the authority.

### Believing every shuffle is bad

Correct distributed aggregation and large joins often require redistribution.

The goal is to eliminate **unnecessary** shuffles and minimize the data moved by necessary ones.

### Believing every `SortMergeJoin` should become broadcast

Broadcasting requires a build side that is genuinely small enough to replicate safely.

Two large relations may correctly use sort-merge.

### Broadcasting an unvalidated dimension

A small dimension with duplicate join keys can still multiply fact rows.

Small size does not imply correct grain.

### Forcing broadcast because it was faster on toy data

A dimension that fits easily in a local experiment may be much larger in production.

Hints should follow realistic size evidence and failure-risk reasoning.

### Repartitioning before every wide operator

Each repartition can create another shuffle.

Do not pay for redistribution unless the new distribution has downstream value.

### Increasing partitions to fix a hot key

A single hot key still hashes to one bucket for normal keyed redistribution.

More buckets do not divide one equal key by themselves.

### Using salting before validating the data

If `'UNKNOWN'` is the hot key because invalid records were allowed into the join, fix the invalid-data path first.

### Salting by the hot key itself

This:

```text
salt = hash(store_id) % N
```

assigns every equal hot key the same salt.

A useful salt must vary across rows that share the hot key.

### Forgetting to remove the salt logically

Salt is a physical distribution aid, not part of the business grain.

Final aggregations/joins must restore the original logical result.

### Looking only at average partition size

One 20 GB partition can hide behind a reasonable average across hundreds of partitions.

Inspect the distribution.

### Using `coalesce()` to solve skew

`coalesce()` collapses existing partitions without globally rebalancing them.

It can preserve or worsen unevenness.

### Caching everything

Cache competes for memory and can create eviction, recomputation, GC pressure, and spill elsewhere.

Cache only when reuse justifies it.

### Caching a raw wide dataset instead of the reusable reduced intermediate

This consumes more memory than necessary and may cache data no downstream action needs.

### Timing a cache-materializing run against a cache-reusing run

Those executions perform different physical work.

Label and control the cache state.

### Assuming AQE will repair bad design

AQE can adapt shuffle partitions, join choices, and skewed runtime partitions.

It cannot correct a wrong join key or make an irrelevant five-year scan logically unnecessary.

### Treating `spark.sql.shuffle.partitions` as the main performance knob

It is one input to shuffle planning, not a universal performance setting.

Partition size, data volume, skew, cluster parallelism, and AQE matter more than memorizing one number.

### Increasing executor memory before fixing giant partitions

If one hot-key partition is enormous, giving every executor more memory may only make the bad distribution more expensive to operate.

Fix the distribution if possible.

### Increasing driver memory for executor failures

Driver memory does not fix a reducer task that spills or runs out of executor memory.

Identify where the pressure occurs.

### Using `collect()` to inspect a large result

This can convert a distributed workload into a driver-memory problem.

Aggregate diagnostics and collect only genuinely small summaries.

### Changing several variables at once

If you simultaneously broadcast, repartition, cache, and increase memory, a faster result has weak explanatory value.

Change one thing.

### Comparing different business work

A `count()` benchmark and a full Parquet write may materialize different parts of the plan.

Use comparable actions when drawing performance conclusions.

### Accepting faster output without reconciliation

Performance engineering ends with:

```text
faster / cheaper physical behavior
+
same correct business result
```

not with runtime alone.

---

[Back to Table of Contents](#toc)

---

<a id="18-phase-7-mastery-reference"></a>
## 18. Phase 7 Mastery Reference

The Phase 7 mastery target is to take an inefficient PySpark pipeline and independently:

1. identify the likely bottleneck;
2. support the diagnosis with plans, partition counts, distribution, file evidence, or controlled runtime evidence;
3. distinguish I/O, join, shuffle, skew, partitioning, caching, AQE, and memory/resource issues;
4. choose the most relevant first optimization;
5. compare before/after behavior;
6. explain why performance changed without breaking correctness.

The formal mastery gate is intentionally deferred until explicitly requested.

Before beginning it, Phase 7 fluency should include the following.

### I/O

You should be able to diagnose:

```text
unnecessary columns
unnecessary historical/file reads
missing partition pruning
missing/limited predicate pushdown
small-file overhead
poor future scan parallelism from output layout
```

You should know what scan evidence supports each claim.

### Joins

You should be able to explain:

```text
why a BroadcastHashJoin can avoid a large-side join shuffle
why a SortMergeJoin can be the correct strategy for two large inputs
why join hints require size evidence
why join-key correctness comes before strategy
why a non-unique dimension can multiply fact rows
why projecting/filtering/pre-aggregating can reduce join cost
```

### Shuffle

Given every important `Exchange`, you should be able to answer:

```text
What distribution is Spark creating?
Why does the downstream operator need it?
Could the shuffle be removed?
If not, can the data moved through it be reduced?
Did an explicit repartition create redundant movement?
```

### Skew

You should be able to distinguish:

```text
partition-count problem
vs.
key-distribution problem
```

You should be able to identify hot keys from data evidence and explain why ordinary hash repartitioning does not split one hot key.

You should understand the salting idea well enough to explain:

```text
how salt spreads one hot logical key across physical buckets
why salt must vary within the hot key
why a second aggregation or compatible join reconstruction is required
how correctness is reconciled afterward
```

### Partitioning

You should be able to diagnose:

```text
too few partitions
too many partitions
poorly balanced partitions
```

and choose among:

```text
leave unchanged
repartition by count
repartition by key
coalesce
AQE coalescing
skew-specific treatment
```

with a reason for the choice.

### Caching and persistence

You should be able to answer:

```text
Is this lineage reused?
How expensive is recomputation?
What smaller reusable intermediate could be cached?
What memory/storage cost does caching introduce?
When should the cache be materialized?
When should it be unpersisted?
```

You should reject caching when reuse does not justify it.

### AQE

You should recognize AQE's performance roles in:

```text
coalescing small post-shuffle partitions
changing join strategy from runtime evidence
handling skewed shuffle partitions
```

and explain why AQE complements rather than replaces good query/data design.

### Resource reasoning

You should distinguish conceptually:

```text
executor memory
executor cores
driver memory
garbage collection
spill
memory pressure
```

and avoid treating more memory/cores as the first response to a query-design problem.

### Optimization explanation standard

A strong answer to a slow pipeline should look like:

```text
1. Correctness/grain
   The result must remain one row per store_id, and dim_store must be unique on store_id.

2. Bottleneck
   The large fact/dimension join is performing a two-sided shuffle.

3. Evidence
   The physical plan contains Exchanges on both sides followed by SortMergeJoin.
   The projected dimension is small and unique.

4. First change
   Broadcast only the projected dimension.

5. Before/after
   Before: two-sided shuffle + SortMergeJoin.
   After: BroadcastExchange + BroadcastHashJoin; the large fact side no longer
   requires ordinary join repartitioning.

6. Measurement
   Execute the same materializing workload under comparable cache conditions.

7. Correctness
   Reconcile row grain, key uniqueness, unmatched stores, and sales totals.

8. Explanation
   Performance improved because Spark replicated a small build side instead of
   redistributing the large fact side for the join. The business join keys and
   output grain did not change.
```

Another strong answer might conclude that **no optimization is warranted**:

```text
The SortMergeJoin is appropriate because both relations are large.
The Exchanges are required by the correct join.
The partition distribution is balanced.
The most relevant improvement is instead to prune unused fact columns before
that necessary shuffle, not to force a different join algorithm.
```

That is performance engineering too.

### Final reasoning standard

The final Phase 7 standard is not:

> I know that broadcast joins are fast, caching avoids recomputation, and more memory can help Spark.

It is:

> **I can diagnose where an inefficient PySpark pipeline is doing unnecessary or badly distributed work, support the diagnosis with physical evidence, change the smallest relevant design choice, measure the same workload before and after, and explain the improvement while proving that schema, grain, and business correctness were preserved.**

---

[Back to Table of Contents](#toc)
