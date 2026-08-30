# Phase 6 — Storage, Partitioning & Shuffle Engineering

<a id="toc"></a>
## Table of Contents

- [Objective](#objective)
- [Phase Mental Model](#phase-mental-model)
- [1. Parquet as Physical Analytical Storage](#1-parquet-as-physical-analytical-storage)
- [2. File Statistics, Column Pruning, and Predicate Pushdown](#2-file-statistics-column-pruning-and-predicate-pushdown)
- [3. Storage Partitioning and Partition Pruning](#3-storage-partitioning-and-partition-pruning)
- [4. File Sizing and the Small-File Problem](#4-file-sizing-and-the-small-file-problem)
- [5. The Partition Vocabulary](#5-the-partition-vocabulary)
- [6. Input and DataFrame Partitions](#6-input-and-dataframe-partitions)
- [7. Shuffle and Output Partitions](#7-shuffle-and-output-partitions)
- [8. `repartition()`](#8-repartition)
- [9. `coalesce()`](#9-coalesce)
- [10. Repartitioning by Key](#10-repartitioning-by-key)
- [11. Excessive vs. Insufficient Partitioning](#11-excessive-vs-insufficient-partitioning)
- [12. Storage Partitions vs. Spark Execution Partitions](#12-storage-partitions-vs-spark-execution-partitions)
- [13. Why Common Operations Shuffle](#13-why-common-operations-shuffle)
- [14. Storage Layout Through Physical Execution](#14-storage-layout-through-physical-execution)
- [15. Prediction-Before-Inspection Workflow](#15-prediction-before-inspection-workflow)
- [16. Engineering Decision Rules](#16-engineering-decision-rules)
- [17. Common Failure Modes](#17-common-failure-modes)
- [18. Phase 6 Mastery Reference](#18-phase-6-mastery-reference)

---

<a id="objective"></a>
## Objective

Understand the relationship between Spark execution, physical files, partitions, and data distribution.

Phase 6 deepens the storage concepts introduced earlier and connects them to the distributed execution model from Phase 4 and the physical-plan evidence from Phase 5.

The required concepts are:

- Parquet;
- columnar storage;
- compression;
- schema preservation;
- file statistics;
- column pruning;
- predicate pushdown;
- partition pruning;
- file sizing and the small-file problem;
- DataFrame partitions;
- input partitions;
- shuffle partitions;
- output partitions;
- `repartition()`;
- `coalesce()`;
- repartitioning by key;
- excessive partitioning;
- insufficient partitioning;
- storage partitioning vs. Spark execution partitions;
- shuffle behavior from `groupBy`, joins, `distinct`, `orderBy`, and `repartition`.

Examples target **PySpark 4.2.0** and use the retail/data-engineering domain.

The central Phase 6 questions are:

> **What kind of partition am I talking about, where will data move, and how does the physical storage layout affect the work Spark must perform?**

Phase 5 concepts such as `Scan`, `Exchange`, and physical plans remain useful evidence here, but Catalyst and AQE are not retaught.

Phase 6 intentionally does **not** expand deeply into:

- broad performance-tuning strategy;
- join-strategy optimization;
- skew remediation and salting;
- caching/persistence strategy;
- executor sizing, memory, spill, or garbage collection;
- Spark UI stage/task diagnosis.

Those topics belong primarily to Phases 7 and 8.

This document is lecture/reference material only. The Phase 6 mastery gate is intentionally not performed here.

---

[Back to Table of Contents](#toc)

---

<a id="phase-mental-model"></a>
## Phase Mental Model

The central model is:

```text
physical files in storage
        ↓
file / directory metadata
        ↓
Spark plans which files and columns are relevant
        ↓
input partitions
        ↓
tasks read those partitions
        ↓
DataFrame transformations
        ↓
wide operation requires new data distribution
        ↓
shuffle / Exchange
        ↓
shuffle partitions
        ↓
downstream tasks
        ↓
output partitions / writer tasks
        ↓
physical output files and directories
```

Three words that must never be treated as interchangeable are:

```text
file
partition
shuffle
```

A **file** is a physical storage object.

A **Spark execution partition** is a unit of distributed data that one task processes at a time.

A **storage partition** is a directory/layout convention such as:

```text
fact_sales/
├── year=2025/
└── year=2026/
```

A **shuffle** is redistribution of records between Spark partitions so a downstream operator receives the distribution it requires.

The Phase 6 habit is:

> **Predict partition/storage behavior → inspect → execute → compare before/after → explain why.**

---

[Back to Table of Contents](#toc)

---

<a id="1-parquet-as-physical-analytical-storage"></a>
## 1. Parquet as Physical Analytical Storage

Parquet is a **columnar binary file format** designed for analytical workloads.

Instead of storing every row's fields together as a row-oriented text format would, Parquet organizes values by column within row groups.

Conceptually:

```text
row-oriented
-----------
row 1: order_id, store_id, product_id, quantity, price
row 2: order_id, store_id, product_id, quantity, price
row 3: order_id, store_id, product_id, quantity, price

columnar
--------
order_id:   ...
store_id:   ...
product_id: ...
quantity:   ...
price:      ...
```

### Why columnar storage helps analytics

Suppose a pipeline needs only:

```python
sales_df.select(
    'store_id',
    'net_sales',
)
```

A columnar reader can avoid reading unrelated column payloads such as:

```text
customer_id
product_name
promotion_description
shipping_address
```

That is the physical foundation of **column pruning**.

### Compression

Columnar organization also improves compression because adjacent values in one column often have similar representations.

Parquet applies encoding and compression to column data rather than storing CSV-like text.

The engineering consequence is:

```text
fewer bytes stored
+
fewer bytes read from storage
+
less parsing work than text formats
```

The best compression codec is workload-dependent. Phase 6 needs the principle, not codec micro-tuning.

### Schema preservation

Parquet stores schema/type metadata with the dataset.

That means Spark can read typed values such as:

```text
integer
decimal
date
timestamp
string
```

without re-inferring every field from raw text.

Example:

```python
sales_df.write.mode('overwrite').parquet(output_path)

reloaded_df = spark.read.parquet(output_path)
reloaded_df.printSchema()
```

Contrast this with CSV, where types are not inherently preserved by the file format and production pipelines often need an explicit read schema.

### Parquet is still made of physical files

A Spark Parquet dataset is usually a **directory containing multiple data files**, not one monolithic file:

```text
fact_sales/
├── part-00000-....snappy.parquet
├── part-00001-....snappy.parquet
├── part-00002-....snappy.parquet
└── _SUCCESS
```

The number and size of those files affect future Spark reads.

That is why storage format and partition engineering are connected.

---

[Back to Table of Contents](#toc)

---

<a id="2-file-statistics-column-pruning-and-predicate-pushdown"></a>
## 2. File Statistics, Column Pruning, and Predicate Pushdown

These mechanisms reduce I/O in different ways.

Keep them separate.

### File / Parquet statistics

Parquet metadata can contain statistics for column chunks and row groups, commonly including information such as:

```text
minimum value
maximum value
null count
value / row counts depending on metadata level
```

Example idea:

```text
Row group 0
order_date min = 2026-01-01
order_date max = 2026-01-31

Row group 1
order_date min = 2026-02-01
order_date max = 2026-02-28
```

A filter such as:

```python
sales_df.filter(
    F.col('order_date') >= F.lit('2026-02-01')
)
```

can give the Parquet reader enough information to prove that some row groups cannot contain matching values.

Those row groups can be skipped without decoding every row.

Do not confuse Parquet file/row-group statistics with Catalyst's separate logical-plan statistics used for decisions such as join planning.

### Column pruning

**Column pruning** means reading only columns required by the query.

Example:

```python
result_df = (
    spark.read.parquet(sales_path)
    .select(
        'store_id',
        'order_date',
        'net_sales',
    )
)
```

If the source contains 40 columns but the query requires 3, Spark can ask the Parquet reader for those required columns rather than reading all 40 column payloads.

Physical-plan evidence often includes a reduced `ReadSchema` in the scan.

The key question is:

> **Which columns does the scan actually need to read?**

### Predicate pushdown

**Predicate pushdown** means Spark passes eligible filter predicates into the data source reader so the source can use its own metadata/reader capabilities to avoid unnecessary data work.

Example:

```python
filtered_df = (
    spark.read.parquet(sales_path)
    .filter(F.col('quantity') >= 5)
)
```

A formatted/file scan can expose details such as:

```text
PushedFilters: ...
```

The pushed predicate can allow the Parquet reader to use metadata such as min/max statistics to skip row groups that cannot match.

A Spark `Filter` may still appear in the physical plan. Pushdown does **not** imply that Spark always removes every higher-level filter operator.

### Three distinct questions

```text
Column pruning
→ Which COLUMNS must be read?

Predicate pushdown
→ Which ROW predicates can be delegated to the source reader?

Statistics-based skipping
→ Which physical row groups/pages can the reader prove are irrelevant?
```

They cooperate, but they are not synonyms.

### Inspect rather than assume

```python
filtered_df.explain('formatted')
```

For a Parquet scan, inspect fields such as:

```text
ReadSchema
PushedFilters
DataFilters
```

Exact labels can vary by Spark/source implementation.

The engineering standard is to explain what Spark's scan evidence actually shows rather than assuming every syntactic filter was fully pushed into storage.

---

[Back to Table of Contents](#toc)

---

<a id="3-storage-partitioning-and-partition-pruning"></a>
## 3. Storage Partitioning and Partition Pruning

Spark can write a dataset into directory-based storage partitions.

Example:

```python
sales_df.write.partitionBy(
    'year',
    'month',
).mode('overwrite').parquet(output_path)
```

Physical layout:

```text
fact_sales/
├── year=2025/
│   ├── month=11/
│   └── month=12/
└── year=2026/
    ├── month=01/
    └── month=02/
```

The values encoded in these directory names become **partition columns** when Spark reads the dataset.

### Partition pruning

Suppose the query is:

```python
january_2026_df = (
    spark.read.parquet(output_path)
    .filter(
        (F.col('year') == 2026)
        & (F.col('month') == 1)
    )
)
```

Because the filter targets storage partition columns, Spark may avoid scanning directories such as:

```text
year=2025/month=11
year=2025/month=12
year=2026/month=02
```

That is **partition pruning**.

Physical-plan evidence commonly appears as:

```text
PartitionFilters: ...
```

### Partition pruning is not predicate pushdown

Both can reduce I/O, but they operate at different physical levels.

```text
Partition pruning
→ skip entire directory/file groups based on partition values

Predicate pushdown
→ pass eligible predicates into the file reader

Parquet statistics
→ skip row groups/pages within files when metadata proves no match
```

A single query can benefit from all three.

### Choosing storage partition columns conceptually

A useful storage partition column usually has:

```text
frequent filtering value
+
manageable cardinality
+
meaningful data volume per partition value
```

Good conceptual candidates often include:

```text
order_date / event_date at an appropriate granularity
year
month
region when cardinality is controlled
```

Dangerous candidates can include very high-cardinality values such as:

```text
order_id
customer_id
transaction_id
```

because they can create enormous numbers of tiny directories/files.

Detailed production sizing strategy belongs to Phase 7. Phase 6 must understand the physical consequences.

### Storage partitioning does not automatically colocate Spark execution

Reading:

```text
year=2026/month=01/
```

does not mean Spark now has exactly one execution partition called `month=01`.

The selected files still become **input splits / input partitions** according to Spark's scan planning.

This distinction is central to the entire phase.

---

[Back to Table of Contents](#toc)

---

<a id="4-file-sizing-and-the-small-file-problem"></a>
## 4. File Sizing and the Small-File Problem

Distributed writes naturally produce multiple output files because multiple tasks write in parallel.

That is healthy until the files become excessively numerous and tiny.

### Why many tiny files are harmful

A dataset such as:

```text
100 GB total
100 files × 1 GB
```

and:

```text
100 GB total
1,000,000 files × ~100 KB
```

contain similar logical data volume but are not operationally equivalent.

The tiny-file layout creates overhead from:

- directory/object listing;
- metadata operations;
- opening/closing many files;
- task/input-split planning;
- repeated file footer reads;
- scheduler overhead from fragmented work;
- inefficient storage requests.

The problem is often **metadata and orchestration overhead**, not raw byte volume.

### Files that are too large also have tradeoffs

Very large files can reduce available read/write parallelism and make individual tasks heavier.

The goal is therefore not:

```text
fewest files possible
```

It is:

```text
reasonably sized files
+
enough parallelism
+
not excessive metadata/task overhead
```

There is no universal correct file size for every workload.

A common engineering order of magnitude is hundreds of megabytes per analytical file, but Phase 6 should not turn that heuristic into a hard rule.

### How Spark partitioning creates file counts

For a simple unpartitioned write, writer task count is closely related to the number of data files produced.

Example:

```python
print(result_df.rdd.getNumPartitions())

result_df.write.mode('overwrite').parquet(output_path)
```

If the DataFrame has many tiny execution partitions, the write can produce many small files.

With storage partitioning:

```python
result_df.write.partitionBy('year', 'month').parquet(output_path)
```

file creation becomes more complex because one writer task may encounter multiple storage-partition values.

Therefore do not rely on the simplistic rule:

```text
one Spark partition = exactly one physical file forever
```

Use it only as a rough mental model for straightforward unpartitioned writes.

### `maxRecordsPerFile`

Spark also supports writer controls such as:

```python
(
    result_df.write
    .option('maxRecordsPerFile', 500000)
    .parquet(output_path)
)
```

This can split task output into additional files.

Therefore physical file count is influenced by both execution partitioning and write configuration/layout.

---

[Back to Table of Contents](#toc)

---

<a id="5-the-partition-vocabulary"></a>
## 5. The Partition Vocabulary

The word **partition** is overloaded in Spark work.

Always qualify which partition you mean.

| Term | What it means | Main engineering question |
|---|---|---|
| Storage partition | Directory/layout grouping such as `year=2026/` | Which files can be pruned? |
| Input partition | Unit of source data assigned to one scan task | How is the physical input divided into tasks? |
| DataFrame partition | Current distributed slice of the DataFrame/RDD lineage | How many parallel task-sized slices exist now? |
| Shuffle partition | Downstream bucket produced by redistribution | How many partitions receive shuffled records? |
| Output partition | DataFrame partition presented to the write stage | How many writer tasks / file-producing units exist? |
| Parquet row group | Internal columnar block inside a Parquet file | Can statistics skip this block? |

A Parquet **row group** is intentionally included because learners often casually call every physical grouping a partition.

Do not do that.

### One row can move through several partition concepts

Conceptually:

```text
storage partition
    year=2026/month=01
        ↓
selected Parquet file
        ↓
input partition
        ↓
DataFrame partition
        ↓
repartition / groupBy shuffle
        ↓
shuffle partition
        ↓
output DataFrame partition
        ↓
new physical output file(s)
```

The word is the same; the layer is not.

---

[Back to Table of Contents](#toc)

---

<a id="6-input-and-dataframe-partitions"></a>
## 6. Input and DataFrame Partitions

### DataFrame partitions

A DataFrame's rows are distributed across execution partitions.

Each task processes one partition at a time for a stage.

Inspect the current partition count with:

```python
sales_df.rdd.getNumPartitions()
```

For small teaching data, you can inspect row-to-partition assignment with:

```python
partitioned_rows_df = sales_df.select(
    '*',
    F.spark_partition_id().alias('partition_id'),
)
```

Do not use `collect()` on large production datasets merely to inspect partition IDs.

### Input partitions

When reading files, Spark plans **input partitions** from the selected physical files/splits.

The relationship is not necessarily:

```text
1 file = 1 input partition
```

A large splittable file can contribute to multiple input partitions.

Multiple small files can also be grouped into one input partition depending on scan planning.

The better model is:

```text
selected files
    ↓
Spark file-scan planning
    ↓
input partitions
    ↓
one task per input partition for the scan stage
```

### Input partition count is a performance boundary

Too few input partitions can leave available executor cores idle.

Too many tiny input partitions can create excessive task/scheduler overhead.

Phase 6 learns to recognize this tradeoff.

Phase 7 handles broader tuning decisions.

### Filters usually do not automatically reduce partition count

A filter such as:

```python
filtered_df = sales_df.filter(
    F.col('quantity') >= 5
)
```

usually preserves the existing partition structure even if many rows disappear.

Some resulting partitions may simply contain fewer rows or become empty.

This is why:

```text
row count
!= partition count
```

A narrow transformation can greatly reduce rows without redistributing them.

### Column pruning also does not mean fewer Spark partitions

Selecting fewer columns reduces bytes read/processed but does not inherently change the number of execution partitions.

Keep these separate:

```text
less data per partition
vs.
fewer partitions
```

---

[Back to Table of Contents](#toc)

---

<a id="7-shuffle-and-output-partitions"></a>
## 7. Shuffle and Output Partitions

### Shuffle partitions

A wide operation can require rows to be redistributed into new downstream buckets.

Example:

```python
store_sales_df = (
    sales_df
    .groupBy('store_id')
    .agg(
        F.sum('net_sales').alias('net_sales')
    )
)
```

All rows/partial states for the same grouping key must reach a compatible downstream partition.

Spark therefore commonly creates a shuffle exchange such as:

```text
Exchange hashpartitioning(store_id, N)
```

`N` is the planned number of shuffle partitions.

The common SQL shuffle-partition setting is:

```text
spark.sql.shuffle.partitions
```

Do not memorize its configured value as the guaranteed final runtime partition count when inherited AQE behavior is enabled. Inspect the actual plan/runtime result.

### Shuffle partition count is downstream parallelism

After the shuffle, downstream tasks generally process the resulting shuffle partitions.

Conceptually:

```text
upstream partitions
P0 P1 P2 P3
      ↓
shuffle by store_id
      ↓
Q0 Q1 Q2 Q3 Q4 Q5 Q6 Q7
      ↓
8 downstream task-sized partitions
```

### Output partitions

Before a write, the DataFrame still has an execution partition structure.

Those partitions drive writer tasks.

Example:

```python
print(final_df.rdd.getNumPartitions())

final_df.write.mode('overwrite').parquet(output_path)
```

For a straightforward unpartitioned write, each non-empty writer task commonly produces a data file.

But output file count can diverge from partition count because of:

- storage `partitionBy()` values;
- empty partitions;
- writer/file-splitting options;
- source/write implementation details.

Use:

```text
execution partition count
→ strong clue about writer parallelism
```

not:

```text
execution partition count
= guaranteed exact file count
```

### Partition counts can change at wide boundaries

A useful baseline prediction is:

```text
select/filter/withColumn
→ usually preserve partition count

repartition(n)
→ shuffle to n partitions

coalesce(n)
→ reduce partitions without a full shuffle

groupBy/join/distinct/orderBy
→ may create a shuffle partition count determined by the physical strategy
```

The physical plan is the final authority on whether an actual `Exchange` was required.

---

[Back to Table of Contents](#toc)

---

<a id="8-repartition"></a>
## 8. `repartition()`

`repartition()` deliberately redistributes rows across a new partitioning scheme.

### Repartition to an explicit count

```python
repartitioned_df = sales_df.repartition(8)
```

Conceptually:

```text
current partitions
P0 P1 P2 P3
      ↓
full redistribution
      ↓
new partitions
Q0 Q1 Q2 Q3 Q4 Q5 Q6 Q7
```

Spark commonly implements this with an `Exchange` using a round-robin-style distribution when no key is supplied.

Inspect:

```python
print(sales_df.rdd.getNumPartitions())
print(repartitioned_df.rdd.getNumPartitions())

repartitioned_df.explain('formatted')
```

### `repartition()` normally means a shuffle

Rows must move because Spark is constructing a new distribution rather than merely collapsing existing partitions.

That gives `repartition()` two major uses:

```text
change partition count
+
change data distribution
```

### Increasing partitions

If a DataFrame has too little downstream parallelism:

```python
more_parallel_df = df.repartition(16)
```

can create more downstream execution partitions, but it pays for redistribution.

Do not increase partitions merely because a larger number looks more distributed.

### Reducing partitions

You can also do:

```python
fewer_df = df.repartition(4)
```

but this still redistributes the dataset.

If the sole requirement is to reduce partition count and no rebalance is needed, `coalesce()` may be more appropriate.

### Repartitioning changes distribution, not storage partitioning by itself

This:

```python
sales_df.repartition(8, 'store_id')
```

changes Spark execution distribution.

This:

```python
sales_df.write.partitionBy('year').parquet(output_path)
```

changes physical directory layout.

They solve different problems even though both use the word partition.

---

[Back to Table of Contents](#toc)

---

<a id="9-coalesce"></a>
## 9. `coalesce()`

`coalesce()` is primarily used to **reduce** the number of Spark execution partitions without a full shuffle.

Example:

```python
coalesced_df = sales_df.coalesce(2)
```

Conceptually:

```text
P0  P1  P2  P3
 \  /    \  /
  Q0      Q1
```

Existing upstream partitions are combined into fewer downstream partitions rather than every row being redistributed globally.

### Why `coalesce()` is usually cheaper than `repartition()` when shrinking

```text
repartition(2)
→ redistribute rows to rebalance 2 new partitions

coalesce(2)
→ collapse existing partitions into 2 groups
```

The tradeoff is that `coalesce()` does not guarantee evenly balanced resulting partitions.

If upstream partitions are already skewed, coalescing them can preserve or worsen that imbalance.

### `coalesce()` is not for meaningfully increasing partition count

If a DataFrame currently has 4 partitions:

```python
sales_df.coalesce(8)
```

does not perform the full redistribution needed to create useful new parallel partitions.

Use `repartition()` when you need to increase partition count.

### The `coalesce(1)` trap

```python
single_partition_df = df.coalesce(1)
```

is attractive when someone wants one output file.

For large data, it can create one heavily loaded downstream partition and one writer task.

That can destroy parallelism.

Use it only when the dataset is deliberately small enough that a single partition is operationally reasonable.

### Decision rule

```text
Need more partitions?
→ repartition()

Need balanced redistribution?
→ repartition()

Need fewer partitions and current distribution is acceptable?
→ coalesce()
```

---

[Back to Table of Contents](#toc)

---

<a id="10-repartitioning-by-key"></a>
## 10. Repartitioning by Key

You can repartition around a business/execution key:

```python
sales_by_store_df = sales_df.repartition(
    8,
    'store_id',
)
```

This asks Spark to hash-partition rows by `store_id` into 8 execution partitions.

Conceptually:

```text
hash(store_id) % 8
        ↓
partition assignment
```

Rows with the same key value are routed to the same hash bucket for that repartitioning expression.

### Why repartition by key?

It can be useful when downstream work also requires that key distribution.

Examples can include:

```text
groupBy('store_id')
join(..., on='store_id')
key-oriented output preparation
```

However, do not assume an earlier repartition permanently eliminates every later shuffle.

Spark still evaluates whether the downstream physical operator's required distribution is satisfied through the actual plan.

Operations, projections, joins, partition counts, and query boundaries can change what distribution information remains useful.

### Repartitioning by key can expose skew

Suppose 70% of rows have:

```text
store_id = S01
```

Hash repartitioning by `store_id` does not magically split the single key across many buckets.

One downstream partition can become much larger than the others.

That is **data skew**.

Phase 6 needs to recognize the relationship:

```text
key distribution
→ partition-size distribution
```

Phase 7 studies deliberate skew remediation.

### Key choice is an execution decision, not merely syntax

Before:

```python
df.repartition(16, 'store_id')
```

ask:

```text
What downstream operation benefits from store_id distribution?
How many distinct keys exist?
Are key frequencies reasonably balanced?
Am I introducing a shuffle that the downstream plan would not otherwise need?
```

---

[Back to Table of Contents](#toc)

---

<a id="11-excessive-vs-insufficient-partitioning"></a>
## 11. Excessive vs. Insufficient Partitioning

Partition count controls the granularity of parallel work.

Neither extreme is universally good.

### Insufficient partitioning

Too few partitions can create:

```text
large tasks
underused executor cores
long straggling tasks
large in-memory working sets
large writer output per task
```

Conceptually:

```text
40 available task slots
but only 4 partitions
        ↓
at most ~4 tasks can make progress in that stage at once
```

### Excessive partitioning

Too many tiny partitions can create:

```text
many tiny tasks
scheduler overhead
shuffle metadata overhead
many small shuffle blocks
many small output files
high file-open/listing overhead later
```

Conceptually:

```text
1 GB dataset
10,000 partitions
≈ 100 KB average per partition
```

The overhead can dominate the actual computation.

### Partition count alone is incomplete

Always consider:

```text
number of partitions
+
bytes per partition
+
rows per partition
+
key skew
+
cluster parallelism
+
downstream operation
```

A count of 200 partitions is neither inherently good nor bad.

### Average size can hide skew

Suppose:

```text
100 partitions
100 GB total
average = 1 GB
```

but one partition contains 50 GB.

The average does not describe the real execution bottleneck.

Phase 6 should therefore distinguish:

```text
partition count problem
vs.
partition distribution problem
```

Deep skew diagnosis belongs to Phase 7/8.

---

[Back to Table of Contents](#toc)

---

<a id="12-storage-partitions-vs-spark-execution-partitions"></a>
## 12. Storage Partitions vs. Spark Execution Partitions

This is the most important distinction in Phase 6.

### Storage partitioning

Example layout:

```text
fact_sales/
├── order_date=2026-08-28/
├── order_date=2026-08-29/
└── order_date=2026-08-30/
```

This is about **where files live**.

Its main benefit is that filters on `order_date` can allow Spark to skip irrelevant directory partitions.

### Spark execution partitions

Example:

```text
DataFrame
├── Partition 0
├── Partition 1
├── Partition 2
└── Partition 3
```

This is about **how distributed rows are divided into task-sized units during execution**.

Its main effects include:

```text
parallelism
shuffle distribution
task size
writer parallelism
```

### The two concepts interact

Suppose a query filters:

```python
sales_2026_08_30_df = (
    spark.read.parquet(sales_path)
    .filter(F.col('order_date') == F.lit('2026-08-30'))
)
```

If storage is partitioned by `order_date`:

```text
storage partition pruning
        ↓
Spark selects only files for 2026-08-30
        ↓
those selected files are divided into input partitions
        ↓
scan tasks process those execution partitions
```

Pruning changes **which physical files are considered**.

Input planning determines **how the remaining physical data becomes Spark task partitions**.

### Same word, different count

A dataset might have:

```text
365 storage partitions
8,000 Parquet files
240 input partitions for today's query
240 scan-stage DataFrame partitions
64 shuffle partitions after aggregation
8 output partitions before write
```

None of those numbers contradict each other.

They describe different layers.

### One sentence test

Whenever you say:

> There are 12 partitions.

force yourself to complete the sentence:

> There are 12 **what kind of partitions, at what point in the pipeline?**

If you cannot answer that, the statement is incomplete.

---

[Back to Table of Contents](#toc)

---

<a id="13-why-common-operations-shuffle"></a>
## 13. Why Common Operations Shuffle

A shuffle occurs when downstream work requires a data distribution that the current partitions do not already satisfy.

Phase 5 showed `Exchange` as physical evidence.

Phase 6 focuses on **what distribution is being created and how partition counts change**.

### `groupBy()`

```python
store_sales_df = (
    sales_df
    .groupBy('store_id')
    .agg(F.sum('net_sales').alias('net_sales'))
)
```

Why shuffle?

```text
S01 rows can begin in P0, P1, P2, P3
        ↓
final S01 aggregate needs compatible colocated state
        ↓
hash redistribution by store_id
```

Common plan idea:

```text
partial HashAggregate
        ↓
Exchange hashpartitioning(store_id, N)
        ↓
final HashAggregate
```

### Join

For a non-broadcast equi-join:

```python
sales_df.join(
    stores_df,
    on='store_id',
    how='inner',
)
```

matching keys from both sides must reach compatible partitions.

A sort-merge strategy commonly requires:

```text
left  → Exchange by key → Sort
right → Exchange by key → Sort
                  ↓
             SortMergeJoin
```

But do not say:

```text
every join shuffles both sides
```

A broadcast join can replicate the small build side and avoid ordinary two-sided repartitioning for the join.

### `distinct()`

```python
distinct_products_df = sales_df.select(
    'product_id'
).distinct()
```

To prove global uniqueness, duplicate values that began in different partitions must meet.

That generally requires key-based redistribution.

The same reasoning applies to global `dropDuplicates()` over the selected deduplication keys.

### `orderBy()`

```python
ordered_df = sales_df.orderBy(
    F.col('net_sales').desc(),
    F.col('sale_id').asc(),
)
```

A **global** order cannot be guaranteed merely by sorting each existing partition independently.

Spark typically needs a global distribution strategy such as range partitioning plus local sorts.

Conceptually:

```text
rows redistributed into ordered key ranges
        ↓
each partition sorts its assigned range
        ↓
partition ranges concatenate into global order
```

Contrast:

```python
df.sortWithinPartitions('store_id')
```

which orders rows **within each existing partition** and does not by itself create a globally ordered DataFrame.

### `repartition()`

```python
df.repartition(8)
```

is explicitly requesting redistribution.

The shuffle is not an accidental side effect; it is the purpose of the operation.

### `coalesce()`

```python
df.coalesce(4)
```

normally reduces partitions without a full redistribution.

That is why it is the important contrast with `repartition()`.

### Summary

| Operation | Typical redistribution reason |
|---|---|
| `groupBy()` | Same grouping keys must meet |
| Non-broadcast equi-join | Same join keys from both sides must meet compatibly |
| `distinct()` | Equal values must meet so duplicates can be removed globally |
| `orderBy()` | Global key ranges/order must be established |
| `repartition()` | User explicitly requests a new distribution |
| `coalesce()` | Usually no full shuffle; existing partitions are collapsed |

Always qualify these as **typical physical requirements**, not immutable text patterns. Existing distribution, broadcast, AQE, and physical planning can alter exact plans.

---

[Back to Table of Contents](#toc)

---

<a id="14-storage-layout-through-physical-execution"></a>
## 14. Storage Layout Through Physical Execution

A professional Spark engineer should be able to trace one query across both storage and execution layers.

Consider a partitioned Parquet dataset:

```text
fact_sales/
├── year=2025/
│   └── month=12/
└── year=2026/
    ├── month=01/
    └── month=02/
```

Query:

```python
monthly_store_sales_df = (
    spark.read.parquet(sales_path)
    .filter(
        (F.col('year') == 2026)
        & (F.col('month') == 2)
        & (F.col('order_status') == 'COMPLETED')
    )
    .select(
        'store_id',
        'net_sales',
    )
    .groupBy('store_id')
    .agg(
        F.sum('net_sales').alias('net_sales')
    )
)
```

Trace it in order.

### Step 1 — Storage partition pruning

The `year` and `month` predicates may allow Spark to select only:

```text
year=2026/month=02/
```

Irrelevant storage directories can be excluded before their data files are scanned.

### Step 2 — File scan planning

The remaining Parquet files are converted into Spark input partitions.

File count and input partition count are related but not identical.

### Step 3 — Column pruning

Only the columns required for filtering and downstream output need to be read from Parquet.

Conceptually:

```text
year
month
order_status
store_id
net_sales
```

Partition columns may be obtained from directory metadata rather than stored as ordinary payload columns in each Parquet file.

### Step 4 — Predicate pushdown/statistics

The non-partition predicate:

```text
order_status = COMPLETED
```

may be pushed to the Parquet reader where eligible.

Parquet statistics can then help skip internal row groups that cannot match.

### Step 5 — Narrow execution

The scan, filter, and projection can process current input partitions without requiring rows to cross partition boundaries.

### Step 6 — Shuffle

`groupBy('store_id')` requires all partial store states to meet by key.

Physical evidence commonly includes:

```text
Exchange hashpartitioning(store_id, N)
```

### Step 7 — Downstream partition structure

The final aggregation executes over shuffle partitions, not over the original storage directories.

This is the key connection:

```text
storage layout influenced what Spark had to read
but grouped execution created a new runtime distribution
```

### Step 8 — Write behavior

If the result is written:

```python
monthly_store_sales_df.write.parquet(output_path)
```

its current execution partition structure influences writer parallelism and resulting file layout.

Therefore storage layout affects execution, and execution creates the next storage layout.

The system is cyclical:

```text
old storage layout
    ↓
read planning
    ↓
execution partitions
    ↓
shuffle / repartition choices
    ↓
writer partitions
    ↓
new storage layout
    ↓
future read planning
```

---

[Back to Table of Contents](#toc)

---

<a id="15-prediction-before-inspection-workflow"></a>
## 15. Prediction-Before-Inspection Workflow

Use this workflow for every important Phase 6 experiment.

```text
pipeline + physical storage layout
        ↓
identify every meaning of 'partition'
        ↓
predict files/directories that should be read
        ↓
predict input partition behavior
        ↓
predict narrow vs. redistribution boundaries
        ↓
predict partition-count changes
        ↓
inspect
        ↓
execute
        ↓
compare before / after
        ↓
explain why
```

### Step 1 — State the storage layout

Example:

```text
Parquet
partitioned by year/month
multiple files per storage partition
```

### Step 2 — State the logical pipeline and grain

Example:

```text
filter February 2026 completed sales
→ aggregate to one row per store_id
```

Correctness/grain reasoning remains mandatory.

### Step 3 — Predict pruning and read behavior

Ask:

```text
Which storage directories should be excluded?
Which columns are required?
Which predicates could be pushed to Parquet?
Could Parquet statistics skip internal blocks?
```

### Step 4 — Record partition counts before important boundaries

```python
print('source:', source_df.rdd.getNumPartitions())
print('filtered:', filtered_df.rdd.getNumPartitions())
print('grouped:', grouped_df.rdd.getNumPartitions())
```

Treat these as local teaching observations. In large production systems, do not trigger expensive actions merely for curiosity.

### Step 5 — Predict shuffles

For every wide-looking operation, say **why data must move**.

Bad:

> `groupBy` shuffles because `groupBy` is wide.

Better:

> Partial states for the same `store_id` can originate in several upstream partitions, so Spark must redistribute them into compatible downstream partitions before final aggregation.

### Step 6 — Inspect physical evidence

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
```

Do not reteach Catalyst; use the physical plan as evidence for storage/distribution behavior.

### Step 7 — Inspect physical files when the experiment writes data

For controlled local experiments, inspect:

```text
number of output files
file sizes
storage partition directories
```

Then compare those observations with the DataFrame's output partition count before the write.

### Step 8 — Explain every mismatch

If you predicted:

```text
8 output files
```

but observed:

```text
24 files
```

investigate:

```text
Was the write storage-partitioned?
Did each task touch multiple partition values?
Was maxRecordsPerFile configured?
Were there hidden/metadata files in the count?
Were some partitions empty?
```

A mismatch is useful evidence when you explain its cause.

---

[Back to Table of Contents](#toc)

---

<a id="16-engineering-decision-rules"></a>
## 16. Engineering Decision Rules

1. **Always qualify the word partition.** Say storage, input, DataFrame, shuffle, or output partition.
2. **Treat Parquet as physical storage, not magic optimization.** Its columnar layout, metadata, and statistics create opportunities that Spark still has to exploit through the scan.
3. **Separate column pruning, predicate pushdown, and partition pruning.** They remove different kinds of unnecessary I/O.
4. **Use the physical plan as evidence.** Inspect `ReadSchema`, pushed/data filters, partition filters, and `Exchange` nodes rather than assuming an optimization happened.
5. **Do not equate file count with input partition count.** Large files can be split and small files can be grouped for scan work.
6. **Do not equate storage partitions with execution partitions.** Directory values select files; Spark partitions determine task-sized distributed work.
7. **Do not assume filters reduce partition count.** Narrow filters often leave the partition structure unchanged while reducing rows inside each partition.
8. **Explain every shuffle causally.** State which downstream distribution requirement forces records to move.
9. **Use `repartition()` when you need a new balanced distribution or more partitions.** Accept that it normally introduces a shuffle.
10. **Use `coalesce()` primarily to reduce partition count when rebalancing is unnecessary.** Understand that it can leave uneven partitions.
11. **Do not use `coalesce(1)` as a generic file-management strategy.** One partition means one lane of downstream parallelism.
12. **Repartition by a key only when downstream work benefits from that key distribution.** Do not pay for a shuffle without a reason.
13. **Think about key frequency before key repartitioning.** Hash partitioning does not solve a hot-key problem by itself.
14. **Judge partition counts relative to workload and resources.** A number has no meaning without bytes, rows, skew, and available parallelism.
15. **Connect output partitioning to future reads.** Today's writer task/file choices become tomorrow's scan-planning problem.
16. **Avoid high-cardinality storage partition columns.** They can create huge directory and small-file counts.
17. **Use storage partitioning for selective access patterns, not as an attempt to pre-shuffle every future computation.** Storage layout and execution distribution are different layers.
18. **Do not say every join shuffles.** Broadcast joins and already-satisfied distributions are counterexamples.
19. **Do not say every sort is a shuffle.** `Sort` satisfies ordering; `Exchange` satisfies distribution. Global `orderBy()` commonly needs both kinds of work.
20. **When AQE is inherited from Phase 5, inspect final runtime partition behavior instead of treating the configured shuffle target as guaranteed.** Deeper AQE tuning belongs to Phase 7.
21. **Preserve correctness and grain while engineering partitions.** A perfectly distributed wrong result is still wrong.
22. **Predict before inspection.** If you only describe behavior after Spark prints it, you are not testing your storage/execution model.

A repeatable Phase 6 checklist is:

```text
1. What is the logical grain of each DataFrame?
2. What physical file format is being read?
3. What storage partition columns exist?
4. Which storage partitions should be pruned?
5. Which columns should the scan read?
6. Which predicates can be pushed to the source?
7. Which Parquet statistics may help skip internal blocks?
8. How many input/DataFrame partitions exist before the transformation?
9. Which operations are narrow?
10. Which operation requires redistribution, and why?
11. What shuffle partitioning strategy/count is planned?
12. How does repartition/coalesce change that count?
13. Is repartitioning by key creating a useful distribution?
14. Could the key distribution create skew?
15. How many output execution partitions reach the writer?
16. What physical files/directories are actually produced?
17. Are those files excessively small or excessively large?
18. How will this storage layout affect the next read?
```

---

[Back to Table of Contents](#toc)

---

<a id="17-common-failure-modes"></a>
## 17. Common Failure Modes

### Saying 'partition' without naming the layer

Weak:

> The dataset has 12 partitions.

Better:

> The read produced 12 Spark input partitions after storage partition pruning.

or:

> The write created 12 `order_date=...` storage partition directories.

Those are different claims.

### Assuming one Parquet file equals one Spark partition

Spark plans input partitions from files/splits.

The relationship can be one-to-many or many-to-one depending on file sizes and scan planning.

### Assuming one Spark partition always equals one output file

That is only a rough model for simple unpartitioned writes.

Storage `partitionBy()`, empty tasks, and writer options can change the physical file count.

### Confusing partition pruning with predicate pushdown

```text
Partition pruning
→ skip storage directory partitions/files

Predicate pushdown
→ delegate eligible row predicates to the file reader
```

They are not two names for the same optimization.

### Confusing predicate pushdown with guaranteed row elimination before Spark

A predicate can appear in pushed-filter metadata and still have a Spark-level filter for correctness or residual evaluation.

Read the actual plan.

### Assuming column pruning means fewer rows or partitions

Column pruning reduces columns/bytes.

It does not inherently reduce row count or Spark partition count.

### Assuming a filter automatically creates fewer partitions

Narrow filtering usually keeps the same partition topology, even if some partitions become empty.

### Assuming `repartition()` is free because it only changes metadata

`repartition()` creates a new physical distribution and normally requires a shuffle.

### Assuming `coalesce()` balances data

It reduces partition count cheaply by collapsing existing partitions.

It does not globally rebalance rows.

### Using `coalesce(1)` to get one pretty file

On large data, this forces the downstream write through one partition/task and sacrifices distributed parallelism.

### Repartitioning before every join or aggregation

An explicit repartition is itself a shuffle.

If the downstream physical operator would already create the necessary exchange, the extra repartition may be redundant.

Inspect the final plan.

### Repartitioning by a skewed key and expecting balance

Equal keys hash to the same bucket.

A hot key can still create one hot partition.

### Treating `spark.sql.shuffle.partitions` as a universal ideal

It is a configured default/target for many SQL shuffles, not a workload-independent truth.

The appropriate structure depends on actual data and execution context.

### Assuming storage partitioning eliminates `groupBy` or join shuffles

Directory partitioning by `year` helps Spark find relevant files.

It does not automatically satisfy a later runtime requirement such as:

```text
group all rows by store_id
```

A new shuffle can still be required.

### Partitioning storage by a high-cardinality business key

Layouts such as:

```text
order_id=000001/
order_id=000002/
order_id=000003/
...
```

can create tiny directories/files and expensive metadata operations.

### Believing fewer files is always better

One enormous file can reduce available parallelism.

The goal is appropriately sized files, not the minimum possible file count.

### Believing more Spark partitions is always faster

More partitions create more scheduling/shuffle/task overhead.

Parallelism only helps until the units of work become too fragmented.

### Reteaching Phase 5 or jumping ahead to Phase 7/8

Phase 6 should answer:

```text
What physical data must Spark read?
What kind of partition is this?
Why does data move?
How does the partition count change?
How do storage and execution layouts interact?
```

Phase 7 goes deeper into:

```text
performance optimization
join strategy decisions
skew remediation
caching
AQE tuning
resource behavior
```

Phase 8 goes deeper into:

```text
Spark UI jobs/stages/tasks
shuffle metrics
spill
executor utilization
runtime diagnosis
```

Keep those layers distinct.

---

[Back to Table of Contents](#toc)

---

<a id="18-phase-6-mastery-reference"></a>
## 18. Phase 6 Mastery Reference

The curriculum mastery target is eventually to take real PySpark pipelines and explain:

- what kind of partition is being discussed;
- why a shuffle occurs;
- why Spark reads certain files/partitions;
- how partition counts change;
- how `repartition()` and `coalesce()` differ;
- how storage layout affects physical execution.

That mastery gate is intentionally deferred until explicitly requested.

Before beginning it, Phase 6 fluency should include the following.

### Parquet and file storage

You should be able to explain:

- why Parquet is columnar;
- why columnar storage supports analytical column pruning;
- why encoding/compression reduce physical I/O;
- how Parquet preserves schema/type metadata;
- what Parquet row groups are conceptually;
- how min/max/null-style metadata can help skip irrelevant blocks;
- why Parquet datasets normally contain multiple physical files.

### Read optimization mechanisms

You should distinguish:

```text
column pruning
→ avoid reading unnecessary columns

partition pruning
→ avoid scanning irrelevant storage partitions/files

predicate pushdown
→ delegate eligible predicates to the data source reader

Parquet statistics
→ prove internal physical blocks cannot match
```

You should be able to inspect a file scan and point to evidence for these mechanisms where Spark exposes it.

### Partition taxonomy

Given a statement such as:

> There are 24 partitions.

You should immediately ask:

```text
storage partitions?
input partitions?
current DataFrame partitions?
shuffle partitions?
output partitions?
```

You should also know that a Parquet row group is another physical grouping, not a Spark execution partition.

### Partition-count reasoning

Given:

```python
source_df = spark.read.parquet(path)
filtered_df = source_df.filter(F.col('quantity') >= 5)
repartitioned_df = filtered_df.repartition(8)
coalesced_df = repartitioned_df.coalesce(3)
```

you should be able to predict approximately:

```text
source partition count
→ determined by input scan planning

filter
→ usually same count, fewer rows per partition

repartition(8)
→ shuffle into 8 execution partitions

coalesce(3)
→ collapse to 3 execution partitions without full rebalance
```

and then inspect the actual counts/plan.

### Shuffle reasoning

You should be able to explain why these operations commonly redistribute data:

```text
groupBy
join
distinct
orderBy
repartition
```

The explanation must identify the downstream distribution/order requirement, not merely label the operation as 'wide'.

### `repartition()` vs. `coalesce()`

You should be able to say:

```text
repartition()
- creates a new distribution
- normally shuffles
- can increase or decrease partition count
- can partition by key
- tends to rebalance rows according to the requested scheme

coalesce()
- primarily reduces partition count
- normally avoids a full shuffle
- combines existing partitions
- can preserve imbalance
- is not a useful way to increase parallelism
```

### Storage vs. execution reasoning

Given:

```text
fact_sales/year=2026/month=08/*.parquet
```

and:

```python
result_df = (
    spark.read.parquet(path)
    .filter(
        (F.col('year') == 2026)
        & (F.col('month') == 8)
    )
    .groupBy('store_id')
    .agg(F.sum('net_sales'))
)
```

you should be able to explain:

```text
1. year/month are storage partition columns.
2. Their filters can prune irrelevant storage directories/files.
3. The remaining files become scan input partitions according to Spark's file
   scan planning; storage partition count does not equal execution partition count.
4. The scan can read only required Parquet columns.
5. Eligible row predicates can be pushed into the Parquet reader.
6. The grouped aggregation requires a new runtime distribution by store_id.
7. Therefore an Exchange/shuffle can occur even though the source was already
   storage-partitioned by year/month.
8. The post-shuffle partition structure now reflects the grouping execution,
   not the original storage-directory layout.
9. If written, that execution structure contributes to the next physical file
   layout, which will influence future reads.
```

### Final reasoning standard

The final Phase 6 standard is not:

> I know that Parquet is columnar and `repartition()` shuffles.

It is:

> **I can trace a PySpark pipeline from physical files through pruning and scan planning into execution partitions, identify exactly where and why Spark redistributes data, predict how partition counts change, and explain how the resulting execution structure creates the storage layout future Spark jobs will read.**

---

[Back to Table of Contents](#toc)
