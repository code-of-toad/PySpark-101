# Phase 6 — Experiment & Applied Practice Guide

<a id="toc"></a>
## Table of Contents

- [Purpose](#purpose)
- [Conventions](#conventions)
- [Practice Dataset](#practice-dataset)
- [Experiment 1 — Parquet Files, Compression, and Schema Preservation](#experiment-1-parquet-files-compression-and-schema-preservation)
- [Experiment 2 — Column Pruning, Predicate Pushdown, and File Statistics](#experiment-2-column-pruning-predicate-pushdown-and-file-statistics)
- [Experiment 3 — Storage Partitioning and Partition Pruning](#experiment-3-storage-partitioning-and-partition-pruning)
- [Experiment 4 — Distinguish Storage, Input, DataFrame, Shuffle, and Output Partitions](#experiment-4-distinguish-storage-input-dataframe-shuffle-and-output-partitions)
- [Experiment 5 — Show That Filtering Rows Does Not Necessarily Reduce Partitions](#experiment-5-show-that-filtering-rows-does-not-necessarily-reduce-partitions)
- [Experiment 6 — Inspect `repartition()`](#experiment-6-inspect-repartition)
- [Experiment 7 — Inspect `coalesce()`](#experiment-7-inspect-coalesce)
- [Experiment 8 — Repartition by Key and Observe Data Distribution](#experiment-8-repartition-by-key-and-observe-data-distribution)
- [Experiment 9 — Excessive vs. Insufficient Partitioning](#experiment-9-excessive-vs-insufficient-partitioning)
- [Experiment 10 — Explain Why Common Operations Shuffle](#experiment-10-explain-why-common-operations-shuffle)
- [Experiment 11 — Output Partitions, Output Files, and the Small-File Problem](#experiment-11-output-partitions-output-files-and-the-small-file-problem)
- [Experiment 12 — Trace Storage → Execution → Storage End to End](#experiment-12-trace-storage-execution-storage-end-to-end)
- [Applied Phase 6 Project](#applied-phase-6-project)
- [Applied Task — Part 1](#applied-task-part-1)
- [After Part 1](#after-part-1)

---

<a id="purpose"></a>
## Purpose

This file is the execution-oriented companion for **Phase 6 — Storage, Partitioning & Shuffle Engineering**.

The Phase 6 `README.md` is the conceptual reference. `phase_06_lecture.py` is the consolidated teaching implementation. This guide converts those concepts into controlled experiments where the main habit is:

```text
predict partition/storage behavior
        ↓
inspect
        ↓
execute
        ↓
compare before/after
        ↓
explain why
```

For every important pipeline, ask:

```text
1. What is the DataFrame grain?
2. What physical files/directories exist before the read?
3. Which storage partitions should Spark be able to prune?
4. Which columns should the file scan need?
5. Which predicates can reach the Parquet reader?
6. How many input/DataFrame partitions exist before wide work?
7. Which transformations are narrow?
8. Which operation requires redistribution, and why?
9. What shuffle partition count/distribution is requested?
10. How do repartition() or coalesce() change the current execution partitions?
11. How many output partitions reach the writer?
12. What physical files/directories are actually created?
13. How will that output layout affect the next Spark read?
```

Phase 5 concepts such as `Exchange`, `Scan`, `Sort`, and `explain('formatted')` are used as evidence. Catalyst and AQE are not retaught.

This guide intentionally does **not** expand deeply into:

- generalized performance tuning;
- broadcast-vs.-sort-merge join optimization;
- skew remediation or salting;
- caching/persistence;
- executor memory/cores;
- spill and garbage collection;
- Spark UI task diagnosis.

Those belong primarily to Phases 7 and 8.

The applied section is practice only. It does **not** perform the formal Phase 6 mastery gate and does not update `ROADMAP.md`.

---

[Back to Table of Contents](#toc)

---

<a id="conventions"></a>
## Conventions

- Use **PySpark 4.2.0**.
- Use **single quotes** for Python strings.
- Prefer `from pyspark.sql import functions as F`.
- Include inline comments explaining both **what** important code does and **why** the observation matters.
- Reuse one deterministic retail dataset through the experiments.
- Preserve and state DataFrame grain before interpreting storage or shuffle behavior.
- Use `df.rdd.getNumPartitions()` only for **Spark execution partitions**.
- Use `F.spark_partition_id()` when you need to inspect how rows are distributed across current execution partitions.
- Use filesystem inspection when discussing **physical files** or **storage partition directories**.
- Use `df.explain('formatted')` when you need physical scan/`Exchange`/`Sort` evidence.
- Keep AQE disabled in partition-count experiments so static shuffle counts remain interpretable.
- Treat configuration changes as **experiment controls**, not universal production recommendations.
- Never use the word **partition** without identifying which layer you mean.
- Do not assume one file equals one Spark input partition.
- Do not assume one storage partition directory equals one Spark execution partition.
- Do not assume one Spark output partition always produces exactly one file in every write configuration.
- Do not treat `partitionBy()` and `repartition()` as equivalent APIs.
- Do not mark Phase 6 complete until the mastery gate is explicitly requested and passed.

### Required partition vocabulary

```text
storage partition
input partition
current DataFrame / execution partition
shuffle partition
output partition
```

Also recognize:

```text
Parquet row group
```

A Parquet row group is a physical storage structure inside a Parquet file. It is **not** a Spark execution partition.

---

[Back to Table of Contents](#toc)

---

<a id="practice-dataset"></a>
# Practice Dataset

Use one deterministic retail fact dataset plus a small store dimension.

The main grains are:

```text
sales_enriched_df
= one row per synthetic sale line

stores_df
= one row per store_id
```

The fact data spans roughly three months and contains:

```text
sale_id
order_date
store_id
product_id
order_status
quantity
unit_price
year
month
gross_sales
```

This supports:

```text
Parquet storage
column pruning
predicate pushdown
storage partitioning
partition pruning
shuffle experiments
output-file experiments
```

## Start one controlled Spark session

```python
from pathlib import Path
from tempfile import TemporaryDirectory

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


spark = (
    SparkSession.builder
    .appName('phase_06_storage_partition_practice')
    # Four local threads preserve visible parallel work while keeping the
    # experiments runnable on one development machine.
    .master('local[4]')
    # A fixed shuffle count keeps partition predictions deterministic.
    .config('spark.sql.shuffle.partitions', '8')
    # Disable AQE here so it does not coalesce shuffle partitions behind the
    # experiments whose purpose is to inspect static partition behavior.
    .config('spark.sql.adaptive.enabled', 'false')
    .getOrCreate()
)
```

## Create deterministic retail facts

```python
sales_df = (
    spark.range(
        start=0,
        end=12000,
        step=1,
        numPartitions=4,
    )
    .select(
        F.col('id').cast('int').alias('sale_id'),
        F.date_add(
            F.lit('2026-01-01').cast('date'),
            (F.col('id') % 90).cast('int'),
        ).alias('order_date'),
        F.concat(
            F.lit('S'),
            F.lpad(
                ((F.col('id') % 12) + F.lit(1)).cast('string'),
                2,
                '0',
            ),
        ).alias('store_id'),
        F.concat(
            F.lit('P'),
            F.lpad(
                ((F.col('id') % 40) + F.lit(1)).cast('string'),
                3,
                '0',
            ),
        ).alias('product_id'),
        F.when(
            (F.col('id') % 10) < 8,
            F.lit('COMPLETED'),
        )
        .when(
            (F.col('id') % 10) == 8,
            F.lit('CANCELLED'),
        )
        .otherwise(F.lit('RETURNED'))
        .alias('order_status'),
        ((F.col('id') % 5) + F.lit(1)).cast('int').alias('quantity'),
        (
            F.lit(5.00)
            + ((F.col('id') % 25) * F.lit(1.25))
        )
        .cast(DecimalType(12, 2))
        .alias('unit_price'),
    )
)


sales_enriched_df = (
    sales_df
    # Storage partition columns are explicit business/date columns, not Spark
    # execution partition IDs.
    .withColumn('year', F.year('order_date'))
    .withColumn('month', F.month('order_date'))
    .withColumn(
        'gross_sales',
        (F.col('quantity') * F.col('unit_price')).cast(DecimalType(16, 2)),
    )
)
```

## Create the store dimension

```python
stores_df = (
    spark.range(
        start=1,
        end=13,
        step=1,
        numPartitions=2,
    )
    .select(
        F.concat(
            F.lit('S'),
            F.lpad(F.col('id').cast('string'), 2, '0'),
        ).alias('store_id'),
        F.concat(
            F.lit('Store '),
            F.col('id').cast('string'),
        ).alias('store_name'),
        F.when(F.col('id') <= 6, F.lit('ON'))
        .otherwise(F.lit('BC'))
        .alias('province'),
    )
)
```

## Create small inspection helpers

```python
def show_partition_summary(df, label):
    '''Print execution-partition count and rows in non-empty partitions.'''

    partition_rows = (
        df
        .select(F.spark_partition_id().alias('partition_id'))
        .groupBy('partition_id')
        .count()
        .orderBy('partition_id')
        .collect()
    )

    print(f'\n{label}')
    print('execution partitions:', df.rdd.getNumPartitions())

    for row in partition_rows:
        print(
            f'partition {row.partition_id}: '
            f'{row.count} rows'
        )


def parquet_data_files(path):
    '''Return only physical Parquet data files below a local path.'''

    return sorted(Path(path).rglob('*.parquet'))


def show_parquet_files(path, label):
    '''Print physical Parquet data-file count and size.'''

    files = parquet_data_files(path)

    print(f'\n{label}')
    print('parquet data files:', len(files))

    for file_path in files:
        print(
            file_path.relative_to(path),
            '->',
            file_path.stat().st_size,
            'bytes',
        )
```

## Create one temporary local storage root

```python
# Keep the TemporaryDirectory object alive for the entire notebook/session.
temp_directory = TemporaryDirectory(prefix='pyspark_phase_06_')
temp = Path(temp_directory.name)
```

Baseline prediction:

```text
sales_enriched_df was created from spark.range(..., numPartitions=4)
→ current execution partition count should begin at 4
```

Inspect:

```python
show_partition_summary(
    sales_enriched_df,
    'BASELINE SALES DATAFRAME',
)
```

---

[Back to Table of Contents](#toc)

---

<a id="experiment-1-parquet-files-compression-and-schema-preservation"></a>
# Experiment 1 — Parquet Files, Compression, and Schema Preservation

## Goal

Connect a Spark DataFrame write to physical Parquet files and verify that Parquet preserves typed schema metadata.

## Predict before writing

State:

```text
Current DataFrame execution partitions:
Expected writer parallelism:
Expected rough number of non-empty data files:
Expected compression codec:
Expected schema behavior after re-read:
```

For the baseline DataFrame, predict approximately:

```text
4 execution partitions
→ roughly 4 writer tasks for a simple unpartitioned write
→ roughly one non-empty Parquet file per writer task
```

Treat this as a useful default mental model, not a universal file-count contract.

## Write Parquet

```python
parquet_path = temp / 'sales_unpartitioned'

print(
    'before write execution partitions:',
    sales_enriched_df.rdd.getNumPartitions(),
)

(
    sales_enriched_df
    .write
    .mode('overwrite')
    .parquet(str(parquet_path))
)
```

## Inspect physical files

```python
show_parquet_files(
    parquet_path,
    'UNPARTITIONED PARQUET OUTPUT',
)
```

Answer:

1. How many physical `.parquet` data files were written?
2. How does that compare with the input/output execution partition count?
3. Why should you still avoid writing the rule `one Spark partition = exactly one file`?

## Inspect compression configuration

```python
print(
    'Parquet compression codec:',
    spark.conf.get('spark.sql.parquet.compression.codec'),
)
```

The important Phase 6 point is:

```text
columnar layout
+
encoding/compression
+
typed metadata
```

make Parquet suitable for analytical storage.

## Re-read and inspect schema

```python
parquet_sales_df = spark.read.parquet(str(parquet_path))
parquet_sales_df.printSchema()
```

Confirm that types such as:

```text
sale_id      integer
order_date   date
quantity     integer
unit_price   decimal
```

survive the storage round trip.

## Required explanation

Explain in ordinary engineering language:

```text
Parquet stores typed columnar data rather than CSV-style raw text. Spark can
therefore recover the stored schema, request only required column chunks, and use
Parquet metadata while planning reads. The output remains a dataset directory of
physical files rather than one monolithic file.
```

---

[Back to Table of Contents](#toc)

---

<a id="experiment-2-column-pruning-predicate-pushdown-and-file-statistics"></a>
# Experiment 2 — Column Pruning, Predicate Pushdown, and File Statistics

## Goal

Separate three related but different read-time ideas:

```text
column pruning
predicate pushdown
Parquet file/row-group statistics
```

## Build a selective query

```python
scan_pruning_df = (
    spark.read.parquet(str(parquet_path))
    .filter(
        (F.col('order_status') == 'COMPLETED')
        & (F.col('quantity') >= 4)
    )
    .select(
        'store_id',
        'product_id',
        'quantity',
        'gross_sales',
    )
)
```

## Predict before inspection

### Column pruning

The final projection needs:

```text
store_id
product_id
quantity
gross_sales
```

The filter also needs:

```text
order_status
quantity
```

Predict that the Parquet scan should not need unrelated columns such as:

```text
sale_id
order_date
unit_price
year
month
```

unless Spark requires one for another reason visible in the plan.

### Predicate pushdown

Predict that eligible predicates such as:

```text
order_status = COMPLETED
quantity >= 4
```

may appear in the scan's `PushedFilters`.

### File statistics

Parquet stores statistics for internal column chunks/row groups. When a predicate reaches the reader, metadata such as minimum/maximum values may let the reader skip blocks that cannot possibly match.

Do **not** claim that every pushed predicate guarantees block skipping. Statistics help only when the metadata proves a block cannot match.

## Inspect

```python
scan_pruning_df.explain('formatted')
```

Locate scan details such as:

```text
ReadSchema
PushedFilters
DataFilters
```

## Questions

1. Which physical columns appear in `ReadSchema`?
2. Which predicates appear in `PushedFilters`?
3. Does a Spark `Filter` operator still appear?
4. Why can a filter remain even if predicate pushdown occurred?
5. What role can Parquet statistics play after a predicate reaches the file reader?

## Required distinction

```text
Column pruning
= avoid reading unnecessary columns

Predicate pushdown
= send eligible row predicates into the data-source reader

Parquet statistics
= metadata that may prove certain internal blocks cannot match
```

Do not merge these into one vague statement such as:

> Spark skips data.

State **what kind of data** is avoided and **which mechanism** enables it.

---

[Back to Table of Contents](#toc)

---

<a id="experiment-3-storage-partitioning-and-partition-pruning"></a>
# Experiment 3 — Storage Partitioning and Partition Pruning

## Goal

See how directory-based storage partitioning changes which physical files Spark needs to consider.

## Write by storage partition columns

```python
partitioned_sales_path = temp / 'sales_partitioned_by_year_month'

(
    sales_enriched_df
    .write
    .mode('overwrite')
    # partitionBy() changes physical directory layout. It is not the same API or
    # concept as DataFrame.repartition().
    .partitionBy('year', 'month')
    .parquet(str(partitioned_sales_path))
)
```

## Inspect the directory layout

```python
storage_partition_directories = sorted(
    path.relative_to(partitioned_sales_path)
    for path in partitioned_sales_path.glob('year=*/month=*')
)

for directory in storage_partition_directories:
    print(directory)
```

Expected conceptual layout:

```text
sales_partitioned_by_year_month/
└── year=2026/
    ├── month=1/
    ├── month=2/
    └── month=3/
```

These are **storage partitions**.

They are not Spark task partitions.

## Query one storage slice

```python
february_completed_df = (
    spark.read.parquet(str(partitioned_sales_path))
    .filter(
        (F.col('year') == 2026)
        & (F.col('month') == 2)
        & (F.col('order_status') == 'COMPLETED')
    )
    .select(
        'store_id',
        'product_id',
        'order_status',
        'quantity',
        'gross_sales',
        'year',
        'month',
    )
)
```

## Predict before inspection

Predict three different scan behaviors:

```text
PartitionFilters
- year = 2026
- month = 2
- exclude unrelated storage directories/files

PushedFilters
- order_status = COMPLETED may reach the Parquet reader

ReadSchema
- read only ordinary Parquet columns needed for filtering/projection
```

## Inspect

```python
february_completed_df.explain('formatted')
```

Find:

```text
PartitionFilters
PushedFilters
ReadSchema
```

## Required distinction

```text
partition pruning
= choose relevant storage directories/files

predicate pushdown
= push eligible row predicates into the file reader

column pruning
= choose relevant physical columns
```

A single query can benefit from all three.

## Engineering question

Why might partitioning a fact dataset by a frequently filtered date column reduce future I/O?

Your answer must mention **physical file selection**, not merely:

> because Spark knows the partition.

---

[Back to Table of Contents](#toc)

---

<a id="experiment-4-distinguish-storage-input-dataframe-shuffle-and-output-partitions"></a>
# Experiment 4 — Distinguish Storage, Input, DataFrame, Shuffle, and Output Partitions

## Goal

Stop using the word `partition` ambiguously.

## Compare storage directories with current execution partitions

```python
full_partitioned_read_df = spark.read.parquet(
    str(partitioned_sales_path)
)

print(
    'storage partition directories:',
    len(storage_partition_directories),
)
print(
    'full read execution partitions:',
    full_partitioned_read_df.rdd.getNumPartitions(),
)
print(
    'February filtered execution partitions:',
    february_completed_df.rdd.getNumPartitions(),
)
```

## Explain why these counts need not match

A correct explanation should include:

```text
storage partition directory
= physical organization in the filesystem

input partition
= task-sized chunk of selected physical input

DataFrame execution partition
= current distributed partitioning of rows at that point in the plan
```

Spark file scans can:

```text
split a sufficiently large file into multiple input partitions
or
pack multiple small files into one input partition
```

Therefore:

```text
one file != guaranteed one input partition
one storage directory != one execution partition
```

## Inspect file-scan planning settings

```python
print(
    'spark.sql.files.maxPartitionBytes:',
    spark.conf.get('spark.sql.files.maxPartitionBytes'),
)
print(
    'spark.sql.files.openCostInBytes:',
    spark.conf.get('spark.sql.files.openCostInBytes'),
)
```

Do not tune them in this phase.

The point is only to recognize that Spark **plans file input into execution work** rather than assigning one task mechanically per directory or per file.

## Complete the vocabulary table

| Term | What it represents | How to inspect |
|---|---|---|
| Storage partition | Filesystem directory/value layout | filesystem paths, `PartitionFilters` |
| Input partition | Task-sized unit of file-scan work | current read partition count / plan behavior |
| DataFrame partition | Current distributed rows at a plan point | `df.rdd.getNumPartitions()` |
| Shuffle partition | Post-redistribution bucket | `Exchange`, shuffle config, current count after wide operation |
| Output partition | Data partition reaching writer tasks | count immediately before write |

---

[Back to Table of Contents](#toc)

---

<a id="experiment-5-show-that-filtering-rows-does-not-necessarily-reduce-partitions"></a>
# Experiment 5 — Show That Filtering Rows Does Not Necessarily Reduce Partitions

## Goal

Separate **row count** from **partition count**.

## Build a narrow filter experiment

```python
narrow_source_df = spark.range(
    start=0,
    end=10000,
    step=1,
    numPartitions=8,
)

very_small_filtered_df = narrow_source_df.filter(
    F.col('id') < 10
)
```

## Predict

Before running anything, record:

```text
source rows:
source partitions:
filtered rows:
filtered partitions:
shuffle expected? yes/no
```

A defensible prediction is:

```text
filter() is narrow
→ rows can be removed independently inside existing partitions
→ no redistribution requirement
→ partition topology can remain at 8
→ some partitions may become empty
```

## Execute and inspect

```python
print('source partitions:', narrow_source_df.rdd.getNumPartitions())
print('filtered partitions:', very_small_filtered_df.rdd.getNumPartitions())
print('filtered rows:', very_small_filtered_df.count())

show_partition_summary(
    very_small_filtered_df,
    'FILTERED PARTITION DISTRIBUTION',
)
```

## Required lesson

```text
less data
!=
fewer Spark partitions
```

A transformation can remove nearly all rows while retaining the current partition structure.

Do not reach for `coalesce()` automatically just because a filter reduced the row count. Whether repartitioning is worthwhile is a later engineering decision based on downstream work.

---

[Back to Table of Contents](#toc)

---

<a id="experiment-6-inspect-repartition"></a>
# Experiment 6 — Inspect `repartition()`

## Goal

Observe that `repartition()` requests a new execution distribution and therefore normally requires redistribution.

## Build the experiment

```python
repartition_source_df = spark.range(
    start=0,
    end=12000,
    step=1,
    numPartitions=4,
)

repartitioned_df = repartition_source_df.repartition(8)
```

## Predict before inspection

```text
before: 4 execution partitions
after:  8 execution partitions

why:
repartition(8) explicitly requests a new distribution

physical evidence expected:
Exchange / shuffle
```

## Inspect counts and plan

```python
print(
    'before repartition:',
    repartition_source_df.rdd.getNumPartitions(),
)
print(
    'after repartition(8):',
    repartitioned_df.rdd.getNumPartitions(),
)

repartitioned_df.explain('formatted')
```

Locate the `Exchange` and explain **why** it exists.

Do not say merely:

> `repartition()` shuffles because `repartition()` shuffles.

Say:

> The operation explicitly requests a new eight-partition distribution, so Spark must redistribute rows from the old four-partition layout into the requested downstream partitioning.

## Inspect balance

```python
show_partition_summary(
    repartitioned_df,
    'REPARTITIONED ROW DISTRIBUTION',
)
```

For this synthetic non-key repartition, distribution should usually be reasonably balanced.

## Critical distinction

```python
some_df.repartition(8)
```

changes **Spark execution partitioning**.

```python
some_df.write.partitionBy('year').parquet(...)
```

changes **physical storage directory layout**.

These are not interchangeable operations.

---

[Back to Table of Contents](#toc)

---

<a id="experiment-7-inspect-coalesce"></a>
# Experiment 7 — Inspect `coalesce()`

## Goal

Understand when `coalesce()` can reduce partitions without a full global redistribution.

## Build the experiment

```python
coalesce_source_df = spark.range(
    start=0,
    end=12000,
    step=1,
    numPartitions=8,
)

coalesced_df = coalesce_source_df.coalesce(3)
```

## Predict

```text
before: 8 execution partitions
after:  3 execution partitions

expected dependency:
narrow reduction of existing partitions

expected full shuffle:
no
```

## Inspect

```python
print(
    'before coalesce:',
    coalesce_source_df.rdd.getNumPartitions(),
)
print(
    'after coalesce(3):',
    coalesced_df.rdd.getNumPartitions(),
)

coalesced_df.explain('formatted')

show_partition_summary(
    coalesced_df,
    'COALESCED ROW DISTRIBUTION',
)
```

## Try to increase with `coalesce()`

```python
coalesce_attempt_to_increase_df = coalesced_df.coalesce(10)

print(
    'coalesce(3) then coalesce(10):',
    coalesce_attempt_to_increase_df.rdd.getNumPartitions(),
)
```

## Decision rule

```text
Need more partitions or a genuinely new/balanced distribution?
    → repartition()

Need fewer partitions and current distribution is acceptable?
    → coalesce()
```

Do not reduce this to:

```text
coalesce = faster repartition
```

They have different semantics and distribution behavior.

---

[Back to Table of Contents](#toc)

---

<a id="experiment-8-repartition-by-key-and-observe-data-distribution"></a>
# Experiment 8 — Repartition by Key and Observe Data Distribution

## Goal

See how key-based repartitioning changes row placement and why equal keys become colocated by hash bucket.

## Repartition by `store_id`

```python
sales_by_store_df = sales_enriched_df.repartition(
    8,
    'store_id',
)
```

## Predict

```text
requested execution partitions = 8
partitioning expression = store_id
shuffle expected = yes

same store_id values
→ same hash result
→ same resulting partition bucket
```

Do **not** predict that all eight partitions must be non-empty.

There are only twelve store keys, and different keys can hash to the same bucket.

## Inspect the plan

```python
sales_by_store_df.explain('formatted')
```

Look for key-based partitioning evidence in the `Exchange`.

## Inspect key placement

```python
store_partition_map_df = (
    sales_by_store_df
    .select(
        'store_id',
        F.spark_partition_id().alias('partition_id'),
    )
    .distinct()
    .orderBy('store_id')
)

store_partition_map_df.show(20, truncate=False)
```

Question:

> Does any one `store_id` appear in more than one resulting execution partition?

## Inspect row balance

```python
show_partition_summary(
    sales_by_store_df,
    'STORE-KEY REPARTITION DISTRIBUTION',
)
```

## Hot-key extension

Create deliberately skewed keys:

```python
skewed_df = (
    spark.range(
        start=0,
        end=10000,
        step=1,
        numPartitions=4,
    )
    .select(
        F.when(
            F.col('id') < 8000,
            F.lit('HOT'),
        )
        .otherwise(
            F.concat(
                F.lit('COLD_'),
                (F.col('id') % 20).cast('string'),
            )
        )
        .alias('business_key')
    )
)

skewed_by_key_df = skewed_df.repartition(
    8,
    'business_key',
)

show_partition_summary(
    skewed_by_key_df,
    'SKEWED KEY DISTRIBUTION',
)
```

## Required lesson

```text
hash repartitioning
= equal keys are routed together

hash repartitioning
!= equal row counts when key frequencies are skewed
```

Phase 6 should recognize the hot-key problem. Phase 7 studies remediation.

---

[Back to Table of Contents](#toc)

---

<a id="experiment-9-excessive-vs-insufficient-partitioning"></a>
# Experiment 9 — Excessive vs. Insufficient Partitioning

## Goal

Understand why partition count is an engineering balance rather than a magic constant.

## Create two extremes

```python
insufficient_df = spark.range(
    start=0,
    end=100000,
    step=1,
    numPartitions=1,
)

excessive_df = spark.range(
    start=0,
    end=100000,
    step=1,
    numPartitions=200,
)
```

Inspect:

```python
print(
    'insufficient example partitions:',
    insufficient_df.rdd.getNumPartitions(),
)
print(
    'excessive example partitions:',
    excessive_df.rdd.getNumPartitions(),
)
```

## Explain the trade-off

### Too few execution partitions

Potential consequences:

```text
underused available cores
large tasks
large per-task memory/I/O burden
less parallelism
```

### Too many execution partitions

Potential consequences:

```text
many tiny tasks
scheduler overhead
extra shuffle bookkeeping
many tiny writer tasks/files
```

## Do not invent a universal target

A partition count is meaningful only together with:

```text
data size
rows/bytes per partition
available parallelism
key distribution/skew
downstream operation
file-writing behavior
```

The goal of this experiment is not to benchmark `1` vs. `200` on your laptop.

The goal is to develop the reasoning:

> **Partition count controls how finely Spark divides work; both extremes can be inefficient for different reasons.**

---

[Back to Table of Contents](#toc)

---

<a id="experiment-10-explain-why-common-operations-shuffle"></a>
# Experiment 10 — Explain Why Common Operations Shuffle

## Goal

Explain each shuffle from the downstream data-distribution requirement it satisfies.

Keep static behavior visible:

```python
spark.conf.set('spark.sql.shuffle.partitions', '8')
spark.conf.set('spark.sql.adaptive.enabled', 'false')
```

For every subsection below:

```text
predict
→ inspect explain('formatted')
→ locate Exchange
→ state the distribution requirement
```

## 10.1 `groupBy()`

```python
grouped_df = (
    sales_enriched_df
    .groupBy('store_id')
    .agg(
        F.sum('gross_sales').alias('gross_sales'),
        F.sum('quantity').alias('units'),
    )
)
```

Prediction:

```text
partial aggregate states for one store can originate in several partitions
→ all states for the same store_id must meet
→ hash redistribution by store_id
→ final aggregation
```

Inspect:

```python
grouped_df.explain('formatted')
print(
    'grouped execution partitions:',
    grouped_df.rdd.getNumPartitions(),
)
```

Explain why the downstream grouped result uses the configured shuffle partition count under this static setup.

## 10.2 `distinct()`

```python
distinct_products_df = (
    sales_enriched_df
    .select('product_id')
    .distinct()
)

# Global uniqueness requires equal product_id values from different input
# partitions to be compared/combined somewhere downstream.
distinct_products_df.explain('formatted')
```

Required explanation:

```text
partition-local deduplication cannot prove global uniqueness
→ equal values from different partitions must meet
→ redistribution is required
```

## 10.3 `orderBy()`

```python
ordered_df = sales_enriched_df.orderBy(
    F.col('gross_sales').desc(),
    F.col('sale_id').asc(),
)

ordered_df.explain('formatted')
```

A global ordering requires more than sorting independently inside the existing partitions.

Compare with:

```python
local_sort_df = sales_enriched_df.sortWithinPartitions(
    'store_id',
    'sale_id',
)

local_sort_df.explain('formatted')
```

Required distinction:

```text
orderBy()
= global ordering requirement

sortWithinPartitions()
= ordering only inside each current partition
```

Do not use `Sort` and `Exchange` as synonyms:

```text
Exchange → distribution
Sort     → ordering
```

## 10.4 Non-broadcast equi-join

```python
# Disable broadcast only so the shuffle behavior is visible and controlled.
spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '-1')

shuffle_join_df = (
    sales_enriched_df
    .hint('merge')
    .join(
        stores_df.hint('merge'),
        on='store_id',
        how='inner',
    )
)

shuffle_join_df.explain('formatted')
```

Prediction:

```text
matching store_id values from the two inputs
must reach compatible downstream partitions
→ Exchange by join key on each required side

SortMergeJoin also needs ordered key streams
→ Sort by store_id
```

Do not conclude:

> every join shuffles both sides.

Broadcast joins are the obvious counterexample from Phase 5.

Reset:

```python
spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '10485760')
```

## 10.5 Explicit `repartition()`

```python
explicit_shuffle_df = sales_enriched_df.repartition(
    6,
    'product_id',
)

explicit_shuffle_df.explain('formatted')
```

Here the redistribution is not an indirect requirement of an aggregate/join.

The user explicitly requested:

```text
new partition count = 6
new partitioning expression = product_id
```

## Shuffle-cause summary

| Operation | Why redistribution is often required |
|---|---|
| `groupBy()` | Equal grouping keys must meet for final aggregation |
| join | Matching join keys may need compatible distributions |
| `distinct()` | Equal values must meet for global deduplication |
| `orderBy()` | Global order requires coordinated distribution/order |
| `repartition()` | A new distribution is explicitly requested |

---

[Back to Table of Contents](#toc)

---

<a id="experiment-11-output-partitions-output-files-and-the-small-file-problem"></a>
# Experiment 11 — Output Partitions, Output Files, and the Small-File Problem

## Goal

Connect the execution partitioning immediately before a write to the next physical storage layout.

## Compare many vs. few writer partitions

```python
many_output_partitions_df = sales_enriched_df.repartition(32)
fewer_output_partitions_df = sales_enriched_df.repartition(4)

many_files_path = temp / 'many_output_files'
fewer_files_path = temp / 'fewer_output_files'
```

Predict:

```text
32 output execution partitions
→ many writer tasks
→ many small files on this tiny dataset

4 output execution partitions
→ fewer writer tasks
→ fewer, larger files on this tiny dataset
```

Write:

```python
(
    many_output_partitions_df
    .write
    .mode('overwrite')
    .parquet(str(many_files_path))
)

(
    fewer_output_partitions_df
    .write
    .mode('overwrite')
    .parquet(str(fewer_files_path))
)
```

Inspect:

```python
show_parquet_files(
    many_files_path,
    'WRITE FROM 32 EXECUTION PARTITIONS',
)

show_parquet_files(
    fewer_files_path,
    'WRITE FROM 4 EXECUTION PARTITIONS',
)
```

## Explain the small-file problem

Avoid the weak statement:

> Small files are bad.

Explain the mechanism:

```text
many tiny output files
→ more files to list/open/track
→ more metadata and scan-planning overhead
→ future reads may spend disproportionate work managing tiny physical objects
```

Do not conclude that four partitions is a universal production target. The dataset here is intentionally small.

## 11.1 Storage `partitionBy()` complicates exact file-count prediction

```python
partitioned_write_path = temp / 'partitioned_write'
storage_write_df = sales_enriched_df.repartition(4)

(
    storage_write_df
    .write
    .mode('overwrite')
    .partitionBy('year', 'month')
    .parquet(str(partitioned_write_path))
)

print(
    'storage-partitioned write execution partitions:',
    storage_write_df.rdd.getNumPartitions(),
)

show_parquet_files(
    partitioned_write_path,
    'STORAGE-PARTITIONED WRITE FILES',
)
```

A writer task can encounter more than one storage-partition value and therefore create files under several directories.

Thus:

```text
output execution partitions
= strong clue about writer parallelism

output execution partitions
!= exact universal file-count contract
```

## 11.2 `maxRecordsPerFile` can split writer output

```python
records_limited_path = temp / 'max_records_per_file'
records_limited_df = sales_enriched_df.coalesce(2)

(
    records_limited_df
    .write
    .mode('overwrite')
    # One writer task can emit multiple files when an explicit per-file record
    # limit causes its output to be split.
    .option('maxRecordsPerFile', 1000)
    .parquet(str(records_limited_path))
)

print(
    'records-limited execution partitions:',
    records_limited_df.rdd.getNumPartitions(),
)

show_parquet_files(
    records_limited_path,
    'MAX RECORDS PER FILE OUTPUT',
)
```

This is direct evidence that:

```text
one output partition
!= guaranteed exactly one physical file
```

---

[Back to Table of Contents](#toc)

---

<a id="experiment-12-trace-storage-execution-storage-end-to-end"></a>
# Experiment 12 — Trace Storage → Execution → Storage End to End

## Goal

Explain one complete pipeline across every Phase 6 layer.

## Build the pipeline

```python
monthly_store_sales_df = (
    spark.read.parquet(str(partitioned_sales_path))
    .filter(
        (F.col('year') == 2026)
        & (F.col('month') == 2)
        & (F.col('order_status') == 'COMPLETED')
    )
    .select(
        'store_id',
        'quantity',
        'gross_sales',
    )
    .groupBy('store_id')
    .agg(
        F.sum('quantity').alias('units'),
        F.sum('gross_sales').alias('gross_sales'),
    )
)
```

Final grain:

```text
one row per store_id for completed February 2026 sales
```

## Predict the complete physical story

Write this prediction **before** calling `explain()`:

```text
1. Storage partition pruning
   year=2026/month=2 should remove irrelevant directory partitions.

2. Column pruning
   the Parquet scan should request only columns required by the filters and
   aggregation.

3. Predicate pushdown
   eligible order_status filtering may reach the Parquet reader.

4. Input/DataFrame partitions
   selected files are converted into task-sized scan work; this count does not
   have to equal the number of storage partition directories.

5. Narrow work
   scan/filter/project operate without requiring key redistribution.

6. Shuffle
   groupBy(store_id) requires partial states for equal store_id values to meet.

7. Shuffle partitions
   with AQE disabled and spark.sql.shuffle.partitions=8, expect the grouped
   downstream distribution to use the static target of 8.

8. Output compaction
   the grouped result is tiny, so coalesce(2) can reduce writer parallelism
   without requesting a full balanced redistribution.

9. Physical write
   the compact output becomes the next Parquet file layout.
```

## Inspect the read and shuffle plan

```python
monthly_store_sales_df.explain('formatted')

print(
    'monthly store-sales execution partitions:',
    monthly_store_sales_df.rdd.getNumPartitions(),
)
```

Identify:

```text
PartitionFilters
PushedFilters
ReadSchema
HashAggregate
Exchange
```

For every item, explain **what physical requirement it satisfies**.

## Reduce output partitions and write

```python
compact_output_df = monthly_store_sales_df.coalesce(2)
final_output_path = temp / 'monthly_store_sales_output'

print(
    'after coalesce(2) output partitions:',
    compact_output_df.rdd.getNumPartitions(),
)

(
    compact_output_df
    .write
    .mode('overwrite')
    .parquet(str(final_output_path))
)

show_parquet_files(
    final_output_path,
    'FINAL MONTHLY STORE-SALES FILES',
)
```

## Explain the entire causal chain

A strong answer should resemble:

```text
The existing year/month storage layout lets Spark exclude unrelated date
folders before reading. The Parquet scan requests only required columns and can
push eligible row predicates into the reader. The selected physical files are
planned into Spark input partitions, which are execution units rather than
storage directories. Narrow scan/filter/project work preserves the current data
distribution. groupBy(store_id) then requires all partial aggregate states for a
store to meet, so Spark inserts a hash-partitioning Exchange and produces the
configured shuffle partition structure. Because the grouped output is tiny, the
result is coalesced to fewer output partitions before writing. Those writer
partitions create the next Parquet file layout, which becomes the physical input
problem for a future Spark job.
```

That is the core Phase 6 mental model.

---

[Back to Table of Contents](#toc)

---

<a id="applied-phase-6-project"></a>
# Applied Phase 6 Project

## Goal

Design and explain a fresh storage-to-storage retail pipeline without copying the worked Experiment 12 answer.

This is **applied practice**, not the formal mastery gate.

Use the same engineering sequence:

```text
business requirement
    ↓
source storage layout
    ↓
predict pruning/read behavior
    ↓
predict input execution partitions
    ↓
predict shuffle requirements
    ↓
predict output partitioning
    ↓
execute + inspect
    ↓
inspect physical files
    ↓
explain mismatches
```

### Business requirement

Produce **March 2026 completed product sales by province** and store the result as Parquet.

Required final grain:

```text
one row per province + product_id
```

Required final columns:

```text
province
product_id
units
gross_sales
```

Business logic:

```text
1. Read the existing year/month-partitioned sales dataset.
2. Keep March 2026 only.
3. Keep COMPLETED sale lines only.
4. Read only columns required by the transformation.
5. Join sales to stores on store_id.
6. Aggregate to province + product_id grain.
7. Sum quantity as units.
8. Sum gross_sales.
9. Prepare a sensible small teaching output without creating dozens of tiny
   files for the final aggregated result.
10. Write the result as unpartitioned Parquet for this exercise.
```

### Correctness expectations

Before talking about partitions, state:

```text
sales source grain
= one row per sale line

stores_df grain
= one row per store_id

post-join grain
= one row per matched sale line if store_id is unique in stores_df

final grain
= one row per province + product_id
```

Partition engineering never replaces grain correctness.

---

[Back to Table of Contents](#toc)

---

<a id="applied-task-part-1"></a>
# Applied Task — Part 1

Implement the applied pipeline yourself using:

```text
partitioned_sales_path
stores_df
```

Create:

```python
province_product_sales_df
```

Then create a write-ready DataFrame:

```python
province_product_sales_output_df
```

and write it to:

```python
applied_output_path = temp / 'province_product_sales'
```

## Before implementation, write your prediction

### 1. Storage partition pruning

State exactly which storage directory values should be relevant:

```text
year = ?
month = ?
```

Then explain why this is **storage partition pruning**, not predicate pushdown.

### 2. Column pruning

List the minimum source fact columns required to satisfy:

```text
March filter
COMPLETED filter
store join
grouping
units
gross_sales
```

Predict which columns should appear in the Parquet scan's `ReadSchema`.

### 3. Predicate pushdown

State which ordinary Parquet data predicate you expect to be eligible for pushdown.

Do not count the year/month storage-directory filters as ordinary pushed Parquet predicates.

### 4. Join behavior

State:

```text
join type:
join key:
fact grain:
dimension grain:
expected output grain before aggregation:
```

You may allow Spark's ordinary join planning to choose the physical algorithm.

Phase 6's main question is:

> **Does this join require a shuffle of either/both inputs under the actual plan, and what distribution requirement explains any `Exchange`?**

Do not turn the exercise into Phase 7 join tuning.

### 5. Aggregation shuffle

Explain why grouping by:

```text
province
product_id
```

requires equal grouping keys to meet downstream.

Predict:

```text
partial aggregation
→ Exchange by grouping keys
→ final aggregation
```

where applicable in the physical plan.

### 6. Partition counts

Record predictions for:

```text
source read execution partitions:
post-join execution partitions:
post-aggregation execution partitions:
final output execution partitions:
```

For any count you cannot justify before execution, explicitly write:

```text
inspect rather than guess
```

### 7. Output-file strategy

The final result is small in this teaching dataset.

Choose between:

```python
repartition(...)
```

and:

```python
coalesce(...)
```

for preparing the final write.

Justify the choice using:

```text
need for new balanced redistribution?
need to increase partitions?
need only to shrink writer parallelism?
```

Do not choose a partition count from a memorized rule.

## Inspect before writing

Run:

```python
province_product_sales_df.explain('formatted')
```

Record evidence for:

```text
PartitionFilters:
PushedFilters:
ReadSchema:
join operator:
join Exchanges:
aggregation Exchange:
Sort operators if any:
post-aggregation partition count:
```

## Execute correctness checks

At minimum, verify:

```text
one row per province + product_id
no NULL province created by the expected dimension join
units and gross_sales are aggregated measures
```

You may use small `collect()` / `count()` / duplicate-key validation because this is a teaching-scale dataset.

## Write and inspect physical output

After preparing `province_product_sales_output_df`:

```python
(
    province_product_sales_output_df
    .write
    .mode('overwrite')
    .parquet(str(applied_output_path))
)

show_parquet_files(
    applied_output_path,
    'APPLIED PHASE 6 OUTPUT',
)
```

Record:

```text
output execution partitions before write:
physical Parquet data files created:
file sizes:
```

Then explain whether the observed file layout matched your prediction.

---

[Back to Table of Contents](#toc)

---

<a id="after-part-1"></a>
# After Part 1

After implementing the applied task, preserve the following observations for review:

```text
1. Source and final DataFrame grains.
2. Source physical storage partition layout.
3. Predicted vs. observed PartitionFilters.
4. Predicted vs. observed ReadSchema.
5. Predicted vs. observed PushedFilters.
6. Input/DataFrame partition counts you observed.
7. Every Exchange in the physical plan and the exact distribution requirement
   that caused it.
8. Any Sort operator and the ordering requirement that caused it.
9. How partition counts changed across the join/aggregation/output pipeline.
10. Why you chose repartition() or coalesce() before the final write.
11. Output execution partition count.
12. Physical data-file count and sizes.
13. Any mismatch between expected writer partitions and actual file count.
14. How the final file layout would affect a future Spark read.
15. Any hot-key, excessive-partition, or small-file concern you observed without
    attempting Phase 7 remediation yet.
```

The next artifact after this guide is the **Phase 6 SOLUTION notebook**. It should implement the experiments in executable notebook form while preserving the same order, setup, predictions, inspections, and explanations.

The **STARTER notebook** should then be derived directly from that SOLUTION notebook, preserving setup/data cells and removing worked solution code.

Do not update `ROADMAP.md`, mark Phase 6 complete, perform the formal mastery gate, or generate Phase 7 materials yet.

### Cleanup after notebook practice

When all experiments are finished:

```python
# Remove the local temporary datasets created only for Phase 6 practice.
temp_directory.cleanup()

# Stop the local teaching Spark application cleanly.
spark.stop()
```

---

[Back to Table of Contents](#toc)
