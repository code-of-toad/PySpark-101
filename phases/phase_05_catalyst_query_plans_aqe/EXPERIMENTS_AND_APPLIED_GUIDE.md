# Phase 5 — Experiment & Applied Practice Guide

<a id="toc"></a>
## Table of Contents

- [Purpose](#purpose)
- [Conventions](#conventions)
- [Practice Dataset](#practice-dataset)
- [Experiment 1 — Predict the Four Plan Layers](#experiment-1-predict-the-four-plan-layers)
- [Experiment 2 — Compare `explain()` Modes](#experiment-2-compare-explain-modes)
- [Experiment 3 — Observe Catalyst Rewriting the Logical Plan](#experiment-3-observe-catalyst-rewriting-the-logical-plan)
- [Experiment 4 — Recognize `Scan`, `Filter`, and `Project`](#experiment-4-recognize-scan-filter-and-project)
- [Experiment 5 — Explain `HashAggregate` and `Exchange`](#experiment-5-explain-hashaggregate-and-exchange)
- [Experiment 6 — Explain a `BroadcastHashJoin`](#experiment-6-explain-a-broadcasthashjoin)
- [Experiment 7 — Explain a `SortMergeJoin`](#experiment-7-explain-a-sortmergejoin)
- [Experiment 8 — Separate `Exchange` from `Sort`](#experiment-8-separate-exchange-from-sort)
- [Experiment 9 — Inspect AQE Before and After Execution](#experiment-9-inspect-aqe-before-and-after-execution)
- [Experiment 10 — Observe an AQE Join-Strategy Change](#experiment-10-observe-an-aqe-join-strategy-change)
- [Experiment 11 — Explain a Complete Retail Pipeline](#experiment-11-explain-a-complete-retail-pipeline)
- [Experiment 12 — Query-Plan Prediction Drills](#experiment-12-query-plan-prediction-drills)
- [Applied Phase 5 Project](#applied-phase-5-project)
- [Applied Task — Part 1](#applied-task-part-1)
- [After Part 1](#after-part-1)

---

<a id="purpose"></a>
## Purpose

This file is the execution-oriented companion for **Phase 5 — Catalyst, Query Plans & AQE**.

The Phase 5 `README.md` is the conceptual reference. `phase_05_lecture.py` is the consolidated teaching implementation. This guide turns those ideas into controlled experiments where you repeatedly predict Spark's plan before inspecting what Spark actually produced.

The central workflow is:

```text
PySpark code
    ↓
predict logical behavior
    ↓
predict physical requirements
    ↓
inspect explain output
    ↓
identify operators
    ↓
explain why Spark chose them
    ↓
execute when AQE runtime evidence is needed
    ↓
inspect again
```

For every important pipeline, ask:

```text
1. What result and grain does the pipeline require?
2. What logical operations are being requested?
3. What should analysis need to resolve?
4. What could Catalyst simplify or rearrange?
5. What physical requirements exist?
6. Which physical operators should satisfy them?
7. Where must data be redistributed?
8. Where must data be ordered?
9. Which join strategy should Spark prefer, and why?
10. Is AQE allowed to revise the initial strategy?
```

The goal is not to memorize plan strings. The goal is to form a defensible prediction, compare it with Spark's evidence, and explain any disagreement.

Phase 4 already established:

```text
jobs
stages
tasks
partitions
narrow / wide dependencies
shuffles
stage boundaries
```

Use those concepts when an `Exchange` appears, but do not turn this guide back into a Phase 4 execution-model lesson.

This guide also intentionally avoids deep:

- Parquet and file-layout engineering;
- partition-pruning experiments;
- small-file analysis;
- skew remediation;
- caching strategy;
- executor sizing;
- Spark UI diagnosis.

Those belong primarily to Phases 6–8.

The applied section does **not** perform the formal Phase 5 mastery gate. It provides a fresh pipeline for independent reasoning and stops before phase completion.

---

[Back to Table of Contents](#toc)

---

<a id="conventions"></a>
## Conventions

- Use **PySpark 4.2.0**.
- Use **single quotes** for Python strings.
- Prefer `from pyspark.sql import functions as F`.
- Include inline comments explaining both **what** important code does and **why** the observation matters.
- Use one consistent retail/data-engineering dataset through most experiments.
- Keep datasets small enough that plan reasoning stays understandable.
- Preserve the intended grain of every DataFrame before interpreting its plan.
- Read physical plan trees from the **leaves upward**: source → downstream operators → final result.
- Use `df.explain('extended')` when studying parsed, analyzed, optimized, and physical plans together.
- Use `df.explain('formatted')` when identifying physical operators and their details.
- Treat `explain()` as inspection. It does not mean the full query result has been computed.
- Keep AQE enabled only for experiments that require runtime adaptation. Disable it when a stable static plan is the teaching target.
- Treat configuration changes and hints in this guide as **experiment controls**, not automatic production recommendations.
- Explain every `Exchange` by the downstream **distribution requirement** it satisfies.
- Explain every `Sort` by the downstream **ordering requirement** it satisfies.
- Do not claim that every join shuffles both sides.
- Do not claim that broadcast joins are always better.
- Do not infer runtime Job/Stage/Task/Partition IDs from expression IDs or formatted-plan node numbers.
- When discussing conceptual execution units, use 0-based indexing: `Job 0 → Stage 0 → Task 0 → Partition 0`.
- When Spark prints actual runtime IDs, preserve Spark's reported IDs.
- Do not mark Phase 5 complete until the mastery gate is explicitly requested and passed.

### Core operator vocabulary

You should become comfortable recognizing at least:

```text
Scan
Filter
Project
HashAggregate
Exchange
Sort
BroadcastHashJoin
SortMergeJoin
```

You will also encounter supporting operators such as:

```text
BroadcastExchange
AdaptiveSparkPlan
AQEShuffleRead
```

The supporting operators are useful because they help explain why the required operators appear.

---

[Back to Table of Contents](#toc)

---

<a id="practice-dataset"></a>
# Practice Dataset

Use one deterministic retail dataset for the static-plan experiments.

The data model is intentionally simple:

```text
sales
- sale_id
- store_id
- product_id
- order_status
- quantity
- unit_price

stores
- store_id
- store_name
- province
```

The main grains are:

```text
sales_df
= one row per sale line

stores_df
= one row per store_id
```

This lets us reason about filters, projections, grouped aggregation, and a fact-to-dimension join without introducing unrelated business complexity.

## Start one controlled Spark session

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


spark = (
    SparkSession.builder
    .appName('phase_05_query_plan_practice')
    # Four local threads retain the Phase 4 task model without pretending this
    # laptop is a real multi-node Spark cluster.
    .master('local[4]')
    # A small fixed shuffle target makes static plans easier to reason about.
    .config('spark.sql.shuffle.partitions', '4')
    .getOrCreate()
)
```

## Create the retail fact DataFrame

```python
sales_rows = [
    (0, 'S01', 'P001', 'COMPLETED', 2, 10.00),
    (1, 'S02', 'P002', 'COMPLETED', 1, 20.00),
    (2, 'S03', 'P003', 'CANCELLED', 4, 5.00),
    (3, 'S01', 'P002', 'COMPLETED', 3, 20.00),
    (4, 'S02', 'P001', 'RETURNED', 1, 10.00),
    (5, 'S03', 'P004', 'COMPLETED', 5, 8.00),
    (6, 'S01', 'P003', 'COMPLETED', 2, 5.00),
    (7, 'S02', 'P004', 'COMPLETED', 2, 8.00),
    (8, 'S03', 'P001', 'COMPLETED', 6, 10.00),
    (9, 'S01', 'P004', 'CANCELLED', 1, 8.00),
    (10, 'S02', 'P003', 'COMPLETED', 7, 5.00),
    (11, 'S03', 'P002', 'COMPLETED', 2, 20.00),
]


sales_schema = StructType([
    StructField('sale_id', IntegerType(), False),
    StructField('store_id', StringType(), False),
    StructField('product_id', StringType(), False),
    StructField('order_status', StringType(), False),
    StructField('quantity', IntegerType(), False),
    StructField('unit_price_raw', StringType(), False),
])


sales_df = (
    spark.createDataFrame(
        [
            (
                sale_id,
                store_id,
                product_id,
                order_status,
                quantity,
                str(unit_price),
            )
            for (
                sale_id,
                store_id,
                product_id,
                order_status,
                quantity,
                unit_price,
            ) in sales_rows
        ],
        sales_schema,
    )
    # Keep money typed as decimal while leaving the small seed convenient to read.
    .withColumn(
        'unit_price',
        F.col('unit_price_raw').cast(DecimalType(12, 2)),
    )
    .drop('unit_price_raw')
)
```

## Create the store dimension

```python
stores_schema = StructType([
    StructField('store_id', StringType(), False),
    StructField('store_name', StringType(), False),
    StructField('province', StringType(), False),
])


stores_df = spark.createDataFrame(
    [
        ('S01', 'Toronto Central', 'ON'),
        ('S02', 'Mississauga West', 'ON'),
        ('S03', 'Vancouver Downtown', 'BC'),
    ],
    stores_schema,
)
```

## Create one reusable enriched relation

```python
# Project a derived business measure once so later experiments can focus on plan
# behavior instead of repeatedly redefining the same expression.
sales_enriched_df = sales_df.withColumn(
    'gross_sales',
    F.col('quantity') * F.col('unit_price'),
)
```

Keep `spark`, `sales_df`, `sales_enriched_df`, and `stores_df` available for Experiments 1–8 and 11–12.

---

[Back to Table of Contents](#toc)

---

<a id="experiment-1-predict-the-four-plan-layers"></a>
# Experiment 1 — Predict the Four Plan Layers

## Goal

Connect one simple DataFrame pipeline to:

```text
parsed logical plan
analyzed logical plan
optimized logical plan
physical plan
```

## Pipeline

```python
plan_lifecycle_df = (
    sales_enriched_df
    .filter(F.col('order_status') == 'COMPLETED')
    .select(
        'store_id',
        'product_id',
        'quantity',
        'gross_sales',
    )
)
```

Do **not** inspect the plan yet.

## Predict first

Write down your prediction using this template:

```text
LOGICAL INTENT
- ...

ANALYSIS MUST RESOLVE
- ...

CATALYST MAY
- ...

PHYSICAL REQUIREMENTS
- ...

EXPECTED OPERATOR FAMILY
- ...
```

A defensible prediction should identify:

```text
filter completed rows
        ↓
project requested expressions
```

At the physical level, there is no grouped key, global order, explicit repartition, or ordinary shuffle join, so there is no obvious reason to predict an `Exchange` from this pipeline.

## Inspect

```python
plan_lifecycle_df.explain('extended')
```

Locate all four headings:

```text
== Parsed Logical Plan ==
== Analyzed Logical Plan ==
== Optimized Logical Plan ==
== Physical Plan ==
```

## Explain the differences

Answer:

1. Which names or expressions look unresolved in the parsed plan?
2. What changed in the analyzed plan?
3. What type information is now visible?
4. Did Catalyst collapse, remove, or rearrange anything?
5. Which physical operators implement the final query?
6. Why is there no obvious grouped-data `Exchange` in this example?

### Important distinction

If the analyzed plan contains identifiers such as:

```text
store_id#12
```

`#12` is an internal expression/attribute identifier.

It is **not**:

```text
Stage 12
Task 12
Partition 12
```

---

[Back to Table of Contents](#toc)

---

<a id="experiment-2-compare-explain-modes"></a>
# Experiment 2 — Compare `explain()` Modes

## Goal

Know which explain mode to use for which question.

Reuse:

```python
plan_lifecycle_df
```

## 2.1 Default physical plan

Predict what this should show:

```python
plan_lifecycle_df.explain()
```

Question:

> Does default `explain()` show all four logical/physical plan layers, or only the physical plan?

## 2.2 Extended plan lifecycle

```python
plan_lifecycle_df.explain('extended')
```

Question:

> Which mode is best when you want to compare parsed, analyzed, optimized, and physical plans in one output?

## 2.3 Formatted physical plan

```python
plan_lifecycle_df.explain('formatted')
```

Use this when you want:

```text
compact physical operator tree
+
numbered node details
```

## Interpret formatted node numbers correctly

If formatted output shows:

```text
(1) Scan
(2) Filter
(3) Project
```

those numbers are presentation labels for physical-plan nodes.

They are not:

```text
Job 1
Stage 2
Task 3
```

## Optional recognition only

You may also inspect:

```python
plan_lifecycle_df.explain('cost')
plan_lifecycle_df.explain('codegen')
```

Do not make these the main Phase 5 workflow.

The required tools remain:

```python
df.explain()
df.explain('extended')
df.explain('formatted')
```

## Decision rule

Use:

```text
Need all four planning layers?
    -> extended

Need a compact physical tree and detailed operator fields?
    -> formatted

Need only a quick physical strategy check?
    -> default
```

---

[Back to Table of Contents](#toc)

---

<a id="experiment-3-observe-catalyst-rewriting-the-logical-plan"></a>
# Experiment 3 — Observe Catalyst Rewriting the Logical Plan

## Goal

See that source-code shape does not have to survive unchanged into the optimized logical plan.

## Build intentionally redundant logical work

```python
catalyst_df = (
    sales_enriched_df
    .select(
        'sale_id',
        'store_id',
        'product_id',
        'order_status',
        'quantity',
        'gross_sales',
    )
    .filter(F.col('order_status') == 'COMPLETED')
    .select(
        'store_id',
        'quantity',
        'gross_sales',
        # A constant expression gives Catalyst an easy simplification target.
        (F.lit(2) + F.lit(3)).alias('constant_five'),
    )
    .select(
        'store_id',
        'gross_sales',
        'constant_five',
    )
)
```

## Predict before inspection

Do not predict one permanent plan node per Python call.

Instead predict:

```text
Analyzed plan
- may preserve more of the intermediate logical structure

Optimized plan
- may collapse redundant projections
- may simplify constant expressions
- must preserve result semantics
```

## Inspect

```python
catalyst_df.explain('extended')
```

Compare only:

```text
Analyzed Logical Plan
vs.
Optimized Logical Plan
```

## Questions

1. Did every `select()` survive as a separate optimized logical node?
2. Was `2 + 3` simplified?
3. Did Catalyst preserve the final requested schema and values?
4. Why is counting PySpark method calls a poor way to estimate physical work?

## Engineering lesson

Catalyst can optimize a structured query because Spark understands relational expressions such as:

```text
projection
filter
aggregation
join
ordering
```

The point is not to memorize every Catalyst rule.

The useful professional understanding is:

> **Spark can rewrite equivalent structured logic before choosing the physical strategy.**

---

[Back to Table of Contents](#toc)

---

<a id="experiment-4-recognize-scan-filter-and-project"></a>
# Experiment 4 — Recognize `Scan`, `Filter`, and `Project`

## Goal

Read a simple physical plan from the leaves upward.

## Pipeline

```python
scan_filter_project_df = (
    sales_enriched_df
    .filter(
        (F.col('order_status') == 'COMPLETED')
        & (F.col('quantity') >= 2)
    )
    .select(
        'sale_id',
        'store_id',
        'product_id',
        'gross_sales',
    )
)
```

## Predict

Expected physical shape:

```text
Scan
  ↓
Filter
  ↓
Project
```

The exact source-scan name can vary. With an in-memory DataFrame created through `createDataFrame()`, you may see a source operator such as `ExistingRDD` rather than a file-specific scan.

## Inspect

```python
scan_filter_project_df.explain('formatted')
```

Read **bottom-up**.

### `Scan`

Ask:

```text
Where do source rows enter this physical plan?
What columns are available from the source operator?
```

### `Filter`

Ask:

```text
What predicate removes rows?
Which source columns are required to evaluate it?
```

### `Project`

Ask:

```text
Which columns or computed expressions are emitted downstream?
```

`Project` does not mean only:

```text
drop unused columns
```

It can also compute expressions.

For example:

```python
sales_df.select(
    'sale_id',
    (F.col('quantity') * F.col('unit_price')).alias('gross_sales'),
)
```

contains projection work that **creates** `gross_sales`.

## Explain the plan in one paragraph

Practice saying something like:

```text
Spark scans the source rows, applies the completed-and-quantity predicate, and
projects the requested identifiers plus the derived gross_sales expression. No
operator in this pipeline requires rows with matching keys to be colocated or
globally ordered, so there is no obvious redistribution requirement.
```

Do not merely list operator names.

---

[Back to Table of Contents](#toc)

---

<a id="experiment-5-explain-hashaggregate-and-exchange"></a>
# Experiment 5 — Explain `HashAggregate` and `Exchange`

## Goal

Connect Phase 4's conceptual shuffle prediction to the concrete physical operators Spark uses for grouped aggregation.

## Make the static plan stable

```python
# AQE is disabled only for this static-plan experiment so runtime adaptation does
# not obscure the initial physical strategy we are trying to learn.
spark.conf.set('spark.sql.adaptive.enabled', 'false')
spark.conf.set('spark.sql.shuffle.partitions', '4')
```

## Build a grouped sales result

```python
store_sales_df = (
    sales_enriched_df
    .filter(F.col('order_status') == 'COMPLETED')
    .groupBy('store_id')
    .agg(
        F.sum('gross_sales').alias('gross_sales'),
        F.sum('quantity').alias('units'),
    )
)
```

Final grain:

```text
one row per store_id
```

## Predict before inspection

A common physical shape is:

```text
Scan
  ↓
Filter
  ↓
HashAggregate       partial
  ↓
Exchange            hashpartitioning(store_id, ...)
  ↓
HashAggregate       final
```

### Why two aggregation operators?

The upstream aggregate can compute **partial aggregation states** within each current partition.

Then Spark redistributes those smaller states by `store_id`.

The downstream aggregate combines all partial states for each store into the final result.

### Why the `Exchange`?

Because the final result requires:

```text
all partial states for S01 together
all partial states for S02 together
all partial states for S03 together
```

That is a distribution requirement.

## Inspect

```python
store_sales_df.explain('formatted')
```

Look for:

```text
HashAggregate
Exchange hashpartitioning(store_id, ...)
HashAggregate
```

Exact expressions and IDs can vary.

## Bridge to Phase 4

Phase 4 prediction:

```text
groupBy('store_id')
    ↓
wide dependency
    ↓
shuffle
    ↓
stage boundary
```

Phase 5 physical evidence:

```text
Exchange hashpartitioning(store_id, ...)
```

The physical plan does not replace the Phase 4 model. It gives you concrete evidence for it.

## Required explanation

Do not stop at:

> There is an `Exchange` because `groupBy` shuffles.

Prefer:

> The final store-level aggregate requires all partial states for the same `store_id` to reach a compatible downstream partition. Spark inserts a hash-partitioning `Exchange` to satisfy that distribution requirement, then the downstream `HashAggregate` finishes each store total.

That is operator-level reasoning.

---

[Back to Table of Contents](#toc)

---

<a id="experiment-6-explain-a-broadcasthashjoin"></a>
# Experiment 6 — Explain a `BroadcastHashJoin`

## Goal

Understand why a small dimension can become the broadcast/build side of an equi-join.

## Controlled join

```python
broadcast_join_df = sales_enriched_df.join(
    # Explicit broadcast is used here to make the experiment deterministic.
    F.broadcast(stores_df),
    on='store_id',
    how='inner',
)
```

The input grains are:

```text
sales_enriched_df
= one row per sale line

stores_df
= one row per store_id
```

Because `stores_df` has one row per join key, the join should enrich sale lines without multiplying them.

## Predict before inspection

Expected physical concepts:

```text
small stores side
    ↓
BroadcastExchange
    ↓
build in-memory hash relation

sales side
    ↓
stream / probe rows

both
    ↓
BroadcastHashJoin
```

## Inspect

```python
broadcast_join_df.explain('formatted')
```

Locate:

```text
BroadcastExchange
BroadcastHashJoin
```

Then inspect the join detail for the build side.

Depending on the plan, Spark may show:

```text
BuildLeft
```

or:

```text
BuildRight
```

Do not assume the build side from visual position alone.

## Explain why Spark can avoid a two-sided shuffle-and-sort join preparation

A broadcast hash join replicates the build-side relation so the streamed side can probe a local hash table.

The important comparison is:

```text
BroadcastHashJoin
- replicate small build side
- streamed side probes local hash relation
- no need to hash-repartition and sort both inputs for the join
```

Do **not** turn this into:

```text
broadcast = always better
```

Broadcasting a relation that is too large can be a bad physical choice. Phase 7 studies deliberate performance engineering.

## Optional correctness check

Because this teaching result is tiny:

```python
broadcast_rows = broadcast_join_df.collect()

assert len(broadcast_rows) == len(sales_rows)
```

The row-count preservation is a **grain/correctness** check, not a performance proof.

---

[Back to Table of Contents](#toc)

---

<a id="experiment-7-explain-a-sortmergejoin"></a>
# Experiment 7 — Explain a `SortMergeJoin`

## Goal

Understand the distribution and ordering requirements commonly surrounding a sort-merge equi-join.

## Control the static planner

```python
# Disable automatic broadcast so this experiment can study a non-broadcast join.
# This is a teaching control, not a blanket production recommendation.
spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '-1')
spark.conf.set('spark.sql.adaptive.enabled', 'false')
```

## Request a merge join

```python
sort_merge_join_df = (
    sales_enriched_df
    .hint('merge')
    .join(
        stores_df.hint('merge'),
        on='store_id',
        how='inner',
    )
)
```

## Predict before inspection

For a typical sort-merge equi-join, predict:

```text
left input
    ↓
Exchange hashpartitioning(store_id, ...)
    ↓
Sort store_id

right input
    ↓
Exchange hashpartitioning(store_id, ...)
    ↓
Sort store_id

then
    ↓
SortMergeJoin
```

## Why both `Exchange` and `Sort`?

They solve different requirements.

```text
Exchange
= satisfy distribution requirement
= matching join keys must reach compatible partitions

Sort
= satisfy ordering requirement
= rows within those partitions must be ordered as required by merge processing
```

Do not describe `Exchange` and `Sort` as two names for the same expensive thing.

## Inspect

```python
sort_merge_join_df.explain('formatted')
```

Locate:

```text
SortMergeJoin
Exchange
Sort
```

## Explain the physical strategy

A good explanation should include causality:

```text
Spark uses SortMergeJoin for the equi-join under the controlled non-broadcast
planning conditions. Each side must first satisfy compatible hash distribution
on store_id, so Spark inserts Exchanges. SortMergeJoin also requires ordered
join-key input within those partitions, so Sort operators appear before the
merge join.
```

That explanation is stronger than:

> There are two shuffles and two sorts.

## Resetting later

Do not forget that disabling automatic broadcast changes later planning behavior.

The AQE experiments will set the relevant thresholds explicitly instead of silently depending on the current session state.

---

[Back to Table of Contents](#toc)

---

<a id="experiment-8-separate-exchange-from-sort"></a>
# Experiment 8 — Separate `Exchange` from `Sort`

## Goal

Develop the habit of asking what requirement each physical operator satisfies.

Reuse the grouped result:

```python
store_sales_df
```

Then add a global order:

```python
ordered_store_sales_df = store_sales_df.orderBy(
    F.col('gross_sales').desc(),
    F.col('store_id').asc(),
)
```

## Predict

The grouped aggregation already required redistribution by `store_id`.

Global ordering creates another physical requirement:

```text
final output must obey one global ordering
```

That can introduce additional exchange/order work.

## Inspect

```python
ordered_store_sales_df.explain('formatted')
```

For every `Exchange`, ask:

> **What distribution must its downstream operator receive?**

For every `Sort`, ask:

> **What ordering must its downstream operator receive?**

## Avoid this weak explanation

```text
Exchange and Sort are expensive Spark operators.
```

That may be directionally true in some workloads, but it does not explain the plan.

Prefer:

```text
The hash-partitioning Exchange exists so partial aggregate states with the same
store_id can be combined. The later ordering work exists because the final
result has a global gross_sales-descending, store_id-ascending requirement.
```

## Scope boundary

Phase 5 identifies **why** the operators exist.

Later phases investigate:

```text
how much data moved
whether partition counts are appropriate
whether the sort is a performance bottleneck
how to redesign the query or storage
```

---

[Back to Table of Contents](#toc)

---

<a id="experiment-9-inspect-aqe-before-and-after-execution"></a>
# Experiment 9 — Inspect AQE Before and After Execution

## Goal

See the difference between an initial adaptive physical plan and the plan after runtime statistics become available.

AQE is enabled by default in Spark 4.2.0, but make the experiment state explicit.

## Configure an observable adaptive shuffle

```python
spark.conf.set('spark.sql.adaptive.enabled', 'true')
spark.conf.set('spark.sql.adaptive.coalescePartitions.enabled', 'true')

# Start with many shuffle partitions relative to this deliberately small output
# so AQE has a clear opportunity to coalesce small post-shuffle partitions.
spark.conf.set('spark.sql.shuffle.partitions', '16')
```

This setting is an experiment control. Do not derive a production rule such as:

> Always configure 16 shuffle partitions and let AQE fix it.

## Create a deterministic grouped workload

```python
aqe_sales_df = (
    spark.range(
        start=0,
        end=10000,
        step=1,
        numPartitions=4,
    )
    .select(
        F.col('id').alias('sale_id'),
        F.concat(
            F.lit('S'),
            F.lpad(
                ((F.col('id') % 8) + F.lit(1)).cast('string'),
                2,
                '0',
            ),
        ).alias('store_id'),
        ((F.col('id') % 5) + F.lit(1)).cast('int').alias('quantity'),
    )
)


aqe_store_totals_df = (
    aqe_sales_df
    .groupBy('store_id')
    .agg(
        F.sum('quantity').alias('units')
    )
)
```

Final grain:

```text
one row per store_id
8 store groups
```

## Predict before the first inspection

Logical requirement:

```text
group 10,000 rows into 8 store totals
```

Static physical requirement:

```text
partial aggregation
    ↓
shuffle by store_id
    ↓
final aggregation
```

Adaptive expectation:

```text
initial shuffle target = 16
runtime map-output sizes become available
AQE may combine small adjacent shuffle partitions
```

## Inspect before executing the query

```python
aqe_store_totals_df.explain('formatted')
```

Look for an adaptive wrapper such as:

```text
AdaptiveSparkPlan
```

and commonly:

```text
isFinalPlan=false
```

Exact printed text can vary.

Important:

> Seeing `AdaptiveSparkPlan` proves AQE owns the physical plan. It does **not** prove AQE has already changed anything.

## Execute

```python
# collect() is safe here because the final result contains only eight rows.
aqe_store_totals_rows = aqe_store_totals_df.collect()

assert len(aqe_store_totals_rows) == 8
```

Execution gives AQE runtime stage statistics that did not exist before the shuffle ran.

## Inspect again

```python
aqe_store_totals_df.explain('formatted')
```

Look for evidence such as:

```text
isFinalPlan=true
AQEShuffleRead
coalesced
Final Plan
Initial Plan
```

Exact output varies by Spark build and plan.

## Explain what changed

A good explanation is causal:

```text
The initial physical strategy planned a shuffle using the configured target.
After the upstream shuffle stage executed, AQE could observe actual map-output
sizes. Because the resulting shuffle blocks were small, AQE could replace the
initial post-shuffle read with a coalesced adaptive shuffle read, reducing the
number of tiny downstream partitions without changing query semantics.
```

## Critical distinction

```text
configured shuffle partition target
!= guaranteed final runtime partition structure under AQE
```

Deep partition-sizing decisions belong to Phases 6 and 7. Phase 5 only needs to understand the adaptive mechanism and read the resulting plan evidence.

---

[Back to Table of Contents](#toc)

---

<a id="experiment-10-observe-an-aqe-join-strategy-change"></a>
# Experiment 10 — Observe an AQE Join-Strategy Change

## Goal

Demonstrate that the initial join strategy can differ from the final adaptive strategy when runtime evidence changes what Spark knows.

## Configure static vs. adaptive broadcast thresholds deliberately

```python
spark.conf.set('spark.sql.adaptive.enabled', 'true')

# Prevent the static planner from choosing a broadcast join before runtime.
spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '-1')

# Allow AQE to broadcast a sufficiently small relation after runtime statistics
# become available.
spark.conf.set(
    'spark.sql.adaptive.autoBroadcastJoinThreshold',
    '10485760',
)

spark.conf.set('spark.sql.shuffle.partitions', '8')
```

The two thresholds represent different information moments:

```text
static autoBroadcastJoinThreshold
= planning-time decision

adaptive autoBroadcastJoinThreshold
= AQE runtime decision
```

## Build the fact side

```python
aqe_fact_df = (
    spark.range(
        start=0,
        end=5000,
        step=1,
        numPartitions=4,
    )
    .select(
        F.col('id').alias('sale_id'),
        (F.col('id') % 20).cast('long').alias('store_key'),
        ((F.col('id') % 5) + F.lit(1)).cast('int').alias('quantity'),
    )
)
```

## Build the small dimension side

```python
aqe_dimension_df = (
    spark.range(
        start=0,
        end=20,
        step=1,
        numPartitions=2,
    )
    .select(
        F.col('id').alias('store_key'),
        F.concat(
            F.lit('Store '),
            F.col('id').cast('string'),
        ).alias('store_name'),
    )
)
```

## Join

```python
aqe_join_df = aqe_fact_df.join(
    aqe_dimension_df,
    on='store_key',
    how='inner',
)
```

## Predict the initial plan

Because static broadcast is disabled, predict a non-broadcast equi-join strategy such as:

```text
SortMergeJoin
```

with distribution and ordering work on the two sides.

Do not claim the conversion has happened yet.

## Inspect before execution

```python
aqe_join_df.explain('formatted')
```

Record:

```text
initial join operator
initial Exchanges
initial Sort operators
AdaptiveSparkPlan finality state
```

## Execute

```python
# Five thousand rows are deliberately small enough for this local experiment.
aqe_join_rows = aqe_join_df.collect()

assert len(aqe_join_rows) == 5000
```

## Inspect after execution

```python
aqe_join_df.explain('formatted')
```

Look for a final-plan join operator such as:

```text
BroadcastHashJoin
```

and broadcast-stage evidence.

Depending on the exact environment, Spark may retain a different eligible strategy. The exercise is successful only if you explain what the plan actually shows rather than forcing your prediction onto the evidence.

## Explain the information change

If Spark converts the join:

```text
Before runtime
- static broadcasting was disabled
- initial plan uses a shuffle-based join

After runtime stage statistics
- AQE discovers one side is small enough for its adaptive threshold
- AQE replaces the eligible join strategy
- final plan uses BroadcastHashJoin
```

The key Phase 5 idea is:

> **AQE can revise eligible physical decisions because runtime statistics are stronger evidence than pre-execution estimates.**

## Do not overgeneralize

AQE does not mean:

```text
Spark always fixes a bad plan automatically
```

Nor does it mean:

```text
every SortMergeJoin becomes BroadcastHashJoin
```

The query, join type, runtime statistics, thresholds, and eligibility still matter.

---

[Back to Table of Contents](#toc)

---

<a id="experiment-11-explain-a-complete-retail-pipeline"></a>
# Experiment 11 — Explain a Complete Retail Pipeline

## Goal

Combine filtering, derived expressions, grouped aggregation, redistribution, a dimension join, and AQE into one coherent data-engineering plan explanation.

## Restore a sensible teaching configuration

```python
spark.conf.set('spark.sql.adaptive.enabled', 'true')
spark.conf.set('spark.sql.shuffle.partitions', '4')

# Re-enable the ordinary static broadcast threshold after the forced SMJ/AQE
# experiments so this final pipeline can naturally use the tiny store dimension.
spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '10485760')
```

## Build the pipeline

```python
worked_store_sales_df = (
    sales_enriched_df
    .filter(F.col('order_status') == 'COMPLETED')
    .groupBy('store_id')
    .agg(
        F.sum('gross_sales').alias('gross_sales'),
        F.sum('quantity').alias('units'),
    )
)


worked_df = (
    worked_store_sales_df
    .join(
        F.broadcast(stores_df),
        on='store_id',
        how='inner',
    )
    .select(
        'store_id',
        'store_name',
        'province',
        'units',
        'gross_sales',
    )
)
```

Final grain:

```text
one row per store_id
```

Because the dimension also has one row per `store_id`, this join should not multiply the grouped fact rows.

## Predict the full physical story

Before inspecting, write a causal prediction.

A strong prediction resembles:

```text
sales side
Scan
  ↓
Filter COMPLETED
  ↓
Project / derive gross_sales where needed
  ↓
partial HashAggregate by store_id
  ↓
Exchange hashpartitioning(store_id, ...)
  ↓
final HashAggregate by store_id

stores side
Scan
  ↓
BroadcastExchange

join
BroadcastHashJoin
  ↓
final Project
```

With AQE enabled, the plan may be wrapped by:

```text
AdaptiveSparkPlan
```

## Inspect all plan layers

```python
worked_df.explain('extended')
```

Answer:

1. What is the logical result?
2. What is its grain?
3. What did analysis resolve?
4. What logical rewriting did Catalyst perform?
5. Which physical operators were selected?

## Inspect physical details

```python
worked_df.explain('formatted')
```

Identify:

```text
Scan
Filter
Project
HashAggregate
Exchange
BroadcastExchange
BroadcastHashJoin
AdaptiveSparkPlan
```

## Execute

```python
worked_rows = worked_df.collect()

assert len(worked_rows) == 3
assert len({row['store_id'] for row in worked_rows}) == 3
```

## Inspect the adaptive plan again

```python
worked_df.explain('formatted')
```

## Produce a complete explanation

Your explanation should answer **why**, not only **what**.

A strong answer should cover:

```text
1. The sales filter reduces the logical input to completed rows.
2. gross_sales is a projected expression derived from quantity and unit_price.
3. Spark performs partial hash aggregation before redistribution to reduce the
   amount of aggregate state moved.
4. The Exchange hash-partitions those partial states by store_id because the
   final aggregate requires all states for one store to meet downstream.
5. The final HashAggregate produces one row per store_id.
6. The tiny stores dimension becomes the broadcast/build side.
7. BroadcastHashJoin lets the aggregated sales side probe the local broadcast
   hash relation instead of preparing both inputs through shuffle + sort.
8. The final Project emits the analytical schema.
9. AQE may revise eligible shuffle/runtime details after execution statistics
   become available without changing the result semantics.
```

### Correctness remains part of plan reasoning

A fast plan that accidentally multiplies rows is still wrong.

Always keep:

```text
query-plan reasoning
+
grain reasoning
+
correctness reasoning
```

connected.

---

[Back to Table of Contents](#toc)

---

<a id="experiment-12-query-plan-prediction-drills"></a>
# Experiment 12 — Query-Plan Prediction Drills

## Goal

Practice predicting operator families before you see Spark's answer.

For each drill:

1. state the output grain;
2. predict logical operations;
3. predict physical requirements;
4. name likely operator families;
5. inspect with `explain('formatted')`;
6. explain every mismatch between prediction and evidence.

Do not simply run the cell first.

## Drill A — Filter and derived projection

```python
drill_a_df = (
    sales_df
    .filter(F.col('quantity') >= 3)
    .select(
        'sale_id',
        'store_id',
        (
            F.col('quantity') * F.col('unit_price')
        ).alias('gross_sales'),
    )
)
```

Predict whether you expect:

```text
Scan
Filter
Project
Exchange
Sort
```

Then inspect:

```python
drill_a_df.explain('formatted')
```

## Drill B — Group by product

```python
drill_b_df = (
    sales_enriched_df
    .groupBy('product_id')
    .agg(
        F.sum('gross_sales').alias('gross_sales')
    )
)
```

Predict:

```text
partial aggregate?
Exchange?
final aggregate?
```

Then inspect:

```python
drill_b_df.explain('formatted')
```

## Drill C — Global ranking output

```python
drill_c_df = (
    drill_b_df
    .orderBy(
        F.col('gross_sales').desc(),
        F.col('product_id').asc(),
    )
)
```

Explain separately:

```text
why grouped aggregation needs distribution
why global order needs ordering
```

Then inspect:

```python
drill_c_df.explain('formatted')
```

## Drill D — Broadcast dimension enrichment

```python
drill_d_df = sales_enriched_df.join(
    F.broadcast(stores_df),
    on='store_id',
    how='left',
)
```

Before inspecting, answer:

```text
Which side do you expect to be built/broadcast?
Why is this a reasonable dimension-side choice?
Should the sale-line grain be preserved if store_id is unique in stores_df?
```

Then inspect:

```python
drill_d_df.explain('formatted')
```

## Drill E — Forced non-broadcast equi-join

```python
spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '-1')
spark.conf.set('spark.sql.adaptive.enabled', 'false')


drill_e_df = (
    sales_enriched_df
    .hint('merge')
    .join(
        stores_df.hint('merge'),
        on='store_id',
        how='inner',
    )
)
```

Predict:

```text
Exchange on left?
Exchange on right?
Sort on left?
Sort on right?
SortMergeJoin?
```

Then inspect:

```python
drill_e_df.explain('formatted')
```

## Drill F — Catalyst source-code simplification

```python
drill_f_df = (
    sales_enriched_df
    .select(
        'sale_id',
        'store_id',
        'order_status',
        'gross_sales',
    )
    .select(
        'store_id',
        'order_status',
        'gross_sales',
    )
    .filter(F.col('order_status') == 'COMPLETED')
    .select(
        'store_id',
        'gross_sales',
    )
)
```

Predict whether the optimized logical plan must preserve three separate `Project` nodes simply because the Python code contained three `select()` calls.

Then inspect:

```python
drill_f_df.explain('extended')
```

## Drill G — AQE reasoning without guessing the final answer

```python
spark.conf.set('spark.sql.adaptive.enabled', 'true')
spark.conf.set('spark.sql.shuffle.partitions', '12')


drill_g_df = (
    spark.range(
        start=0,
        end=6000,
        step=1,
        numPartitions=4,
    )
    .withColumn(
        'group_id',
        (F.col('id') % 6).cast('int'),
    )
    .groupBy('group_id')
    .count()
)
```

Before execution, predict only what you can defend:

```text
- grouped aggregation needs redistribution
- AQE owns the physical plan
- runtime statistics do not exist yet
- AQE may revise post-shuffle partition structure
```

Do **not** predict an exact final adaptive partition count unless the experiment actually establishes it.

Inspect, execute, inspect again:

```python
drill_g_df.explain('formatted')

drill_g_rows = drill_g_df.collect()
assert len(drill_g_rows) == 6

drill_g_df.explain('formatted')
```

This is the Phase 5 discipline:

```text
known evidence -> make claim
unknown runtime detail -> inspect instead of inventing
```

---

[Back to Table of Contents](#toc)

---

<a id="applied-phase-5-project"></a>
# Applied Phase 5 Project

## Goal

Analyze a fresh retail pipeline without relying on a worked explanation first.

This is **applied practice**, not the formal mastery gate.

You should use the same professional reasoning sequence you would use when reviewing a real data-engineering transformation:

```text
business requirement
    ↓
output grain
    ↓
PySpark transformation
    ↓
predict logical plan
    ↓
predict physical requirements
    ↓
inspect Spark plan
    ↓
explain operator choices
    ↓
execute if AQE evidence is required
    ↓
reconcile prediction with observed plan
```

### Business requirement

Produce one row per province containing completed retail sales totals.

Required final columns:

```text
province
gross_sales
units
store_count
```

Business semantics:

```text
1. Keep only COMPLETED sale lines.
2. Join sale lines to the store dimension using store_id.
3. Aggregate to province grain.
4. Sum gross sales.
5. Sum units.
6. Count distinct contributing stores.
7. Order final provinces by gross_sales descending, province ascending.
```

### Correctness expectations

Before discussing query plans, be able to state:

```text
sales grain before join
= one row per sale line

stores grain
= one row per store_id

join grain
= one row per matched sale line if store_id is unique in stores_df

final grain
= one row per province
```

If `stores_df` accidentally contains duplicate `store_id` values, the join can multiply sale lines and corrupt every downstream aggregate. Query-plan fluency never replaces grain validation.

---

[Back to Table of Contents](#toc)

---

<a id="applied-task-part-1"></a>
# Applied Task — Part 1

Implement the pipeline yourself using the existing:

```text
sales_enriched_df
stores_df
```

Do not copy the worked pipeline from Experiment 11 because the aggregation grain is different here.

## Requirements

Create:

```python
province_sales_df
```

with final columns:

```text
province
gross_sales
units
store_count
```

and final ordering:

```text
gross_sales DESC
province ASC
```

Use DataFrame API expressions.

## Before calling `explain()`

Write a prediction containing all of the following.

### 1. Logical intent

```text
Filter
Join
Aggregate
Order
Project if applicable
```

Explain why each is logically necessary.

### 2. Output grain

State exactly what one row of `province_sales_df` represents.

### 3. Join strategy prediction

Do **not** write only:

```text
stores_df is small, so broadcast
```

Instead explain:

```text
- which side is dimension-like;
- whether the join is an equi-join;
- whether a broadcast hint or threshold is influencing planning;
- which side would become the build side if BroadcastHashJoin is chosen;
- why this would avoid two-sided sort-merge preparation.
```

### 4. Aggregation prediction

Predict whether grouped aggregation should involve:

```text
HashAggregate
Exchange
HashAggregate
```

Then explain the `Exchange` through the downstream province grouping requirement.

### 5. Ordering prediction

Explain why the final `orderBy()` has an ordering requirement distinct from the province aggregation's distribution requirement.

### 6. AQE prediction

If AQE is enabled, state only what is justified before execution:

```text
AQE may revise eligible physical decisions after runtime statistics arrive.
```

Do not invent a specific runtime change before observing it.

## Inspect the plan

Run:

```python
province_sales_df.explain('extended')
province_sales_df.explain('formatted')
```

Record:

```text
Parsed logical plan:
Analyzed logical plan:
Optimized logical plan:
Physical operator sequence:
Join strategy:
Join build side if applicable:
Exchange purpose(s):
Sort purpose(s):
AQE state:
```

## Execute only after the prediction is written

Because the result is tiny:

```python
province_rows = province_sales_df.collect()
```

Then inspect again if AQE is enabled:

```python
province_sales_df.explain('formatted')
```

## Required final explanation

Write a concise paragraph answering:

> **Why did Spark choose this resulting physical execution strategy?**

Your answer must connect:

```text
business grain
logical operations
Catalyst plan evolution
physical operator requirements
join strategy
redistribution
ordering
AQE evidence if any
```

Do not evaluate the plan solely by whether it contains the operator names you predicted.

A prediction mismatch is useful if you can explain the assumption that was wrong.

---

[Back to Table of Contents](#toc)

---

<a id="after-part-1"></a>
# After Part 1

After implementing and explaining `province_sales_df`, preserve the following observations for review:

```text
1. Your prediction before inspection.
2. The relevant extended-plan observations.
3. The physical operator sequence from formatted explain.
4. Any differences between predicted and observed strategy.
5. Why each Exchange exists.
6. Why each Sort exists.
7. Why the join algorithm was chosen.
8. Which side was built/broadcast if applicable.
9. Whether AQE produced a final-plan change.
10. Whether the final row grain was correct.
```

The next step after this guide is the **Phase 5 SOLUTION notebook**, which should implement these experiments in executable notebook form while preserving the same sequence and reasoning prompts.

The **STARTER notebook** should later be derived directly from that solution notebook by retaining setup/data cells and removing worked solution code.

Do not update `ROADMAP.md`, mark Phase 5 complete, or perform the formal mastery gate yet.

---

[Back to Table of Contents](#toc)
