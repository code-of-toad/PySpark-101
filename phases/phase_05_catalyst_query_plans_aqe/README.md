# Phase 5 — Catalyst, Query Plans & AQE

<a id="toc"></a>
## Table of Contents

- [Objective](#objective)
- [Phase Mental Model](#phase-mental-model)
- [1. From PySpark Code to an Executable Plan](#1-from-pyspark-code-to-an-executable-plan)
- [2. Parsed Logical Plan](#2-parsed-logical-plan)
- [3. Analyzed Logical Plan](#3-analyzed-logical-plan)
- [4. Optimized Logical Plan and Catalyst](#4-optimized-logical-plan-and-catalyst)
- [5. Physical Plan](#5-physical-plan)
- [6. Reading `explain()` Output](#6-reading-explain-output)
- [7. Core Physical Operators](#7-core-physical-operators)
- [8. Reading Aggregation Plans](#8-reading-aggregation-plans)
- [9. Reading Join Plans](#9-reading-join-plans)
- [10. `Exchange`, `Sort`, and the Phase 4 Execution Model](#10-exchange-sort-and-the-phase-4-execution-model)
- [11. Adaptive Query Execution](#11-adaptive-query-execution)
- [12. Prediction-Before-Inspection Workflow](#12-prediction-before-inspection-workflow)
- [13. Worked Query-Plan Reasoning](#13-worked-query-plan-reasoning)
- [14. Engineering Decision Rules](#14-engineering-decision-rules)
- [15. Common Failure Modes](#15-common-failure-modes)
- [16. Phase 5 Mastery Reference](#16-phase-5-mastery-reference)

---

<a id="objective"></a>
## Objective

Stop treating Spark execution as a black box.

Phase 5 focuses on how Spark turns structured PySpark work into query plans and executable physical operators.

The required concepts are:

- parsed logical plan;
- analyzed logical plan;
- optimized logical plan;
- physical plan;
- Catalyst optimizer;
- Adaptive Query Execution (AQE);
- `df.explain()`;
- `df.explain('formatted')`.

You must also learn to recognize and explain operators such as:

- `Scan`;
- `Filter`;
- `Project`;
- `HashAggregate`;
- `Exchange`;
- `Sort`;
- `BroadcastHashJoin`;
- `SortMergeJoin`.

Examples target **PySpark 4.2.0**.

The central Phase 5 question is:

> **Why did Spark choose this physical execution strategy for this pipeline?**

Phase 4 already established jobs, stages, tasks, partitions, narrow/wide dependencies, shuffles, and stage boundaries. Those concepts remain useful here, especially when a physical plan contains `Exchange`, but they are supporting knowledge rather than the main subject.

Phase 5 intentionally does **not** expand into:

- deep Parquet/file-layout engineering;
- partition pruning and file-size strategy;
- extensive shuffle tuning;
- skew-remediation techniques;
- caching strategy;
- executor sizing or memory tuning;
- Spark UI diagnosis.

Those topics belong primarily to Phases 6, 7, and 8.

This document is lecture/reference material only. The Phase 5 mastery gate is intentionally not performed here.

---

[Back to Table of Contents](#toc)

---

<a id="phase-mental-model"></a>
## Phase Mental Model

The central model is:

```text
PySpark DataFrame / Spark SQL expression
                │
                ↓
initial unresolved logical representation
                │
                ↓
parsed logical plan
                │
                │ resolve names, relations, functions, and types
                ↓
analyzed logical plan
                │
                │ apply semantics-preserving optimization rules
                ↓
optimized logical plan
                │
                │ choose executable operator strategies
                ↓
physical plan
                │
                │ AQE may revise parts using runtime statistics
                ↓
final runtime execution strategy
```

A useful distinction is:

```text
Logical plan
= WHAT result is required

Physical plan
= HOW Spark intends to produce that result
```

For example:

```python
store_sales_df = (
    sales_df
    .filter(F.col('order_status') == 'COMPLETED')
    .groupBy('store_id')
    .agg(
        F.sum('net_sales').alias('net_sales')
    )
)
```

At the logical level, Spark needs to:

```text
filter completed rows
        ↓
group by store_id
        ↓
sum net_sales
```

At the physical level, Spark must decide details such as:

```text
How is the source scanned?
Where is the filter executed?
Which aggregation implementation is used?
Must rows be redistributed by store_id?
Where does that redistribution appear?
Does AQE change the planned execution after runtime statistics arrive?
```

The most important Phase 5 habit is:

> **Predict the plan shape first, then inspect Spark's plan, then explain any difference.**

---

[Back to Table of Contents](#toc)

---

<a id="1-from-pyspark-code-to-an-executable-plan"></a>
## 1. From PySpark Code to an Executable Plan

Spark DataFrames are structured query descriptions, not ordinary Python collections that execute one method at a time.

Given:

```python
result_df = (
    sales_df
    .filter(F.col('quantity') > 0)
    .select(
        'store_id',
        'product_id',
        'quantity',
        'unit_price',
    )
    .withColumn(
        'gross_sales',
        F.col('quantity') * F.col('unit_price'),
    )
)
```

Spark receives enough structured information to reason about:

- named columns;
- expressions;
- data types;
- filters;
- projections;
- aggregations;
- joins;
- ordering requirements;
- distribution requirements.

That structure lets Spark analyze and transform the requested computation before choosing executable operators.

### SQL and DataFrame API share the same structured engine

For SQL text, Spark parses the SQL statement into an unresolved logical plan.

For the DataFrame API, the client constructs an equivalent logical query representation programmatically rather than requiring Spark to parse SQL text first.

In both cases, Spark ultimately analyzes, optimizes, and physically plans structured relational work.

That is why Phase 3's core principle matters here:

```text
Spark SQL
and
PySpark DataFrame API
        ↓
structured logical plans
        ↓
Spark SQL execution engine
```

### The plan is a tree

Query plans are trees of operators.

A simple logical shape may look like:

```text
Aggregate
   ↓
Project
   ↓
Filter
   ↓
Relation
```

The final physical tree might instead look like:

```text
HashAggregate
   ↓
Exchange
   ↓
HashAggregate
   ↓
Filter
   ↓
Scan
```

The second tree exposes execution choices that are not visible from the original PySpark alone.

### Read physical plan trees from the leaves upward

Spark prints the final output operator near the top and its inputs below it.

For execution reasoning, start near the bottom:

```text
Scan
  ↓
Filter
  ↓
partial HashAggregate
  ↓
Exchange
  ↓
final HashAggregate
```

This makes data movement easier to follow.

---

[Back to Table of Contents](#toc)

---

<a id="2-parsed-logical-plan"></a>
## 2. Parsed Logical Plan

The **parsed logical plan** is the initial unresolved description of the requested computation.

It captures the query structure before Spark has fully resolved all referenced objects.

Typical unresolved elements may include:

```text
'unresolved_column
'UnresolvedRelation
'unresolved_function
```

For a SQL query such as:

```sql
SELECT store_id, SUM(net_sales)
FROM sales
WHERE order_status = 'COMPLETED'
GROUP BY store_id
```

Spark first represents concepts such as:

```text
Project / Aggregate
Filter
UnresolvedRelation [sales]
UnresolvedAttribute [store_id]
UnresolvedAttribute [net_sales]
```

The plan expresses intent, but Spark has not yet proved that every table, column, function, and type is valid.

### What to ask when reading it

```text
What relations did the query request?
What columns and expressions were requested?
What joins, filters, projections, and aggregates exist logically?
Which names are still unresolved?
```

### What not to infer yet

Do **not** infer physical execution strategy from the parsed plan.

The parsed plan does not tell you that Spark will use:

```text
BroadcastHashJoin
SortMergeJoin
HashAggregate
Exchange
```

Those are physical decisions made later.

### Important PySpark nuance

When working through the DataFrame API, do not over-focus on the word **parsed** as though Spark must convert your Python method chain into SQL text first.

The useful Phase 5 interpretation is:

> **This is Spark's earliest unresolved logical representation of the structured query.**

---

[Back to Table of Contents](#toc)

---

<a id="3-analyzed-logical-plan"></a>
## 3. Analyzed Logical Plan

The **analyzed logical plan** is the resolved, type-aware logical plan.

During analysis, Spark determines what the query's names and expressions actually refer to.

Conceptually, the analyzer resolves:

- table/relation references;
- column references;
- aliases;
- functions;
- expression data types;
- compatible coercions where allowed;
- ambiguities and invalid references.

### Why analysis matters

Suppose the requested code contains:

```python
sales_df.select('store_identifier')
```

but the schema contains only:

```text
store_id
product_id
net_sales
```

Spark cannot simply invent a meaning for `store_identifier`.

The query fails during analysis because the requested attribute cannot be resolved.

Likewise, an ambiguous column after a join can fail analysis when Spark cannot determine which relation's column was intended.

### Expression IDs are not execution IDs

Analyzed plans commonly contain internal identifiers such as:

```text
store_id#12
net_sales#18
```

The `#12` and `#18` values identify expressions/attributes inside Spark's query representation.

They are **not**:

```text
Job 12
Stage 12
Task 12
Partition 12
```

Do not mix query-plan expression IDs with Phase 4 runtime IDs.

### Analysis does not mean optimization is finished

At this point Spark knows what the query means, but it has not yet applied the full set of logical optimization rewrites.

The progression is still:

```text
resolved meaning
      ↓
logical optimization
      ↓
physical strategy selection
```

---

[Back to Table of Contents](#toc)

---

<a id="4-optimized-logical-plan-and-catalyst"></a>
## 4. Optimized Logical Plan and Catalyst

The **optimized logical plan** is a semantics-preserving rewrite of the analyzed logical plan.

The result should mean the same thing as the original query while removing unnecessary work or arranging operations more effectively.

### Catalyst

**Catalyst** is Spark SQL's query analysis and optimization framework.

For Phase 5, the useful mental model is:

```text
logical plan tree
      ↓
analysis + optimization rules
      ↓
cleaner logical plan
      ↓
physical planning strategies
      ↓
executable physical operators
```

You do not need to memorize Catalyst's internal Scala rule classes.

You do need to recognize what Catalyst is accomplishing at the engineering level:

> **Spark is free to transform your structured query into an equivalent form before execution.**

### Common logical optimization ideas

Without turning Phase 5 into a storage/performance-tuning phase, recognize examples such as:

- constant folding;
- boolean simplification;
- removing redundant projections;
- collapsing compatible projections;
- combining filters;
- pushing filters closer to the data they constrain when semantically safe;
- pruning logically unnecessary columns;
- simplifying expressions.

Example source code:

```python
result_df = (
    sales_df
    .select(
        'store_id',
        'quantity',
        'unit_price',
        'order_status',
    )
    .filter(F.col('order_status') == 'COMPLETED')
    .select(
        'store_id',
        'quantity',
        'unit_price',
    )
)
```

The optimized logical plan does not need to preserve every intermediate `select()` exactly as written.

Spark may collapse or rearrange logical operators while preserving the requested result.

### Source code shape is not plan shape

Do not expect:

```text
one PySpark method
=
one logical node forever
```

Catalyst may rewrite the tree.

The correct question is:

> **Does the optimized plan still produce the same semantics with less unnecessary work?**

### Scope boundary: logical pruning vs. storage engineering

Seeing filters or columns moved closer to a relation is valid Phase 5 Catalyst reasoning.

Detailed questions such as:

```text
Did Parquet use row-group statistics?
Was a predicate pushed into the file reader?
Were storage partitions pruned?
Was file sizing appropriate?
```

belong primarily to Phase 6.

---

[Back to Table of Contents](#toc)

---

<a id="5-physical-plan"></a>
## 5. Physical Plan

The **physical plan** describes executable operators Spark intends to use to produce the optimized logical result.

This is where abstract relational intent becomes concrete distributed execution strategy.

Logical intent:

```text
join sales to stores on store_id
```

Possible physical implementations include:

```text
BroadcastHashJoin
SortMergeJoin
```

Logical intent:

```text
group rows by store_id and sum net_sales
```

A common physical implementation includes:

```text
HashAggregate
Exchange
HashAggregate
```

### Physical operators answer HOW

For a physical plan, ask:

```text
What is being scanned?
Where are rows filtered?
Where are columns computed or projected?
Where is aggregation performed?
Where must data move?
Where must data be sorted?
Which join algorithm was selected?
Which side of a join is the build/broadcast side?
Is AQE wrapping the plan?
```

### Physical planning is not arbitrary

Spark's choices can depend on factors such as:

- operation semantics;
- join type;
- equi-join keys;
- estimated relation sizes;
- available statistics;
- distribution requirements;
- ordering requirements;
- configured thresholds;
- explicit hints;
- AQE runtime statistics.

Phase 5 learns to identify these causes.

Later phases learn when and how to deliberately change them for production performance.

### Plans are evidence, not guarantees across every environment

Do not memorize one exact textual plan and assume every machine must print it forever.

Physical plans can differ because of:

- source type;
- statistics;
- Spark configuration;
- AQE;
- query hints;
- Spark version;
- whether inputs already satisfy distribution/order requirements.

Learn the **operator relationships**, not only one copied output.

---

[Back to Table of Contents](#toc)

---

<a id="6-reading-explain-output"></a>
## 6. Reading `explain()` Output

`DataFrame.explain()` is the main Phase 5 inspection tool.

### Default: physical plan

```python
result_df.explain()
```

The default output shows the physical plan.

Use it when the primary question is:

> **What executable strategy did Spark plan?**

### Extended: all four required plan layers

```python
result_df.explain('extended')
```

Use `extended` when studying the full planning pipeline.

It shows:

```text
== Parsed Logical Plan ==

== Analyzed Logical Plan ==

== Optimized Logical Plan ==

== Physical Plan ==
```

This is the most direct tool for learning the required Phase 5 sequence.

### Formatted: physical outline plus operator details

```python
result_df.explain('formatted')
```

Formatted mode separates the physical plan into:

1. a compact operator tree;
2. numbered node details.

Conceptually:

```text
== Physical Plan ==
* HashAggregate (4)
+- Exchange (3)
   +- * HashAggregate (2)
      +- * Scan ... (1)

(1) Scan ...
Output: ...

(2) HashAggregate ...
Input: ...
...
```

This is often easier than the default plan when you want to answer:

```text
What does this node consume?
What does it output?
Which expressions belong to it?
```

### Formatted node numbers are not runtime execution IDs

Numbers such as:

```text
(1)
(2)
(3)
```

in formatted explain output identify physical-plan nodes for that presentation.

They are **not** Spark Job, Stage, Task, or Partition IDs.

The project's 0-based indexing convention continues to apply to conceptual execution units:

```text
Job 0 → Stage 0 → Task 0 → Partition 0
```

When Spark itself reports runtime IDs, preserve the IDs Spark reports.

### Optional modes worth recognizing

Phase 5 does not require deep use of every explain mode, but recognize:

```python
result_df.explain('cost')
result_df.explain('codegen')
```

`cost` can expose logical-plan statistics when available.

`codegen` exposes generated-code information when applicable.

They are secondary to:

```python
explain()
explain('extended')
explain('formatted')
```

### `explain()` is inspection, not a substitute for execution evidence

Printing a plan does not mean the full query result has been computed.

This matters especially for AQE because AQE uses statistics discovered while query stages actually execute.

---

[Back to Table of Contents](#toc)

---

<a id="7-core-physical-operators"></a>
## 7. Core Physical Operators

Phase 5 requires recognizing common physical operators and explaining why they exist.

### `Scan`

A **scan** reads rows from a source relation.

Depending on the source, plan text may contain names such as:

```text
Scan ExistingRDD
FileScan parquet
BatchScan
```

For Phase 5, read them as:

```text
physical source read
```

Ask:

```text
Which columns does this scan output?
Which downstream operator consumes them?
```

Do not yet turn every scan into a deep Parquet/file-layout analysis; that belongs to Phase 6.

### `Filter`

A **Filter** applies a row predicate.

PySpark:

```python
completed_sales_df = sales_df.filter(
    F.col('order_status') == 'COMPLETED'
)
```

Physical idea:

```text
Scan
  ↓
Filter order_status = COMPLETED
```

Ask:

```text
Which rows survive?
How early in the physical flow is the filter applied?
```

### `Project`

A **Project** computes or selects output expressions.

It can correspond to work such as:

```python
sales_df.select(
    'store_id',
    'product_id',
    (
        F.col('quantity') * F.col('unit_price')
    ).alias('gross_sales'),
)
```

A `Project` can therefore mean more than dropping columns. It can also compute expressions and aliases.

### `HashAggregate`

A **HashAggregate** performs aggregation using hash-based grouping state.

For grouped aggregation, Spark commonly uses two aggregation phases:

```text
HashAggregate partial
        ↓
Exchange by grouping key
        ↓
HashAggregate final
```

The first aggregate reduces data partition-locally before the shuffle.

The second combines the redistributed partial results into the final groups.

Do not assume every aggregation must always use exactly this operator, but learn this common pattern thoroughly.

### `Exchange`

An **Exchange** changes how data is distributed between physical operators.

A common shuffle exchange looks like:

```text
Exchange hashpartitioning(store_id, N)
```

Interpretation:

```text
rows must be redistributed so downstream partitions satisfy a key-based distribution requirement
```

This is the physical-plan evidence that connects directly to Phase 4 shuffle reasoning.

A broadcast uses a different exchange form such as `BroadcastExchange`, so do not treat every exchange-like node as the same kind of redistribution.

### `Sort`

A **Sort** establishes an ordering required by downstream work.

Examples include:

- global `orderBy()`;
- ordering needed before a `SortMergeJoin`;
- other operators with explicit ordering requirements.

Ask:

```text
Why does the downstream operator require this order?
Is the sort local within already partitioned data, or part of a broader global-ordering strategy?
```

### `BroadcastHashJoin`

A **BroadcastHashJoin** is commonly used for an equi-join when one side is small enough or explicitly hinted for broadcast.

Conceptually:

```text
small relation
    ↓
BroadcastExchange
    ↓
replicated to executors

large relation partitions
    ↓
probe broadcast hash table locally
    ↓
joined rows
```

A typical physical shape is:

```text
BroadcastHashJoin [...], [...], Inner, BuildRight
:- large-side plan
+- BroadcastExchange ...
   +- small-side plan
```

`BuildRight` means the right side is used to build the in-memory hash relation.

The important benefit in plan reasoning is:

> **The streamed/large side does not need the same two-sided shuffle-and-sort preparation required by a standard sort-merge join.**

### `SortMergeJoin`

A **SortMergeJoin** is a common strategy for large equi-joins when both sides must be partitioned compatibly on the join keys and sorted within those partitions.

Typical shape:

```text
SortMergeJoin [store_id], [store_id], Inner
:- Sort [store_id]
:  +- Exchange hashpartitioning(store_id, N)
:     +- left input
+- Sort [store_id]
   +- Exchange hashpartitioning(store_id, N)
      +- right input
```

Interpretation:

```text
redistribute both sides by join key
        ↓
sort each side by join key
        ↓
merge matching ordered key streams
```

If an input already satisfies the required distribution or ordering, some preparation may be unnecessary. Do not memorize the exact number of `Exchange` or `Sort` nodes independently of the input properties.

### Operator summary

| Operator | Main physical role | Key question |
|---|---|---|
| `Scan` | Read source rows | What source and columns enter the plan? |
| `Filter` | Remove rows by predicate | Which rows survive, and how early? |
| `Project` | Select/compute expressions | What columns or expressions are produced? |
| `HashAggregate` | Hash-based grouped/global aggregation | Is this partial or final aggregation? |
| `Exchange` | Change data distribution | Why must data move? |
| `Sort` | Establish required ordering | Which downstream operator needs this order? |
| `BroadcastHashJoin` | Join using a broadcast hash table | Which side is broadcast/build side, and why? |
| `SortMergeJoin` | Merge compatibly partitioned and sorted equi-join inputs | Why were shuffle/sort preparations necessary? |

---

[Back to Table of Contents](#toc)

---

<a id="8-reading-aggregation-plans"></a>
## 8. Reading Aggregation Plans

Consider:

```python
store_sales_df = (
    sales_df
    .filter(F.col('order_status') == 'COMPLETED')
    .groupBy('store_id')
    .agg(
        F.sum('net_sales').alias('net_sales'),
        F.sum('quantity').alias('units'),
    )
)
```

Before inspecting the plan, predict:

```text
Scan source rows
    ↓
Filter completed sales
    ↓
perform partition-local partial aggregation
    ↓
redistribute partial results by store_id
    ↓
finish final aggregation per store_id
```

A common physical shape is:

```text
HashAggregate final
+- Exchange hashpartitioning(store_id, N)
   +- HashAggregate partial
      +- Filter
         +- Scan
```

### Why two `HashAggregate` nodes?

Do not read them as duplicate wasted work.

They represent a distributed aggregation strategy.

#### Partial aggregation

Each upstream partition can locally reduce rows such as:

```text
S01 10
S01 20
S02 15
```

into partial state such as:

```text
S01 30
S02 15
```

before data crosses the shuffle boundary.

#### `Exchange`

Partial states are redistributed so all states for the same `store_id` arrive in the same downstream partition.

#### Final aggregation

The downstream aggregate merges those partial states into the final result.

### Phase 4 bridge

Phase 4 prediction:

```text
groupBy is wide
→ expect a shuffle
→ expect a stage boundary
```

Phase 5 evidence:

```text
HashAggregate
Exchange hashpartitioning(...)
HashAggregate
```

This is exactly the progression the curriculum intends:

```text
predict distributed behavior
        ↓
inspect physical evidence
        ↓
explain why Spark chose it
```

---

[Back to Table of Contents](#toc)

---

<a id="9-reading-join-plans"></a>
## 9. Reading Join Plans

Join plans are where Phase 5 physical reasoning becomes especially important.

The logical request:

```python
sales_with_store_df = sales_df.join(
    stores_df,
    on='store_id',
    how='inner',
)
```

does not specify one universal physical algorithm.

Spark must choose an implementation.

### Prediction questions before inspection

Ask:

```text
Is this an equi-join?
What is the join type?
Could one side plausibly be small enough to broadcast?
Does Spark have statistics supporting that conclusion?
Was a broadcast hint supplied?
If broadcast is not selected, will both sides need compatible key distribution?
Would a merge strategy require sorting?
Is AQE enabled, allowing the strategy to change at runtime?
```

### Broadcast hash join reasoning

Suppose `stores_df` is a small dimension and broadcast is deliberately requested:

```python
sales_with_store_df = sales_df.join(
    F.broadcast(stores_df),
    on='store_id',
    how='inner',
)
```

Prediction:

```text
small stores side
      ↓
broadcast to executors
      ↓
build hash relation
      ↓
probe it while processing sales partitions
```

Expected operator family:

```text
BroadcastHashJoin
BroadcastExchange
```

The important explanation is not merely:

> Spark used a broadcast join because broadcast joins are fast.

A defensible explanation is:

> Spark can replicate the small build side and probe it locally from the other side's partitions, avoiding the standard requirement to repartition and sort both join inputs by the join key.

### Sort-merge join reasoning

If neither side is broadcast and Spark chooses sort-merge for an equi-join, predict:

```text
left side
  ↓
Exchange by join key
  ↓
Sort by join key

right side
  ↓
Exchange by join key
  ↓
Sort by join key

        ↓
SortMergeJoin
```

The `Exchange` nodes satisfy the required compatible distribution.

The `Sort` nodes satisfy the required ordering.

Then the merge join can stream through matching ordered keys.

### Broadcast threshold is context, not a Phase 5 tuning target

Spark 4.2.0 has an automatic broadcast size threshold controlled by:

```text
spark.sql.autoBroadcastJoinThreshold
```

The default is 10 MB.

You should understand that estimated relation size can influence join selection.

Do **not** turn Phase 5 into trial-and-error threshold tuning. The deeper performance-engineering question of when to force, avoid, or tune join strategies belongs to Phase 7.

### Hints influence planning but are not magic

A broadcast hint can be expressed with:

```python
F.broadcast(stores_df)
```

or DataFrame hints.

Hints tell Spark which strategy to prioritize where supported, but they do not make an impossible strategy valid for every join type.

Use hints in Phase 5 primarily to create controlled experiments where two physical strategies can be compared.

---

[Back to Table of Contents](#toc)

---

<a id="10-exchange-sort-and-the-phase-4-execution-model"></a>
## 10. `Exchange`, `Sort`, and the Phase 4 Execution Model

Phase 5 should make Phase 4 predictions more precise rather than replace them.

### `Exchange` is physical evidence of redistribution

Phase 4 reasoning:

```text
groupBy('store_id')
      ↓
rows for the same key may begin in different partitions
      ↓
shuffle required
```

Phase 5 plan evidence:

```text
Exchange hashpartitioning(store_id, N)
```

Now you can point to the exact physical operator that satisfies the distribution requirement.

### `Exchange` often corresponds to a stage boundary

A shuffle exchange means upstream tasks produce shuffle output that downstream work consumes.

Conceptually, using the project's 0-based execution notation:

```text
Stage 0
- Scan
- Filter
- partial HashAggregate
- produce shuffle output

========== Exchange / shuffle ==========

Stage 1
- read shuffled partitions
- final HashAggregate
- produce result
```

Do not infer actual Spark runtime stage IDs from the printed physical-plan node numbers.

When inspecting a real execution, preserve the IDs Spark actually reports.

### `Sort` is not synonymous with shuffle

A plan can require sorting because a downstream operator needs ordered rows.

Do not collapse these two concepts:

```text
Exchange = satisfy distribution requirement
Sort     = satisfy ordering requirement
```

They often appear together before `SortMergeJoin`, but they solve different physical requirements.

### Physical-plan trees and stage DAGs are related but not identical views

A physical plan shows executable SQL/DataFrame operators.

A stage DAG shows how runtime work is separated around shuffle dependencies.

They are complementary:

```text
Physical plan
explains operator strategy

Stage DAG / Spark UI
explains runtime execution boundaries and metrics
```

Deep Spark UI analysis belongs to Phase 8.

---

[Back to Table of Contents](#toc)

---

<a id="11-adaptive-query-execution"></a>
## 11. Adaptive Query Execution

**Adaptive Query Execution (AQE)** allows Spark SQL to revise parts of the physical execution strategy using statistics collected while the query is running.

Spark 4.2.0 enables AQE by default through:

```text
spark.sql.adaptive.enabled = true
```

### Why AQE exists

Before execution, Spark plans using information available at planning time.

That information may be incomplete or inaccurate.

Once shuffle/query stages execute, Spark can observe more accurate runtime facts such as actual partition sizes.

The model becomes:

```text
initial physical plan
        ↓
execute query stage(s)
        ↓
collect runtime statistics
        ↓
AQE re-evaluates eligible decisions
        ↓
adapted physical plan
        ↓
continue execution
```

### Recognizing an adaptive plan

With AQE enabled, physical explain output commonly includes a wrapper such as:

```text
AdaptiveSparkPlan isFinalPlan=false
```

Interpretation:

```text
Spark has an adaptive physical plan
and the final runtime plan has not yet been finalized
```

After execution has produced the required runtime information, adaptive plan output can expose the finalized strategy and may show both final and initial plan information.

Do not memorize one exact text layout; learn the meaning of the adaptive wrapper and final-vs-initial distinction.

### AQE behavior 1 — coalescing post-shuffle partitions

A query can begin with a configured shuffle-partition target that is deliberately generous.

After Spark sees actual shuffle output sizes, AQE can combine contiguous small shuffle partitions.

Conceptually:

```text
initial shuffle plan
Partition 0
Partition 1
Partition 2
Partition 3
Partition 4
Partition 5
Partition 6
Partition 7

runtime statistics reveal very small outputs
        ↓
AQE
        ↓
fewer, larger post-shuffle partitions
```

Physical plans may expose adaptive shuffle-reader nodes indicating coalescing.

Phase 5 takeaway:

> **The configured pre-execution shuffle partition count is not always the final runtime partition structure when AQE is enabled.**

Detailed partition sizing strategy belongs to Phases 6 and 7.

### AQE behavior 2 — changing join strategy

Spark can initially plan a sort-merge join because planning-time statistics do not justify broadcasting.

After runtime statistics become available, AQE can discover that one side is actually small enough to broadcast.

Conceptually:

```text
Initial plan
Exchange + Sort
        ↓
SortMergeJoin

runtime statistics
show one side is small
        ↓
AQE
        ↓
Final plan may use
BroadcastHashJoin
```

This is one of the most important Phase 5 ideas:

> **The initial physical strategy and the final runtime strategy can differ.**

### AQE behavior 3 — skew-aware adaptation

AQE can also react to skewed shuffle partitions in supported scenarios.

For Phase 5, understand only the core idea:

```text
runtime partition sizes are badly uneven
        ↓
AQE detects eligible skew
        ↓
adaptive execution can split/restructure problematic work
```

Do not yet turn this into a salting, skew-threshold, or production-remediation lesson. That deeper work belongs to Phase 7.

### Catalyst vs. AQE

Keep this distinction clear:

```text
Catalyst / normal query planning
- analyze query semantics
- optimize logical plan
- choose an initial physical strategy
- primarily uses information available before execution

AQE
- operates during execution
- uses runtime statistics
- can revise eligible physical decisions
```

They cooperate, but they are not synonyms.

### AQE does not mean Spark ignores initial planning

AQE needs an executable initial strategy so work can begin and runtime statistics can be collected.

The correct model is:

```text
plan first
then adapt eligible parts when runtime evidence justifies it
```

not:

```text
Spark waits until all data is known and only then creates a plan
```

---

[Back to Table of Contents](#toc)

---

<a id="12-prediction-before-inspection-workflow"></a>
## 12. Prediction-Before-Inspection Workflow

Use the same workflow for every important Phase 5 pipeline.

```text
PySpark code
    ↓
predict logical behavior
    ↓
predict likely physical requirements
    ↓
inspect explain output
    ↓
identify operators
    ↓
explain why Spark chose them
    ↓
execute when AQE/runtime evidence is needed
    ↓
inspect again
    ↓
explain any adaptive change
```

### Step 1 — State the logical requirement

Example:

```text
Keep completed sales.
Group them by store_id.
Compute total net_sales.
```

Do not start by guessing operator names.

### Step 2 — Predict physical requirements

Ask:

```text
Does this require scanning?
Filtering?
Projection?
Grouping?
Data redistribution?
Ordering?
A join algorithm?
```

Example prediction:

```text
Scan
→ Filter
→ partial aggregate
→ Exchange by store_id
→ final aggregate
```

### Step 3 — Inspect the full planning pipeline

```python
result_df.explain('extended')
```

Compare:

```text
parsed
vs.
analyzed
vs.
optimized
vs.
physical
```

### Step 4 — Inspect physical nodes clearly

```python
result_df.explain('formatted')
```

Identify required operators.

### Step 5 — Explain each important operator causally

Bad explanation:

> There is an `Exchange` because Spark uses exchanges.

Better explanation:

> The downstream final aggregation needs all partial states for the same `store_id` in the same partition, so Spark inserts a hash-partitioning `Exchange` to redistribute by `store_id`.

Bad explanation:

> Spark used `BroadcastHashJoin` because the table is small.

Better explanation:

> The build side is small enough or explicitly broadcast, so Spark can replicate it and probe a local hash relation from the streamed side, avoiding two-sided shuffle-and-sort preparation.

### Step 6 — For AQE, execute and compare

If the point of the experiment is adaptation:

```python
# An action is required for runtime statistics to become available to AQE.
result_df.collect()

# Inspect again after execution to study the finalized adaptive strategy.
result_df.explain('formatted')
```

Do not expect `explain()` before execution to reveal runtime facts that do not exist yet.

### Step 7 — Connect back to Phase 4 only where useful

When you see:

```text
Exchange
```

say:

```text
physical redistribution
→ shuffle
→ stage boundary implication
```

When you see only:

```text
Scan → Filter → Project
```

do not invent a shuffle merely because several operators exist.

---

[Back to Table of Contents](#toc)

---

<a id="13-worked-query-plan-reasoning"></a>
## 13. Worked Query-Plan Reasoning

Use one retail pipeline and reason from source code to expected physical strategy.

### Pipeline

```python
completed_sales_df = sales_df.filter(
    F.col('order_status') == 'COMPLETED'
)

store_totals_df = (
    completed_sales_df
    .groupBy('store_id')
    .agg(
        F.sum('net_sales').alias('net_sales'),
        F.sum('quantity').alias('units'),
    )
)

result_df = store_totals_df.join(
    F.broadcast(stores_df),
    on='store_id',
    how='inner',
)
```

### Question 1 — What is the logical intent?

```text
1. Filter sales to COMPLETED rows.
2. Group completed rows by store_id.
3. Compute store-level sales and unit totals.
4. Enrich those store-level totals with store attributes.
```

Grain after the aggregation is:

```text
one row per store_id
```

The join should preserve that grain if `stores_df` has one row per `store_id`.

### Question 2 — What logical optimization might occur?

Spark may simplify or collapse intermediate projections and push safe filters closer to their source relation.

Do not require the optimized plan to mirror the Python variables:

```text
completed_sales_df
store_totals_df
result_df
```

Python variable boundaries do not force query-plan boundaries.

### Question 3 — What physical aggregation pattern is likely?

Prediction:

```text
Scan sales
   ↓
Filter order_status = COMPLETED
   ↓
partial HashAggregate by store_id
   ↓
Exchange hashpartitioning(store_id, N)
   ↓
final HashAggregate by store_id
```

Why?

Rows for the same store can originate in different source partitions, so the final group requires compatible key distribution.

### Question 4 — What join strategy is likely?

Because the code explicitly uses:

```python
F.broadcast(stores_df)
```

predict a broadcast join strategy where supported.

Likely operator family:

```text
BroadcastHashJoin
BroadcastExchange
```

### Question 5 — What does the combined physical idea look like?

Conceptually:

```text
sales side
----------
Scan
  ↓
Filter
  ↓
HashAggregate partial
  ↓
Exchange by store_id
  ↓
HashAggregate final
  ↓
                BroadcastHashJoin
                       ↑
stores side             │
-----------             │
Scan                     │
  ↓                      │
BroadcastExchange -------+
```

### Question 6 — Where is the Phase 4 shuffle evidence?

The grouped aggregation contains:

```text
Exchange hashpartitioning(store_id, N)
```

That is the physical shuffle boundary.

The broadcast side also has a broadcast exchange, but it is a different data-movement strategy from hash-shuffling both join inputs.

### Question 7 — What could AQE change?

The explicitly hinted broadcast join already strongly constrains the intended join strategy.

AQE can still adapt eligible shuffle behavior elsewhere in the query, such as post-shuffle partition structure.

A better experiment for observing **runtime join conversion** would avoid forcing broadcast and construct a case where initial statistics and runtime statistics lead Spark to reconsider the join strategy.

### Inspection sequence

```python
# Full logical-to-physical planning pipeline.
result_df.explain('extended')

# Cleaner physical operator inspection.
result_df.explain('formatted')

# Execute only when runtime/AQE behavior is part of the question.
result_df.collect()

# Re-inspect the adaptive physical strategy after execution.
result_df.explain('formatted')
```

### Required explanation standard

Do not stop at:

```text
I see HashAggregate, Exchange, and BroadcastHashJoin.
```

Explain:

```text
The filter removes non-completed rows before grouping.
The first HashAggregate performs partition-local partial aggregation.
The hash-partitioning Exchange moves partial states so each store's states meet.
The second HashAggregate finishes the store-level totals.
The small/hinted stores relation is broadcast and used as the hash build side.
The aggregated sales side can probe that broadcast relation without a two-sided
shuffle-and-sort join preparation.
AQE may still revise eligible runtime decisions after stage statistics arrive.
```

That is Phase 5 reasoning.

---

[Back to Table of Contents](#toc)

---

<a id="14-engineering-decision-rules"></a>
## 14. Engineering Decision Rules

1. **Start with semantics, not operator names.** State what result the pipeline requires before guessing how Spark will implement it.
2. **Separate logical from physical reasoning.** `Aggregate` is logical intent; `HashAggregate` is one physical implementation.
3. **Use `explain('extended')` to study plan evolution.** It is the direct view of parsed → analyzed → optimized → physical planning.
4. **Use `explain('formatted')` to read physical operators cleanly.** The outline and node details are especially useful for operator-by-operator reasoning.
5. **Read physical plans from the leaves upward.** Start at scans and follow data toward the root result.
6. **Treat `Exchange` as a distribution clue.** Ask exactly what downstream requirement forced data movement.
7. **Do not treat `Sort` and `Exchange` as synonyms.** One satisfies ordering; the other satisfies distribution.
8. **Expect partial and final aggregation phases.** Two `HashAggregate` nodes around an `Exchange` are a normal distributed aggregation pattern.
9. **Explain join strategy from evidence.** Use join type, keys, relation sizes/statistics, hints, distribution requirements, and AQE—not vague claims that one join is always faster.
10. **Identify the build side of a broadcast hash join.** `BuildLeft` / `BuildRight` is part of explaining the strategy.
11. **Do not assume every join shuffles both sides.** Broadcast and pre-existing distribution can change the plan substantially.
12. **Do not assume every equi-join becomes a broadcast join just because one DataFrame looks small in Python.** Spark's decision depends on planning information and strategy rules.
13. **Distinguish estimated from runtime information.** Initial planning and AQE do not have the same evidence available.
14. **Do not call an adaptive plan final merely because `AdaptiveSparkPlan` appears.** Inspect whether the plan is finalized and whether runtime execution has occurred.
15. **Use Phase 4 to interpret, not replace, the physical plan.** `Exchange` explains where a shuffle occurs; the plan explains why that exchange was required by a specific operator strategy.
16. **Do not infer runtime Job/Stage/Task IDs from explain node numbers.** They are different identifiers.
17. **Do not overfit to one exact printed plan.** Learn operator requirements and causal reasoning across reasonable plan variations.
18. **Keep deep storage and performance tuning out of scope.** Recognize plan evidence now; defer deep file engineering to Phase 6 and deliberate tuning strategy to Phase 7.
19. **Predict before inspection.** If you only explain after seeing the answer, you are not yet testing your execution model.
20. **When prediction and plan disagree, investigate the assumption that failed.** That gap is often the most valuable learning evidence.

A repeatable Phase 5 checklist is:

```text
1. What is the pipeline's logical result and grain?
2. What does the parsed plan request?
3. What names/types are resolved in the analyzed plan?
4. What did Catalyst simplify or rearrange logically?
5. What physical operators were selected?
6. Where are Scan, Filter, and Project nodes?
7. Where are partial/final aggregates?
8. Where are Exchange nodes, and what distribution do they satisfy?
9. Where are Sort nodes, and what ordering do they satisfy?
10. Which join algorithm was chosen, and why?
11. What planning-time evidence supported that choice?
12. Is AQE enabled?
13. Is the adaptive plan final yet?
14. After execution, what changed and what runtime evidence explains it?
15. How does the physical plan connect back to Phase 4 shuffle/stage reasoning?
```

---

[Back to Table of Contents](#toc)

---

<a id="15-common-failure-modes"></a>
## 15. Common Failure Modes

### Treating the physical plan as the same thing as the logical plan

Logical plans describe relational intent.

Physical plans choose executable algorithms and distribution/order operations.

Keep:

```text
WHAT
vs.
HOW
```

separate.

### Assuming `df.explain()` shows all four plan layers

By default:

```python
df.explain()
```

prints the physical plan.

Use:

```python
df.explain('extended')
```

for parsed, analyzed, optimized, and physical plans.

### Assuming `df.explain('formatted')` is the full logical history

Formatted mode is primarily a detailed physical-plan view.

Use it for operator inspection, not as a replacement for `extended` when studying logical-plan evolution.

### Reading the plan top-down and losing data flow

The root result appears near the top.

For causal execution reasoning, start from the scans near the leaves and trace upward.

### Treating every `Project` as merely dropping columns

`Project` can also compute expressions, aliases, and derived columns.

### Treating two `HashAggregate` nodes as duplicated work

Partial aggregation before shuffle and final aggregation after shuffle are a standard distributed pattern.

### Treating every `Exchange` as exactly the same operation

A hash-partitioning shuffle exchange and a broadcast exchange represent different movement strategies.

Read the exchange details.

### Treating `Exchange` and `Sort` as the same cost or requirement

They satisfy different requirements:

```text
Exchange → distribution
Sort     → ordering
```

### Saying every join must shuffle both inputs

A broadcast hash join is the obvious counterexample.

The small build side is broadcast, while the other side can be streamed and probed partition-locally.

### Saying broadcast is always better

Broadcast is appropriate only when the build side and join semantics make it viable.

Phase 5 explains the strategy; Phase 7 handles deliberate performance decisions.

### Assuming a tiny Python-created DataFrame will automatically broadcast

Spark's automatic join strategy depends on its available statistics and rules.

For deterministic teaching experiments, an explicit broadcast hint can be used when the purpose is to study `BroadcastHashJoin` itself.

### Assuming `SortMergeJoin` means Spark arbitrarily decided to sort everything

Sort-merge requires compatible key distribution and ordering. The `Exchange` and `Sort` nodes satisfy those physical requirements.

### Assuming Catalyst means only the optimized logical plan

Catalyst is the broader Spark SQL query analysis/optimization framework.

The optimized logical plan is one important product of that framework, not the entire meaning of Catalyst.

### Assuming optimized means physically executable

The optimized logical plan still describes relational operations logically.

Physical planning must still select actual execution operators.

### Assuming AQE chooses the entire query from scratch after execution begins

AQE starts from an initial physical plan and revises eligible parts when runtime statistics become available.

### Assuming `AdaptiveSparkPlan` proves Spark changed something

It proves the query is under adaptive execution.

The initial and final strategy may still be identical if runtime evidence does not justify a change.

### Expecting runtime AQE changes before an action

AQE needs runtime statistics from actual query-stage execution.

`explain()` alone cannot manufacture those runtime statistics.

### Confusing explain node IDs with jobs, stages, tasks, or partitions

Formatted node `(3)` is a plan-node label, not Stage 3.

Keep plan identifiers and runtime execution identifiers separate.

### Drifting into Phase 6 or 7 too early

Phase 5 should answer:

```text
What plan did Spark create?
What operators are present?
What physical requirements caused them?
What did AQE change at runtime?
```

Phase 6 goes deeper into:

```text
files
storage layout
execution/storage partitions
shuffle engineering
```

Phase 7 goes deeper into:

```text
performance diagnosis
join optimization
skew
caching
AQE tuning
resource tuning
```

Keep those layers distinct.

---

[Back to Table of Contents](#toc)

---

<a id="16-phase-5-mastery-reference"></a>
## 16. Phase 5 Mastery Reference

The curriculum mastery target is eventually to take real PySpark pipelines and explain:

> **Why did Spark choose the resulting physical execution strategy?**

That mastery gate is intentionally deferred until explicitly requested.

Before beginning it, Phase 5 fluency should include being able to explain the following.

### Planning sequence

- what the parsed logical plan represents;
- why unresolved names can appear there;
- what the analyzed logical plan resolves;
- why analysis errors occur;
- what the optimized logical plan represents;
- why optimized logical structure can differ from source-code structure;
- what the physical plan represents;
- how logical intent differs from physical execution strategy.

### Catalyst

- what Catalyst does at a practical level;
- why Spark can rewrite a query while preserving semantics;
- why Python variable/method boundaries do not force execution-plan boundaries;
- why optimized logical plans should be judged by semantics rather than textual similarity to source code.

### Explain tools

- what `df.explain()` prints by default;
- why `df.explain('extended')` is useful for plan evolution;
- why `df.explain('formatted')` is useful for physical operator inspection;
- why formatted node numbers are not runtime execution IDs;
- why an explain plan is planning evidence rather than proof that the full result has executed.

### Core operators

- what a scan does;
- what a filter does;
- what a project does;
- why grouped aggregation commonly has partial and final `HashAggregate` nodes;
- what an `Exchange` tells you about distribution requirements;
- what a `Sort` tells you about ordering requirements;
- how `BroadcastHashJoin` works conceptually;
- how `SortMergeJoin` works conceptually;
- why broadcast and sort-merge have different preparation requirements.

### Aggregation reasoning

Given:

```python
sales_df.groupBy('store_id').agg(
    F.sum('net_sales').alias('net_sales')
)
```

you should be able to predict and explain a common shape such as:

```text
Scan
  ↓
HashAggregate partial
  ↓
Exchange hashpartitioning(store_id, N)
  ↓
HashAggregate final
```

and connect the `Exchange` to the wide dependency learned in Phase 4.

### Join reasoning

Given a real join, you should be able to explain:

- why Spark selected a broadcast-hash or sort-merge strategy;
- which side is the build/broadcast side when applicable;
- why a sort-merge plan may contain exchanges and sorts on both sides;
- why join type, key structure, statistics, hints, distribution, ordering, and AQE can affect the strategy;
- why saying "all joins shuffle" is incorrect.

### AQE

- what AQE is;
- why it needs runtime statistics;
- why the initial physical plan can differ from the final runtime strategy;
- what `AdaptiveSparkPlan` means;
- why `isFinalPlan=false` matters;
- how AQE can coalesce post-shuffle partitions conceptually;
- how AQE can convert an eligible sort-merge join to a broadcast-hash join conceptually;
- how AQE can respond to skew conceptually;
- why deeper AQE tuning is deferred to Phase 7.

### Final reasoning standard

Given a pipeline such as:

```python
result_df = (
    sales_df
    .filter(F.col('order_status') == 'COMPLETED')
    .groupBy('store_id')
    .agg(
        F.sum('net_sales').alias('net_sales')
    )
    .join(
        F.broadcast(stores_df),
        on='store_id',
        how='inner',
    )
)
```

you should be able to say, **before running it**:

```text
1. Logically, Spark must filter, aggregate by store, then enrich with store data.
2. Catalyst may rewrite intermediate logical structure while preserving semantics.
3. The grouped aggregate likely needs partial aggregation, redistribution by
   store_id, and final aggregation.
4. Therefore I expect HashAggregate → Exchange → HashAggregate behavior around
   the grouping boundary.
5. The explicit broadcast hint should make a broadcast join strategy likely
   where supported.
6. Therefore I expect BroadcastHashJoin plus a BroadcastExchange on the stores
   build side rather than two-sided shuffle-and-sort join preparation.
7. With AQE enabled, eligible shuffle/runtime decisions can still be revised
   after query-stage statistics become available.
```

Then you should inspect:

```python
result_df.explain('extended')
result_df.explain('formatted')
```

and explain any differences between prediction and evidence.

When AQE behavior is part of the question, you should execute an action, inspect again, and explain the final adaptive strategy using runtime evidence.

The final Phase 5 standard is not:

> I can point out `Exchange` and `BroadcastHashJoin` in an explain plan.

It is:

> **I can trace a real PySpark pipeline from logical intent through Catalyst planning into physical operators, connect those operators to distributed execution requirements, and explain why Spark selected or adaptively changed the resulting strategy.**

---

[Back to Table of Contents](#toc)
