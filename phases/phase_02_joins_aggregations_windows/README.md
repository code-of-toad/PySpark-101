# Phase 2 — Joins, Aggregations, Windows & Data Modeling

<a id="toc"></a>
## Table of Contents

- [Objective](#objective)
- [Phase Mental Model](#phase-mental-model)
- [1. Grain: The First Question](#1-grain)
- [2. Aggregations](#2-aggregations)
- [3. Joins](#3-joins)
- [4. Window Functions](#4-window-functions)
- [5. Other Transformation Patterns](#5-other-transformation-patterns)
- [6. Data Modeling](#6-data-modeling)
- [7. Retail Analytical Model](#7-retail-analytical-model)
- [8. Engineering Decision Rules](#8-engineering-decision-rules)
- [9. Common Failure Modes](#9-common-failure-modes)
- [10. Phase 2 Mastery Reference](#10-phase-2-mastery-reference)

---

<a id="objective"></a>
## Objective

Become able to express common batch data-engineering transformations while preserving the intended **grain** of every dataset.

Phase 2 is not primarily about memorizing join types or window syntax. The central engineering skill is being able to reason about what one row represents before and after every transformation.

The governing questions are:

> **Before a join:** What is the grain of the left DataFrame? What is the grain of the right DataFrame? Can this join multiply rows?

> **Before an output table:** What does one row represent?

Examples target **PySpark 4.2.0** and reuse a retail domain throughout.

This document is lecture/reference material only. Exercises and applied mastery work remain separate.

---

[Back to Table of Contents](#toc)

---

<a id="phase-mental-model"></a>
## Phase Mental Model

Use this order of reasoning:

```text
Source grain
    ↓
Validate keys
    ↓
Choose target grain
    ↓
Choose transformation
    ↓
Check whether rows collapse, multiply, or expand
    ↓
Validate output grain and row counts
```

A retail pipeline for this phase is:

```text
Raw Orders
Raw Order Items
Raw Products
Raw Stores
Inventory Snapshots
        ↓
      PySpark
        ↓
dim_product
dim_store
dim_date
fact_sales
fact_inventory_snapshot
```

The important point is not merely that these tables can be produced. It is that each output has an intentional grain:

| Dataset | Intended grain |
|---|---|
| `raw_orders` | one row per order |
| `raw_order_items` | one row per order line |
| `raw_products` | one row per product business key |
| `raw_stores` | one row per store business key |
| `inventory_snapshots` | one source snapshot event per store, product, and observation |
| `dim_product` | one row per current product |
| `dim_store` | one row per current store |
| `dim_date` | one row per calendar date |
| `fact_sales` | one row per order line |
| `fact_inventory_snapshot` | one row per store, product, and snapshot date |

Every transformation in this phase can be understood as one of three grain effects:

```text
Collapse rows   → aggregation
Preserve rows   → many windows, column expressions, many-to-one joins
Expand rows     → one-to-many joins, many-to-many joins, explode, cross join
```

A transformation is not good or bad because it changes row count. It is correct when its row-count behavior matches the intended target grain.

---

[Back to Table of Contents](#toc)

---

<a id="1-grain"></a>
## 1. Grain: The First Question

**Grain** means what one row represents.

Examples:

```text
orders
one row = one order

order_items
one row = one line within an order

daily_store_sales
one row = one store on one calendar date

inventory_snapshot
one row = one product in one store on one snapshot date
```

### Keys are evidence of grain

A dataset at order-line grain might have the composite key:

```text
(order_id, line_number)
```

A daily inventory fact might have:

```text
(snapshot_date, store_id, product_id)
```

If the supposed key is not unique, at least one of these is true:

1. the assumed grain is wrong;
2. duplicates exist;
3. a source event is more detailed than the target table;
4. upstream logic already multiplied rows.

A reusable duplicate-key check is:

```python
from pyspark.sql import functions as F

duplicate_keys_df = (
    df
    .groupBy('order_id', 'line_number')
    .agg(F.count('*').alias('row_count'))
    .filter(F.col('row_count') > 1)
)
```

### Row-count reasoning

For a transformation, predict the expected effect before running it:

| Transformation | Typical row-count effect |
|---|---|
| `select`, `withColumn`, `when` | preserve rows |
| `filter` | same or fewer rows |
| `groupBy(...).agg(...)` | collapse to one row per group |
| many-to-one join | usually preserve left rows if every left key matches at most one right row |
| one-to-many join | can multiply left rows |
| many-to-many join | can multiply rows dramatically |
| window function | preserves rows |
| `dropDuplicates` | same or fewer rows |
| `union` / `unionByName` | stacks rows |
| `explode` | expands one parent row into zero or more child rows |
| cross join | left row count × right row count |

### Grain is more important than table names

A DataFrame called `sales_df` does not tell you whether it contains:

- one row per transaction;
- one row per order;
- one row per order line;
- one row per store-day;
- one row per product-month.

Always state the grain explicitly in code comments, documentation, tests, or design notes.

---

[Back to Table of Contents](#toc)

---

<a id="2-aggregations"></a>
## 2. Aggregations

Aggregation intentionally **changes grain** by collapsing multiple input rows into one output row per group.

### `groupBy()` and `agg()`

Suppose `sales_lines_df` is at:

```text
one row per order line
```

Then:

```python
store_sales_df = (
    sales_lines_df
    .groupBy('store_id')
    .agg(
        F.sum('net_sales').alias('net_sales'),
    )
)
```

changes the grain to:

```text
one row per store
```

With two grouped columns:

```python
daily_store_sales_df = (
    sales_lines_df
    .groupBy('order_date', 'store_id')
    .agg(
        F.sum('net_sales').alias('net_sales'),
    )
)
```

the output grain is:

```text
one row per order date and store
```

### Grouped columns vs. aggregated columns

After grouping, every selected output must conceptually be either:

1. part of the group key; or
2. reduced with an aggregate function.

If the target is one row per store, `store_id` belongs in the group key. A product name does not belong in the output unless the target grain also includes product or there is a justified aggregate for it.

This prevents a common mistake: grouping at one grain while mentally interpreting the result at another.

### Core aggregate functions

Prefer explicit expressions inside `agg()` when producing analytical outputs because the resulting column names and business meaning stay visible.

```python
summary_df = (
    sales_lines_df
    .groupBy('store_id')
    .agg(
        F.count('*').alias('line_count'),
        F.count('customer_id').alias('non_null_customer_count'),
        F.sum('quantity').alias('units_sold'),
        F.avg('net_sales').alias('avg_line_sales'),
        F.min('net_sales').alias('min_line_sales'),
        F.max('net_sales').alias('max_line_sales'),
        F.countDistinct('order_id').alias('order_count'),
    )
)
```

Important `count` distinction:

```text
count('*')       → counts rows
count(column)    → counts non-NULL values in that column
countDistinct(x) → counts distinct non-NULL values of x
```

PySpark also exposes `count_distinct()`. `countDistinct()` is the familiar camel-case alias used widely in PySpark code.

### Conditional aggregation

Conditional aggregation computes metrics for subsets of rows without creating separate filtered pipelines.

```python
store_metrics_df = (
    sales_lines_df
    .groupBy('store_id')
    .agg(
        F.sum('net_sales').alias('net_sales'),
        F.sum(
            F.when(
                F.col('order_status') == 'COMPLETED',
                F.col('net_sales'),
            ).otherwise(F.lit(0))
        ).alias('completed_sales'),
        F.sum(
            F.when(
                F.col('order_status') == 'CANCELLED',
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias('cancelled_line_count'),
    )
)
```

Mental model:

```text
CASE/WHEN chooses the value contributed by each row.
The aggregate then reduces those contributions within each group.
```

### Pre-aggregation to the required join grain

Suppose promotions contains multiple records per product:

```text
product_promotions
one row = one product-promotion
```

and order lines contain multiple rows per product:

```text
order_items
one row = one order line
```

Joining directly on `product_id` creates a many-to-many relationship.

If the required metric is the number of active promotions per product, aggregate promotions first:

```python
promotion_summary_df = (
    product_promotions_df
    .groupBy('product_id')
    .agg(
        F.countDistinct('promotion_id').alias('promotion_count'),
    )
)
```

Now the right DataFrame is:

```text
one row per product
```

and can safely participate in a many-to-one join from order lines.

### Aggregation mistakes that corrupt metrics

**Mistake 1 — summing after an accidental row-multiplying join**

If each sales line is duplicated three times by a join, summing revenue after the join triples the result.

**Mistake 2 — grouping too finely**

```python
df.groupBy('store_id', 'order_id').agg(...)
```

does not produce store-grain output. It produces one row per store-order combination.

**Mistake 3 — grouping too coarsely**

If product is omitted from a target that should be store-product grain, product-level detail is lost.

**Mistake 4 — using `count(column)` when NULLs should count as rows**

Use `count('*')` when the requirement is row count.

**Mistake 5 — averaging pre-aggregated averages**

An average of store averages is not generally the overall average unless the groups have equal weights. Preserve the numerator/denominator needed for weighted reconciliation.

---

[Back to Table of Contents](#toc)

---

<a id="3-joins"></a>
## 3. Joins

A join combines **columns** from related datasets. Its correctness depends on the cardinality of the join keys.

Before every join, write down:

```text
Left grain:
Right grain:
Join keys:
Relationship:
Can rows multiply?
Expected unmatched behavior:
```

### Join cardinalities

#### One-to-one

Each key appears at most once on both sides.

```text
left:  one row per product
right: one row per product
```

A matched key contributes at most one joined row.

#### One-to-many

One left key may match several right rows.

```text
orders      one row per order
order_items one row per order line
```

Joining orders to items expands an order into its lines.

That expansion is correct if the desired output is order-line grain.

#### Many-to-one

Many left rows can match one right row.

```text
order_items one row per order line
products    one row per product
```

This is the normal fact-to-dimension enrichment pattern.

If the right key is unique, the join should not multiply left rows.

#### Many-to-many

Both sides contain duplicate join keys.

```text
order_items       many rows per product
product_promotions many rows per product
```

For a product with 10 order lines and 3 promotions, a direct join can produce:

```text
10 × 3 = 30 rows
```

Many-to-many joins are not inherently invalid, but they are dangerous because the multiplied grain is often not the intended analytical grain.

### Join types

Assume:

```python
left_df.join(right_df, on='product_id', how='...')
```

| Join | Keeps |
|---|---|
| `inner` | matching rows only |
| `left` | all left rows plus right matches |
| `right` | all right rows plus left matches |
| `full` / `outer` | all rows from both sides |
| `left_semi` | left rows whose key has a match; no right columns |
| `left_anti` | left rows whose key has no match; no right columns |
| `cross` | every left row paired with every right row |

Common syntax:

```python
inner_df = left_df.join(right_df, on='product_id', how='inner')
left_df_result = left_df.join(right_df, on='product_id', how='left')
right_df_result = left_df.join(right_df, on='product_id', how='right')
full_df = left_df.join(right_df, on='product_id', how='full')
semi_df = left_df.join(right_df, on='product_id', how='left_semi')
anti_df = left_df.join(right_df, on='product_id', how='left_anti')
cross_df = left_df.crossJoin(right_df)
```

### When semi and anti joins are especially useful

**Semi join** answers:

> Which left records have at least one valid reference?

Example: keep order items whose `product_id` exists in the product dimension.

**Anti join** answers:

> Which left records have no valid reference?

Example: identify orphaned order items.

```python
orphan_items_df = order_items_df.join(
    products_df,
    on='product_id',
    how='left_anti',
)
```

Anti joins are a natural referential-integrity validation pattern.

### Multi-column joins

When the relationship is defined by a composite key, join on the complete key.

Example:

```text
reorder_thresholds
one row per store and product
key = (store_id, product_id)
```

```python
inventory_with_threshold_df = inventory_df.join(
    reorder_thresholds_df,
    on=['store_id', 'product_id'],
    how='left',
)
```

Joining on only `product_id` would ignore the store portion of the grain and can create incorrect matches or row multiplication.

### Join-key validation

Before a supposed many-to-one join, verify the right-side key is actually unique:

```python
duplicate_products_df = (
    products_df
    .groupBy('product_id')
    .agg(F.count('*').alias('row_count'))
    .filter(F.col('row_count') > 1)
)
```

For a composite key:

```python
duplicate_thresholds_df = (
    reorder_thresholds_df
    .groupBy('store_id', 'product_id')
    .agg(F.count('*').alias('row_count'))
    .filter(F.col('row_count') > 1)
)
```

To validate referential integrity:

```python
orphan_items_df = order_items_df.join(
    products_df.select('product_id'),
    on='product_id',
    how='left_anti',
)
```

### Handling duplicate column names

If both DataFrames contain a non-key column such as `status`, using a boolean join expression can leave both columns in the result.

Use aliases and explicit projection:

```python
orders = orders_df.alias('o')
stores = stores_df.alias('s')

joined_df = (
    orders
    .join(
        stores,
        F.col('o.store_id') == F.col('s.store_id'),
        how='left',
    )
    .select(
        F.col('o.order_id'),
        F.col('o.store_id'),
        F.col('o.status').alias('order_status'),
        F.col('s.store_name'),
    )
)
```

When both sides use the same key name and the join is simple equality, this form is concise:

```python
joined_df = orders_df.join(
    stores_df,
    on='store_id',
    how='left',
)
```

Spark keeps one `store_id` join-key column in that form.

### NULL join-key reasoning

Standard equality does not treat `NULL == NULL` as a match.

A missing foreign key is therefore not the same thing as an unmatched non-null foreign key. Distinguish:

```text
foreign key is NULL
vs.
foreign key has a value that does not exist in the dimension
```

Both may violate a business rule, but they are different failure causes.

### Cross joins

A cross join has no matching predicate:

```python
calendar_store_df = date_df.crossJoin(store_df)
```

If there are 365 dates and 100 stores:

```text
365 × 100 = 36,500 rows
```

Cross joins are appropriate when the target grain is deliberately every combination, such as generating a complete store-date scaffold. They are dangerous when produced accidentally.

### Row-count validation around joins

For a many-to-one left join where the right key is unique:

```text
expected output row count = left row count
```

provided the join does not filter left rows.

For an inner join:

```text
output row count may be lower because unmatched left rows disappear
```

For one-to-many:

```text
output row count may be higher because one left row expands into multiple matches
```

Do not treat row-count changes as surprises. Predict them from the relationship first.

---

[Back to Table of Contents](#toc)

---

<a id="4-window-functions"></a>
## 4. Window Functions

Window functions calculate across related rows **without collapsing the DataFrame**.

That is their essential difference from aggregation.

### `groupBy` vs. window

Suppose the input has 100 sales rows.

```python
store_totals_df = (
    sales_df
    .groupBy('store_id')
    .agg(F.sum('net_sales').alias('store_sales'))
)
```

The output has one row per store.

A window can attach the store total back to every original row:

```python
from pyspark.sql import Window

store_window = Window.partitionBy('store_id')

sales_with_store_total_df = sales_df.withColumn(
    'store_sales',
    F.sum('net_sales').over(store_window),
)
```

The output remains at sales-row grain.

Mental model:

```text
groupBy:
many rows → one row per group

window:
many related rows → calculate across them → keep every original row
```

### `Window.partitionBy()`

`partitionBy()` defines the independent groups over which a window calculation operates.

```python
store_window = Window.partitionBy('store_id')
```

This does **not** collapse rows. It defines which rows can see one another for the window calculation.

Do not confuse:

```python
df.groupBy('store_id')
```

with:

```python
Window.partitionBy('store_id')
```

They both describe grouping boundaries, but they produce different result semantics.

### `Window.orderBy()`

Ranking, sequencing, `lag`, `lead`, running calculations, and latest-record selection require a meaningful row order inside each partition.

```python
order_window = (
    Window
    .partitionBy('store_id')
    .orderBy('order_ts')
)
```

If ties are possible and the result must be deterministic, add stable tie-breakers.

```python
latest_window = (
    Window
    .partitionBy('store_id', 'product_id', 'snapshot_date')
    .orderBy(
        F.col('snapshot_ts').desc(),
        F.col('ingestion_id').desc(),
    )
)
```

Without a complete ordering, two tied rows may be interchangeable from Spark's perspective.

### `row_number()`

Assigns a unique sequence number within each ordered window partition.

```python
ranked_df = inventory_df.withColumn(
    'row_number',
    F.row_number().over(latest_window),
)
```

Typical use:

```python
latest_df = ranked_df.filter(
    F.col('row_number') == 1
)
```

This is the standard deterministic latest-record pattern when the ordering uniquely resolves ties.

### `rank()` vs. `dense_rank()`

Given values:

```text
100
100
90
80
```

the conceptual rankings are:

```text
row_number: 1, 2, 3, 4
rank:       1, 1, 3, 4
dense_rank: 1, 1, 2, 3
```

Use:

- `row_number()` when exactly one row must occupy each sequence position;
- `rank()` when ties should share rank and gaps are meaningful;
- `dense_rank()` when ties should share rank without gaps.

### `lag()` and `lead()`

`lag()` accesses a previous row within the ordered partition.

```python
sales_with_previous_df = sales_df.withColumn(
    'previous_sales',
    F.lag('net_sales').over(order_window),
)
```

`lead()` accesses a following row:

```python
sales_with_next_df = sales_df.withColumn(
    'next_sales',
    F.lead('net_sales').over(order_window),
)
```

Typical engineering uses:

- change from prior snapshot;
- previous order date;
- next effective date;
- gap detection;
- transition analysis.

### Window frames

A window specification can include a **frame** that defines which ordered rows contribute to the calculation for the current row.

#### Running frame

```python
running_window = (
    Window
    .partitionBy('store_id')
    .orderBy('order_date')
    .rowsBetween(
        Window.unboundedPreceding,
        Window.currentRow,
    )
)
```

Meaning:

```text
from the first row in the partition
through the current row
```

Example:

```python
df.withColumn(
    'running_sales',
    F.sum('net_sales').over(running_window),
)
```

#### Rolling row window

```python
rolling_3_row_window = (
    Window
    .partitionBy('store_id')
    .orderBy('order_date')
    .rowsBetween(-2, 0)
)
```

Meaning:

```text
current row plus two preceding rows
```

This is a **three-row** window, not necessarily three days.

#### Rolling value/range window

A range frame is based on values of the ordering expression rather than physical row positions.

For a real seven-day calculation, create a numeric day value and use:

```python
daily_sales_df = daily_sales_df.withColumn(
    'day_number',
    F.datediff(
        F.col('order_date'),
        F.lit('1970-01-01'),
    ),
)

rolling_7_day_window = (
    Window
    .partitionBy('store_id')
    .orderBy('day_number')
    .rangeBetween(-6, 0)
)
```

That means:

```text
current calendar day and previous six day-number values
```

The distinction matters when some days contain no rows or multiple rows.

### Latest-record selection

A professional latest-record pattern is:

1. define the business grain;
2. define the recency column;
3. define deterministic tie-breakers;
4. assign `row_number`;
5. keep row 1.

```python
latest_window = (
    Window
    .partitionBy(
        'snapshot_date',
        'store_id',
        'product_id',
    )
    .orderBy(
        F.col('snapshot_ts').desc(),
        F.col('ingestion_id').desc(),
    )
)

latest_inventory_df = (
    inventory_df
    .withColumn(
        '_row_number',
        F.row_number().over(latest_window),
    )
    .filter(F.col('_row_number') == 1)
    .drop('_row_number')
)
```

The result grain is one row per:

```text
snapshot_date, store_id, product_id
```

### Deterministic deduplication

`dropDuplicates()` answers:

> Keep one row from each duplicate-key group.

It does **not** answer:

> Keep the newest row, and if timestamps tie, keep the greatest ingestion ID.

When a specific survivor matters, use `row_number()` with a complete deterministic ordering.

---

[Back to Table of Contents](#toc)

---

<a id="5-other-transformation-patterns"></a>
## 5. Other Transformation Patterns

### Deduplication

Whole-row duplicates:

```python
unique_rows_df = df.distinct()
```

Duplicates based on selected columns:

```python
one_per_order_df = df.dropDuplicates(['order_id'])
```

Use `dropDuplicates` when any survivor is acceptable for the business requirement.

Use a deterministic window when the survivor is meaningful.

### Conditional transformations

Use `when()` / `otherwise()` for declarative business logic.

```python
classified_df = inventory_df.withColumn(
    'inventory_status',
    F.when(
        F.col('on_hand_quantity') <= F.col('reorder_point'),
        F.lit('LOW_STOCK'),
    )
    .when(
        F.col('on_hand_quantity') == 0,
        F.lit('OUT_OF_STOCK'),
    )
    .otherwise(F.lit('HEALTHY')),
)
```

Order conditions carefully: the first matching condition wins.

For the example above, `0` also satisfies `<= reorder_point`; if out-of-stock must be a distinct class, test it first.

```python
classified_df = inventory_df.withColumn(
    'inventory_status',
    F.when(
        F.col('on_hand_quantity') == 0,
        F.lit('OUT_OF_STOCK'),
    )
    .when(
        F.col('on_hand_quantity') <= F.col('reorder_point'),
        F.lit('LOW_STOCK'),
    )
    .otherwise(F.lit('HEALTHY')),
)
```

### Union vs. join

A **join** combines columns using a relationship between rows:

```text
orders + stores
      ↓ join on store_id
order columns + store columns
```

A **union** stacks compatible rows vertically:

```text
January sales
February sales
      ↓ union
January + February rows
```

Use a join to enrich or relate data.

Use a union to combine datasets representing the same conceptual row type.

### `union()` vs. `unionByName()`

`union()` resolves columns **by position**, not by name, and does not automatically remove duplicate rows.

```python
combined_df = january_df.union(february_df)
```

This is safe only when column order and compatible types are deliberately aligned.

`unionByName()` resolves columns by name:

```python
combined_df = january_df.unionByName(february_df)
```

For evolving batch schemas:

```python
combined_df = january_df.unionByName(
    february_df,
    allowMissingColumns=True,
)
```

Missing columns are filled with `NULL`.

Engineering rule:

> For pipeline batches, prefer `unionByName()` unless positional alignment is intentionally guaranteed and obvious.

### `explode()`

`explode()` converts each element of an array or map into its own row.

Suppose:

```text
product_id | tags
P100       | [grocery, organic]
```

Then:

```python
exploded_df = products_df.select(
    'product_id',
    F.explode('tags').alias('tag'),
)
```

produces:

```text
P100 | grocery
P100 | organic
```

The grain changed from:

```text
one row per product
```

to:

```text
one row per product-tag
```

For an empty array, `explode()` emits no child row. For a `NULL` array, `explode()` also emits no child row. `explode_outer()` is available when a parent row should be preserved with a null child value.

### Pivot

Pivot turns values from one column into multiple columns and requires aggregation.

Long input:

```text
store_id | month   | net_sales
S01      | 2026-01 | ...
S01      | 2026-02 | ...
```

Pivoted output:

```text
store_id | 2026-01 | 2026-02
S01      | ...     | ...
```

PySpark pattern:

```python
wide_df = (
    monthly_sales_df
    .groupBy('store_id')
    .pivot('month', ['2026-01', '2026-02'])
    .agg(F.sum('net_sales'))
)
```

Pivot is useful for reporting-oriented shapes, but long/narrow forms are often easier to process generically.

When the domain is known, supplying the pivot values explicitly avoids Spark having to discover distinct pivot values first.

### Unpivot

Unpivot reverses a wide representation into a long representation.

PySpark 4.2.0 provides `DataFrame.unpivot()` and the equivalent `melt()` alias.

```python
long_df = wide_df.unpivot(
    ids='store_id',
    values=['2026-01', '2026-02'],
    variableColumnName='month',
    valueColumnName='net_sales',
)
```

Conceptually:

```text
wide measures in columns
        ↓
identifier + variable name + value
```

Unpivot can increase row count because each source row may become one output row per unpivoted measure column.

---

[Back to Table of Contents](#toc)

---

<a id="6-data-modeling"></a>
## 6. Data Modeling

Data modeling gives transformation logic a target. The code should follow the desired analytical grain rather than inventing the grain accidentally.

### Facts vs. dimensions

A **fact table** records measurable business events or states.

Examples:

- sales transaction line;
- daily inventory snapshot.

A **dimension table** describes entities used to categorize and analyze facts.

Examples:

- product;
- store;
- date.

A star schema places facts at the center with dimensions around them:

```text
             dim_product
                  |
dim_date --- fact_sales --- dim_store
```

### Transaction facts

A transaction fact records a business event.

For this phase:

```text
fact_sales
grain = one row per order line
```

Typical columns:

```text
order_id
line_number
date_key
product_key
store_key
quantity
unit_price
discount_amount
gross_sales
net_sales
```

The fact preserves the detailed event grain and stores measures that can be aggregated.

### Snapshot facts

A snapshot fact records the state of something at a point or period in time.

For this phase:

```text
fact_inventory_snapshot
grain = one row per snapshot date, store, and product
```

Typical measures:

```text
on_hand_quantity
reorder_point
```

Unlike a transaction fact, a snapshot row describes state rather than a sale event.

### Business keys

A **business key** identifies an entity in the source/business domain.

Examples:

```text
product_id
store_id
order_id
```

Business keys can be meaningful externally, can arrive from source systems, and may sometimes change.

### Surrogate keys

A **surrogate key** is a warehouse-managed identifier used to identify a dimension row independently of the source business key.

Examples:

```text
product_key
store_key
date_key
```

Why they matter:

- isolate facts from source-system identifier changes;
- support dimension-history patterns;
- provide stable warehouse relationships;
- allow multiple source systems to map into one conformed dimension.

Do not confuse a surrogate key with a random row number generated afresh on every pipeline run. A production surrogate-key strategy must preserve key stability.

### Conformed dimensions

A conformed dimension is shared consistently across multiple facts.

In this model:

```text
dim_product
dim_store
dim_date
```

can be used by both:

```text
fact_sales
fact_inventory_snapshot
```

That gives both facts the same definitions of product, store, and date.

### Slowly changing dimensions

Dimension attributes can change over time.

Example:

```text
product category changes
store region changes
product description changes
```

Conceptual patterns:

**Type 1**

```text
overwrite the old attribute value
```

Use when only the current state matters.

**Type 2**

```text
insert a new dimension version
preserve the old version
track effective dates/current flag
```

Use when facts must retain historical context.

Phase 2 requires the concept, not a full SCD implementation.

### Derived measures

A derived measure is calculated from source columns.

Example:

```python
sales_df = (
    sales_df
    .withColumn(
        'gross_sales',
        F.col('quantity') * F.col('unit_price'),
    )
    .withColumn(
        'discount_amount',
        F.col('gross_sales') * F.col('discount_pct'),
    )
    .withColumn(
        'net_sales',
        F.col('gross_sales') - F.col('discount_amount'),
    )
)
```

Engineering requirements for derived measures:

- define the formula;
- use appropriate numeric types;
- specify NULL behavior;
- calculate at the correct grain;
- reconcile aggregated results back to trusted inputs.

### Fact-table grain must be declared before joins

If `fact_sales` must be one row per order line, then enrichment joins must preserve that grain.

Typical path:

```text
order_items: one row per order line
    ↓ many-to-one join
orders: one row per order
    ↓ many-to-one join
dim_product: one row per product
    ↓ many-to-one join
dim_store: one row per store
    ↓ many-to-one join
dim_date: one row per date
    ↓
fact_sales remains one row per order line
```

If any supposedly unique right-side key is duplicated, this path can multiply rows and corrupt measures.

---

[Back to Table of Contents](#toc)

---

<a id="7-retail-analytical-model"></a>
## 7. Retail Analytical Model

The intended architecture is:

```text
Raw Orders
Raw Order Items
Raw Products
Raw Stores
Inventory Snapshots
        ↓
      PySpark
        ↓
dim_product
dim_store
dim_date
fact_sales
fact_inventory_snapshot
```

### Source grains

#### `raw_orders`

```text
one row = one order
business key = order_id
```

Typical attributes:

```text
order_id
store_id
order_date
order_ts
order_status
customer_id
```

#### `raw_order_items`

```text
one row = one line within an order
business key = (order_id, line_number)
```

Typical attributes:

```text
order_id
line_number
product_id
quantity
unit_price
discount_pct
```

#### `raw_products`

```text
one row = one product
business key = product_id
```

Typical attributes:

```text
product_id
product_name
category
```

#### `raw_stores`

```text
one row = one store
business key = store_id
```

Typical attributes:

```text
store_id
store_name
province
```

#### `inventory_snapshots`

Source event grain:

```text
one row = one observed inventory record
```

The source may contain multiple arrivals for the same target snapshot grain, so deterministic latest-record selection may be required before modeling.

Target daily grain:

```text
(snapshot_date, store_id, product_id)
```

### Dimension grains

#### `dim_product`

```text
one row = one current product dimension member
business key = product_id
surrogate key = product_key
```

#### `dim_store`

```text
one row = one current store dimension member
business key = store_id
surrogate key = store_key
```

#### `dim_date`

```text
one row = one calendar date
surrogate/business-friendly warehouse key = date_key
```

A common deterministic date key is:

```text
YYYYMMDD
```

Example:

```text
2026-08-23 → 20260823
```

### Fact grains

#### `fact_sales`

```text
one row = one order line
natural grain = (order_id, line_number)
```

Dimension relationships:

```text
product_key → dim_product
store_key   → dim_store
date_key    → dim_date
```

Typical measures:

```text
quantity
unit_price
gross_sales
discount_amount
net_sales
```

This is a **transaction fact**.

#### `fact_inventory_snapshot`

```text
one row = one product in one store on one snapshot date
grain = (date_key, store_key, product_key)
```

Typical measures:

```text
on_hand_quantity
reorder_point
```

This is a **snapshot fact**.

### Why both facts can share dimensions

The sales fact and inventory fact measure different business processes but both refer to:

```text
product
store
date
```

Using the same conformed dimensions enables analysis such as:

```text
sales and inventory by the same product category
sales and inventory by the same store
sales and inventory by the same calendar period
```

### Correct construction order

A safe conceptual order is:

```text
1. Validate source keys.
2. Deduplicate sources only according to an explicit rule.
3. Build or resolve dimensions at their intended grain.
4. Enrich fact sources only with many-to-one joins.
5. Calculate measures at fact grain.
6. Validate fact keys/grain.
7. Validate dimension referential integrity.
8. Reconcile row counts and measures.
```

The target model should drive the transformation choices, not the other way around.

---

[Back to Table of Contents](#toc)

---

<a id="8-engineering-decision-rules"></a>
## 8. Engineering Decision Rules

Use these rules while writing Phase 2 transformations.

### Before an aggregation

Ask:

```text
What is the current grain?
What should the output grain be?
Which columns define that output grain?
Which metrics are valid to aggregate?
```

### Before a join

Ask:

```text
What is the grain of the left DataFrame?
What is the grain of the right DataFrame?
What columns form the complete join key?
Are those keys unique where they are supposed to be?
Is the relationship one-to-one, one-to-many, many-to-one, or many-to-many?
Can this join multiply rows?
What should happen to unmatched rows?
```

### Before a window

Ask:

```text
What rows belong in the same window partition?
What ordering represents the business sequence?
Are tie-breakers required for deterministic output?
Does the calculation need the entire partition, a running frame, a row frame, or a value/range frame?
Should the original grain remain unchanged?
```

### Before deduplication

Ask:

```text
What defines a duplicate?
Does any survivor work?
If not, what exact ordering chooses the survivor?
Is that ordering deterministic under ties?
```

### Before `explode`

Ask:

```text
What does one array element represent?
What will one output row represent after expansion?
Should NULL/empty parents disappear or be retained?
```

### Before a union

Ask:

```text
Do both DataFrames represent the same conceptual row type?
Are schemas compatible?
Should matching be positional or by name?
Are duplicates expected to be retained?
```

### Before writing a fact or dimension

Ask:

```text
What does one row represent?
What key proves that grain?
Which columns are business keys?
Which are surrogate keys?
Which measures are additive or derived?
What dimension relationships must be valid?
```

---

[Back to Table of Contents](#toc)

---

<a id="9-common-failure-modes"></a>
## 9. Common Failure Modes

### 1. Joining before understanding grain

Symptom:

```text
revenue suddenly doubles or triples
```

Cause:

```text
a supposedly unique join key was duplicated
or
a many-to-many join was performed unintentionally
```

Fix:

```text
validate keys
state cardinality
pre-aggregate if necessary
reconcile row counts and measures
```

### 2. Treating a left join as automatically row-preserving

A left join preserves every left row, but it can still produce **more** than one output row per left row if the right key is duplicated.

### 3. Using `dropDuplicates` for latest-record logic

`dropDuplicates(['business_key'])` does not encode newest, highest priority, or most trusted.

Use an ordered `row_number()` window when the survivor matters.

### 4. Nondeterministic `row_number`

Ordering only by a timestamp is insufficient if timestamps can tie.

Add stable tie-breakers such as ingestion sequence or source record ID.

### 5. Confusing `partitionBy` with `groupBy`

`groupBy` collapses rows.

`Window.partitionBy` defines analytic groups while preserving rows.

### 6. Confusing a three-row rolling window with three calendar days

`rowsBetween(-2, 0)` uses row positions.

A time-based window should use an ordering value appropriate for a range frame.

### 7. Using `union` with reordered columns

`union` is positional. Matching column names do not protect against incorrect column order.

Use `unionByName` for most batch-combination pipelines.

### 8. Forgetting that `explode` changes grain

A product-level row with five tags becomes five product-tag rows. Any downstream metric must now be interpreted at that expanded grain.

### 9. Joining on an incomplete composite key

Joining inventory thresholds on `product_id` when uniqueness is `(store_id, product_id)` can match a product to thresholds from multiple stores.

### 10. Summing measures after a row-multiplying enrichment

Measures are only trustworthy if the enrichment preserves the intended fact grain or the metric intentionally operates at the expanded grain.

---

[Back to Table of Contents](#toc)

---

<a id="10-phase-2-mastery-reference"></a>
## 10. Phase 2 Mastery Reference

Phase 2 lecture knowledge should make the following reasoning natural before applied work begins.

### Aggregations

You should be able to explain:

- what `groupBy()` changes about grain;
- why grouped columns define the output grain;
- when to use `count`, `sum`, `avg`, `min`, `max`, and `countDistinct`;
- how conditional aggregation works;
- why aggregating after a multiplied join can corrupt metrics;
- when pre-aggregation is required before a join.

### Joins

You should be able to explain and use:

- inner;
- left;
- right;
- full;
- semi;
- anti;
- cross;
- multi-column joins;
- aliases and explicit projections for duplicate column names;
- duplicate-key checks;
- referential-integrity checks;
- one-to-one;
- one-to-many;
- many-to-one;
- many-to-many;
- row-count expectations for each relationship.

### Windows

You should be able to explain and use:

- `Window.partitionBy()`;
- `Window.orderBy()`;
- `row_number()`;
- `rank()`;
- `dense_rank()`;
- `lag()`;
- `lead()`;
- running aggregates;
- rolling row frames;
- rolling range frames;
- latest-record selection;
- deterministic deduplication.

You should be able to state why:

```text
groupBy collapses rows
window functions preserve rows
```

### Other transformations

You should be able to distinguish:

```text
union vs. join
union vs. unionByName
dropDuplicates vs. row_number selection
explode before vs. after grain
pivot vs. unpivot
```

### Data modeling

You should be able to explain:

- grain;
- facts vs. dimensions;
- transaction facts;
- snapshot facts;
- business keys;
- surrogate keys;
- star schemas;
- conformed dimensions;
- Type 1 vs. Type 2 SCD concepts;
- derived measures.

For the retail model, you should be able to state the intended grain of:

```text
dim_product
dim_store
dim_date
fact_sales
fact_inventory_snapshot
```

The final governing rule is:

> **Choose the transformation from the target grain, then prove that the result still means what you think one row means.**

---

[Back to Table of Contents](#toc)
