# Phase 3 — Spark SQL

<a id="toc"></a>
## Table of Contents

- [Objective](#objective)
- [Phase Mental Model](#phase-mental-model)
- [1. DataFrames, Temp Views, and `spark.sql()`](#1-dataframes-temp-views-and-spark-sql)
- [2. SQL/DataFrame Equivalence](#2-sql-dataframe-equivalence)
- [3. Common Table Expressions](#3-common-table-expressions)
- [4. Joins in Spark SQL](#4-joins-in-spark-sql)
- [5. Aggregations in Spark SQL](#5-aggregations-in-spark-sql)
- [6. Window Functions in Spark SQL](#6-window-functions-in-spark-sql)
- [7. SQL for Data Validation](#7-sql-for-data-validation)
- [8. Reconciliation](#8-reconciliation)
- [9. Engineering Decision Rules](#9-engineering-decision-rules)
- [10. Common Failure Modes](#10-common-failure-modes)
- [11. Phase 3 Mastery Reference](#11-phase-3-mastery-reference)

---

<a id="objective"></a>
## Objective

Become fluent moving between **Spark SQL** and the **PySpark DataFrame API**, understanding them as two interfaces to essentially the same Spark query engine.

Phase 3 covers:

- `createOrReplaceTempView()`;
- `spark.sql()`;
- SQL/DataFrame equivalence;
- CTEs;
- joins;
- aggregations;
- window functions;
- validation queries;
- reconciliation queries.

Examples target **PySpark 4.2.0** and continue the retail domain from Phase 2: orders, order items, products, stores, inventory snapshots, and line-level sales measures.

The priority is not SQL syntax memorization. It is being able to express the same data-engineering intent clearly in either interface while preserving **grain, keys, schemas, and correctness**.

This document is lecture/reference material only. The Phase 3 mastery pipeline is intentionally not implemented here.

---

[Back to Table of Contents](#toc)

---

<a id="phase-mental-model"></a>
## Phase Mental Model

The core model is:

```text
DataFrame API                         Spark SQL
     │                                    │
     └──────────── logical query ─────────┘
                         │
                    Spark engine
                         │
              optimized physical work
                         │
                    distributed data
```

A DataFrame method chain and a SQL query are not two unrelated processing systems. Both describe a computation that Spark can analyze, optimize, and execute.

For example:

```text
DataFrame API
sales_df.groupBy('store_id').agg(...)

                ↕

Spark SQL
SELECT store_id, ...
FROM sales
GROUP BY store_id
```

The exact parsed plans need not be textually identical for every equivalent spelling, but equivalent expressions usually converge toward equivalent Spark query plans and execution strategies.

The engineering reasoning is therefore shared across both interfaces:

```text
What is the input grain?
        ↓
What output grain is required?
        ↓
What keys and predicates define the relationship?
        ↓
Will rows be preserved, collapsed, filtered, or multiplied?
        ↓
How will correctness be validated and reconciled?
```

### Grain rules still govern SQL

SQL does not make grain problems disappear.

- `GROUP BY` collapses rows to the grouped grain.
- Window functions preserve rows.
- A one-to-many join can multiply rows.
- An incomplete join key can create false matches.
- A many-to-many join can overstate measures.
- `ROW_NUMBER()` can deterministically select one row per business key.

The same Phase 2 question remains mandatory before every join:

```text
What is the grain of each input?
What are the actual join keys?
Is the relationship one-to-one, one-to-many, many-to-one, or many-to-many?
Can this join multiply rows?
```

---

[Back to Table of Contents](#toc)

---

<a id="1-dataframes-temp-views-and-spark-sql"></a>
## 1. DataFrames, Temp Views, and `spark.sql()`

### `createOrReplaceTempView()`

A DataFrame exists in Python as a reference to a Spark query plan. To make that relation addressable from SQL, register it under a temporary view name:

```python
sales_enriched_df.createOrReplaceTempView('sales_enriched')
```

Conceptually:

```text
Python variable
sales_enriched_df
      │
      │ createOrReplaceTempView('sales_enriched')
      ↓
Session catalog name
sales_enriched
      │
      ↓
Logical relation / query plan
```

`createOrReplaceTempView()` does **not** normally:

- copy every row into a new dataset;
- write a table to disk;
- cache the DataFrame;
- trigger a Spark job merely to register the name.

It registers a session-scoped SQL name that refers to the DataFrame's logical relation.

If a view with the same name already exists in that Spark session, `createOrReplaceTempView()` replaces the view definition.

### Scope and lifetime

A normal temporary view is **session-scoped**.

It is available to SQL executed through the Spark session that owns it and disappears when that session ends. It is not a durable warehouse table and should not be treated as persisted storage.

You can remove it explicitly:

```python
spark.catalog.dropTempView('sales_enriched')
```

Spark also has global temporary views, but they solve a different scope problem and are not required for Phase 3.

### `spark.sql()` returns a DataFrame

A `SELECT` query submitted with `spark.sql()` returns another PySpark DataFrame:

```python
completed_sales_df = spark.sql(
    '''
    SELECT *
    FROM sales_enriched
    WHERE order_status = 'COMPLETED'
    '''
)
```

This means you can move between interfaces whenever it improves clarity:

```python
result_df = spark.sql(
    '''
    SELECT store_id, SUM(net_sales) AS net_sales
    FROM sales_enriched
    GROUP BY store_id
    '''
)

final_df = result_df.filter(F.col('net_sales') > 50)
```

Likewise, a DataFrame created through the DataFrame API can be registered and queried again from SQL.

### SQL remains lazy for query transformations

For the `SELECT`-style analytical queries used in this phase:

```python
result_df = spark.sql('SELECT * FROM sales_enriched')
```

constructs a DataFrame representing the query. It does not scan all source data merely because `spark.sql()` was called.

An action such as these triggers execution:

```python
result_df.show()
result_df.count()
result_df.collect()
result_df.write.parquet('...')
```

So the familiar model remains:

```text
SQL text
   ↓
spark.sql(...)
   ↓
DataFrame / logical query
   ↓
action
   ↓
Spark job executes
```

Do not overgeneralize this rule to every possible SQL command: DDL or DML commands can have side effects. The important Phase 3 point is that analytical `SELECT` queries participate in Spark's normal lazy DataFrame execution model.

### DataFrame, temp view, and SQL query

These are three different concepts:

| Concept | Meaning |
|---|---|
| DataFrame | Python-side reference to a Spark relation/query plan |
| Temporary view | Session-scoped SQL name for a relation |
| SQL query | Declarative expression that Spark converts into a DataFrame/query plan |

A useful flow is:

```text
orders_df
    ↓ register
orders
    ↓ SQL query
spark.sql('SELECT ... FROM orders')
    ↓
new DataFrame
```

No separate SQL engine is introduced.

---

[Back to Table of Contents](#toc)

---

<a id="2-sql-dataframe-equivalence"></a>
## 2. SQL/DataFrame Equivalence

The goal is not to translate every method mechanically. Learn the major semantic correspondences.

### Projection: `select()` ↔ `SELECT`

**DataFrame API**

```python
projected_df = sales_df.select(
    'order_id',
    'store_id',
    'net_sales',
)
```

**Spark SQL**

```sql
SELECT
    order_id,
    store_id,
    net_sales
FROM sales
```

Projection normally preserves row grain while changing the columns carried forward.

### Filtering: `filter()` / `where()` ↔ `WHERE`

**DataFrame API**

```python
completed_df = sales_df.filter(
    F.col('order_status') == 'COMPLETED'
)
```

**Spark SQL**

```sql
SELECT *
FROM sales
WHERE order_status = 'COMPLETED'
```

Filtering preserves the meaning of one row but keeps only qualifying rows.

### Derived columns ↔ SQL expressions

**DataFrame API**

```python
sales_with_cost_df = sales_df.withColumn(
    'extended_cost',
    F.col('quantity') * F.col('unit_cost'),
)
```

**Spark SQL**

```sql
SELECT
    *,
    quantity * unit_cost AS extended_cost
FROM sales
```

### `when()` / `otherwise()` ↔ `CASE WHEN`

**DataFrame API**

```python
status_df = inventory_df.withColumn(
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

**Spark SQL**

```sql
SELECT
    *,
    CASE
        WHEN on_hand_quantity = 0 THEN 'OUT_OF_STOCK'
        WHEN on_hand_quantity <= reorder_point THEN 'LOW_STOCK'
        ELSE 'HEALTHY'
    END AS inventory_status
FROM inventory
```

Condition order matters in both interfaces.

### Aggregation ↔ `GROUP BY`

**DataFrame API**

```python
store_sales_df = (
    sales_df
    .groupBy('store_id')
    .agg(F.sum('net_sales').alias('net_sales'))
)
```

**Spark SQL**

```sql
SELECT
    store_id,
    SUM(net_sales) AS net_sales
FROM sales
GROUP BY store_id
```

Both change the grain to one row per `store_id`.

### Window specification ↔ `OVER (...)`

**DataFrame API**

```python
w = (
    Window
    .partitionBy('store_id')
    .orderBy('order_date')
)

result_df = daily_sales_df.withColumn(
    'previous_sales',
    F.lag('daily_net_sales').over(w),
)
```

**Spark SQL**

```sql
SELECT
    *,
    LAG(daily_net_sales) OVER (
        PARTITION BY store_id
        ORDER BY order_date
    ) AS previous_sales
FROM daily_sales
```

### Latest-record selection

Both interfaces usually express deterministic deduplication as:

```text
partition by target business key
order by recency descending
add deterministic tie-breaker
assign ROW_NUMBER
keep row_number = 1
```

This is stronger than arbitrary deduplication because the survivor rule is explicit.

### Prefer the clearer interface

Choose based on maintainability and context rather than ideology.

DataFrame API is often convenient when:

- transformations are composed dynamically in Python;
- logic is already inside reusable Python functions;
- expressions depend on Python configuration;
- typed PySpark APIs improve discoverability.

Spark SQL is often convenient when:

- the logic is naturally relational;
- analysts or data engineers need to review the transformation quickly;
- several joins/aggregations read more clearly as a query;
- validation and reconciliation logic is easier to express declaratively.

A professional Spark codebase can legitimately use both.

---

[Back to Table of Contents](#toc)

---

<a id="3-common-table-expressions"></a>
## 3. Common Table Expressions

A **CTE** is a named query expression defined with `WITH` and usable within the surrounding SQL statement.

### Basic `WITH`

```sql
WITH completed_sales AS (
    SELECT *
    FROM sales_enriched
    WHERE order_status = 'COMPLETED'
)
SELECT
    store_id,
    SUM(net_sales) AS net_sales
FROM completed_sales
GROUP BY store_id
```

The CTE name improves readability by giving the intermediate relation a meaningful name.

### Multiple CTEs

```sql
WITH completed_sales AS (
    SELECT *
    FROM sales_enriched
    WHERE order_status = 'COMPLETED'
),
product_sales AS (
    SELECT
        product_id,
        SUM(net_sales) AS net_sales
    FROM completed_sales
    GROUP BY product_id
)
SELECT
    p.product_id,
    p.product_name,
    s.net_sales
FROM products p
INNER JOIN product_sales s
    ON p.product_id = s.product_id
```

### Chained transformation logic

CTEs are especially useful when a transformation has several clear grain transitions:

```text
sales_enriched
    ↓ filter
completed_sales           order-line grain
    ↓ aggregate
product_sales             product grain
    ↓ join
product_sales_named       product grain
    ↓ window
ranked_products           product grain
```

Writing each stage as a CTE makes the grain transition visible.

### CTEs vs. nested subqueries

This:

```sql
SELECT *
FROM (
    SELECT ...
    FROM (
        SELECT ...
    ) x
) y
```

may be logically valid but hard to read.

The equivalent CTE form names the intermediate meaning:

```sql
WITH cleaned AS (...),
enriched AS (...),
aggregated AS (...)
SELECT *
FROM aggregated
```

Use CTEs to express a pipeline as named relational steps.

### A CTE is not automatically persisted

A CTE is primarily a **query-organization construct**.

Do not assume that:

- Spark writes the CTE to disk;
- Spark caches the CTE;
- the CTE is a durable table;
- every CTE forces a separate stage or materialization boundary.

The optimizer sees the query as a whole and decides how to execute it.

If an intermediate result truly needs persistence or reuse across separate actions, that is a separate engineering decision involving views, tables, caching/persistence, or writes—not a consequence of merely using `WITH`.

---

[Back to Table of Contents](#toc)

---

<a id="4-joins-in-spark-sql"></a>
## 4. Joins in Spark SQL

The join syntax is easy. Correct join reasoning is the actual skill.

### Join checklist

Before writing any SQL join, state:

```text
LEFT grain:
RIGHT grain:
Join keys:
Cardinality:
Expected output grain:
Could rows multiply?
```

### Inner join

```sql
SELECT
    o.order_id,
    i.line_number,
    i.product_id
FROM orders o
INNER JOIN order_items i
    ON o.order_id = i.order_id
```

If `orders` is one row per order and `order_items` is one row per order line, this is a one-to-many join that intentionally produces order-line grain.

### Left join

```sql
SELECT
    i.order_id,
    i.line_number,
    i.product_id,
    p.product_name
FROM order_items i
LEFT JOIN products p
    ON i.product_id = p.product_id
```

This preserves every left row, but it preserves **left row count only if the right-side join key is unique**. Duplicate product rows can multiply order lines.

### Semi join

A left semi join returns only left rows that have a match:

```sql
SELECT i.*
FROM order_items i
LEFT SEMI JOIN products p
    ON i.product_id = p.product_id
```

This is useful for existence filtering.

### Anti join

A left anti join returns only left rows with no match:

```sql
SELECT i.*
FROM order_items i
LEFT ANTI JOIN products p
    ON i.product_id = p.product_id
```

This directly supports orphan detection and referential-integrity validation.

### Multi-column joins

If the relationship is defined by multiple columns, use the complete key:

```sql
SELECT
    i.snapshot_date,
    i.store_id,
    i.product_id,
    i.on_hand_quantity,
    t.target_on_hand
FROM latest_inventory i
LEFT JOIN store_product_targets t
    ON i.store_id = t.store_id
   AND i.product_id = t.product_id
```

Joining only on `product_id` would ignore part of the target table's grain and could create false matches across stores.

### Aliases and qualified references

Aliases make ownership explicit:

```sql
SELECT
    o.order_id,
    o.store_id,
    s.store_name,
    s.province
FROM orders o
LEFT JOIN stores s
    ON o.store_id = s.store_id
```

Read:

```text
o.order_id     → order_id from alias o
s.store_name   → store_name from alias s
```

Qualified references are especially important when both inputs have columns with the same name.

### `ON` vs. `USING`

When both sides use the same join-key name, Spark SQL can also use:

```sql
SELECT *
FROM orders
INNER JOIN order_items
USING (order_id)
```

`USING` is concise and emits a single merged join-key column. `ON` is more general and is required when key names differ or the condition is more complex.

### Many-to-many danger

Suppose:

```text
order_items
one row per order line
product_id repeats

product_promotions
one row per product-promotion
product_id repeats
```

Then:

```sql
SELECT *
FROM order_items i
INNER JOIN product_promotions p
    ON i.product_id = p.product_id
```

is many-to-many by `product_id` and can duplicate sales measures.

If the business requirement needs only product-level promotion attributes, pre-aggregate promotions to one row per product before joining.

---

[Back to Table of Contents](#toc)

---

<a id="5-aggregations-in-spark-sql"></a>
## 5. Aggregations in Spark SQL

Grouped columns define the result grain.

### Core aggregates

```sql
SELECT
    product_id,
    COUNT(*) AS line_count,
    SUM(quantity) AS units_sold,
    SUM(net_sales) AS net_sales,
    AVG(unit_price) AS avg_unit_price,
    MIN(unit_price) AS min_unit_price,
    MAX(unit_price) AS max_unit_price,
    COUNT(DISTINCT order_id) AS distinct_order_count
FROM sales_enriched
GROUP BY product_id
```

Output grain:

```text
one row per product_id
```

Add another grouped column:

```sql
GROUP BY store_id, product_id
```

and the grain becomes:

```text
one row per store and product
```

### `COUNT(*)` vs. `COUNT(column)`

- `COUNT(*)` counts rows.
- `COUNT(column)` counts non-NULL values in that column.
- `COUNT(DISTINCT column)` counts distinct non-NULL values.

Pick the aggregate that matches the business question.

### Conditional aggregation

Conditional aggregation lets one grouped query calculate metrics for row subsets:

```sql
SELECT
    product_id,
    SUM(
        CASE
            WHEN discount_pct > 0 THEN net_sales
            ELSE 0
        END
    ) AS discounted_net_sales,
    SUM(
        CASE
            WHEN discount_pct > 0 THEN 1
            ELSE 0
        END
    ) AS discounted_line_count
FROM sales_enriched
GROUP BY product_id
```

The `CASE` expression defines each row's contribution; `SUM` reduces those values at the grouped grain.

### Filtering before vs. after aggregation

`WHERE` filters input rows before grouping:

```sql
SELECT
    product_id,
    SUM(net_sales) AS net_sales
FROM sales_enriched
WHERE order_status = 'COMPLETED'
GROUP BY product_id
```

`HAVING` filters groups after aggregation:

```sql
SELECT
    product_id,
    SUM(net_sales) AS net_sales
FROM sales_enriched
WHERE order_status = 'COMPLETED'
GROUP BY product_id
HAVING SUM(net_sales) >= 50
```

Do not use these interchangeably: they apply at different points in the relational logic.

---

[Back to Table of Contents](#toc)

---

<a id="6-window-functions-in-spark-sql"></a>
## 6. Window Functions in Spark SQL

The central distinction is:

```text
GROUP BY
    → collapses rows

window function
    → preserves rows
```

A window function computes across related rows while retaining each input row.

### Window syntax

```sql
FUNCTION(...) OVER (
    PARTITION BY ...
    ORDER BY ...
    ROWS BETWEEN ... AND ...
)
```

Conceptually:

```text
PARTITION BY
    → which rows belong to the same independent group?

ORDER BY
    → what sequence exists inside each partition?

frame
    → which rows around the current row participate?
```

This maps directly to PySpark:

```python
Window.partitionBy(...).orderBy(...).rowsBetween(...)
```

### `ROW_NUMBER()`

```sql
SELECT
    *,
    ROW_NUMBER() OVER (
        PARTITION BY category
        ORDER BY net_sales DESC, product_id ASC
    ) AS row_number
FROM product_revenue
```

`ROW_NUMBER()` assigns a unique sequence position. If ties are possible and output must be deterministic, include a stable tie-breaker such as `product_id`.

### `RANK()` and `DENSE_RANK()`

```sql
SELECT
    *,
    RANK() OVER (
        PARTITION BY category
        ORDER BY net_sales DESC
    ) AS rank,
    DENSE_RANK() OVER (
        PARTITION BY category
        ORDER BY net_sales DESC
    ) AS dense_rank
FROM product_revenue
```

For equal ordering values:

```text
RANK        → ties share rank; later ranks can have gaps
DENSE_RANK  → ties share rank; no gaps
ROW_NUMBER  → every row gets a unique sequence number
```

Do not add a unique tie-breaker to the `RANK()` ordering if the business rule intends equal sales to remain tied.

### `LAG()` and `LEAD()`

```sql
SELECT
    *,
    LAG(daily_net_sales) OVER (
        PARTITION BY store_id
        ORDER BY order_date
    ) AS previous_observed_sales,
    LEAD(daily_net_sales) OVER (
        PARTITION BY store_id
        ORDER BY order_date
    ) AS next_observed_sales
FROM daily_store_sales
```

These refer to neighboring **observed rows**. If dates are missing, the previous row is not necessarily the previous calendar day.

### Running aggregate

```sql
SELECT
    *,
    SUM(daily_net_sales) OVER (
        PARTITION BY store_id
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_net_sales
FROM daily_store_sales
```

The result remains one row per store-day.

### Rolling row frame

```sql
SELECT
    *,
    SUM(daily_net_sales) OVER (
        PARTITION BY store_id
        ORDER BY order_date
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS rolling_3_observation_sales
FROM daily_store_sales
```

This means current row plus the two previous **observed rows**, not necessarily three calendar days.

### Rolling value/range frame

For a true seven-day window, convert the date to a numeric day coordinate and use a range frame:

```sql
WITH dated AS (
    SELECT
        *,
        DATEDIFF(order_date, DATE '1970-01-01') AS day_number
    FROM daily_store_sales
)
SELECT
    store_id,
    order_date,
    daily_net_sales,
    SUM(daily_net_sales) OVER (
        PARTITION BY store_id
        ORDER BY day_number
        RANGE BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7_day_sales
FROM dated
```

`ROWS` uses row positions. `RANGE` uses ordering values.

### Latest-record selection

Suppose raw inventory contains multiple observations at target grain:

```text
(snapshot_date, store_id, product_id)
```

A deterministic latest-record rule is:

```sql
WITH ranked_inventory AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY snapshot_date, store_id, product_id
            ORDER BY snapshot_ts DESC, ingestion_id DESC
        ) AS row_number
    FROM inventory
)
SELECT
    snapshot_date,
    snapshot_ts,
    ingestion_id,
    store_id,
    product_id,
    on_hand_quantity,
    reorder_point
FROM ranked_inventory
WHERE row_number = 1
```

This preserves exactly one row per target key while encoding which source record wins.

---

[Back to Table of Contents](#toc)

---

<a id="7-sql-for-data-validation"></a>
## 7. SQL for Data Validation

SQL is not only an analytical language. It is one of the clearest ways to express data contracts and investigate violations.

### Row count

```sql
SELECT COUNT(*) AS row_count
FROM fact_sales
```

Row counts are useful evidence, but they are meaningful only in relation to expected grain and transformation behavior.

### Primary-key duplicate detection

For a table expected to have one row per `order_id`:

```sql
SELECT
    order_id,
    COUNT(*) AS row_count
FROM orders
GROUP BY order_id
HAVING COUNT(*) > 1
```

Expected result: zero rows.

### Composite-key duplicate detection

For order-line grain:

```sql
SELECT
    order_id,
    line_number,
    COUNT(*) AS row_count
FROM order_items
GROUP BY order_id, line_number
HAVING COUNT(*) > 1
```

For inventory snapshot fact grain:

```sql
SELECT
    snapshot_date,
    store_id,
    product_id,
    COUNT(*) AS row_count
FROM fact_inventory_snapshot
GROUP BY snapshot_date, store_id, product_id
HAVING COUNT(*) > 1
```

A non-empty result disproves the claimed uniqueness contract.

### Required-field / NULL checks

```sql
SELECT *
FROM fact_sales
WHERE order_id IS NULL
   OR line_number IS NULL
   OR product_id IS NULL
   OR store_id IS NULL
   OR net_sales IS NULL
```

For validation, returning the violating rows is often more useful than returning only a count because it gives evidence for debugging.

### Referential-integrity / orphan checks

```sql
SELECT f.*
FROM fact_sales f
LEFT ANTI JOIN products p
    ON f.product_id = p.product_id
```

Expected result: zero rows when every fact product key resolves to a valid product.

Equivalent DataFrame pattern:

```python
orphans_df = fact_sales_df.join(
    products_df.select('product_id'),
    on='product_id',
    how='left_anti',
)
```

### Grain validation

Grain is usually validated through a combination of:

1. known row meaning;
2. uniqueness of the key that represents that meaning;
3. required-key non-nullness;
4. expected row-count behavior after transformations.

For example, claiming:

```text
fact_sales grain = one row per order line
```

implies that `(order_id, line_number)` should be unique and non-null.

### Validation queries should be executable evidence

Prefer queries whose expected result is explicit:

```text
Duplicate-key query → expected 0 rows
Orphan query        → expected 0 rows
Required-null query → expected 0 rows
Row-count query     → expected known relationship
```

This turns assumptions into observable contracts.

---

[Back to Table of Contents](#toc)

---

<a id="8-reconciliation"></a>
## 8. Reconciliation

**Validation** asks whether a dataset satisfies its own rules.

**Reconciliation** asks whether two representations of data agree in the way the transformation contract says they should.

Treat reconciliation as first-class engineering work, not a final eyeballing step.

### Row-count reconciliation

If an enrichment is many-to-one and no rows are intentionally filtered, the left row count should normally be preserved:

```text
order lines before product join
=
order lines after product join
```

If the transformation intentionally aggregates from order-line grain to store-day grain, those row counts should **not** be expected to match.

Reconcile what the transformation promises, not what happens to be easy to count.

### Measure-total reconciliation

Suppose `fact_sales` preserves accepted completed order lines. A useful reconciliation is:

```sql
SELECT SUM(net_sales) AS source_net_sales
FROM source_sales_lines
WHERE order_status = 'COMPLETED'
```

against:

```sql
SELECT SUM(net_sales) AS fact_net_sales
FROM fact_sales
```

The filters and business eligibility rules must be aligned before comparing totals.

### DataFrame implementation vs. SQL implementation

At Phase 3 mastery, equivalent implementations should agree on:

- schema or intentionally normalized schema;
- row count;
- target-grain uniqueness;
- key coverage;
- measures;
- exact rows, including duplicate multiplicity where duplicates are meaningful.

### Exact mismatch detection

Do not rely on two `.show()` outputs looking the same.

For aligned schemas, compare both directions with duplicate-sensitive set difference.

**DataFrame API**

```python
left_only_df = dataframe_result_df.exceptAll(sql_result_df)
right_only_df = sql_result_df.exceptAll(dataframe_result_df)
```

Exact reconciliation requires both mismatch DataFrames to be empty.

**Spark SQL**

```sql
SELECT * FROM dataframe_result
EXCEPT ALL
SELECT * FROM sql_result
```

and the reverse direction:

```sql
SELECT * FROM sql_result
EXCEPT ALL
SELECT * FROM dataframe_result
```

Why both directions?

```text
A EXCEPT ALL B
```

only finds rows overrepresented or unique on side `A`. The reverse finds discrepancies on side `B`.

Why `ALL`?

Duplicate-sensitive reconciliation must not silently discard row multiplicity.

### Schema reconciliation

Exact row equality is not the whole contract. Also compare:

- column names;
- column order when required by downstream interfaces;
- Spark data types;
- nullability expectations where relevant.

A query can return numerically equal values under a different type, which may still matter to downstream systems.

### Decimal vs. floating-point reconciliation

For currency-like values, prefer `DecimalType` and decimal SQL expressions so exact equality is meaningful.

If a pipeline intentionally uses floating-point values, exact equality may be inappropriate due to binary floating-point behavior; use a documented numerical tolerance instead.

### What should reconcile after grain changes?

| Transformation | Expected reconciliation |
|---|---|
| projection / derived column | row count usually preserved; keys preserved |
| filter | output rows = qualifying source rows |
| many-to-one enrichment | left row count and key multiplicity preserved if right key unique |
| one-to-many expansion | row count can increase by design |
| aggregation | row count changes; grouped key uniqueness and additive measures should reconcile |
| latest-record selection | one output row per target key; every survivor must come from source |
| invalid-record rejection | accepted + rejected should reconcile to source under exclusive/exhaustive rules |

Never demand source row count = target row count when the transformation intentionally changes grain.

---

[Back to Table of Contents](#toc)

---

<a id="9-engineering-decision-rules"></a>
## 9. Engineering Decision Rules

1. **State grain before syntax.** SQL and DataFrame API can both produce logically wrong results with perfectly valid syntax.
2. **Validate join-side uniqueness when row preservation depends on it.** A left join does not guarantee unchanged row count.
3. **Use complete business keys.** Omitting one component of a composite relationship can create false matches.
4. **Use aliases aggressively in multi-table SQL.** `o.order_id` is easier to reason about than an ambiguous `order_id`.
5. **Name CTEs by business meaning.** `completed_sales` is more maintainable than `cte1`.
6. **Treat CTEs as logical organization, not persistence.** Do not infer caching or materialization.
7. **Separate grouped grain from window partitioning.** `GROUP BY` changes row count; windows do not.
8. **Make deduplication deterministic.** Define recency and tie-breakers explicitly.
9. **Write validations that return violations.** Zero-row outputs are easy to interpret and automate later.
10. **Reconcile contracts, not screenshots.** Use counts, keys, measures, schemas, and exact mismatch detection.
11. **Prefer decimal arithmetic for business measures requiring exact reconciliation.**
12. **Use the interface that makes intent clearest.** DataFrame API and SQL can coexist in one pipeline.

---

[Back to Table of Contents](#toc)

---

<a id="10-common-failure-modes"></a>
## 10. Common Failure Modes

### Assuming a temp view copies data

It does not normally materialize a second dataset. It registers a session-scoped SQL name for a relation.

### Assuming `spark.sql()` is a separate execution engine

It returns DataFrames that participate in the same Spark query planning and distributed execution machinery as DataFrame API transformations.

### Assuming a CTE is cached

`WITH` improves query structure. It does not automatically persist intermediate data.

### Using `SELECT *` after complex joins

This can carry duplicate or ambiguous columns and makes ownership unclear. Prefer explicit projections for durable transformations.

### Believing a left join cannot multiply rows

If the right-side key is duplicated, one left row can match multiple right rows.

### Using incomplete join keys

Joining store-product data on product alone can create cross-store false matches.

### Mixing `WHERE` and `HAVING`

`WHERE` filters source rows before aggregation. `HAVING` filters groups after aggregation.

### Using a window when aggregation is required

A window preserves rows. If the desired result is one row per store, `GROUP BY store_id` is usually the correct grain-changing operation.

### Using `ROW_NUMBER()` without deterministic ordering

If ties exist and no stable tie-breaker is provided, which row receives `1` may be nondeterministic.

### Confusing rolling rows with rolling time

`ROWS BETWEEN 6 PRECEDING AND CURRENT ROW` means seven observed rows, not necessarily seven calendar days.

### Reconciliation by `.show()`

Matching samples prove almost nothing about full-output equality. Use programmatic mismatch detection.

### Comparing only row counts

Two datasets can have the same row count while containing different keys, measures, duplicates, or schemas.

### Using `EXCEPT` instead of `EXCEPT ALL` for duplicate-sensitive comparison

Distinct set semantics can hide multiplicity mismatches.

---

[Back to Table of Contents](#toc)

---

<a id="11-phase-3-mastery-reference"></a>
## 11. Phase 3 Mastery Reference

The curriculum mastery requirement is eventually to implement the **same analytical transformation pipeline twice**:

```text
Implementation A
PySpark DataFrame API

Implementation B
Spark SQL

        ↓
Exact reconciliation
```

That mastery work is intentionally deferred until explicitly requested.

Before beginning it, Phase 3 fluency should include being able to explain and implement:

- how a DataFrame becomes queryable through `createOrReplaceTempView()`;
- why registering a temp view does not copy or persist the data;
- the scope and lifetime of a normal temp view;
- why `spark.sql()` returns a DataFrame;
- lazy execution for analytical SQL queries;
- important SQL/DataFrame equivalents;
- single and multiple CTEs;
- inner, left, semi, anti, and multi-column joins;
- join grain/cardinality consequences;
- grouped aggregates and conditional aggregation;
- `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG`, and `LEAD`;
- running and rolling windows;
- deterministic latest-record selection;
- duplicate, NULL, orphan, and grain validation queries;
- row-count and measure reconciliation;
- exact DataFrame-vs-SQL mismatch detection.

The final standard is not:

> I can translate PySpark syntax into SQL syntax.

It is:

> I can express the same data contract through either interface, predict the grain and row-count consequences, and prove that the outputs agree.

---

[Back to Table of Contents](#toc)
