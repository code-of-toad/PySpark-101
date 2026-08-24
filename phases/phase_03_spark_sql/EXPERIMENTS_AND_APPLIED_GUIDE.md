# Phase 3 — Experiment & Applied Practice Guide

<a id="toc"></a>
## Table of Contents

- [Purpose](#purpose)
- [Conventions](#conventions)
- [Practice Dataset](#practice-dataset)
- [Experiment 1 — Temporary Views and `spark.sql()`](#experiment-1-temp-views-and-spark-sql)
- [Experiment 2 — Core SQL/DataFrame Equivalence](#experiment-2-core-sql-dataframe-equivalence)
- [Experiment 3 — `CASE WHEN`, NULLs, and Expression Equivalence](#experiment-3-case-when-nulls-and-expressions)
- [Experiment 4 — CTEs and Chained Relational Logic](#experiment-4-ctes-and-chained-logic)
- [Experiment 5 — SQL Joins, Cardinality, Aliases, Semi, and Anti](#experiment-5-sql-joins)
- [Experiment 6 — Multi-Column Joins and Incomplete-Key Failures](#experiment-6-multi-column-joins)
- [Experiment 7 — Aggregations, Conditional Aggregation, `WHERE`, and `HAVING`](#experiment-7-aggregations)
- [Experiment 8 — `GROUP BY` vs. Window Functions](#experiment-8-group-by-vs-windows)
- [Experiment 9 — Ranking, `LAG`, `LEAD`, Running, and Rolling Windows](#experiment-9-analytical-windows)
- [Experiment 10 — Deterministic Latest-Record Selection](#experiment-10-latest-record-selection)
- [Experiment 11 — SQL Validation Queries](#experiment-11-validation-queries)
- [Experiment 12 — Reconciliation and Exact SQL/DataFrame Equivalence](#experiment-12-reconciliation)
- [Experiment 13 — Deliberate Logic Bug and Mismatch Detection](#experiment-13-deliberate-mismatch)
- [Applied Phase 3 Project](#applied-phase-3-project)
- [Applied Task — Part 1](#applied-task-part-1)
- [After Part 1](#after-part-1)

---

<a id="purpose"></a>
## Purpose

This file is the execution-oriented companion for **Phase 3 — Spark SQL**.

The Phase 3 README is the conceptual reference. This guide is for hands-on practice.

The central skill is not memorizing SQL syntax independently from PySpark. It is learning to express the **same relational intent** through either interface while preserving the same engineering contracts:

```text
DataFrame API                         Spark SQL
     │                                    │
     └──────────── Spark query ────────────┘
                         │
                 same execution engine
```

Throughout the guide, ask:

```text
What is the input grain?
What is the intended output grain?
What keys define the relationship?
Can rows disappear, collapse, or multiply?
Should row counts reconcile?
Should measures reconcile?
How will I prove the two implementations agree?
```

The goal is to become comfortable moving in both directions:

```text
DataFrame → temp view → SQL → DataFrame API

and

SQL result → DataFrame → temp view → more SQL
```

This guide does **not** complete the Phase 3 mastery pipeline for you. The applied section begins the work in controlled pieces and deliberately stops before the final dual-implementation mastery requirement.

---

[Back to Table of Contents](#toc)

---

<a id="conventions"></a>
## Conventions

- Use **PySpark 4.2.0**.
- Use **single quotes** for Python strings.
- Include inline comments that explain both **what** the code does and **why** the pattern matters.
- Reuse one coherent retail domain throughout.
- Treat Spark SQL and the DataFrame API as two interfaces to the same Spark engine.
- State the grain before joins, aggregations, windows, and reconciliations.
- Validate keys before depending on many-to-one joins.
- Use aliases and qualified references when SQL joins introduce ambiguous column names.
- Use CTEs to express meaningful relational stages, not merely to make a query longer.
- Remember that CTEs and temporary views are logical query constructs; neither automatically means persisted data.
- Treat validation queries as executable data contracts.
- Treat reconciliation as proof, not visual inspection.
- Compare exact outputs in **both directions** when equivalence is required.
- Keep intentionally bad seed rows isolated in dedicated variants unless the experiment specifically needs them in the source relation.
- Do not mark Phase 3 complete until the mastery requirement has been satisfied.
- Do not mark Phase 2 complete merely because Phase 3 practice has begun.

---

[Back to Table of Contents](#toc)

---

<a id="practice-dataset"></a>
# Practice Dataset

Use one coherent set of retail DataFrames throughout the experiments.

The seed data is intentionally chosen so the concepts are visible:

- completed **and** cancelled orders exist;
- one customer is `NULL` so NULL behavior is observable;
- several order lines are discounted and several are not;
- one store has no orders so a left join can produce an unmatched parent row;
- inventory contains repeated observations at the same target grain;
- two inventory observations share the same timestamp so a deterministic tie-breaker is necessary;
- the store-product target table repeats `product_id` across stores so an incomplete join key can produce false matches;
- separate dirty variants will add duplicate keys, NULL required fields, and orphaned foreign keys for validation experiments;
- a dedicated ranking dataset contains ties so `ROW_NUMBER`, `RANK`, and `DENSE_RANK` produce visibly different results.

## Source grains

```text
orders
one row = one order
key = order_id

order_items
one row = one line within one order
key = (order_id, line_number)

products
one row = one product
key = product_id

stores
one row = one store
key = store_id

inventory
one row = one observed inventory record
target daily grain = (snapshot_date, store_id, product_id)

store_product_targets
one row = one store-product target
key = (store_id, product_id)
```

## Create the DataFrames

```python
from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# Create one SparkSession for the Phase 3 practice session.
spark = (
    SparkSession.builder
    .appName('phase03-practice')
    .master('local[*]')
    .getOrCreate()
)


# -------------------------------------------------------------------------
# Orders
# -------------------------------------------------------------------------

# GRAIN:
#     one row per order
#
# KEY:
#     order_id
#
# The seed deliberately includes:
#     - COMPLETED and CANCELLED statuses for filtering;
#     - repeated customers across orders;
#     - one NULL customer_id to make NULL semantics observable;
#     - multiple dates per store for later LAG/LEAD/window exercises.
orders_schema = StructType([
    StructField('order_id', LongType(), False),
    StructField('store_id', StringType(), False),
    StructField('customer_id', StringType(), True),
    StructField('order_date', DateType(), False),
    StructField('order_ts', TimestampType(), False),
    StructField('order_status', StringType(), False),
])

orders_df = spark.createDataFrame(
    [
        (
            1001,
            'S01',
            'C001',
            date(2026, 1, 3),
            datetime(2026, 1, 3, 9, 15),
            'COMPLETED',
        ),
        (
            1002,
            'S01',
            'C002',
            date(2026, 1, 5),
            datetime(2026, 1, 5, 14, 30),
            'COMPLETED',
        ),
        (
            1003,
            'S02',
            'C001',
            date(2026, 1, 5),
            datetime(2026, 1, 5, 17, 45),
            'CANCELLED',
        ),
        (
            1004,
            'S02',
            'C003',
            date(2026, 1, 6),
            datetime(2026, 1, 6, 11, 20),
            'COMPLETED',
        ),
        (
            1005,
            'S01',
            'C004',
            date(2026, 1, 8),
            datetime(2026, 1, 8, 10, 5),
            'COMPLETED',
        ),
        (
            1006,
            'S02',
            None,
            date(2026, 1, 10),
            datetime(2026, 1, 10, 13, 40),
            'COMPLETED',
        ),
        (
            1007,
            'S01',
            'C005',
            date(2026, 1, 12),
            datetime(2026, 1, 12, 16, 10),
            'COMPLETED',
        ),
    ],
    schema=orders_schema,
)


# -------------------------------------------------------------------------
# Order items
# -------------------------------------------------------------------------

# GRAIN:
#     one row per order line
#
# COMPOSITE KEY:
#     (order_id, line_number)
#
# The seed deliberately mixes discounted and undiscounted rows so CASE and
# conditional aggregation produce different values.
order_items_schema = StructType([
    StructField('order_id', LongType(), False),
    StructField('line_number', IntegerType(), False),
    StructField('product_id', StringType(), False),
    StructField('quantity', IntegerType(), False),
    StructField('unit_price', DecimalType(12, 2), False),
    StructField('discount_pct', DecimalType(5, 4), False),
])

order_items_df = spark.createDataFrame(
    [
        (1001, 1, 'P001', 2, Decimal('12.00'), Decimal('0.0000')),
        (1001, 2, 'P002', 1, Decimal('30.00'), Decimal('0.1000')),
        (1002, 1, 'P001', 1, Decimal('12.00'), Decimal('0.0000')),
        (1002, 2, 'P005', 2, Decimal('10.00'), Decimal('0.0500')),
        (1003, 1, 'P002', 2, Decimal('30.00'), Decimal('0.0000')),
        (1004, 1, 'P003', 1, Decimal('50.00'), Decimal('0.1000')),
        (1004, 2, 'P004', 4, Decimal('8.00'), Decimal('0.0000')),
        (1005, 1, 'P004', 5, Decimal('8.00'), Decimal('0.0500')),
        (1006, 1, 'P003', 1, Decimal('50.00'), Decimal('0.0000')),
        (1007, 1, 'P001', 3, Decimal('12.00'), Decimal('0.0500')),
    ],
    schema=order_items_schema,
)


# -------------------------------------------------------------------------
# Products
# -------------------------------------------------------------------------

# GRAIN:
#     one row per product
#
# KEY:
#     product_id
products_schema = StructType([
    StructField('product_id', StringType(), False),
    StructField('product_name', StringType(), False),
    StructField('category', StringType(), False),
    StructField('unit_cost', DecimalType(12, 2), False),
])

products_df = spark.createDataFrame(
    [
        ('P001', 'Coffee Beans', 'GROCERY', Decimal('7.00')),
        ('P002', 'Coffee Grinder', 'EQUIPMENT', Decimal('18.00')),
        ('P003', 'Kettle', 'EQUIPMENT', Decimal('32.00')),
        ('P004', 'Paper Filters', 'GROCERY', Decimal('3.00')),
        ('P005', 'Tea Bags', 'GROCERY', Decimal('4.00')),
        # P006 has no sales and is useful when reasoning about unmatched rows.
        ('P006', 'Digital Scale', 'EQUIPMENT', Decimal('14.00')),
    ],
    schema=products_schema,
)


# -------------------------------------------------------------------------
# Stores
# -------------------------------------------------------------------------

# GRAIN:
#     one row per store
#
# KEY:
#     store_id
#
# S03 intentionally has no orders. A store-left-join-to-orders experiment will
# therefore produce a genuinely unmatched parent row.
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
    schema=stores_schema,
)


# -------------------------------------------------------------------------
# Inventory source observations
# -------------------------------------------------------------------------

# SOURCE GRAIN:
#     one row per observed inventory record
#
# TARGET DAILY GRAIN:
#     one row per snapshot_date, store_id, and product_id
#
# This seed contains TWO kinds of duplicate target-grain observations:
#
# 1. S01/P001 on 2026-01-05 has different timestamps.
#    The later timestamp should win.
#
# 2. S02/P003 on 2026-01-06 has the SAME timestamp twice.
#    ingestion_id must break the tie deterministically.
inventory_schema = StructType([
    StructField('snapshot_date', DateType(), False),
    StructField('snapshot_ts', TimestampType(), False),
    StructField('ingestion_id', LongType(), False),
    StructField('store_id', StringType(), False),
    StructField('product_id', StringType(), False),
    StructField('on_hand_quantity', IntegerType(), False),
    StructField('reorder_point', IntegerType(), False),
])

inventory_df = spark.createDataFrame(
    [
        (
            date(2026, 1, 5),
            datetime(2026, 1, 5, 8, 0),
            1,
            'S01',
            'P001',
            15,
            10,
        ),
        (
            date(2026, 1, 5),
            datetime(2026, 1, 5, 18, 0),
            2,
            'S01',
            'P001',
            12,
            10,
        ),
        (
            date(2026, 1, 5),
            datetime(2026, 1, 5, 18, 0),
            3,
            'S01',
            'P002',
            4,
            5,
        ),
        (
            date(2026, 1, 5),
            datetime(2026, 1, 5, 18, 0),
            4,
            'S02',
            'P003',
            0,
            2,
        ),
        (
            date(2026, 1, 6),
            datetime(2026, 1, 6, 9, 0),
            5,
            'S02',
            'P003',
            3,
            2,
        ),
        (
            date(2026, 1, 6),
            datetime(2026, 1, 6, 9, 0),
            6,
            'S02',
            'P003',
            5,
            2,
        ),
        (
            date(2026, 1, 8),
            datetime(2026, 1, 8, 18, 0),
            7,
            'S01',
            'P004',
            7,
            8,
        ),
    ],
    schema=inventory_schema,
)


# -------------------------------------------------------------------------
# Store-product targets
# -------------------------------------------------------------------------

# GRAIN:
#     one row per store and product
#
# COMPOSITE KEY:
#     (store_id, product_id)
#
# P001 exists for BOTH S01 and S02. This is deliberate: joining inventory to
# targets on product_id alone is therefore demonstrably wrong.
store_product_targets_schema = StructType([
    StructField('store_id', StringType(), False),
    StructField('product_id', StringType(), False),
    StructField('target_on_hand', IntegerType(), False),
])

store_product_targets_df = spark.createDataFrame(
    [
        ('S01', 'P001', 20),
        ('S02', 'P001', 18),
        ('S01', 'P002', 8),
        ('S02', 'P003', 6),
        ('S01', 'P004', 12),
    ],
    schema=store_product_targets_schema,
)
```

## Register the base temp views

```python
# Register session-scoped SQL names for the base relations.
orders_df.createOrReplaceTempView('orders')
order_items_df.createOrReplaceTempView('order_items')
products_df.createOrReplaceTempView('products')
stores_df.createOrReplaceTempView('stores')
inventory_df.createOrReplaceTempView('inventory')
store_product_targets_df.createOrReplaceTempView('store_product_targets')
```

Keep these DataFrames and views available while working through the experiments.

---

[Back to Table of Contents](#toc)

---

<a id="experiment-1-temp-views-and-spark-sql"></a>
# Experiment 1 — Temporary Views and `spark.sql()`

## Goal

Understand:

- what `createOrReplaceTempView()` creates;
- that registering a view does not copy the dataset;
- session scope;
- replacement semantics;
- that `spark.sql()` returns a DataFrame;
- that `SELECT`-style SQL remains lazy until an action occurs;
- that SQL results can immediately return to the DataFrame API.

## 1.1 Query a registered DataFrame

```python
# spark.sql() parses a SQL query and returns a DataFrame representing the
# result relation.
completed_orders_sql_df = spark.sql(
    '''
    SELECT
        order_id,
        store_id,
        customer_id,
        order_date,
        order_status
    FROM orders
    WHERE order_status = 'COMPLETED'
    '''
)

# No separate SQL table object is required. The returned object is a normal
# PySpark DataFrame and can continue through the DataFrame API.
completed_order_ids_df = completed_orders_sql_df.select(
    'order_id',
    'store_id',
)

completed_order_ids_df.show(truncate=False)
```

## 1.2 Replace a temp view definition

```python
# Create a working view that initially refers to ALL orders.
orders_df.createOrReplaceTempView('orders_working')

spark.sql(
    '''
    SELECT COUNT(*) AS row_count
    FROM orders_working
    '''
).show()


# Replace the SQL name with a filtered relation.
#
# This changes what the NAME resolves to. It does not mutate orders_df.
orders_df.filter(
    orders_df.order_status == 'COMPLETED'
).createOrReplaceTempView('orders_working')

spark.sql(
    '''
    SELECT COUNT(*) AS row_count
    FROM orders_working
    '''
).show()


# The original DataFrame still contains every source order.
print(f'Original orders_df rows: {orders_df.count()}')
```

Expected observation:

```text
orders_working count changes after replacement
orders_df count does not change
```

That is evidence that `createOrReplaceTempView()` registered a **logical name** rather than copying and mutating the original DataFrame.

## 1.3 Drop the view name

```python
# Remove the temporary SQL name.
spark.catalog.dropTempView('orders_working')


# The Python DataFrame variable still exists after the SQL name is removed.
orders_df.show(truncate=False)
```

## 1.4 Observe laziness conceptually

```python
# This defines a query result DataFrame.
# A SELECT query does not need to scan all source rows merely to return this
# Python object.
store_sales_query_df = spark.sql(
    '''
    SELECT
        store_id,
        COUNT(*) AS order_count
    FROM orders
    GROUP BY store_id
    '''
)


# explain() inspects the query plan; it is useful evidence that Spark has a
# planned computation before an output action materializes results.
store_sales_query_df.explain()


# show() is an action and therefore asks Spark to execute the query.
store_sales_query_df.show(truncate=False)
```

## Questions

1. What exactly does `createOrReplaceTempView('orders')` register?
2. Does it write a table to storage?
3. Does replacing a temp view mutate the original DataFrame?
4. What type of object does `spark.sql('SELECT ...')` return?
5. Why can you call `.select()` or `.filter()` immediately on a SQL result?
6. What happens to a normal temporary view when the owning Spark session ends?

---

[Back to Table of Contents](#toc)

---

<a id="experiment-2-core-sql-dataframe-equivalence"></a>
# Experiment 2 — Core SQL/DataFrame Equivalence

## Goal

Practice important equivalences without duplicating syntax for its own sake:

- `select()` ↔ `SELECT`;
- `filter()` / `where()` ↔ `WHERE`;
- derived columns ↔ SQL expressions;
- mixing SQL and DataFrame API intentionally;
- exact equivalence checks.

## 2.1 Projection

```python
# DATAFRAME API
selected_api_df = order_items_df.select(
    'order_id',
    'line_number',
    'product_id',
    'quantity',
)


# SPARK SQL
selected_sql_df = spark.sql(
    '''
    SELECT
        order_id,
        line_number,
        product_id,
        quantity
    FROM order_items
    '''
)


# Both outputs should describe the same rows and columns.
selected_api_df.show(truncate=False)
selected_sql_df.show(truncate=False)
```

## 2.2 Filtering

```python
from pyspark.sql import functions as F


# DATAFRAME API
completed_api_df = orders_df.filter(
    F.col('order_status') == 'COMPLETED'
)


# SPARK SQL
completed_sql_df = spark.sql(
    '''
    SELECT *
    FROM orders
    WHERE order_status = 'COMPLETED'
    '''
)
```

Because the seed includes order `1003` with `CANCELLED` status, the filter actually removes a row.

## 2.3 Derived sales-line measures

```python
from pyspark.sql import functions as F


# INPUT GRAIN:
#     one row per order line
#
# OUTPUT GRAIN:
#     still one row per order line
#
# DATAFRAME API
sales_lines_api_df = (
    order_items_df
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


# SPARK SQL
#
# The CTEs make each dependency explicit because a SELECT-list alias is not a
# general-purpose replacement for a named relational step.
sales_lines_sql_df = spark.sql(
    '''
    WITH gross AS (
        SELECT
            *,
            quantity * unit_price AS gross_sales
        FROM order_items
    ),
    discounted AS (
        SELECT
            *,
            gross_sales * discount_pct AS discount_amount
        FROM gross
    )
    SELECT
        *,
        gross_sales - discount_amount AS net_sales
    FROM discounted
    '''
)

sales_lines_api_df.createOrReplaceTempView('sales_lines_api')
sales_lines_sql_df.createOrReplaceTempView('sales_lines_sql')
```

The seed includes discount percentages of `0`, `0.05`, and `0.10`, so `gross_sales`, `discount_amount`, and `net_sales` are not trivially identical on every row.

## 2.4 Exact equivalence, not eyeballing

```python
# EXCEPT ALL is duplicate-sensitive.
# Compare BOTH directions so neither side can contain extra rows unnoticed.
api_only_df = sales_lines_api_df.exceptAll(
    sales_lines_sql_df
)

sql_only_df = sales_lines_sql_df.exceptAll(
    sales_lines_api_df
)

print(f'API-only rows: {api_only_df.count()}')
print(f'SQL-only rows: {sql_only_df.count()}')
```

Expected:

```text
API-only rows: 0
SQL-only rows: 0
```

## Questions

1. Why is `.show()` on both outputs weaker evidence than `exceptAll()` in both directions?
2. Why does adding derived columns not change order-line grain?
3. Why does `discount_pct = 0.1000` mean multiplying by `0.10`, not dividing by `100` again?
4. Why is it useful that a SQL result can become a temp view for later reconciliation?

---

[Back to Table of Contents](#toc)

---

<a id="experiment-3-case-when-nulls-and-expressions"></a>
# Experiment 3 — `CASE WHEN`, NULLs, and Expression Equivalence

## Goal

Understand:

- `when()` / `otherwise()` ↔ `CASE WHEN`;
- condition ordering;
- `IS NULL` semantics;
- why SQL `= NULL` is not the correct NULL predicate;
- expression equivalence across interfaces.

## 3.1 Inventory status with meaningful branches

The inventory seed deliberately contains:

```text
on_hand = 0      → OUT_OF_STOCK
on_hand <= point → LOW_STOCK
on_hand > point  → HEALTHY
```

```python
from pyspark.sql import functions as F


# DATAFRAME API
inventory_status_api_df = inventory_df.withColumn(
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


# SPARK SQL
inventory_status_sql_df = spark.sql(
    '''
    SELECT
        *,
        CASE
            WHEN on_hand_quantity = 0 THEN 'OUT_OF_STOCK'
            WHEN on_hand_quantity <= reorder_point THEN 'LOW_STOCK'
            ELSE 'HEALTHY'
        END AS inventory_status
    FROM inventory
    '''
)

inventory_status_sql_df.select(
    'store_id',
    'product_id',
    'on_hand_quantity',
    'reorder_point',
    'inventory_status',
).show(truncate=False)
```

Why condition order matters:

```text
0 <= reorder_point
```

If `LOW_STOCK` were tested first, a zero-quantity row could never reach the more specific `OUT_OF_STOCK` branch.

## 3.2 NULL filtering

```python
# The seed contains one order with customer_id = NULL.
null_customers_sql_df = spark.sql(
    '''
    SELECT
        order_id,
        customer_id
    FROM orders
    WHERE customer_id IS NULL
    '''
)

null_customers_sql_df.show(truncate=False)
```

Do **not** write:

```sql
WHERE customer_id = NULL
```

Use:

```sql
WHERE customer_id IS NULL
```

or:

```sql
WHERE customer_id IS NOT NULL
```

## 3.3 Reconcile the `CASE` implementations

```python
inventory_status_api_df.createOrReplaceTempView('inventory_status_api')
inventory_status_sql_df.createOrReplaceTempView('inventory_status_sql')

api_only_df = spark.sql(
    '''
    SELECT *
    FROM inventory_status_api
    EXCEPT ALL
    SELECT *
    FROM inventory_status_sql
    '''
)

sql_only_df = spark.sql(
    '''
    SELECT *
    FROM inventory_status_sql
    EXCEPT ALL
    SELECT *
    FROM inventory_status_api
    '''
)

print(f'API-only rows: {api_only_df.count()}')
print(f'SQL-only rows: {sql_only_df.count()}')
```

## Questions

1. Why must the zero-stock branch appear before the low-stock branch?
2. Why does `IS NULL` differ from `= NULL`?
3. What is the grain of `inventory_status_sql_df`?
4. Did `CASE WHEN` collapse, expand, or preserve rows?

---

[Back to Table of Contents](#toc)

---

<a id="experiment-4-ctes-and-chained-logic"></a>
# Experiment 4 — CTEs and Chained Relational Logic

## Goal

Practice:

- `WITH`;
- multiple CTEs;
- chained transformations;
- CTEs vs. nested subqueries;
- choosing meaningful relational stages;
- remembering that a CTE is not automatically persisted.

## 4.1 Build a realistic multi-stage query

Business requirement:

```text
1. derive order-line measures;
2. keep completed orders;
3. enrich lines with product category;
4. aggregate completed net sales by store and category;
5. keep only groups above a revenue threshold.
```

```python
store_category_sales_sql_df = spark.sql(
    '''
    WITH line_measures AS (
        SELECT
            order_id,
            line_number,
            product_id,
            quantity,
            unit_price,
            discount_pct,
            quantity * unit_price AS gross_sales,
            quantity * unit_price * discount_pct AS discount_amount,
            quantity * unit_price
                - quantity * unit_price * discount_pct AS net_sales
        FROM order_items
    ),
    completed_lines AS (
        SELECT
            o.store_id,
            o.order_id,
            o.order_date,
            l.line_number,
            l.product_id,
            l.net_sales
        FROM orders o
        INNER JOIN line_measures l
            ON o.order_id = l.order_id
        WHERE o.order_status = 'COMPLETED'
    ),
    categorized_lines AS (
        SELECT
            c.store_id,
            c.order_id,
            c.order_date,
            c.line_number,
            p.category,
            c.net_sales
        FROM completed_lines c
        INNER JOIN products p
            ON c.product_id = p.product_id
    ),
    store_category_totals AS (
        SELECT
            store_id,
            category,
            SUM(net_sales) AS net_sales
        FROM categorized_lines
        GROUP BY
            store_id,
            category
    )
    SELECT
        store_id,
        category,
        net_sales
    FROM store_category_totals
    WHERE net_sales >= 30
    ORDER BY
        store_id,
        category
    '''
)

store_category_sales_sql_df.show(truncate=False)
```

## 4.2 State the grain after every CTE

```text
line_measures
one row per order line

completed_lines
one row per completed order line

categorized_lines
one row per completed order line

store_category_totals
one row per store and category

final output
one row per qualifying store and category
```

That progression matters more than the number of CTEs.

## 4.3 Compare with a nested subquery

A nested query can express the same logic, but once several meaningful stages exist, named CTEs often make debugging and grain reasoning easier.

```python
nested_example_df = spark.sql(
    '''
    SELECT
        store_id,
        SUM(net_sales) AS net_sales
    FROM (
        SELECT
            o.store_id,
            i.quantity * i.unit_price
                - i.quantity * i.unit_price * i.discount_pct AS net_sales
        FROM orders o
        INNER JOIN order_items i
            ON o.order_id = i.order_id
        WHERE o.order_status = 'COMPLETED'
    ) completed_lines
    GROUP BY store_id
    '''
)

nested_example_df.show(truncate=False)
```

## Important distinction

```text
CTE
named relation inside the query

TEMP VIEW
session-scoped catalog name available to later queries

PERSISTED TABLE
stored data managed beyond one query/session
```

A CTE does **not** mean Spark automatically materializes and stores each intermediate result.

## Questions

1. What is the grain of each CTE above?
2. Which CTE changes grain?
3. Why is `categorized_lines` still at order-line grain?
4. Why can CTEs improve maintainability without creating persisted tables?
5. When would a temp view be more appropriate than a CTE?

---

[Back to Table of Contents](#toc)

---

<a id="experiment-5-sql-joins"></a>
# Experiment 5 — SQL Joins, Cardinality, Aliases, Semi, and Anti

## Goal

Practice Spark SQL equivalents for:

- inner joins;
- left joins;
- semi joins;
- anti joins;
- aliases;
- qualified column references;
- grain/cardinality reasoning.

Before **every** join, answer:

```text
What is the grain of the left input?
What is the grain of the right input?
What are the full join keys?
What is the cardinality?
Can this join multiply rows?
```

## 5.1 Inner join: orders → order items

```text
Left grain:
one row per order

Right grain:
one row per order line

Cardinality:
one-to-many

Expected output grain:
one row per order line

Can rows expand?
Yes, intentionally
```

```python
orders_with_items_sql_df = spark.sql(
    '''
    SELECT
        o.order_id,
        o.store_id,
        o.customer_id,
        o.order_date,
        o.order_status,
        i.line_number,
        i.product_id,
        i.quantity,
        i.unit_price,
        i.discount_pct
    FROM orders o
    INNER JOIN order_items i
        ON o.order_id = i.order_id
    '''
)

print(f'Orders: {orders_df.count()}')
print(f'Order items: {order_items_df.count()}')
print(f'Joined rows: {orders_with_items_sql_df.count()}')
```

## 5.2 Left join that actually produces an unmatched row

Because `S03` has no orders, a store-left-join-to-orders demonstrates NULL right-side columns.

```python
stores_with_orders_sql_df = spark.sql(
    '''
    SELECT
        s.store_id,
        s.store_name,
        o.order_id,
        o.order_status
    FROM stores s
    LEFT JOIN orders o
        ON s.store_id = o.store_id
    ORDER BY
        s.store_id,
        o.order_id
    '''
)

stores_with_orders_sql_df.show(truncate=False)
```

Observe:

```text
S03 survives the LEFT JOIN
order_id and order_status are NULL for S03
```

## 5.3 Aliases and qualified references

```python
# o.store_id and s.store_id identify column ownership explicitly.
orders_with_store_sql_df = spark.sql(
    '''
    SELECT
        o.order_id,
        o.store_id,
        o.order_status,
        s.store_name,
        s.province
    FROM orders o
    LEFT JOIN stores s
        ON o.store_id = s.store_id
    '''
)

orders_with_store_sql_df.show(truncate=False)
```

## 5.4 Create an orphan specifically for semi/anti practice

Keep the base source clean. Create a dedicated dirty variant:

```python
from decimal import Decimal


orphan_item_df = spark.createDataFrame(
    [
        (
            9999,
            1,
            'P404',
            1,
            Decimal('10.00'),
            Decimal('0.0000'),
        ),
    ],
    schema=order_items_schema,
)

order_items_with_orphan_df = order_items_df.unionByName(
    orphan_item_df
)

order_items_with_orphan_df.createOrReplaceTempView(
    'order_items_with_orphan'
)
```

## 5.5 Left semi join

```python
# Keep only left rows whose product_id has a parent product match.
valid_product_items_sql_df = spark.sql(
    '''
    SELECT i.*
    FROM order_items_with_orphan i
    LEFT SEMI JOIN products p
        ON i.product_id = p.product_id
    '''
)

valid_product_items_sql_df.show(truncate=False)
```

The P404 row should disappear.

## 5.6 Left anti join

```python
# Keep only child rows with NO matching product.
# This directly expresses an orphan check.
orphan_product_items_sql_df = spark.sql(
    '''
    SELECT i.*
    FROM order_items_with_orphan i
    LEFT ANTI JOIN products p
        ON i.product_id = p.product_id
    '''
)

orphan_product_items_sql_df.show(truncate=False)
```

The result should contain the deliberately seeded P404 row.

## Questions

1. Why does the orders-to-items inner join legitimately produce more rows than `orders`?
2. Why does `S03` survive the store-left-join-to-orders query?
3. Which join answers only whether a left row has a match without adding right-side columns?
4. Which join is naturally suited to orphan detection?
5. Why do aliases become increasingly important as queries join more relations?

---

[Back to Table of Contents](#toc)

---

<a id="experiment-6-multi-column-joins"></a>
# Experiment 6 — Multi-Column Joins and Incomplete-Key Failures

## Goal

Understand:

- composite relationship keys;
- why a key must match the grain of the relationship;
- how an incomplete key creates false matches and row multiplication;
- how SQL makes the difference explicit in the `ON` clause.

## 6.1 Correct join on `(store_id, product_id)`

```text
inventory
many observations per store-product across time

store_product_targets
one row per store-product

Complete relationship key:
(store_id, product_id)
```

```python
correct_target_join_sql_df = spark.sql(
    '''
    SELECT
        i.snapshot_date,
        i.snapshot_ts,
        i.ingestion_id,
        i.store_id,
        i.product_id,
        i.on_hand_quantity,
        t.target_on_hand
    FROM inventory i
    LEFT JOIN store_product_targets t
        ON i.store_id = t.store_id
       AND i.product_id = t.product_id
    ORDER BY
        i.ingestion_id
    '''
)

correct_target_join_sql_df.show(truncate=False)
```

## 6.2 Deliberately use the incomplete key

`P001` exists in target rows for both `S01` and `S02`.

Therefore this query is **provably wrong**:

```python
incomplete_target_join_sql_df = spark.sql(
    '''
    SELECT
        i.ingestion_id,
        i.store_id AS inventory_store,
        i.product_id,
        t.store_id AS target_store,
        t.target_on_hand
    FROM inventory i
    LEFT JOIN store_product_targets t
        ON i.product_id = t.product_id
    WHERE i.product_id = 'P001'
    ORDER BY
        i.ingestion_id,
        t.store_id
    '''
)

incomplete_target_join_sql_df.show(truncate=False)
```

You should see each S01/P001 inventory observation match **both** the S01 target and the unrelated S02 target.

## 6.3 Compare row counts

```python
correct_count = correct_target_join_sql_df.count()
incomplete_count = spark.sql(
    '''
    SELECT
        i.*,
        t.target_on_hand
    FROM inventory i
    LEFT JOIN store_product_targets t
        ON i.product_id = t.product_id
    '''
).count()

print(f'Correct-key rows: {correct_count}')
print(f'Incomplete-key rows: {incomplete_count}')
```

The incorrect row count should be larger because P001 matches multiple right-side target rows.

## Questions

1. What is the grain of `store_product_targets`?
2. Why is `product_id` alone not enough to identify one target row?
3. Why is this bug dangerous even if every selected value looks individually plausible?
4. How could the incomplete join corrupt a later `SUM(on_hand_quantity)`?
5. What Phase 2 grain rule still governs this SQL query?

---

[Back to Table of Contents](#toc)

---

<a id="experiment-7-aggregations"></a>
# Experiment 7 — Aggregations, Conditional Aggregation, `WHERE`, and `HAVING`

## Goal

Practice Spark SQL equivalents for:

- `GROUP BY`;
- `COUNT`;
- `SUM`;
- `AVG`;
- `MIN`;
- `MAX`;
- `COUNT(DISTINCT ...)`;
- conditional aggregation with `CASE WHEN`;
- `WHERE` vs. `HAVING`;
- output-grain reasoning.

First register the sales-line measures from Experiment 2:

```python
sales_lines_sql_df.createOrReplaceTempView('sales_lines')
```

## 7.1 Core aggregate functions

```python
product_metrics_sql_df = spark.sql(
    '''
    SELECT
        product_id,
        COUNT(*) AS line_count,
        COUNT(DISTINCT order_id) AS order_count,
        SUM(quantity) AS units_sold,
        SUM(net_sales) AS net_sales,
        AVG(unit_price) AS avg_unit_price,
        MIN(unit_price) AS min_unit_price,
        MAX(unit_price) AS max_unit_price
    FROM sales_lines
    GROUP BY product_id
    ORDER BY product_id
    '''
)

product_metrics_sql_df.show(truncate=False)
```

Output grain:

```text
one row per product_id
```

The grouped columns define the output grain just as `groupBy('product_id')` does in the DataFrame API.

## 7.2 Conditional aggregation

The seed has both discounted and undiscounted lines, so these metrics differ meaningfully.

```python
conditional_metrics_sql_df = spark.sql(
    '''
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
        ) AS discounted_line_count,
        SUM(
            CASE
                WHEN quantity >= 3 THEN quantity
                ELSE 0
            END
        ) AS large_line_units
    FROM sales_lines
    GROUP BY product_id
    ORDER BY product_id
    '''
)

conditional_metrics_sql_df.show(truncate=False)
```

Mental model:

```text
CASE WHEN
chooses each row's contribution

SUM
reduces those contributions within the group
```

## 7.3 `WHERE` vs. `HAVING`

`WHERE` filters **input rows before grouping**.

```python
completed_store_sales_sql_df = spark.sql(
    '''
    SELECT
        o.store_id,
        SUM(s.net_sales) AS net_sales
    FROM orders o
    INNER JOIN sales_lines s
        ON o.order_id = s.order_id
    WHERE o.order_status = 'COMPLETED'
    GROUP BY o.store_id
    '''
)
```

`HAVING` filters **groups after aggregation**.

```python
high_revenue_products_sql_df = spark.sql(
    '''
    SELECT
        product_id,
        SUM(net_sales) AS net_sales
    FROM sales_lines
    GROUP BY product_id
    HAVING SUM(net_sales) >= 40
    ORDER BY net_sales DESC
    '''
)

high_revenue_products_sql_df.show(truncate=False)
```

## 7.4 Equivalent DataFrame aggregation

```python
from pyspark.sql import functions as F


product_metrics_api_df = (
    sales_lines_api_df
    .groupBy('product_id')
    .agg(
        F.count('*').alias('line_count'),
        F.countDistinct('order_id').alias('order_count'),
        F.sum('quantity').alias('units_sold'),
        F.sum('net_sales').alias('net_sales'),
        F.avg('unit_price').alias('avg_unit_price'),
        F.min('unit_price').alias('min_unit_price'),
        F.max('unit_price').alias('max_unit_price'),
    )
)
```

## Questions

1. What defines the grain of `product_metrics_sql_df`?
2. What does `COUNT(*)` count?
3. What does `COUNT(DISTINCT order_id)` mean at product grain?
4. Why does `WHERE order_status = 'COMPLETED'` belong before the aggregation?
5. When is `HAVING` appropriate instead of `WHERE`?
6. Why is an aggregate query not merely a different syntax for a window function?

---

[Back to Table of Contents](#toc)

---

<a id="experiment-8-group-by-vs-windows"></a>
# Experiment 8 — `GROUP BY` vs. Window Functions

## Goal

Make this distinction automatic:

```text
GROUP BY
    → collapses rows

window function
    → preserves rows
```

First create one enriched completed-sales relation at order-line grain.

```python
sales_enriched_sql_df = spark.sql(
    '''
    SELECT
        o.order_id,
        i.line_number,
        o.store_id,
        s.store_name,
        o.customer_id,
        o.order_date,
        o.order_status,
        i.product_id,
        p.product_name,
        p.category,
        i.quantity,
        i.unit_price,
        p.unit_cost,
        i.discount_pct,
        i.gross_sales,
        i.discount_amount,
        i.net_sales,
        i.net_sales - i.quantity * p.unit_cost AS gross_margin
    FROM orders o
    INNER JOIN sales_lines i
        ON o.order_id = i.order_id
    INNER JOIN products p
        ON i.product_id = p.product_id
    INNER JOIN stores s
        ON o.store_id = s.store_id
    WHERE o.order_status = 'COMPLETED'
    '''
)

sales_enriched_sql_df.createOrReplaceTempView('sales_enriched')
```

Input grain:

```text
one row per completed order line
```

## 8.1 `GROUP BY` collapses to store grain

```python
store_totals_sql_df = spark.sql(
    '''
    SELECT
        store_id,
        SUM(net_sales) AS store_net_sales
    FROM sales_enriched
    GROUP BY store_id
    ORDER BY store_id
    '''
)

store_totals_sql_df.show(truncate=False)
```

Output grain:

```text
one row per store
```

## 8.2 Window aggregate preserves order-line grain

```python
sales_with_store_total_sql_df = spark.sql(
    '''
    SELECT
        order_id,
        line_number,
        store_id,
        product_id,
        net_sales,
        SUM(net_sales) OVER (
            PARTITION BY store_id
        ) AS store_net_sales
    FROM sales_enriched
    ORDER BY
        store_id,
        order_id,
        line_number
    '''
)

sales_with_store_total_sql_df.show(truncate=False)
```

Output grain:

```text
still one row per completed order line
```

## 8.3 Compare row counts

```python
print(f'Source line rows: {sales_enriched_sql_df.count()}')
print(f'GROUP BY rows: {store_totals_sql_df.count()}')
print(
    'Window rows: '
    f'{sales_with_store_total_sql_df.count()}'
)
```

## Questions

1. Why can both queries calculate a store total but return different numbers of rows?
2. What does `PARTITION BY store_id` mean here?
3. Why is a window calculation useful when line detail must remain available?
4. If the target output is one row per store, which approach better matches the intended grain?

---

[Back to Table of Contents](#toc)

---

<a id="experiment-9-analytical-windows"></a>
# Experiment 9 — Ranking, `LAG`, `LEAD`, Running, and Rolling Windows

## Goal

Practice SQL syntax for:

- `OVER`;
- `PARTITION BY`;
- `ORDER BY`;
- `ROW_NUMBER`;
- `RANK`;
- `DENSE_RANK`;
- `LAG`;
- `LEAD`;
- running aggregates;
- row frames;
- range/value frames.

## 9.1 Ranking with deliberate ties

The main retail data is realistic, but rank functions are easiest to compare when ties are guaranteed.

Create a dedicated product-revenue relation whose seed **forces** ties:

```python
from decimal import Decimal

from pyspark.sql.types import (
    DecimalType,
    StringType,
    StructField,
    StructType,
)


ranking_demo_schema = StructType([
    StructField('category', StringType(), False),
    StructField('product_id', StringType(), False),
    StructField('product_name', StringType(), False),
    StructField('net_sales', DecimalType(12, 2), False),
])

ranking_demo_df = spark.createDataFrame(
    [
        ('EQUIPMENT', 'P002', 'Coffee Grinder', Decimal('100.00')),
        ('EQUIPMENT', 'P003', 'Kettle', Decimal('100.00')),
        ('EQUIPMENT', 'P006', 'Digital Scale', Decimal('60.00')),
        ('GROCERY', 'P001', 'Coffee Beans', Decimal('80.00')),
        ('GROCERY', 'P004', 'Paper Filters', Decimal('50.00')),
        ('GROCERY', 'P005', 'Tea Bags', Decimal('50.00')),
    ],
    schema=ranking_demo_schema,
)

ranking_demo_df.createOrReplaceTempView('ranking_demo')
```

Run all three ranking functions:

```python
ranked_products_sql_df = spark.sql(
    '''
    SELECT
        category,
        product_id,
        product_name,
        net_sales,
        ROW_NUMBER() OVER (
            PARTITION BY category
            ORDER BY
                net_sales DESC,
                product_id ASC
        ) AS row_number,
        RANK() OVER (
            PARTITION BY category
            ORDER BY net_sales DESC
        ) AS rank,
        DENSE_RANK() OVER (
            PARTITION BY category
            ORDER BY net_sales DESC
        ) AS dense_rank
    FROM ranking_demo
    ORDER BY
        category,
        net_sales DESC,
        product_id
    '''
)

ranked_products_sql_df.show(truncate=False)
```

Observe the EQUIPMENT tie at `100.00` and the GROCERY tie at `50.00`.

Expected conceptual difference:

```text
ROW_NUMBER
always unique sequence positions

RANK
ties share rank and leave gaps afterward

DENSE_RANK
ties share rank but no gaps afterward
```

## 9.2 Daily store sales for sequential windows

```python
daily_store_sales_sql_df = spark.sql(
    '''
    SELECT
        store_id,
        order_date,
        SUM(net_sales) AS daily_net_sales
    FROM sales_enriched
    GROUP BY
        store_id,
        order_date
    ORDER BY
        store_id,
        order_date
    '''
)

daily_store_sales_sql_df.createOrReplaceTempView('daily_store_sales')

daily_store_sales_sql_df.show(truncate=False)
```

The dates intentionally contain gaps, which makes `LAG` semantics visible.

## 9.3 `LAG` and `LEAD`

```python
neighbor_sales_sql_df = spark.sql(
    '''
    SELECT
        store_id,
        order_date,
        daily_net_sales,
        LAG(daily_net_sales) OVER (
            PARTITION BY store_id
            ORDER BY order_date
        ) AS previous_observed_sales,
        LEAD(daily_net_sales) OVER (
            PARTITION BY store_id
            ORDER BY order_date
        ) AS next_observed_sales
    FROM daily_store_sales
    ORDER BY
        store_id,
        order_date
    '''
)

neighbor_sales_sql_df.show(truncate=False)
```

Important:

```text
LAG means previous ORDERED ROW.
It does not automatically mean previous calendar day.
```

## 9.4 Running total

```python
running_sales_sql_df = spark.sql(
    '''
    SELECT
        store_id,
        order_date,
        daily_net_sales,
        SUM(daily_net_sales) OVER (
            PARTITION BY store_id
            ORDER BY order_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_net_sales
    FROM daily_store_sales
    ORDER BY
        store_id,
        order_date
    '''
)

running_sales_sql_df.show(truncate=False)
```

## 9.5 Three-observation rolling window

```python
rolling_3_rows_sql_df = spark.sql(
    '''
    SELECT
        store_id,
        order_date,
        daily_net_sales,
        SUM(daily_net_sales) OVER (
            PARTITION BY store_id
            ORDER BY order_date
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS rolling_3_observed_rows
    FROM daily_store_sales
    ORDER BY
        store_id,
        order_date
    '''
)

rolling_3_rows_sql_df.show(truncate=False)
```

This means:

```text
current row + previous two observed store-day rows
```

It does **not** mean three calendar days.

## 9.6 Seven-calendar-day rolling range

Create a numeric day ordering value:

```python
daily_store_sales_with_day_sql_df = spark.sql(
    '''
    SELECT
        *,
        DATEDIFF(order_date, DATE '1970-01-01') AS day_number
    FROM daily_store_sales
    '''
)

daily_store_sales_with_day_sql_df.createOrReplaceTempView(
    'daily_store_sales_with_day'
)
```

Then use a value-based frame:

```python
rolling_7_day_sql_df = spark.sql(
    '''
    SELECT
        store_id,
        order_date,
        daily_net_sales,
        SUM(daily_net_sales) OVER (
            PARTITION BY store_id
            ORDER BY day_number
            RANGE BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS rolling_7_day_sales
    FROM daily_store_sales_with_day
    ORDER BY
        store_id,
        order_date
    '''
)

rolling_7_day_sql_df.show(truncate=False)
```

## Questions

1. Why does the dedicated ranking seed make `RANK` vs. `DENSE_RANK` observable?
2. Why does `ROW_NUMBER` include `product_id` as a tie-breaker?
3. Why does `LAG` not necessarily mean yesterday?
4. What is the difference between `ROWS BETWEEN 2 PRECEDING` and `RANGE BETWEEN 6 PRECEDING` in these examples?
5. Why is the ordering expression part of the business meaning of a window?

---

[Back to Table of Contents](#toc)

---

<a id="experiment-10-latest-record-selection"></a>
# Experiment 10 — Deterministic Latest-Record Selection

## Goal

Practice:

- deduplication by business rule;
- `ROW_NUMBER()` for latest-record selection;
- partition keys;
- recency ordering;
- deterministic tie-breakers;
- exact SQL/DataFrame equivalence.

## 10.1 Prove the raw source contains duplicate target keys

```python
raw_inventory_duplicates_sql_df = spark.sql(
    '''
    SELECT
        snapshot_date,
        store_id,
        product_id,
        COUNT(*) AS row_count
    FROM inventory
    GROUP BY
        snapshot_date,
        store_id,
        product_id
    HAVING COUNT(*) > 1
    ORDER BY
        snapshot_date,
        store_id,
        product_id
    '''
)

raw_inventory_duplicates_sql_df.show(truncate=False)
```

The seed should expose both:

```text
2026-01-05 / S01 / P001
2026-01-06 / S02 / P003
```

## 10.2 SQL latest-record selection

```python
latest_inventory_sql_df = spark.sql(
    '''
    WITH ranked AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY
                    snapshot_date,
                    store_id,
                    product_id
                ORDER BY
                    snapshot_ts DESC,
                    ingestion_id DESC
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
    FROM ranked
    WHERE row_number = 1
    ORDER BY
        snapshot_date,
        store_id,
        product_id
    '''
)

latest_inventory_sql_df.show(truncate=False)
```

The seed demonstrates two rules:

```text
S01/P001 on 2026-01-05
latest timestamp wins

S02/P003 on 2026-01-06
same timestamp exists twice
greatest ingestion_id wins
```

## 10.3 Equivalent DataFrame implementation

```python
from pyspark.sql import Window
from pyspark.sql import functions as F


latest_inventory_window = (
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

latest_inventory_api_df = (
    inventory_df
    .withColumn(
        '_row_number',
        F.row_number().over(latest_inventory_window),
    )
    .filter(
        F.col('_row_number') == 1
    )
    .drop('_row_number')
)
```

## 10.4 Reconcile exact rows

```python
latest_inventory_columns = [
    'snapshot_date',
    'snapshot_ts',
    'ingestion_id',
    'store_id',
    'product_id',
    'on_hand_quantity',
    'reorder_point',
]

latest_api_aligned_df = latest_inventory_api_df.select(
    *latest_inventory_columns
)

latest_sql_aligned_df = latest_inventory_sql_df.select(
    *latest_inventory_columns
)

api_only_df = latest_api_aligned_df.exceptAll(
    latest_sql_aligned_df
)

sql_only_df = latest_sql_aligned_df.exceptAll(
    latest_api_aligned_df
)

print(f'API-only rows: {api_only_df.count()}')
print(f'SQL-only rows: {sql_only_df.count()}')
```

## Questions

1. Why is `dropDuplicates()` insufficient for this business rule?
2. What columns define the target grain?
3. Why does `snapshot_ts DESC` come before `ingestion_id DESC`?
4. Which seeded rows prove that the timestamp alone is not always deterministic?
5. Why should exact SQL/DataFrame reconciliation return zero mismatches in both directions?

---

[Back to Table of Contents](#toc)

---

<a id="experiment-11-validation-queries"></a>
# Experiment 11 — SQL Validation Queries

## Goal

Use SQL as a practical data-engineering validation language.

Practice:

- row counts;
- primary-key duplicate detection;
- composite-key duplicate detection;
- required-field/NULL checks;
- referential-integrity/orphan checks;
- grain validation;
- healthy vs. deliberately broken data.

A useful validation query often returns **violations**.

Therefore:

```text
healthy result
= zero rows
```

for many uniqueness, NULL, orphan, and grain checks.

## 11.1 Row counts

```python
spark.sql(
    '''
    SELECT COUNT(*) AS order_count
    FROM orders
    '''
).show()

spark.sql(
    '''
    SELECT COUNT(*) AS order_line_count
    FROM order_items
    '''
).show()
```

## 11.2 Healthy primary-key validation

```python
healthy_duplicate_orders_sql_df = spark.sql(
    '''
    SELECT
        order_id,
        COUNT(*) AS row_count
    FROM orders
    GROUP BY order_id
    HAVING COUNT(*) > 1
    '''
)

healthy_duplicate_orders_sql_df.show(truncate=False)
```

Expected:

```text
zero rows
```

## 11.3 Deliberately create a duplicate primary key

```python
# Duplicate order 1001 only in a validation-specific variant.
duplicate_order_df = orders_df.filter(
    orders_df.order_id == 1001
)

orders_with_duplicate_df = orders_df.unionByName(
    duplicate_order_df
)

orders_with_duplicate_df.createOrReplaceTempView(
    'orders_with_duplicate'
)
```

Now run the same pattern:

```python
duplicate_orders_sql_df = spark.sql(
    '''
    SELECT
        order_id,
        COUNT(*) AS row_count
    FROM orders_with_duplicate
    GROUP BY order_id
    HAVING COUNT(*) > 1
    '''
)

duplicate_orders_sql_df.show(truncate=False)
```

The seed should expose:

```text
order_id = 1001
row_count = 2
```

## 11.4 Composite-key validation

Healthy check:

```python
healthy_duplicate_lines_sql_df = spark.sql(
    '''
    SELECT
        order_id,
        line_number,
        COUNT(*) AS row_count
    FROM order_items
    GROUP BY
        order_id,
        line_number
    HAVING COUNT(*) > 1
    '''
)

healthy_duplicate_lines_sql_df.show(truncate=False)
```

Create a broken variant:

```python
duplicate_line_df = order_items_df.filter(
    (order_items_df.order_id == 1001)
    & (order_items_df.line_number == 1)
)

order_items_with_duplicate_df = order_items_df.unionByName(
    duplicate_line_df
)

order_items_with_duplicate_df.createOrReplaceTempView(
    'order_items_with_duplicate'
)
```

Validate:

```python
duplicate_lines_sql_df = spark.sql(
    '''
    SELECT
        order_id,
        line_number,
        COUNT(*) AS row_count
    FROM order_items_with_duplicate
    GROUP BY
        order_id,
        line_number
    HAVING COUNT(*) > 1
    '''
)

duplicate_lines_sql_df.show(truncate=False)
```

## 11.5 Required-field / NULL validation

`customer_id` is intentionally nullable in the business data, so it should **not** be used as an example of a required-field violation.

Instead, create a dedicated invalid relation where `store_id` is NULL:

```python
invalid_required_fields_sql_df = spark.sql(
    '''
    SELECT
        order_id,
        CAST(NULL AS STRING) AS store_id,
        customer_id,
        order_date,
        order_ts,
        order_status
    FROM orders
    WHERE order_id = 1001
    '''
)

invalid_required_fields_sql_df.createOrReplaceTempView(
    'invalid_required_fields'
)
```

Validate:

```python
required_null_violations_sql_df = spark.sql(
    '''
    SELECT *
    FROM invalid_required_fields
    WHERE order_id IS NULL
       OR store_id IS NULL
       OR order_date IS NULL
       OR order_status IS NULL
    '''
)

required_null_violations_sql_df.show(truncate=False)
```

The row should appear because `store_id` is deliberately NULL.

## 11.6 Referential integrity

Reuse `order_items_with_orphan` from Experiment 5:

```python
orphan_product_items_sql_df = spark.sql(
    '''
    SELECT i.*
    FROM order_items_with_orphan i
    LEFT ANTI JOIN products p
        ON i.product_id = p.product_id
    '''
)

orphan_product_items_sql_df.show(truncate=False)
```

Expected violation:

```text
product_id = P404
```

## 11.7 Grain validation after latest-record selection

Register the deduplicated relation:

```python
latest_inventory_sql_df.createOrReplaceTempView(
    'latest_inventory'
)
```

Validate the target key:

```python
latest_inventory_grain_violations_sql_df = spark.sql(
    '''
    SELECT
        snapshot_date,
        store_id,
        product_id,
        COUNT(*) AS row_count
    FROM latest_inventory
    GROUP BY
        snapshot_date,
        store_id,
        product_id
    HAVING COUNT(*) > 1
    '''
)

latest_inventory_grain_violations_sql_df.show(truncate=False)
```

Expected:

```text
zero rows
```

The raw inventory source had duplicate target keys. The selected latest relation should not.

## Questions

1. Why are deliberately dirty variants useful for validation practice?
2. Why should the base source remain clean for most other experiments?
3. Why does a non-empty duplicate-key query disprove the claimed grain/key contract?
4. Why is `customer_id` a poor choice for the required-field violation in this dataset?
5. Why is a left anti join a natural referential-integrity query?
6. What does an empty grain-violation query prove about `latest_inventory`?

---

[Back to Table of Contents](#toc)

---

<a id="experiment-12-reconciliation"></a>
# Experiment 12 — Reconciliation and Exact SQL/DataFrame Equivalence

## Goal

Treat reconciliation as a first-class engineering activity.

Practice:

- source row count vs. transformed row count;
- source measure totals vs. fact-style totals;
- DataFrame API vs. SQL implementation;
- exact mismatch detection;
- schema comparison;
- knowing when row counts **should not** reconcile.

## 12.1 Row-count reconciliation for a grain-preserving enrichment

`sales_enriched` is built from completed order lines only.

Therefore compare it to the **same eligibility scope**, not to every raw order line.

```python
row_count_reconciliation_sql_df = spark.sql(
    '''
    SELECT
        source.completed_line_count,
        enriched.enriched_line_count,
        source.completed_line_count
            - enriched.enriched_line_count AS difference
    FROM (
        SELECT COUNT(*) AS completed_line_count
        FROM order_items i
        INNER JOIN orders o
            ON i.order_id = o.order_id
        WHERE o.order_status = 'COMPLETED'
    ) source
    CROSS JOIN (
        SELECT COUNT(*) AS enriched_line_count
        FROM sales_enriched
    ) enriched
    '''
)

row_count_reconciliation_sql_df.show(truncate=False)
```

Expected:

```text
difference = 0
```

Why?

```text
completed order-line grain
        ↓
join to unique order/product/store parents
        ↓
still completed order-line grain
```

## 12.2 A row count that should NOT reconcile

Daily store sales intentionally changes grain:

```text
completed order lines
        ↓ GROUP BY store_id, order_date
store-day rows
```

Therefore this difference is expected:

```python
print(
    'Completed line rows: '
    f'{sales_enriched_sql_df.count()}'
)
print(
    'Store-day rows: '
    f'{daily_store_sales_sql_df.count()}'
)
```

A different row count is not automatically an error when the transformation intentionally collapses grain.

## 12.3 Measure reconciliation across a grain change

Even though row counts change, an additive measure can still reconcile.

```python
measure_reconciliation_sql_df = spark.sql(
    '''
    SELECT
        line_level.net_sales AS line_level_net_sales,
        daily_level.net_sales AS daily_level_net_sales,
        line_level.net_sales
            - daily_level.net_sales AS difference
    FROM (
        SELECT SUM(net_sales) AS net_sales
        FROM sales_enriched
    ) line_level
    CROSS JOIN (
        SELECT SUM(daily_net_sales) AS net_sales
        FROM daily_store_sales
    ) daily_level
    '''
)

measure_reconciliation_sql_df.show(truncate=False)
```

Expected:

```text
difference = 0
```

This is a key reconciliation principle:

```text
row counts may legitimately differ
while additive measures still reconcile
```

## 12.4 Exact DataFrame-vs-SQL comparison

Use the sales-line measures from Experiment 2:

```python
api_only_df = sales_lines_api_df.exceptAll(
    sales_lines_sql_df
)

sql_only_df = sales_lines_sql_df.exceptAll(
    sales_lines_api_df
)

assert api_only_df.count() == 0
assert sql_only_df.count() == 0
```

SQL version:

```python
sales_lines_api_df.createOrReplaceTempView('sales_lines_api')
sales_lines_sql_df.createOrReplaceTempView('sales_lines_sql')

api_only_sql_df = spark.sql(
    '''
    SELECT *
    FROM sales_lines_api
    EXCEPT ALL
    SELECT *
    FROM sales_lines_sql
    '''
)

sql_only_sql_df = spark.sql(
    '''
    SELECT *
    FROM sales_lines_sql
    EXCEPT ALL
    SELECT *
    FROM sales_lines_api
    '''
)
```

## 12.5 Schema reconciliation

```python
# Exact row equality is not the whole downstream contract.
# Column names, order, Spark types, and nullability can also matter.
print(sales_lines_api_df.schema)
print(sales_lines_sql_df.schema)

assert sales_lines_api_df.schema == sales_lines_sql_df.schema
```

## Questions

1. Why must reconciliation compare datasets under the same business scope?
2. Why should completed source lines reconcile to completed enriched lines?
3. Why should line rows not reconcile to store-day rows?
4. Why can `SUM(net_sales)` still reconcile after aggregation?
5. Why must exact set-difference checks run in both directions?
6. Why can schema equality matter even when row values agree?

---

[Back to Table of Contents](#toc)

---

<a id="experiment-13-deliberate-mismatch"></a>
# Experiment 13 — Deliberate Logic Bug and Mismatch Detection

## Goal

Prove that reconciliation catches a plausible transformation bug.

A good equivalence test is more convincing when you deliberately break one implementation and watch the checks fail.

## 13.1 Create a buggy SQL implementation

The source stores `discount_pct` as a **fraction**:

```text
0.0500 = 5%
0.1000 = 10%
```

The following SQL mistakenly divides by `100` again:

```python
buggy_sales_lines_sql_df = spark.sql(
    '''
    SELECT
        order_id,
        line_number,
        product_id,
        quantity,
        unit_price,
        discount_pct,
        quantity * unit_price AS gross_sales,
        quantity * unit_price
            * discount_pct / 100 AS discount_amount,
        quantity * unit_price
            - quantity * unit_price
                * discount_pct / 100 AS net_sales
    FROM order_items
    '''
)
```

This is a realistic bug because both formulas look superficially plausible.

The seed deliberately contains non-zero discounts, so the bug creates actual mismatches.

## 13.2 Exact mismatch detection

Align columns first:

```python
comparison_columns = [
    'order_id',
    'line_number',
    'product_id',
    'quantity',
    'unit_price',
    'discount_pct',
    'gross_sales',
    'discount_amount',
    'net_sales',
]

correct_aligned_df = sales_lines_api_df.select(
    *comparison_columns
)

buggy_aligned_df = buggy_sales_lines_sql_df.select(
    *comparison_columns
)
```

Compare both directions:

```python
correct_only_df = correct_aligned_df.exceptAll(
    buggy_aligned_df
)

buggy_only_df = buggy_aligned_df.exceptAll(
    correct_aligned_df
)

print(f'Correct-only rows: {correct_only_df.count()}')
print(f'Buggy-only rows: {buggy_only_df.count()}')
```

Expected:

```text
both counts > 0
```

## 13.3 Inspect only the mismatching business keys

```python
correct_aligned_df.createOrReplaceTempView('correct_sales_lines')
buggy_aligned_df.createOrReplaceTempView('buggy_sales_lines')

mismatch_keys_sql_df = spark.sql(
    '''
    SELECT
        c.order_id,
        c.line_number,
        c.discount_pct,
        c.discount_amount AS correct_discount_amount,
        b.discount_amount AS buggy_discount_amount,
        c.net_sales AS correct_net_sales,
        b.net_sales AS buggy_net_sales
    FROM correct_sales_lines c
    INNER JOIN buggy_sales_lines b
        ON c.order_id = b.order_id
       AND c.line_number = b.line_number
    WHERE c.discount_amount <> b.discount_amount
       OR c.net_sales <> b.net_sales
    ORDER BY
        c.order_id,
        c.line_number
    '''
)

mismatch_keys_sql_df.show(truncate=False)
```

Because the seed includes both zero-discount and non-zero-discount rows, you should observe an important debugging fact:

```text
undiscounted rows may still match

discounted rows expose the bug
```

That is precisely why seed data must exercise each logical branch.

## 13.4 Measure-level impact

```python
correct_aligned_df.createOrReplaceTempView('correct_sales_lines')
buggy_aligned_df.createOrReplaceTempView('buggy_sales_lines')

bug_impact_sql_df = spark.sql(
    '''
    SELECT
        correct.total_net_sales AS correct_total_net_sales,
        buggy.total_net_sales AS buggy_total_net_sales,
        correct.total_net_sales
            - buggy.total_net_sales AS difference
    FROM (
        SELECT SUM(net_sales) AS total_net_sales
        FROM correct_sales_lines
    ) correct
    CROSS JOIN (
        SELECT SUM(net_sales) AS total_net_sales
        FROM buggy_sales_lines
    ) buggy
    '''
)

bug_impact_sql_df.show(truncate=False)
```

## Questions

1. Why would this bug be invisible if every `discount_pct` were zero?
2. Why is meaningful seed coverage part of correctness testing?
3. Why can exact mismatch detection be more useful than only comparing grand totals?
4. Why can a grand-total reconciliation still be valuable after row-level mismatch detection?
5. What should happen after repairing the SQL formula?

---

[Back to Table of Contents](#toc)

---

<a id="applied-phase-3-project"></a>
# Applied Phase 3 Project

## Goal

Prepare for the eventual Phase 3 mastery requirement:

```text
same analytical transformation pipeline
            ↓
DataFrame API implementation
            ↕ exact reconciliation
Spark SQL implementation
```

The final mastery work is **not implemented here**.

The applied project should eventually demonstrate:

- source DataFrames with explicit grain and keys;
- temp-view registration;
- SQL validation of source contracts;
- derived order-line measures;
- completed-order eligibility filtering;
- many-to-one enrichment joins;
- grouped analytical outputs;
- at least one meaningful window result;
- deterministic latest-inventory selection;
- the same transformation logic expressed through both interfaces;
- row-count reconciliation where grain is preserved;
- additive-measure reconciliation where grain changes intentionally;
- exact DataFrame-vs-SQL mismatch detection;
- schema reconciliation where exact schemas are part of the contract.

## Engineering contract

Before each major step, write:

```text
INPUT GRAIN:

TARGET GRAIN:

KEYS:

CARDINALITY:

EXPECTED ROW-COUNT EFFECT:

EXPECTED RECONCILIATION:
```

Possible reconciliation statements include:

```text
row count must match exactly

row count should decrease because rows are filtered

row count should collapse because grain changes

row count differs, but SUM(net_sales) must reconcile

DataFrame API and SQL outputs must match exactly
```

## Important restriction

Do **not** jump directly to the final dual analytical pipeline.

First establish that you can independently:

1. register relations correctly;
2. write equivalent core expressions;
3. validate keys and foreign keys with SQL;
4. reason about join cardinality;
5. reconcile equivalent outputs exactly.

---

[Back to Table of Contents](#toc)

---

<a id="applied-task-part-1"></a>
# Applied Task — Part 1

Implement only:

1. the source DataFrames;
2. source grain/key comments;
3. temp-view registration;
4. SQL row-count checks;
5. SQL primary-key validation;
6. SQL composite-key validation;
7. SQL orphan checks;
8. line-level sales measures with the DataFrame API;
9. the same line-level sales measures with Spark SQL;
10. exact reconciliation of those two line-level outputs;
11. one CTE-based completed-sales enrichment query;
12. row-count validation proving that the enrichment remains at completed order-line grain;
13. schema and row inspection.

Do **not** yet build:

- the final grouped analytical output;
- the final window analytical output;
- the full latest-inventory dual implementation;
- the final Phase 3 mastery pipeline.

## Starter skeleton

```python
from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# Create the Spark entry point for Phase 3 applied practice.
spark = (
    SparkSession.builder
    .appName('phase03-applied-part-1')
    .master('local[*]')
    .getOrCreate()
)


# =========================================================================
# SOURCE DATA
# =========================================================================

# TODO:
# Create orders_df.
#
# GRAIN:
#     one row per order
#
# KEY:
#     order_id


# TODO:
# Create order_items_df.
#
# GRAIN:
#     one row per order line
#
# KEY:
#     (order_id, line_number)


# TODO:
# Create products_df.
#
# GRAIN:
#     one row per product
#
# KEY:
#     product_id


# TODO:
# Create stores_df.
#
# GRAIN:
#     one row per store
#
# KEY:
#     store_id


# TODO:
# Create inventory_df.
#
# SOURCE GRAIN:
#     one row per observed inventory record
#
# TARGET DAILY GRAIN:
#     (snapshot_date, store_id, product_id)


# =========================================================================
# TEMP VIEWS
# =========================================================================

# TODO:
# Register:
#     orders
#     order_items
#     products
#     stores
#     inventory
#
# Use createOrReplaceTempView().


# =========================================================================
# SOURCE VALIDATION WITH SQL
# =========================================================================

# TODO:
# Query row counts for every source relation.


# TODO:
# Validate orders.order_id uniqueness.
#
# Healthy result:
#     zero violation rows


# TODO:
# Validate the order_items composite key:
#     (order_id, line_number)


# TODO:
# Find order items whose order_id does not exist in orders.
#
# Prefer LEFT ANTI JOIN.


# TODO:
# Find order items whose product_id does not exist in products.


# TODO:
# Find orders whose store_id does not exist in stores.


# =========================================================================
# DATAFRAME API — LINE MEASURES
# =========================================================================

# INPUT GRAIN:
#     one row per order line
#
# OUTPUT GRAIN:
#     one row per order line
#
# TODO:
# Build sales_lines_api_df with:
#     gross_sales
#     discount_amount
#     net_sales


# =========================================================================
# SPARK SQL — SAME LINE MEASURES
# =========================================================================

# TODO:
# Build sales_lines_sql_df with the SAME:
#     columns
#     formulas
#     row grain
#
# Use one or more CTEs if that keeps dependencies clear.


# =========================================================================
# EXACT RECONCILIATION
# =========================================================================

# TODO:
# Align column order if necessary.


# TODO:
# Compute:
#     API EXCEPT ALL SQL
#     SQL EXCEPT ALL API
#
# Both should contain zero rows.


# TODO:
# Compare schemas when exact schema equivalence is intended.


# =========================================================================
# COMPLETED-SALES ENRICHMENT WITH SQL CTEs
# =========================================================================

# TODO:
# Register sales_lines_sql_df as a temp view.


# TODO:
# Use a CTE-based SQL query that:
#     1. keeps COMPLETED orders;
#     2. joins order lines to orders;
#     3. joins products;
#     4. joins stores;
#     5. derives gross_margin;
#     6. returns one row per completed order line.
#
# Before coding, document the join cardinality at each step.


# =========================================================================
# RECONCILIATION OF ENRICHMENT GRAIN
# =========================================================================

# TODO:
# Count completed source order lines.


# TODO:
# Count enriched completed order lines.


# TODO:
# Prove the counts are equal.
#
# Explain WHY equality is expected.


# TODO:
# Validate the enriched composite grain:
#     (order_id, line_number)
#
# Healthy result:
#     zero duplicate-key rows


# =========================================================================
# INSPECTION
# =========================================================================

# TODO:
# Print schemas for:
#     sales_lines_api_df
#     sales_lines_sql_df
#     completed enriched SQL result


# TODO:
# Show the outputs ordered by stable business keys.


spark.stop()
```

## Questions to answer before Part 2

1. What does `createOrReplaceTempView()` create, and what does it **not** create?
2. Why can `spark.sql()` return a DataFrame without immediately materializing all rows?
3. What is the grain of `sales_lines_api_df` and `sales_lines_sql_df`?
4. Why must their exact mismatch sets be empty in both directions?
5. Which source keys must be unique before the enrichment joins can safely preserve order-line grain?
6. What cardinality should each of these have from the order-line perspective?

```text
order line → order
order line → product
order line → store through order
```

7. Why must the completed-line source count exclude cancelled orders before comparing with a completed-only enrichment?
8. Why is `(order_id, line_number)` still the correct output key after many-to-one enrichment?
9. Why is a CTE useful for multi-stage SQL without implying persisted intermediate storage?
10. What would a non-empty `API EXCEPT ALL SQL` result prove?

---

[Back to Table of Contents](#toc)

---

<a id="after-part-1"></a>
# After Part 1

Next steps:

1. Implement the equivalent completed-sales enrichment with the DataFrame API.
2. Reconcile the SQL and DataFrame enriched outputs exactly.
3. Build one grouped analytical output in both interfaces.
4. Reconcile grouped measures, not source row count, when grain intentionally changes.
5. Build one window-based analytical output in both interfaces.
6. Compare `GROUP BY` and window grain explicitly.
7. Implement ranking with deliberate ties.
8. Implement `LAG`/`LEAD` over a business sequence with date gaps.
9. Implement running and rolling windows.
10. Implement latest-inventory selection in both interfaces with deterministic tie-breaking.
11. Validate latest-inventory target grain.
12. Add SQL validation queries for keys, NULLs, and orphans.
13. Deliberately break one SQL implementation and prove reconciliation catches it.
14. Repair the bug and return both mismatch sets to zero rows.
15. Only then proceed toward the eventual Phase 3 mastery requirement: the same analytical transformation pipeline implemented twice and reconciled exactly.
16. Do not mark Phase 3 complete until the mastery gate is actually passed.
17. Do not mark Phase 2 complete unless its own mastery requirements are independently satisfied.
18. Update `ROADMAP.md` only after the relevant phase's mastery requirements are satisfied.

[Back to Table of Contents](#toc)
