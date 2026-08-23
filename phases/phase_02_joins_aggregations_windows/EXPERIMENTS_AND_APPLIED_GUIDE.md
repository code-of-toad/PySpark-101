# Phase 2 — Experiment & Applied Practice Guide

<a id="toc"></a>
## Table of Contents

- [Purpose](#purpose)
- [Conventions](#conventions)
- [Practice Dataset](#practice-dataset)
- [Experiment 1 — Aggregation Grain](#experiment-1-aggregation-grain)
- [Experiment 2 — Aggregate Functions and Conditional Aggregation](#experiment-2-aggregate-functions-and-conditional-aggregation)
- [Experiment 3 — Join Cardinality and Row Multiplication](#experiment-3-join-cardinality-and-row-multiplication)
- [Experiment 4 — Join Types](#experiment-4-join-types)
- [Experiment 5 — Multi-Column Joins and Duplicate Column Names](#experiment-5-multi-column-joins-and-duplicate-column-names)
- [Experiment 6 — Join-Key Validation and Referential Integrity](#experiment-6-join-key-validation-and-referential-integrity)
- [Experiment 7 — Many-to-Many Joins and Pre-Aggregation](#experiment-7-many-to-many-joins-and-pre-aggregation)
- [Experiment 8 — groupBy vs. Window.partitionBy](#experiment-8-groupby-vs-window-partitionby)
- [Experiment 9 — row_number, rank, dense_rank, and Latest-Record Selection](#experiment-9-ranking-and-latest-record-selection)
- [Experiment 10 — lag, lead, Running Totals, and Rolling Windows](#experiment-10-lag-lead-running-and-rolling)
- [Experiment 11 — Deduplication, union, and unionByName](#experiment-11-deduplication-and-unions)
- [Experiment 12 — explode, Pivot, and Unpivot](#experiment-12-explode-pivot-and-unpivot)
- [Experiment 13 — Dimensional Modeling](#experiment-13-dimensional-modeling)
- [Applied Phase 2 Project](#applied-phase-2-project)
- [Applied Task — Part 1](#applied-task-part-1)
- [After Part 1](#after-part-1)

---

<a id="purpose"></a>
## Purpose

This file is the execution-oriented companion for **Phase 2 — Joins, Aggregations, Windows & Data Modeling**.

The Phase 2 README is the conceptual reference. This guide is for hands-on practice.

The central skill is not remembering APIs in isolation. It is learning to preserve the intended **grain** of the data while using aggregations, joins, windows, and modeling transformations.

Before every join, explicitly answer:

```text
What is the grain of the left DataFrame?
What is the grain of the right DataFrame?
Can this join multiply rows?
```

Before writing any analytical output, answer:

```text
What does one row represent?
```

---

[Back to Table of Contents](#toc)

---

<a id="conventions"></a>
## Conventions

- Use **PySpark 4.2.0**.
- Use **single quotes** for Python strings.
- Include inline comments that explain both **what** the code does and **why** the pattern matters.
- Reuse the same retail domain throughout.
- Treat grain as an explicit data contract.
- Validate join keys before depending on many-to-one relationships.
- Predict row-count changes before executing joins, aggregations, windows, unions, or `explode()`.
- Do not use `dropDuplicates()` when the business rule requires a specific surviving row.
- Do not sum analytical measures after a row-multiplying join unless that multiplication is intentional.
- Keep applied work separate from the lecture reference.
- Do not mark Phase 2 complete until its applied work and mastery gate are satisfied.
- Phase 1 remains incomplete until its own applied mastery work is finished.

---

[Back to Table of Contents](#toc)

---

<a id="practice-dataset"></a>
# Practice Dataset

Use one coherent set of retail DataFrames throughout the experiments.

## Source grains

```text
orders
one row = one order
key = order_id

order_items
one row = one line within an order
key = (order_id, line_number)

products
one row = one product
key = product_id

stores
one row = one store
key = store_id

inventory_snapshots
one row = one observed inventory record
target daily grain = (snapshot_date, store_id, product_id)
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



# Create one SparkSession for the Phase 2 practice session.
spark = (
    SparkSession.builder
    .appName('phase02-practice')
    .master('local[*]')
    .getOrCreate()
)



# -------------------------------------------------------------------------
# Orders
# -------------------------------------------------------------------------

# GRAIN:
#     one row per order
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
            date(2026, 1, 12),
            datetime(2026, 1, 12, 11, 20),
            'COMPLETED',
        ),
        (
            1005,
            'S01',
            'C004',
            date(2026, 2, 2),
            datetime(2026, 2, 2, 10, 5),
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
        (1002, 1, 'P001', 3, Decimal('12.00'), Decimal('0.0500')),
        (1002, 2, 'P003', 1, Decimal('50.00'), Decimal('0.0000')),
        (1003, 1, 'P002', 2, Decimal('30.00'), Decimal('0.0000')),
        (1004, 1, 'P003', 2, Decimal('50.00'), Decimal('0.1000')),
        (1004, 2, 'P004', 4, Decimal('8.00'), Decimal('0.0000')),
        (1005, 1, 'P001', 1, Decimal('12.00'), Decimal('0.0000')),
        (1005, 2, 'P004', 5, Decimal('8.00'), Decimal('0.0500')),
    ],
    schema=order_items_schema,
)



# -------------------------------------------------------------------------
# Products
# -------------------------------------------------------------------------

# GRAIN:
#     one row per product
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
    ],
    schema=products_schema,
)



# -------------------------------------------------------------------------
# Stores
# -------------------------------------------------------------------------

# GRAIN:
#     one row per store
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
# Inventory snapshot source events
# -------------------------------------------------------------------------

# SOURCE GRAIN:
#     one row per observed inventory record
#
# TARGET DAILY GRAIN:
#     one row per snapshot date, store, and product
#
# P001/S01 intentionally appears twice on 2026-01-05 so that later
# experiments can practice deterministic latest-record selection.
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
            date(2026, 1, 12),
            datetime(2026, 1, 12, 18, 0),
            5,
            'S02',
            'P003',
            5,
            2,
        ),
        (
            date(2026, 2, 2),
            datetime(2026, 2, 2, 18, 0),
            6,
            'S01',
            'P004',
            7,
            8,
        ),
    ],
    schema=inventory_schema,
)
```

Keep these DataFrames available while working through the experiments.

---

[Back to Table of Contents](#toc)

---

<a id="experiment-1-aggregation-grain"></a>
# Experiment 1 — Aggregation Grain

## Goal

Understand:

- `groupBy()`;
- `agg()`;
- how grouped columns define output grain;
- how aggregation collapses rows.

## 1.1 Create line-level measures

```python
from pyspark.sql import functions as F



# INPUT GRAIN:
#     one row per order line
#
# withColumn() adds derived values without changing that grain.
sales_lines_df = (
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

sales_lines_df.show(truncate=False)
```

## 1.2 Aggregate to product grain

```python
from pyspark.sql import functions as F



# INPUT GRAIN:
#     one row per order line
#
# OUTPUT GRAIN:
#     one row per product
#
# product_id defines the output grain because it is the only group key.
product_sales_df = (
    sales_lines_df
    .groupBy('product_id')
    .agg(
        F.sum('quantity').alias('units_sold'),
        F.sum('net_sales').alias('net_sales'),
    )
)

product_sales_df.show(truncate=False)
```

## 1.3 Aggregate to order-product grain

```python
from pyspark.sql import functions as F



# OUTPUT GRAIN:
#     one row per order and product
#
# Adding order_id to groupBy() produces a finer output grain.
order_product_sales_df = (
    sales_lines_df
    .groupBy(
        'order_id',
        'product_id',
    )
    .agg(
        F.sum('quantity').alias('units_sold'),
        F.sum('net_sales').alias('net_sales'),
    )
)

order_product_sales_df.show(truncate=False)
```

## Questions

1. What is the grain of `sales_lines_df`?
2. Why does `product_sales_df` contain fewer rows?
3. What does one row of `order_product_sales_df` represent?
4. Why would adding `line_number` to `groupBy()` largely defeat the purpose of aggregating this dataset?

---

[Back to Table of Contents](#toc)

---

<a id="experiment-2-aggregate-functions-and-conditional-aggregation"></a>
# Experiment 2 — Aggregate Functions and Conditional Aggregation

## Goal

Practice:

- `count`;
- `sum`;
- `avg`;
- `min`;
- `max`;
- `countDistinct`;
- conditional aggregation.

## 2.1 Core aggregate functions

```python
from pyspark.sql import functions as F



# OUTPUT GRAIN:
#     one row per product
product_metrics_df = (
    sales_lines_df
    .groupBy('product_id')
    .agg(
        # count('*') counts rows.
        F.count('*').alias('line_count'),

        # Sum additive measures at the intended group grain.
        F.sum('quantity').alias('units_sold'),
        F.sum('net_sales').alias('net_sales'),

        # These summarize line-level prices within each product group.
        F.avg('unit_price').alias('avg_unit_price'),
        F.min('unit_price').alias('min_unit_price'),
        F.max('unit_price').alias('max_unit_price'),

        # Count distinct orders containing each product.
        F.countDistinct('order_id').alias('order_count'),
    )
)

product_metrics_df.show(truncate=False)
```

## 2.2 `count('*')` vs. `count(column)`

```python
from pyspark.sql import functions as F



count_demo_df = spark.createDataFrame(
    [
        ('A', 1),
        ('A', None),
        ('A', 3),
    ],
    ['group_id', 'value'],
)

count_result_df = (
    count_demo_df
    .groupBy('group_id')
    .agg(
        # Counts all rows in the group.
        F.count('*').alias('row_count'),

        # Counts only non-NULL values.
        F.count('value').alias('non_null_value_count'),
    )
)

count_result_df.show(truncate=False)
```

## 2.3 Conditional aggregation

```python
from pyspark.sql import functions as F



# Calculate multiple subset metrics within the same product grouping.
conditional_metrics_df = (
    sales_lines_df
    .groupBy('product_id')
    .agg(
        F.sum(
            F.when(
                F.col('discount_pct') > 0,
                F.col('net_sales'),
            ).otherwise(F.lit(0))
        ).alias('discounted_net_sales'),

        F.sum(
            F.when(
                F.col('discount_pct') > 0,
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias('discounted_line_count'),

        F.sum(
            F.when(
                F.col('quantity') >= 3,
                F.col('quantity'),
            ).otherwise(F.lit(0))
        ).alias('large_line_units'),
    )
)

conditional_metrics_df.show(truncate=False)
```

## Questions

1. Why does `count('*')` differ from `count('value')` when `value` contains NULL?
2. What is the output grain of `conditional_metrics_df`?
3. In conditional aggregation, what does `when()` decide and what does `sum()` decide?
4. Why can an average of subgroup averages be wrong for an overall average?

---

[Back to Table of Contents](#toc)

---

<a id="experiment-3-join-cardinality-and-row-multiplication"></a>
# Experiment 3 — Join Cardinality and Row Multiplication

## Goal

Understand:

- one-to-one;
- one-to-many;
- many-to-one;
- many-to-many;
- row-count reasoning before joins.

## 3.1 One-to-many: orders to order items

Before running the join:

```text
Left grain:
one row per order

Right grain:
one row per order line

Relationship:
one order → many order lines

Can rows multiply?
Yes

Target grain:
one row per order line
```

```python



# The expansion is intentional because an order with multiple lines becomes
# multiple joined rows at the finer order-line grain.
orders_with_items_df = orders_df.join(
    sales_lines_df,
    on='order_id',
    how='inner',
)

orders_with_items_df.show(truncate=False)
```

Compare counts:

```python



print(f'Orders: {orders_df.count()}')
print(f'Order lines: {sales_lines_df.count()}')
print(f'Joined rows: {orders_with_items_df.count()}')
```

## 3.2 Many-to-one: order items to products

Before running the join:

```text
Left grain:
one row per order line

Right grain:
one row per product

Relationship:
many order lines → one product

Can rows multiply?
Not if product_id is unique on the right

Expected target grain:
one row per order line
```

```python



items_with_products_df = sales_lines_df.join(
    products_df,
    on='product_id',
    how='left',
)

print(f'Before join: {sales_lines_df.count()}')
print(f'After join: {items_with_products_df.count()}')
```

## 3.3 Deliberately break right-side uniqueness

```python



# Add a duplicate P001 row to simulate a broken dimension key.
duplicate_products_df = products_df.unionByName(
    products_df.filter(
        products_df.product_id == 'P001'
    )
)



# LEFT JOIN still preserves every left row, but it can produce MORE rows when
# a left key matches multiple right rows.
bad_join_df = sales_lines_df.join(
    duplicate_products_df,
    on='product_id',
    how='left',
)

print(f'Before join: {sales_lines_df.count()}')
print(f'After bad join: {bad_join_df.count()}')
```

## Questions

1. Why does the orders-to-items join legitimately expand rows?
2. Why should the item-to-product join preserve row count?
3. Why is a left join **not** automatically row-count preserving?
4. What business assumption did the duplicated `P001` record violate?

---

[Back to Table of Contents](#toc)

---

<a id="experiment-4-join-types"></a>
# Experiment 4 — Join Types

## Goal

Practice:

- inner;
- left;
- right;
- full;
- semi;
- anti;
- cross.

Create a small product-reference DataFrame containing one unmatched product:

```python
from decimal import Decimal

from pyspark.sql.types import (
    DecimalType,
    StringType,
    StructField,
    StructType,
)



products_with_extra_schema = StructType([
    StructField('product_id', StringType(), False),
    StructField('product_name', StringType(), False),
    StructField('category', StringType(), False),
    StructField('unit_cost', DecimalType(12, 2), False),
])

products_with_extra_df = spark.createDataFrame(
    [
        ('P001', 'Coffee Beans', 'GROCERY', Decimal('7.00')),
        ('P002', 'Coffee Grinder', 'EQUIPMENT', Decimal('18.00')),
        ('P003', 'Kettle', 'EQUIPMENT', Decimal('32.00')),
        ('P004', 'Paper Filters', 'GROCERY', Decimal('3.00')),
        ('P999', 'Unordered Product', 'GROCERY', Decimal('4.00')),
    ],
    schema=products_with_extra_schema,
)
```

## 4.1 Inner join

```python



# Keep only rows with matches on both sides.
inner_df = sales_lines_df.join(
    products_with_extra_df,
    on='product_id',
    how='inner',
)

inner_df.show(truncate=False)
```

## 4.2 Left join

```python



# Keep every sales-line row even if no product match exists.
left_df = sales_lines_df.join(
    products_with_extra_df,
    on='product_id',
    how='left',
)

left_df.show(truncate=False)
```

## 4.3 Right join

```python



# Keep every product row, including P999 with no sales-line match.
right_df = sales_lines_df.join(
    products_with_extra_df,
    on='product_id',
    how='right',
)

right_df.show(truncate=False)
```

## 4.4 Full join

```python



# Keep unmatched rows from both sides.
full_df = sales_lines_df.join(
    products_with_extra_df,
    on='product_id',
    how='full',
)

full_df.show(truncate=False)
```

## 4.5 Semi join

```python



# LEFT SEMI keeps only left rows whose product_id has at least one match.
#
# Right-side columns are not returned.
semi_df = sales_lines_df.join(
    products_with_extra_df,
    on='product_id',
    how='left_semi',
)

semi_df.show(truncate=False)
```

## 4.6 Anti join

```python



# LEFT ANTI keeps only left rows with no product match.
#
# This is especially useful for referential-integrity checks.
anti_df = sales_lines_df.join(
    products_with_extra_df,
    on='product_id',
    how='left_anti',
)

anti_df.show(truncate=False)
```

## 4.7 Cross join

```python
from datetime import date



dates_df = spark.createDataFrame(
    [
        (date(2026, 1, 5),),
        (date(2026, 1, 6),),
    ],
    ['calendar_date'],
)



# CROSS JOIN intentionally creates every date-store combination.
#
# Expected row count:
#     number of dates × number of stores
date_store_scaffold_df = dates_df.crossJoin(
    stores_df
)

print(f'Dates: {dates_df.count()}')
print(f'Stores: {stores_df.count()}')
print(f'Cross rows: {date_store_scaffold_df.count()}')
```

## Questions

1. Which join type is best for finding orphaned foreign keys?
2. Which join type answers only whether a left row has a match, without adding right-side columns?
3. Why does `P999` appear in a right or full join but not an inner join?
4. When can a cross join be legitimate in data engineering?

---

[Back to Table of Contents](#toc)

---

<a id="experiment-5-multi-column-joins-and-duplicate-column-names"></a>
# Experiment 5 — Multi-Column Joins and Duplicate Column Names

## Goal

Understand:

- composite join keys;
- incomplete-key mistakes;
- aliases;
- explicit projection after joins.

## 5.1 Multi-column join

Create store-product targets:

```python
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)



# GRAIN:
#     one row per store and product
targets_schema = StructType([
    StructField('store_id', StringType(), False),
    StructField('product_id', StringType(), False),
    StructField('target_on_hand', IntegerType(), False),
])

targets_df = spark.createDataFrame(
    [
        ('S01', 'P001', 20),
        ('S01', 'P002', 8),
        ('S01', 'P004', 12),
        ('S02', 'P003', 6),
    ],
    schema=targets_schema,
)
```

Join on the complete key:

```python



# Complete join key:
#     (store_id, product_id)
inventory_targets_df = inventory_df.join(
    targets_df,
    on=[
        'store_id',
        'product_id',
    ],
    how='left',
)

inventory_targets_df.show(truncate=False)
```

Now deliberately join on only `product_id`:

```python



# This ignores the store portion of targets_df's grain.
#
# If the same product has target rows for multiple stores, rows can match
# unrelated store-specific targets.
incomplete_key_join_df = inventory_df.join(
    targets_df,
    on='product_id',
    how='left',
)

incomplete_key_join_df.show(truncate=False)
```

## 5.2 Duplicate column names

```python
from pyspark.sql import functions as F



orders = orders_df.alias('o')
stores = stores_df.alias('s')



# A boolean join expression retains both sides' columns.
#
# Use aliases and explicit projection so every selected field has clear
# ownership and ambiguous names do not leak downstream.
orders_with_store_df = (
    orders
    .join(
        stores,
        F.col('o.store_id') == F.col('s.store_id'),
        how='left',
    )
    .select(
        F.col('o.order_id'),
        F.col('o.store_id'),
        F.col('o.order_status'),
        F.col('s.store_name'),
        F.col('s.province'),
    )
)

orders_with_store_df.show(truncate=False)
```

## Questions

1. What is the grain of `targets_df`?
2. Why is `product_id` alone an incomplete join key for that dataset?
3. Why are aliases useful when two DataFrames contain identically named columns?
4. When is `on=['store_id', 'product_id']` preferable to a boolean join expression?

---

[Back to Table of Contents](#toc)

---

<a id="experiment-6-join-key-validation-and-referential-integrity"></a>
# Experiment 6 — Join-Key Validation and Referential Integrity

## Goal

Practice:

- duplicate-key detection;
- composite-key validation;
- referential-integrity checks;
- validating assumptions before joins.

## 6.1 Validate a supposed unique key

```python
from pyspark.sql import functions as F



# A non-empty result means product_id is NOT unique.
duplicate_product_keys_df = (
    products_df
    .groupBy('product_id')
    .agg(
        F.count('*').alias('row_count')
    )
    .filter(
        F.col('row_count') > 1
    )
)

duplicate_product_keys_df.show(truncate=False)
```

## 6.2 Validate a composite key

```python
from pyspark.sql import functions as F



# order_items is supposed to contain one row per:
#     (order_id, line_number)
duplicate_order_line_keys_df = (
    order_items_df
    .groupBy(
        'order_id',
        'line_number',
    )
    .agg(
        F.count('*').alias('row_count')
    )
    .filter(
        F.col('row_count') > 1
    )
)

duplicate_order_line_keys_df.show(truncate=False)
```

## 6.3 Referential integrity with a left anti join

```python



# Find order-item rows whose product_id does not exist in products.
orphan_product_items_df = order_items_df.join(
    products_df.select('product_id'),
    on='product_id',
    how='left_anti',
)

orphan_product_items_df.show(truncate=False)
```

## 6.4 Create an orphan deliberately

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



# P404 should now appear because no parent product exists.
orphan_product_items_df = order_items_with_orphan_df.join(
    products_df.select('product_id'),
    on='product_id',
    how='left_anti',
)

orphan_product_items_df.show(truncate=False)
```

## Questions

1. Why should a supposed dimension key be validated before a many-to-one join?
2. What does an empty duplicate-key result mean?
3. What does an empty anti-join result mean?
4. Why is key validation part of metric correctness rather than merely data cleaning?

---

[Back to Table of Contents](#toc)

---

<a id="experiment-7-many-to-many-joins-and-pre-aggregation"></a>
# Experiment 7 — Many-to-Many Joins and Pre-Aggregation

## Goal

Understand:

- why many-to-many joins multiply rows;
- why multiplied rows can corrupt measures;
- when pre-aggregation is necessary.

## 7.1 Create multiple promotions per product

```python
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)



# GRAIN:
#     one row per product-promotion
promotions_schema = StructType([
    StructField('promotion_id', StringType(), False),
    StructField('product_id', StringType(), False),
    StructField('promotion_type', StringType(), False),
])

promotions_df = spark.createDataFrame(
    [
        ('PROMO-01', 'P001', 'LOYALTY'),
        ('PROMO-02', 'P001', 'WEEKEND'),
        ('PROMO-03', 'P002', 'LOYALTY'),
    ],
    schema=promotions_schema,
)
```

## 7.2 Direct many-to-many join

Before running:

```text
sales_lines_df
many rows per product

promotions_df
many rows per product

Join:
product_id

Relationship:
many-to-many
```

```python



# Every P001 sales line matches BOTH P001 promotions.
#
# The resulting sales values are repeated once per matching promotion.
many_to_many_df = sales_lines_df.join(
    promotions_df,
    on='product_id',
    how='inner',
)

many_to_many_df.select(
    'product_id',
    'order_id',
    'line_number',
    'promotion_id',
    'net_sales',
).show(truncate=False)
```

Compare revenue:

```python
from pyspark.sql import functions as F



source_revenue = (
    sales_lines_df
    .agg(
        F.sum('net_sales').alias('net_sales')
    )
    .first()['net_sales']
)

multiplied_revenue = (
    many_to_many_df
    .agg(
        F.sum('net_sales').alias('net_sales')
    )
    .first()['net_sales']
)

print(f'Source revenue: {source_revenue}')
print(f'After many-to-many join: {multiplied_revenue}')
```

## 7.3 Pre-aggregate before joining

```python
from pyspark.sql import functions as F



# Collapse promotions to one row per product because that is the level of
# information required by the sales-line enrichment.
promotion_summary_df = (
    promotions_df
    .groupBy('product_id')
    .agg(
        F.countDistinct(
            'promotion_id'
        ).alias('promotion_count'),

        F.collect_set(
            'promotion_type'
        ).alias('promotion_types'),
    )
)



# RIGHT GRAIN:
#     one row per product
#
# The join is now many-to-one from sales lines.
safe_promotion_join_df = sales_lines_df.join(
    promotion_summary_df,
    on='product_id',
    how='left',
)

print(f'Before join: {sales_lines_df.count()}')
print(f'After join: {safe_promotion_join_df.count()}')
```

## Questions

1. Why does the direct promotion join overstate revenue?
2. What determines whether pre-aggregation is the correct fix?
3. What is the grain of `promotion_summary_df`?
4. Why is many-to-many not universally wrong, even though it is dangerous?

---

[Back to Table of Contents](#toc)

---

<a id="experiment-8-groupby-vs-window-partitionby"></a>
# Experiment 8 — `groupBy()` vs. `Window.partitionBy()`

## Goal

Understand:

- why `groupBy()` collapses rows;
- why windows preserve rows;
- `Window.partitionBy()`.

First enrich sales lines with order/store information:

```python



# Each join is many-to-one from the order-line grain when right-side keys are
# valid, so the intended grain remains one row per order line.
sales_enriched_df = (
    sales_lines_df
    .join(
        orders_df,
        on='order_id',
        how='inner',
    )
    .join(
        products_df,
        on='product_id',
        how='inner',
    )
    .join(
        stores_df,
        on='store_id',
        how='inner',
    )
)
```

## 8.1 `groupBy()` collapses rows

```python
from pyspark.sql import functions as F



# OUTPUT GRAIN:
#     one row per store
store_totals_df = (
    sales_enriched_df
    .groupBy('store_id')
    .agg(
        F.sum('net_sales').alias('store_net_sales')
    )
)

store_totals_df.show(truncate=False)
```

## 8.2 Window preserves rows

```python
from pyspark.sql import Window
from pyspark.sql import functions as F



# Window.partitionBy() defines which rows can see one another.
#
# It does NOT collapse those rows.
store_window = Window.partitionBy('store_id')

sales_with_store_total_df = (
    sales_enriched_df
    .withColumn(
        'store_net_sales',
        F.sum('net_sales').over(store_window),
    )
)

sales_with_store_total_df.select(
    'order_id',
    'line_number',
    'store_id',
    'net_sales',
    'store_net_sales',
).show(truncate=False)
```

Compare counts:

```python



print(f'Source rows: {sales_enriched_df.count()}')
print(f'groupBy rows: {store_totals_df.count()}')
print(f'Window rows: {sales_with_store_total_df.count()}')
```

## Questions

1. What does one row represent after the `groupBy()`?
2. What does one row represent after the window?
3. Why can both techniques compute store totals but produce different shapes?
4. When would you prefer the window version?

---

[Back to Table of Contents](#toc)

---

<a id="experiment-9-ranking-and-latest-record-selection"></a>
# Experiment 9 — `row_number()`, `rank()`, `dense_rank()`, and Latest-Record Selection

## Goal

Practice:

- `Window.orderBy()`;
- `row_number()`;
- `rank()`;
- `dense_rank()`;
- deterministic latest-record selection.

## 9.1 Rank products within category

First aggregate to product grain:

```python
from pyspark.sql import functions as F



product_revenue_df = (
    sales_enriched_df
    .filter(
        F.col('order_status') == 'COMPLETED'
    )
    .groupBy(
        'category',
        'product_id',
        'product_name',
    )
    .agg(
        F.sum('net_sales').alias('net_sales')
    )
)
```

Create ranking windows:

```python
from pyspark.sql import Window
from pyspark.sql import functions as F



# rank() and dense_rank() should treat equal revenue values as true ties.
revenue_rank_window = (
    Window
    .partitionBy('category')
    .orderBy(
        F.col('net_sales').desc()
    )
)



# row_number() must choose one deterministic sequence.
#
# product_id is added as a stable tie-breaker.
row_number_window = (
    Window
    .partitionBy('category')
    .orderBy(
        F.col('net_sales').desc(),
        F.col('product_id').asc(),
    )
)

ranked_products_df = (
    product_revenue_df
    .withColumn(
        'row_number',
        F.row_number().over(row_number_window),
    )
    .withColumn(
        'rank',
        F.rank().over(revenue_rank_window),
    )
    .withColumn(
        'dense_rank',
        F.dense_rank().over(revenue_rank_window),
    )
)

ranked_products_df.show(truncate=False)
```

## 9.2 Latest inventory record

```python
from pyspark.sql import Window
from pyspark.sql import functions as F



# TARGET GRAIN:
#     one row per snapshot date, store, and product
#
# BUSINESS RULE:
#     newest snapshot timestamp wins
#
# TIE-BREAKER:
#     greatest ingestion_id wins
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

latest_inventory_df = (
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

latest_inventory_df.show(truncate=False)
```

## 9.3 Compare with `dropDuplicates()`

```python



# This keeps one row per business key, but does not encode which timestamp
# should survive.
arbitrary_inventory_df = inventory_df.dropDuplicates([
    'snapshot_date',
    'store_id',
    'product_id',
])

arbitrary_inventory_df.show(truncate=False)
```

## Questions

1. Why does `row_number()` need a tie-breaker for deterministic selection?
2. What is the difference between `rank()` and `dense_rank()` under ties?
3. Why is `dropDuplicates()` insufficient for latest-record selection?
4. What is the target grain of `latest_inventory_df`?

---

[Back to Table of Contents](#toc)

---

<a id="experiment-10-lag-lead-running-and-rolling"></a>
# Experiment 10 — `lag()`, `lead()`, Running Totals, and Rolling Windows

## Goal

Practice:

- `lag()`;
- `lead()`;
- running aggregates;
- `rowsBetween()`;
- `rangeBetween()`;
- row-based vs. time/value-based windows.

## 10.1 Daily store sales

```python
from pyspark.sql import functions as F



# OUTPUT GRAIN:
#     one row per store and order date
daily_store_sales_df = (
    sales_enriched_df
    .filter(
        F.col('order_status') == 'COMPLETED'
    )
    .groupBy(
        'store_id',
        'order_date',
    )
    .agg(
        F.sum('net_sales').alias('daily_net_sales')
    )
)

daily_store_sales_df.show(truncate=False)
```

## 10.2 `lag()` and `lead()`

```python
from pyspark.sql import Window
from pyspark.sql import functions as F



store_day_window = (
    Window
    .partitionBy('store_id')
    .orderBy('order_date')
)

neighbor_sales_df = (
    daily_store_sales_df
    .withColumn(
        'previous_observed_sales',
        F.lag('daily_net_sales').over(store_day_window),
    )
    .withColumn(
        'next_observed_sales',
        F.lead('daily_net_sales').over(store_day_window),
    )
)

neighbor_sales_df.show(truncate=False)
```

Important:

```text
lag() means previous ordered ROW.
It does not automatically mean previous calendar day.
```

## 10.3 Running total

```python
from pyspark.sql import Window
from pyspark.sql import functions as F



running_window = (
    Window
    .partitionBy('store_id')
    .orderBy('order_date')
    .rowsBetween(
        Window.unboundedPreceding,
        Window.currentRow,
    )
)

running_sales_df = (
    daily_store_sales_df
    .withColumn(
        'running_net_sales',
        F.sum('daily_net_sales').over(running_window),
    )
)

running_sales_df.show(truncate=False)
```

## 10.4 Three-row rolling calculation

```python
from pyspark.sql import Window
from pyspark.sql import functions as F



rolling_3_row_window = (
    Window
    .partitionBy('store_id')
    .orderBy('order_date')
    .rowsBetween(-2, 0)
)

rolling_3_row_df = (
    daily_store_sales_df
    .withColumn(
        'rolling_3_observed_rows',
        F.sum('daily_net_sales').over(rolling_3_row_window),
    )
)

rolling_3_row_df.show(truncate=False)
```

This means:

```text
current row + previous two observed rows
```

It does **not** necessarily mean three calendar days.

## 10.5 Seven-calendar-day rolling calculation

```python
from pyspark.sql import Window
from pyspark.sql import functions as F



daily_with_day_number_df = (
    daily_store_sales_df
    .withColumn(
        # Convert the date to a numeric day offset because rangeBetween()
        # works with the ordering expression's values.
        'day_number',
        F.datediff(
            F.col('order_date'),
            F.lit('1970-01-01'),
        ),
    )
)

rolling_7_day_window = (
    Window
    .partitionBy('store_id')
    .orderBy('day_number')
    .rangeBetween(-6, 0)
)

rolling_7_day_df = (
    daily_with_day_number_df
    .withColumn(
        'rolling_7_day_sales',
        F.sum('daily_net_sales').over(rolling_7_day_window),
    )
    .drop('day_number')
)

rolling_7_day_df.show(truncate=False)
```

## Questions

1. Why does `lag()` not automatically mean yesterday?
2. What rows belong to the running window for the current row?
3. What is the difference between `rowsBetween(-2, 0)` and `rangeBetween(-6, 0)` in these examples?
4. Why is the ordering column part of the business meaning of a window?

---

[Back to Table of Contents](#toc)

---

<a id="experiment-11-deduplication-and-unions"></a>
# Experiment 11 — Deduplication, `union()`, and `unionByName()`

## Goal

Understand:

- `dropDuplicates()` vs. deterministic selection;
- union vs. join;
- positional `union()`;
- name-based `unionByName()`.

## 11.1 Deterministic deduplication

```python
from datetime import datetime

from pyspark.sql import Window
from pyspark.sql import functions as F



events_df = spark.createDataFrame(
    [
        ('E001', datetime(2026, 1, 1, 10, 0), 1),
        ('E001', datetime(2026, 1, 1, 10, 5), 2),
        ('E002', datetime(2026, 1, 1, 11, 0), 1),
    ],
    [
        'event_id',
        'event_ts',
        'payload_version',
    ],
)



# Any survivor is acceptable only when the business rule truly does not care.
arbitrary_survivor_df = events_df.dropDuplicates([
    'event_id'
])



# Here the business rule DOES care:
# keep the newest event, then greatest payload version under ties.
event_window = (
    Window
    .partitionBy('event_id')
    .orderBy(
        F.col('event_ts').desc(),
        F.col('payload_version').desc(),
    )
)

latest_event_df = (
    events_df
    .withColumn(
        '_row_number',
        F.row_number().over(event_window),
    )
    .filter(
        F.col('_row_number') == 1
    )
    .drop('_row_number')
)
```

## 11.2 `union()` is positional

```python



batch_a_df = spark.createDataFrame(
    [
        ('R001', 'READY'),
        ('R002', 'READY'),
    ],
    [
        'record_id',
        'status',
    ],
)

batch_b_reordered_df = spark.createDataFrame(
    [
        ('READY', 'R003'),
        ('FAILED', 'R004'),
    ],
    [
        'status',
        'record_id',
    ],
)



# union() aligns columns by POSITION.
#
# Both columns are strings, so this can run while silently putting values under
# the wrong semantic column names.
unsafe_union_df = batch_a_df.union(
    batch_b_reordered_df
)

unsafe_union_df.show(truncate=False)
```

## 11.3 `unionByName()`

```python



# Resolve columns by NAME instead of position.
safe_union_df = batch_a_df.unionByName(
    batch_b_reordered_df
)

safe_union_df.show(truncate=False)
```

## 11.4 Missing columns

```python



batch_c_df = spark.createDataFrame(
    [
        ('R005', 'READY', 'API'),
    ],
    [
        'record_id',
        'status',
        'source_system',
    ],
)



# Missing columns are filled with NULL when allowMissingColumns=True.
evolving_union_df = batch_a_df.unionByName(
    batch_c_df,
    allowMissingColumns=True,
)

evolving_union_df.show(truncate=False)
```

## Key distinction

```text
JOIN
combines columns based on a row relationship

UNION
stacks rows representing the same conceptual row type
```

## Questions

1. Why can positional union be dangerous even if Spark raises no error?
2. Why is `unionByName()` usually safer for pipeline batches?
3. Does union remove duplicates automatically?
4. When does `dropDuplicates()` remain appropriate?

---

[Back to Table of Contents](#toc)

---

<a id="experiment-12-explode-pivot-and-unpivot"></a>
# Experiment 12 — `explode()`, Pivot, and Unpivot

## Goal

Understand:

- `explode()`;
- grain expansion;
- pivot;
- unpivot;
- long vs. wide analytical shapes.

## 12.1 `explode()`

```python
from pyspark.sql import functions as F



tagged_products_df = spark.createDataFrame(
    [
        ('P001', ['grocery', 'coffee']),
        ('P002', ['equipment', 'coffee']),
        ('P003', []),
        ('P004', None),
    ],
    [
        'product_id',
        'tags',
    ],
)



# INPUT GRAIN:
#     one row per product
#
# OUTPUT GRAIN:
#     one row per product-tag
product_tags_df = tagged_products_df.select(
    'product_id',
    F.explode('tags').alias('tag'),
)

product_tags_df.show(truncate=False)
```

Compare with `explode_outer()`:

```python
from pyspark.sql import functions as F



# explode_outer() can preserve a parent whose collection is NULL or empty by
# producing a row with a NULL child value.
product_tags_outer_df = tagged_products_df.select(
    'product_id',
    F.explode_outer('tags').alias('tag'),
)

product_tags_outer_df.show(truncate=False)
```

## 12.2 Pivot

Create monthly store sales:

```python
from pyspark.sql import functions as F



monthly_store_sales_df = (
    sales_enriched_df
    .filter(
        F.col('order_status') == 'COMPLETED'
    )
    .withColumn(
        'month',
        F.date_format(
            F.col('order_date'),
            'yyyy-MM',
        ),
    )
    .groupBy(
        'store_id',
        'month',
    )
    .agg(
        F.sum('net_sales').alias('net_sales')
    )
)
```

Pivot:

```python
from pyspark.sql import functions as F



# Convert values from month into separate output columns.
#
# Explicit pivot values make the intended output schema predictable.
wide_sales_df = (
    monthly_store_sales_df
    .groupBy('store_id')
    .pivot(
        'month',
        [
            '2026-01',
            '2026-02',
        ],
    )
    .agg(
        F.sum('net_sales')
    )
)

wide_sales_df.show(truncate=False)
```

## 12.3 Unpivot

```python



# Convert wide month columns back into:
#     store_id, month, net_sales
#
# PySpark 4.2.0 provides DataFrame.unpivot().
long_sales_df = wide_sales_df.unpivot(
    ids='store_id',
    values=[
        '2026-01',
        '2026-02',
    ],
    variableColumnName='month',
    valueColumnName='net_sales',
)

long_sales_df.show(truncate=False)
```

## Questions

1. How does `explode()` change grain?
2. Why can exploding arrays before a sum duplicate parent-level measures?
3. Why is long-form data often easier for generic analytical processing?
4. Why can unpivot increase row count?

---

[Back to Table of Contents](#toc)

---

<a id="experiment-13-dimensional-modeling"></a>
# Experiment 13 — Dimensional Modeling

## Goal

Connect Phase 2 transformations to:

- grain;
- facts;
- dimensions;
- transaction facts;
- snapshot facts;
- business keys;
- surrogate keys;
- star schemas;
- conformed dimensions;
- slowly changing dimensions;
- derived measures.

## 13.1 Intended analytical model

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

## Intended grains

```text
dim_product
one row per current product

dim_store
one row per current store

dim_date
one row per calendar date

fact_sales
one row per order line

fact_inventory_snapshot
one row per date, store, and product
```

## 13.2 Business keys vs. surrogate keys

```text
Business key:
identifier from the source/business domain

Examples:
product_id
store_id
order_id

Surrogate key:
warehouse-managed identifier for a dimension row

Examples:
product_key
store_key
date_key
```

For practice, use fixed surrogate-key maps:

```python
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)



# Fixed mappings make the key assignment stable for the experiment.
#
# A production warehouse needs a persistent surrogate-key strategy.
product_key_map_df = spark.createDataFrame(
    [
        (101, 'P001'),
        (102, 'P002'),
        (103, 'P003'),
        (104, 'P004'),
    ],
    StructType([
        StructField('product_key', IntegerType(), False),
        StructField('product_id', StringType(), False),
    ]),
)

store_key_map_df = spark.createDataFrame(
    [
        (201, 'S01'),
        (202, 'S02'),
        (203, 'S03'),
    ],
    StructType([
        StructField('store_key', IntegerType(), False),
        StructField('store_id', StringType(), False),
    ]),
)
```

## 13.3 Build dimensions

```python



# GRAIN:
#     one row per current product
dim_product_df = (
    product_key_map_df
    .join(
        products_df,
        on='product_id',
        how='inner',
    )
)



# GRAIN:
#     one row per current store
dim_store_df = (
    store_key_map_df
    .join(
        stores_df,
        on='store_id',
        how='inner',
    )
)
```

Build a conformed date dimension:

```python
from pyspark.sql import functions as F



sales_dates_df = orders_df.select(
    F.col('order_date').alias('full_date')
)

inventory_dates_df = latest_inventory_df.select(
    F.col('snapshot_date').alias('full_date')
)



# UNION because both inputs represent the same conceptual row type:
# one calendar date.
dim_date_df = (
    sales_dates_df
    .unionByName(inventory_dates_df)
    .distinct()
    .withColumn(
        'date_key',
        F.date_format(
            F.col('full_date'),
            'yyyyMMdd',
        ).cast('int'),
    )
    .withColumn(
        'year',
        F.year('full_date'),
    )
    .withColumn(
        'month',
        F.month('full_date'),
    )
    .withColumn(
        'day',
        F.dayofmonth('full_date'),
    )
)
```

## 13.4 Transaction fact concept

```text
fact_sales
grain = one row per order line
```

Typical measures:

```text
quantity
unit_price
gross_sales
discount_amount
net_sales
gross_margin
```

Each dimension join should be many-to-one from the fact grain.

## 13.5 Snapshot fact concept

```text
fact_inventory_snapshot
grain = one row per date, store, and product
```

Typical measures:

```text
on_hand_quantity
reorder_point
```

The source inventory events must first be reduced deterministically to the target snapshot grain.

## 13.6 Conformed dimensions

`dim_product`, `dim_store`, and `dim_date` are **conformed** when both facts use the same definitions and keys for those dimensions.

That makes analyses across business processes consistent:

```text
sales by product category
inventory by product category

sales by store
inventory by store

sales by date
inventory by date
```

## 13.7 Slowly changing dimensions

Conceptual distinction:

```text
Type 1
overwrite the old dimension attribute

Type 2
insert a new historical version and preserve the old version
```

Example:

```text
product P001 changes category
```

Type 1 keeps only the current category.

Type 2 preserves both historical versions so old fact rows can remain associated with the category that was valid at the time.

No full SCD implementation is required in this phase.

## Questions

1. Why is `fact_sales` a transaction fact?
2. Why is `fact_inventory_snapshot` a snapshot fact?
3. What makes `dim_product` conformed?
4. Why should surrogate keys not be generated with an arbitrary fresh row number on every run?
5. What must be true about each dimension join if `fact_sales` is to remain at order-line grain?

---

[Back to Table of Contents](#toc)

---

<a id="applied-phase-2-project"></a>
# Applied Phase 2 Project

## Goal

Build a small dimensional retail model:

```text
orders
order_items
products
stores
inventory_snapshots
        ↓
key validation
        ↓
deduplication / latest-record logic
        ↓
dimension construction
        ↓
fact construction
        ↓
grain validation
        ↓
referential-integrity validation
        ↓
metric reconciliation
```

Target outputs:

```text
dim_product
dim_store
dim_date
fact_sales
fact_inventory_snapshot
```

## Required target grains

```text
dim_product
one row per current product

dim_store
one row per current store

dim_date
one row per calendar date

fact_sales
one row per order line
key = (order_id, line_number)

fact_inventory_snapshot
one row per date, store, and product
key = (date_key, store_key, product_key)
```

## Required engineering behaviors

The project must eventually demonstrate:

- duplicate-key detection;
- referential-integrity reasoning;
- deterministic inventory latest-record selection;
- dimension joins that preserve fact grain;
- derived sales measures;
- fact-grain validation;
- row-count reconciliation;
- measure reconciliation;
- no accidental many-to-many joins.

## Important restriction

Do not begin by writing joins.

First document:

```text
source grain
source key
target grain
join relationship
expected row-count effect
```

for every transformation.

---

[Back to Table of Contents](#toc)

---

<a id="applied-task-part-1"></a>
# Applied Task — Part 1

Implement only:

1. source DataFrames;
2. source grain comments;
3. duplicate-key validation;
4. referential-integrity validation;
5. deterministic latest inventory selection;
6. construction of `dim_product`;
7. construction of `dim_store`;
8. construction of `dim_date`;
9. schema inspection;
10. row inspection.

Do **not** build the fact tables yet.

## Starter skeleton

```python
from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import SparkSession, Window
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



# Create the Spark entry point for the Phase 2 applied model.
spark = (
    SparkSession.builder
    .appName('phase02-retail-modeling')
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
# KEY VALIDATION
# =========================================================================

# TODO:
# Detect duplicate order_id values.



# TODO:
# Detect duplicate (order_id, line_number) values.



# TODO:
# Detect duplicate product_id values.



# TODO:
# Detect duplicate store_id values.



# =========================================================================
# REFERENTIAL INTEGRITY
# =========================================================================

# TODO:
# Find order items whose order_id does not exist in orders_df.



# TODO:
# Find order items whose product_id does not exist in products_df.



# TODO:
# Find orders whose store_id does not exist in stores_df.



# =========================================================================
# INVENTORY LATEST-RECORD SELECTION
# =========================================================================

# TODO:
# Build a deterministic window.
#
# PARTITION BY:
#     snapshot_date
#     store_id
#     product_id
#
# ORDER BY:
#     newest snapshot_ts
#     greatest ingestion_id as tie-breaker



# TODO:
# Keep exactly row_number == 1.



# =========================================================================
# DIMENSIONS
# =========================================================================

# TODO:
# Create or use stable product surrogate-key mappings.



# TODO:
# Build dim_product_df.
#
# TARGET GRAIN:
#     one row per current product



# TODO:
# Create or use stable store surrogate-key mappings.



# TODO:
# Build dim_store_df.
#
# TARGET GRAIN:
#     one row per current store



# TODO:
# Build dim_date_df from sales and inventory dates.
#
# TARGET GRAIN:
#     one row per calendar date
#
# Use unionByName() because both date sources represent the same row type.



# =========================================================================
# INSPECTION
# =========================================================================

# TODO:
# Print schemas for all three dimensions.



# TODO:
# Show all three dimensions ordered by their keys.



spark.stop()
```

## Questions to answer before Part 2

1. What is the grain of every source DataFrame?
2. Which keys must be unique before fact construction begins?
3. Why is a left anti join useful for referential-integrity checks?
4. Why must inventory latest-record selection use both recency and a tie-breaker?
5. Why is `dim_date` built with a union rather than a join?
6. Why must surrogate-key assignment be stable across reruns?
7. What cardinality should the future order-line-to-product-dimension join have?
8. What would happen to `fact_sales` if `dim_product.product_id` were duplicated?

---

[Back to Table of Contents](#toc)

---

<a id="after-part-1"></a>
# After Part 1

Next steps:

1. Build line-level derived measures.
2. Build `fact_sales` at `(order_id, line_number)` grain.
3. Build `fact_inventory_snapshot` at `(date_key, store_key, product_key)` grain.
4. Validate both fact grains.
5. Validate fact-to-dimension referential integrity.
6. Reconcile source order-line count to `fact_sales` row count.
7. Reconcile source sales measures to fact measures.
8. Add targeted aggregation checks.
9. Add a window-based analytical result.
10. Add one deliberate many-to-many failure experiment and repair it.
11. Complete independent Phase 2 problems.
12. Complete mastery questions.
13. Pass the Phase 2 mastery gate.
14. Preserve worthwhile repository work.
15. Update `ROADMAP.md` only after the mastery requirements are satisfied.
16. Commit the finalized phase.
17. Generate the Phase 3 starter prompt only after Phase 2 is actually complete.

[Back to Table of Contents](#toc)
