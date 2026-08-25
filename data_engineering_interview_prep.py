"""
Step 1 — SparkSession + DataFrame creation

QUESTIONS
---------

1. What is a `SparkSession`?
----------------------------
The main entry point for working with Spark SQL and DataFrames.
It lets you create/read DataFrames and execute Spark operations.


2. Why use an explicit schema instead of inference?
---------------------------------------------------
It gives predictable data types and nullability, avoids inference overhead,
and helps catch malformed data early.


3. Why `DecimalType` instead of `DoubleType` for money?
-------------------------------------------------------
Because it provides exact fixed-point precision.
Floating-point types can introduce rounding errors.


4. What does `nullable=False` accomplish?
-----------------------------------------
The field is defined as required and should not contain `NULL` values.


5. Is `show()` a transformation or an action?
---------------------------------------------
An action. It triggers Spark execution to materialize rows for display.

"""
# 1. Import what you need.
from decimal import Decimal
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# 2. Create a SparkSession named 'walmart-interview-prep'.
spark = (
    SparkSession.builder
    .appName('walmart-interview-prep')
    .master('local[*]')
    .getOrCreate()
)

# 3. Define an explicit schema for this dataset:
#
#      order_id: integer, non-null
#      store_id: string, non-null
#      product_id: string, non-null
#      quantity: integer, non-null
#      unit_price: decimal (10, 2), non-null
schema = StructType([
    StructField('order_id',   IntegerType(),      nullable=False),
    StructField('store_id',   StringType(),       nullable=False),
    StructField('product_id', StringType(),       nullable=False),
    StructField('quantity',   IntegerType(),      nullable=False),
    StructField('unit_price', DecimalType(10, 2), nullable=False),
])

# 4. Create a DataFrame from these rows:
#      1001, S01, P001, 2, 12.50
#      1002, S01, P002, 1, 20.00
#      1003, S02, P001, 3, 12.50
data = [
    (1001, 'S01', 'P001', 2, Decimal('12.50')),
    (1002, 'S01', 'P002', 1, Decimal('20.00')),
    (1003, 'S02', 'P001', 3, Decimal('12.50')),
]
sales_df = spark.createDataFrame(
    data,
    schema,
)

# 5. Print the schema.
#
# df.printSchema()
# df.show(truncate=False)
"""
root
 |-- order_id: integer (nullable = false)
 |-- store_id: string (nullable = false)
 |-- product_id: string (nullable = false)
 |-- quantity: integer (nullable = false)
 |-- unit_price: decimal(10,2) (nullable = false)

+--------+--------+----------+--------+----------+                              
|order_id|store_id|product_id|quantity|unit_price|
+--------+--------+----------+--------+----------+
|1001    |S01     |P001      |2       |12.50     |
|1002    |S01     |P002      |1       |20.00     |
|1003    |S02     |P001      |3       |12.50     |
+--------+--------+----------+--------+----------+
"""



# =============================================================================
# =============================================================================
# =============================================================================



"""
Step 2 — Aggregation in SQL + PySpark

QUESTIONS
---------

1. What is the grain of the input DataFrame?
--------------------------------------------
One row per order line in this simplified dataset.


2. What is the grain of the output?
-----------------------------------
One row per store.


3. Why does `GROUP BY` change the grain?
----------------------------------------
Because it collapses multiple input rows into one result row per grouping key.


4. Is there a performance reason to prefer Spark SQL over the DataFrame API?
----------------------------------------------------------------------------
Usually not. Both are optimized by Spark's Catalyst optimizer and can produce
similar execution plans. Choose based on readability and maintainability.


5. When might Spark SQL be preferable?
---------------------------------------
When the transformation is naturally relational and easier to express with
joins, aggregations, CTEs, or window functions.


6. When might the DataFrame API be preferable?
----------------------------------------------
When transformations need to be dynamic, reusable, or integrated with Python
application logic.


7. What is the interview-relevant habit before aggregation?
-----------------------------------------------------------
Identify the current grain and the intended output grain before writing code.

"""
# =============================================================================
# Step 2A — Spark SQL
# =============================================================================

# 1. Register the existing `df` as a temporary view named 'sales'.
#
# This does NOT copy the data into a separate table.
# It merely gives the DataFrame a SQL-accessible name for this SparkSession.
sales_df.createOrReplaceTempView('sales')

# 2. Write a Spark SQL query that returns:
#
#      store_id
#      total_revenue
#
#    where:
#
#      revenue = quantity * unit_price
#
#    Requirements:
#      - aggregate to one row per store
#      - sort by store_id
#
#    Expected result:
#
#      +--------+-------------+
#      |store_id|total_revenue|
#      +--------+-------------+
#      |S01     |45.00        |
#      |S02     |37.50        |
#      +--------+-------------+
"""
`sales_df`
+--------+--------+----------+--------+----------+                              
|order_id|store_id|product_id|quantity|unit_price|
+--------+--------+----------+--------+----------+
|1001    |S01     |P001      |2       |12.50     |
|1002    |S01     |P002      |1       |20.00     |
|1003    |S02     |P001      |3       |12.50     |
+--------+--------+----------+--------+----------+
"""
store_revenues_sql_df = spark.sql(
    """
    SELECT
        store_id,
        SUM(quantity * unit_price) AS total_revenue
    FROM sales
    GROUP BY store_id
    ORDER BY store_id;
    """
)

# 3. Display the SQL result.
# store_revenues_sql_df.show(truncate=False)
"""
`store_revenues_sql_df`
+--------+-------------+                                                        
|store_id|total_revenue|
+--------+-------------+
|S01     |45.00        |
|S02     |37.50        |
+--------+-------------+
"""


# =============================================================================
# Step 2B — PySpark DataFrame API
# =============================================================================

# 4. Starting from `df`, create a `revenue` column:
#
#      revenue = quantity * unit_price
"""
`sales_df`
+--------+--------+----------+--------+----------+                              
|order_id|store_id|product_id|quantity|unit_price|
+--------+--------+----------+--------+----------+
|1001    |S01     |P001      |2       |12.50     |
|1002    |S01     |P002      |1       |20.00     |
|1003    |S02     |P001      |3       |12.50     |
+--------+--------+----------+--------+----------+
"""
revenue_df = sales_df.withColumn(
    'revenue',
    F.col('quantity') * F.col('unit_price'),
)
# revenue_df.show(truncate=False)
"""
`revenue_df`
+--------+--------+----------+--------+----------+-------+                      
|order_id|store_id|product_id|quantity|unit_price|revenue|
+--------+--------+----------+--------+----------+-------+
|1001    |S01     |P001      |2       |12.50     |25.00  |
|1002    |S01     |P002      |1       |20.00     |20.00  |
|1003    |S02     |P001      |3       |12.50     |37.50  |
+--------+--------+----------+--------+----------+-------+
"""

# 5. Aggregate the DataFrame to one row per store.
#
#    Return:
#      - store_id
#      - total_revenue
#
# 6. Sort the result by store_id.
# 7. Display the DataFrame API result.
store_revenues_df = (
    revenue_df
    .groupBy('store_id')
    .agg(
        F.sum('revenue').alias('total_revenue')
    )
    .orderBy('store_id')
)
# store_revenues_df.show(truncate=False)
"""
`store_revenues_df`
+--------+-------------+                                                         
|store_id|total_revenue|
+--------+-------------+
|S01     |45.00        |
|S02     |37.50        |
+--------+-------------+
"""

# =============================================================================
# Step 2C — Reconciliation
# =============================================================================

# 8. Verify that the Spark SQL result and DataFrame API result contain the
#    same rows and values.
#
#    Think about:
#      - same output grain?
#      - same column values?
#      - same ordering?



# =============================================================================
# =============================================================================
# =============================================================================



"""
Step 3 — Joins + Aggregation in SQL and PySpark

QUESTIONS
---------

1. What does an INNER JOIN do?
------------------------------
Returns only rows whose join keys match in both datasets.


2. What does a LEFT JOIN do?
----------------------------
Returns every row from the left dataset and matching rows from the right.
Unmatched right-side columns become NULL.


3. Why should you identify table grain before joining?
------------------------------------------------------
Because joins can multiply rows when the join keys are not unique.
That can cause duplicated records and incorrect aggregates.


4. What is the grain of `sales_df`?
-----------------------------------
One row per order line.


5. What is the grain of `products_df`?
--------------------------------------
One row per product.


6. What is the expected grain after joining sales to products on product_id?
-----------------------------------------------------------------------------
One row per order line, assuming product_id is unique in products_df.


7. What could happen if products_df contained duplicate product_id values?
--------------------------------------------------------------------------
Each matching sales row could be duplicated, causing measures such as revenue
to be overstated.

"""
# =============================================================================
# Step 3A — Create product dimension
# =============================================================================
# 1. Define an explicit schema:
#
#      product_id: string, non-null
#      product_name: string, non-null
#      category: string, non-null
schema = StructType([
    StructField('product_id',   StringType(), False),
    StructField('product_name', StringType(), False),
    StructField('category',     StringType(), False),
])
# 2. Create `products_df` using:
#
#      P001, Coffee, Grocery
#      P002, Headphones, Electronics
products_df = spark.createDataFrame(
    [
        ('P001', 'Coffee', 'Grocery'),
        ('P002', 'Headphones', 'Electronics'),
    ],
    schema,
)
# 3. Display products_df.
# products_df.show(truncate=False)
"""
`products_df`
+----------+------------+-----------+                                           
|product_id|product_name|category   |
+----------+------------+-----------+
|P001      |Coffee      |Grocery    |
|P002      |Headphones  |Electronics|
+----------+------------+-----------+
"""


# =============================================================================
# Step 3B — Spark SQL
# =============================================================================
# 4. Register products_df as a temporary view named 'products'.
#
#    The existing sales data should already be available as 'sales'.
products_sql_df = products_df.createOrReplaceTempView('products')

# 5. Write a SQL query that:
#
#      - INNER JOINs sales to products on product_id
#      - calculates revenue = quantity * unit_price
#      - returns total revenue per category
#      - sorts categories alphabetically
#
#    Expected result:
#
#      +-----------+-------------+
#      |category   |total_revenue|
#      +-----------+-------------+
#      |Electronics|20.00        |
#      |Grocery    |62.50        |
#      +-----------+-------------+
"""
`products_df`
+----------+------------+-----------+                                           
|product_id|product_name|category   |
+----------+------------+-----------+
|P001      |Coffee      |Grocery    |
|P002      |Headphones  |Electronics|
+----------+------------+-----------+
`sales_df`
+--------+--------+----------+--------+----------+                              
|order_id|store_id|product_id|quantity|unit_price|
+--------+--------+----------+--------+----------+
|1001    |S01     |P001      |2       |12.50     |
|1002    |S01     |P002      |1       |20.00     |
|1003    |S02     |P001      |3       |12.50     |
+--------+--------+----------+--------+----------+
"""
products_sql_df = spark.sql(
    """
    SELECT
        p.category AS category,
        SUM(s.quantity * s.unit_price) AS total_revenue
    FROM products AS p
    INNER JOIN sales AS s
        ON p.product_id = s.product_id
    GROUP BY category
    ORDER BY category;
    """
)
# 6. Display the SQL result.
# products_sql_df.show(truncate=False)
"""
`products_sql_df`
+-----------+-------------+
|category   |total_revenue|
+-----------+-------------+
|Electronics|20.00        |
|Grocery    |62.50        |
+-----------+-------------+
"""


# =============================================================================
# Step 3C — PySpark DataFrame API
# =============================================================================
# 7. INNER JOIN sales_df to products_df on product_id.
#
#    Before coding, identify:
#
#      sales_df grain    = one row per order line
#      products_df grain = one row per product
#      joined grain      = one row per order line
# 
# 8. Calculate revenue.
#
# 9. Aggregate total revenue by category.
#
# 10. Sort by category.
"""
`products_df`
+----------+------------+-----------+                                           
|product_id|product_name|category   |
+----------+------------+-----------+
|P001      |Coffee      |Grocery    |
|P002      |Headphones  |Electronics|
+----------+------------+-----------+
`sales_df`
+--------+--------+----------+--------+----------+                              
|order_id|store_id|product_id|quantity|unit_price|
+--------+--------+----------+--------+----------+
|1001    |S01     |P001      |2       |12.50     |
|1002    |S01     |P002      |1       |20.00     |
|1003    |S02     |P001      |3       |12.50     |
+--------+--------+----------+--------+----------+
"""
product_category_total_revenues_df = (
    sales_df
    .join(
        products_df,
        on='product_id',
        how='inner',
    )
    .groupBy(
        F.col('category'),
    )
    .agg(
        F.sum(F.col('quantity') * F.col('unit_price')).alias('total_revenue'),
    )
    .orderBy('category')
)
# product_category_total_revenues_df.show(truncate=False)
"""
`product_category_total_revenues_df`
+-----------+-------------+                                                     
|category   |total_revenue|
+-----------+-------------+
|Electronics|20.00        |
|Grocery    |62.50        |
+-----------+-------------+
"""

# 11. Display the result.

# =============================================================================
# Step 3D — Engineering check
# =============================================================================

# 12. Answer:
#
#     If products_df accidentally contained TWO rows for P001,
#     what would happen to the P001 sales rows and the Grocery revenue?
#
#     A: If `products_df` contained two rows for `P001`, every `P001` sales
#        row would match twice. The joined dataset would thus duplicate those
#        sales rows, and `Grocery` revenue would be double-counted.



# =============================================================================
# =============================================================================
# =============================================================================



"""
Step 4 — LEFT JOIN and finding unmatched records

QUESTIONS
---------

1. What is the difference between INNER JOIN and LEFT JOIN?
----------------------------------------------------------
INNER JOIN keeps only matching rows.
LEFT JOIN keeps every left-side row and fills unmatched right-side columns
with NULL.


2. What is an anti-join?
------------------------
An anti-join returns rows from one dataset that have no matching row in
another dataset.


3. How can you implement an anti-join with a LEFT JOIN?
-------------------------------------------------------
LEFT JOIN the datasets, then filter for NULL in a right-side join key.


4. Why are anti-joins useful in data engineering?
-------------------------------------------------
They are commonly used to detect orphan records, missing dimension members,
unmatched keys, and referential-integrity problems.

"""
# =============================================================================
# Step 4A — Create store dimension
# =============================================================================
# 1. Create `stores_df` with:
#
#      S01, Toronto
#      S02, Mississauga
#      S03, Brampton
#
#    Schema:
#      store_id: string, non-null
#      city: string, non-null
schema = StructType([
    StructField('store_id', StringType(), False),
    StructField('city',     StringType(), False),
])
stores_df = spark.createDataFrame(
    [
        ('S01', 'Toronto'),
        ('S02', 'Mississauga'),
        ('S03', 'Brampton'),
    ],
    schema,
)
# stores_df.show(truncate=False)
"""
`stores_df`
+--------+-----------+                                                          
|store_id|city       |
+--------+-----------+
|S01     |Toronto    |
|S02     |Mississauga|
|S03     |Brampton   |
+--------+-----------+
"""


# =============================================================================
# Step 4B — Spark SQL
# =============================================================================
# 2. Register stores_df as temporary view 'stores'.
stores_df.createOrReplaceTempView('stores')

# 3. Write SQL that returns ALL stores and their total revenue.
#
#    Requirements:
#      - preserve S03 even though it has no sales
#      - return:
#           store_id
#           city
#           total_revenue
#      - replace missing revenue with 0
#      - sort by store_id
#
#    Expected:
#
#      S01 | Toronto     | 45.00
#      S02 | Mississauga | 37.50
#      S03 | Brampton    | 0.00
"""
`stores_df`
+--------+-----------+                                                          
|store_id|city       |
+--------+-----------+
|S01     |Toronto    |
|S02     |Mississauga|
|S03     |Brampton   |
+--------+-----------+

`sales_df`
+--------+--------+----------+--------+----------+                              
|order_id|store_id|product_id|quantity|unit_price|
+--------+--------+----------+--------+----------+
|1001    |S01     |P001      |2       |12.50     |
|1002    |S01     |P002      |1       |20.00     |
|1003    |S02     |P001      |3       |12.50     |
+--------+--------+----------+--------+----------+
"""
store_all_revenues_sql_df = spark.sql(
    """
    SELECT
        stores.store_id,
        stores.city,
        COALESCE(SUM(quantity * unit_price), 0) AS total_revenue
    FROM stores
    LEFT JOIN sales
        ON stores.store_id = sales.store_id
    GROUP BY
        stores.store_id,
        stores.city
    ORDER BY stores.store_id;
    """
)
# store_all_revenues_sql_df.show(truncate=False)
"""
`store_all_revenues_sql_df`
+--------+-----------+-------------+                                            
|store_id|city       |total_revenue|
+--------+-----------+-------------+
|S01     |Toronto    |45.00        |
|S02     |Mississauga|37.50        |
|S03     |Brampton   |0.00         |
+--------+-----------+-------------+
"""


# =============================================================================
# Step 4C — PySpark DataFrame API
# =============================================================================
# 4. Produce the same result using:
#
#      LEFT JOIN
#      groupBy
#      agg
#      coalesce
#      orderBy
"""
`stores_df`
+--------+-----------+                                                          
|store_id|city       |
+--------+-----------+
|S01     |Toronto    |
|S02     |Mississauga|
|S03     |Brampton   |
+--------+-----------+

`sales_df`
+--------+--------+----------+--------+----------+                              
|order_id|store_id|product_id|quantity|unit_price|
+--------+--------+----------+--------+----------+
|1001    |S01     |P001      |2       |12.50     |
|1002    |S01     |P002      |1       |20.00     |
|1003    |S02     |P001      |3       |12.50     |
+--------+--------+----------+--------+----------+
"""
store_all_revenues_df = (
    stores_df
    .join(
        sales_df,
        on='store_id',
        how='left',
    )
    .groupBy(
        F.col('store_id'),
        F.col('city'),
    )
    .agg(
        F.coalesce(
            F.sum(F.col('quantity') * F.col('unit_price')),
            F.lit(0),
        ).alias('total_revenue')
    )
    .orderBy('store_id')
)
# store_all_revenues_df.show(truncate=False)
"""
`store_all_revenues_df`
+--------+-----------+-------------+                                            
|store_id|city       |total_revenue|
+--------+-----------+-------------+
|S01     |Toronto    |45.00        |
|S02     |Mississauga|37.50        |
|S03     |Brampton   |0.00         |
+--------+-----------+-------------+
"""


# =============================================================================
# Step 4D — Anti-join
# =============================================================================
# 5. Find stores that have NO sales.
#
#    Expected:
#
#      S03 | Brampton
#
#    Solve it:
#      A. using SQL
#      B. using the PySpark DataFrame API
"""
`stores_df`
+--------+-----------+                                                          
|store_id|city       |
+--------+-----------+
|S01     |Toronto    |
|S02     |Mississauga|
|S03     |Brampton   |
+--------+-----------+

`sales_df`
+--------+--------+----------+--------+----------+                              
|order_id|store_id|product_id|quantity|unit_price|
+--------+--------+----------+--------+----------+
|1001    |S01     |P001      |2       |12.50     |
|1002    |S01     |P002      |1       |20.00     |
|1003    |S02     |P001      |3       |12.50     |
+--------+--------+----------+--------+----------+
"""
stores_with_no_sales_sql_df = spark.sql(
    """
    SELECT
        stores.*
    FROM stores
    LEFT JOIN sales
        ON stores.store_id = sales.store_id
    WHERE sales.store_id IS NULL
    """
)
# stores_with_no_sales_sql_df.show(truncate=False)
"""
`stores_with_no_sales_sql_df`
+--------+--------+                                                             
|store_id|city    |
+--------+--------+
|S03     |Brampton|
+--------+--------+
"""
stores_with_no_sales_df = (
    stores_df
    .join(
        sales_df,
        on='store_id',
        how='left_anti',
    )
)
# stores_with_no_sales_df.show(truncate=False)
"""
`stores_with_no_sales_df`
+--------+--------+                                                             
|store_id|city    |
+--------+--------+
|S03     |Brampton|
+--------+--------+
"""



# =============================================================================
# =============================================================================
# =============================================================================



"""
Step 5 — Multi-table joins and conditional aggregation

QUESTIONS
---------

1. What should you do before joining three or more tables?
----------------------------------------------------------
Identify the grain and join key of every dataset first.
This helps prevent accidental row multiplication and incorrect aggregates.


2. What is conditional aggregation?
-----------------------------------
Using an aggregate function together with conditional logic so that only
qualifying rows contribute to a measure.


3. What is a common SQL pattern for conditional aggregation?
------------------------------------------------------------
SUM(
    CASE
        WHEN condition THEN measure
        ELSE 0
    END
)


4. What is the equivalent PySpark pattern?
------------------------------------------
F.sum(
    F.when(condition, measure)
     .otherwise(F.lit(0))
)


5. Why might filtering before aggregation be preferable?
--------------------------------------------------------
If non-qualifying rows are not needed at all, filtering earlier is simpler
and can reduce the amount of data processed.


6. When is conditional aggregation especially useful?
------------------------------------------------------
When multiple measures for different conditions must be calculated in the
same aggregation.

"""
# =============================================================================
# Step 5A — Create order dimension
# =============================================================================
# 1. Create `orders_df` with:
#
#      1001, COMPLETED
#      1002, CANCELLED
#      1003, COMPLETED
#
#    Schema:
#      order_id: integer, non-null
#      order_status: string, non-null
schema = StructType([
    StructField('order_id',     IntegerType(), False),
    StructField('order_status', StringType(),  False),
])
orders_df = spark.createDataFrame(
    [
        (1001, 'COMPLETED'),
        (1002, 'CANCELLED'),
        (1003, 'COMPLETED'),
    ],
    schema,
)

# 2. Display orders_df.
# orders_df.show(truncate=False)
"""
`orders_df`
+--------+------------+                                                         
|order_id|order_status|
+--------+------------+
|1001    |COMPLETED   |
|1002    |CANCELLED   |
|1003    |COMPLETED   |
+--------+------------+
"""


# =============================================================================
# Step 5B — Understand the grains
# =============================================================================
# Before writing either solution, identify:
#
#      sales_df grain    = one row per order line
#      orders_df grain   = one row per order
#      products_df grain = one row per product
#
# After joining all three correctly:
#
#      joined grain      = one order line
#
# 3. Register orders_df as temporary view 'orders'.
#
orders_df.createOrReplaceTempView('orders')

# =============================================================================
# Step 5C — Spark SQL
# =============================================================================
# 4. Write a SQL query returning ONE ROW PER CATEGORY with:
#
#      category
#      gross_revenue
#      completed_revenue
#
#    Definitions:
#
#      gross_revenue
#          = revenue from ALL orders
#
#      completed_revenue
#          = revenue only where order_status = 'COMPLETED'
#
#    Join:
#
#      sales
#        → orders using order_id
#        → products using product_id
#
#    Sort by category.
#
#    Expected:
#
#      +-----------+-------------+-----------------+
#      |category   |gross_revenue|completed_revenue|
#      +-----------+-------------+-----------------+
#      |Electronics|20.00        |0.00             |
#      |Grocery    |62.50        |62.50            |
#      +-----------+-------------+-----------------+
#
"""
`sales_df`
+--------+--------+----------+--------+----------+                              
|order_id|store_id|product_id|quantity|unit_price|
+--------+--------+----------+--------+----------+
|1001    |S01     |P001      |2       |12.50     |
|1002    |S01     |P002      |1       |20.00     |
|1003    |S02     |P001      |3       |12.50     |
+--------+--------+----------+--------+----------+

`orders_df`
+--------+------------+                                                         
|order_id|order_status|
+--------+------------+
|1001    |COMPLETED   |
|1002    |CANCELLED   |
|1003    |COMPLETED   |
+--------+------------+

`products_df`
+----------+------------+-----------+                                           
|product_id|product_name|category   |
+----------+------------+-----------+
|P001      |Coffee      |Grocery    |
|P002      |Headphones  |Electronics|
+----------+------------+-----------+
"""
product_category_revenues_sql_df = spark.sql(
    """
    SELECT
        p.category,
        SUM(s.quantity * s.unit_price) AS gross_revenue,
        SUM(
            CASE
                WHEN o.order_status = 'COMPLETED'
                THEN s.quantity * s.unit_price
                ELSE 0
            END
        ) AS completed_revenue
    FROM sales AS s
    INNER JOIN orders AS o
        ON s.order_id = o.order_id
    INNER JOIN products AS p
        ON s.product_id = p.product_id
    GROUP BY p.category
    ORDER BY p.category;
    """
)
# product_category_revenues_sql_df.show(truncate=False)
"""
`product_category_revenues_sql_df`
+-----------+-------------+-----------------+                                   
|category   |gross_revenue|completed_revenue|
+-----------+-------------+-----------------+
|Electronics|20.00        |0.00             |
|Grocery    |62.50        |62.50            |
+-----------+-------------+-----------------+
"""


# =============================================================================
# Step 5D — PySpark DataFrame API
# =============================================================================
# 5. Join:
#
#      sales_df
#        → orders_df
#        → products_df
#
#    Preserve one row per order line.
#
# 6. Aggregate to one row per category.
#
# 7. Calculate:
#
#      gross_revenue
#
#    using all rows.
#
# 8. Calculate:
#
#      completed_revenue
#
#    using F.when(...).otherwise(...).
#
# 9. Sort by category.
#
"""
`sales_df`
+--------+--------+----------+--------+----------+                              
|order_id|store_id|product_id|quantity|unit_price|
+--------+--------+----------+--------+----------+
|1001    |S01     |P001      |2       |12.50     |
|1002    |S01     |P002      |1       |20.00     |
|1003    |S02     |P001      |3       |12.50     |
+--------+--------+----------+--------+----------+

`orders_df`
+--------+------------+                                                         
|order_id|order_status|
+--------+------------+
|1001    |COMPLETED   |
|1002    |CANCELLED   |
|1003    |COMPLETED   |
+--------+------------+

`products_df`
+----------+------------+-----------+                                           
|product_id|product_name|category   |
+----------+------------+-----------+
|P001      |Coffee      |Grocery    |
|P002      |Headphones  |Electronics|
+----------+------------+-----------+
"""
product_category_revenues_df = (
    sales_df
    .join(
        orders_df,
        on='order_id',
        how='inner'
    )
    .join(
        products_df,
        on='product_id',
        how='inner'
    )
    .groupBy(
        F.col('category')
    )
    .agg(
        F.sum(
            F.col('quantity') * F.col('unit_price')
        ).alias('gross_revenue'),
        F.sum(
            F.when(
                F.col('order_status') == 'COMPLETED',
                F.col('quantity') * F.col('unit_price')
            ).otherwise(
                # F.lit(0)
                F.lit(Decimal('0.00'))
            )
        ).alias('completed_revenue'),
    )
    .orderBy('category')
)
# 10. Display the result.
# product_category_revenues_df.show(truncate=False)
"""
`product_category_revenues_df`
+-----------+-------------+-----------------+                                   
|category   |gross_revenue|completed_revenue|
+-----------+-------------+-----------------+
|Electronics|20.00        |0.00             |
|Grocery    |62.50        |62.50            |
+-----------+-------------+-----------------+
"""


# =============================================================================
# Step 5E — Engineering question
# =============================================================================
# 11. Suppose orders_df accidentally contained TWO rows for order_id = 1001.
#
#     What would happen after joining it to sales_df?
#
#     What would happen to revenue?
#
#     What data-quality check would you perform before this join?
#
#     A: Rows for order_id = 1001 would be multiplied by 2, thereby producing
#        inaccurate revenue values.
#        
#        You must ensure that the one-side join keys are unique:
# (
#     orders_df
#     .groupBy('order_id')
#     .count()
#     .filter(F.col('count') > 1)
#     .show(truncate=False)
# )
"""
UNIQUE JOIN KEY VALIDATION (Good)
+--------+-----+                                                                
|order_id|count|
+--------+-----+
+--------+-----+
"""



# =============================================================================
# =============================================================================
# =============================================================================



"""
Step 6 — Window Functions: Latest Record and Deduplication

QUESTIONS
---------

1. What does a window function do?
----------------------------------
It performs calculations across related rows while preserving the original
row-level grain.


2. What does PARTITION BY do?
-----------------------------
It divides rows into independent groups for the window calculation.


3. What does ORDER BY do inside a window?
-----------------------------------------
It defines the sequence of rows within each partition.


4. When is ROW_NUMBER() useful?
-------------------------------
For latest-record selection, deduplication, and top-N-per-group problems.


5. Why is ROW_NUMBER() often preferable for deduplication?
----------------------------------------------------------
It assigns exactly one unique position to each row, so you can deterministically
keep one record per business key.


6. What is the difference between GROUP BY and a window function?
-----------------------------------------------------------------
GROUP BY collapses rows and changes grain.
A window function normally preserves the existing rows.


7. Why do we usually use a CTE/subquery before filtering on ROW_NUMBER()?
-------------------------------------------------------------------------
Because WHERE is evaluated before the window-function result is available
in standard SQL processing.

"""
# =============================================================================
# Step 6A — Create inventory snapshot data
# =============================================================================
# 1. Create `inventory_df` with this grain:
#
#      one row per store + product + snapshot date
#
#    Schema:
#
#      store_id: string, non-null
#      product_id: string, non-null
#      snapshot_date: date, non-null
#      quantity_on_hand: integer, non-null
#
#    Data:
#
#      S01, P001, 2026-08-20, 10
#      S01, P001, 2026-08-21, 8
#      S01, P002, 2026-08-20, 5
#      S01, P002, 2026-08-22, 7
#      S02, P001, 2026-08-20, 20
#      S02, P001, 2026-08-23, 16
inventory_schema = StructType([
    StructField('store_id',         StringType(),  False),
    StructField('product_id',       StringType(),  False),
    StructField('snapshot_date',    DateType(),    False),
    StructField('quantity_on_hand', IntegerType(), False),
])
inventory_df = spark.createDataFrame(
    [
        ('S01', 'P001', date(2026, 8, 20), 10),
        ('S01', 'P001', date(2026, 8, 21), 8),
        ('S01', 'P002', date(2026, 8, 20), 5),
        ('S01', 'P002', date(2026, 8, 22), 7),
        ('S02', 'P001', date(2026, 8, 20), 20),
        ('S02', 'P001', date(2026, 8, 23), 16),
    ],
    inventory_schema,
)
# 2. Display inventory_df.
# inventory_df.show(truncate=False)
"""
`inventory_df`
+--------+----------+-------------+----------------+                            
|store_id|product_id|snapshot_date|quantity_on_hand|
+--------+----------+-------------+----------------+
|S01     |P001      |2026-08-20   |10              |
|S01     |P001      |2026-08-21   |8               |
|S01     |P002      |2026-08-20   |5               |
|S01     |P002      |2026-08-22   |7               |
|S02     |P001      |2026-08-20   |20              |
|S02     |P001      |2026-08-23   |16              |
+--------+----------+-------------+----------------+
"""


# =============================================================================
# Step 6B — Spark SQL: latest record per store/product
# =============================================================================
# 3. Register inventory_df as temporary view 'inventory'.
inventory_df.createOrReplaceTempView('inventory')

# 4. Write SQL that returns ONLY the latest inventory snapshot for each:
#
#      store_id + product_id
#
#    Use:
#
#      ROW_NUMBER()
#      PARTITION BY store_id, product_id
#      ORDER BY snapshot_date DESC
#
#    Expected:
#
#      S01 | P001 | 2026-08-21 | 8
#      S01 | P002 | 2026-08-22 | 7
#      S02 | P001 | 2026-08-23 | 16
#
#    Hint:
#
#      CTE
#        → assign row number
#        → filter rn = 1
"""
`inventory_df`
+--------+----------+-------------+----------------+                            
|store_id|product_id|snapshot_date|quantity_on_hand|
+--------+----------+-------------+----------------+
|S01     |P001      |2026-08-20   |10              |
|S01     |P001      |2026-08-21   |8               |
|S01     |P002      |2026-08-20   |5               |
|S01     |P002      |2026-08-22   |7               |
|S02     |P001      |2026-08-20   |20              |
|S02     |P001      |2026-08-23   |16              |
+--------+----------+-------------+----------------+
"""
inventory_latest_snapshots_sql_df = spark.sql(
    """
    SELECT
        store_id,
        product_id,
        snapshot_date,
        quantity_on_hand
    FROM
    (
        SELECT
            *,
            ROW_NUMBER() OVER(
                PARTITION BY
                    store_id,
                    product_id
                ORDER BY
                    snapshot_date DESC
            ) AS rn
        FROM inventory
    )
    WHERE rn = 1
    ORDER BY
        store_id,
        product_id;
    """
)
# inventory_latest_snapshots_sql_df.show(truncate=False)
"""
`inventory_latest_snapshots_sql_df`
+--------+----------+-------------+----------------+                            
|store_id|product_id|snapshot_date|quantity_on_hand|
+--------+----------+-------------+----------------+
|S01     |P001      |2026-08-21   |8               |
|S01     |P002      |2026-08-22   |7               |
|S02     |P001      |2026-08-23   |16              |
+--------+----------+-------------+----------------+
"""


# =============================================================================
# Step 6C — PySpark DataFrame API
# =============================================================================

# 5. Define a Window specification partitioned by:
#
#      store_id
#      product_id
#
#    and ordered by:
#
#      snapshot_date DESC
#
# 6. Add a row-number column.
#
# 7. Filter to rn = 1.
#
# 8. Remove the helper rn column.
#
# 9. Sort by store_id, product_id.
#
"""
`inventory_df`
+--------+----------+-------------+----------------+                            
|store_id|product_id|snapshot_date|quantity_on_hand|
+--------+----------+-------------+----------------+
|S01     |P001      |2026-08-20   |10              |
|S01     |P001      |2026-08-21   |8               |
|S01     |P002      |2026-08-20   |5               |
|S01     |P002      |2026-08-22   |7               |
|S02     |P001      |2026-08-20   |20              |
|S02     |P001      |2026-08-23   |16              |
+--------+----------+-------------+----------------+
"""
window_1 = (
    Window
    .partitionBy(
        F.col('store_id'),
        F.col('product_id'),
    )
    .orderBy(
        F.col('snapshot_date').desc()
    )
)
inventory_latest_snapshots_df = (
    inventory_df
    .withColumn(
        'rn',
        F.row_number().over(window_1)
    )
    .filter(
        F.col('rn') == 1
    )
    .drop('rn')
    .orderBy(
        F.col('store_id'),
        F.col('product_id'),
    )
)

# 10. Display the result.
# inventory_latest_snapshots_df.show(truncate=False)
"""
`inventory_latest_snapshots_df`
+--------+----------+-------------+----------------+                            
|store_id|product_id|snapshot_date|quantity_on_hand|
+--------+----------+-------------+----------------+
|S01     |P001      |2026-08-21   |8               |
|S01     |P002      |2026-08-22   |7               |
|S02     |P001      |2026-08-23   |16              |
+--------+----------+-------------+----------------+
"""


# =============================================================================
# Step 6D — Deduplication reasoning
# =============================================================================

# 11. Suppose this dataset instead represented multiple versions of the same
#     business record.
#
#     Explain why:
#
#         dropDuplicates(['store_id', 'product_id'])
#
#     is NOT sufficient if you specifically need the latest record.
#
#     A: This is because then you have NO control over which record is indeed
#        the latest. The windown function `ROW_NUMBER()` rank-orders the
#        duplicate-key rows based on the snapshot date.
#        

# =============================================================================
# Step 6E — Tie-breaking
# =============================================================================

# 12. Suppose two records for the same store/product had the SAME snapshot_date.
#
#     What problem would that create for ROW_NUMBER()?
#
#     What additional ordering column could you add to make the result
#     deterministic?
#
#     A: If two store-product record has the SAME snapshot_date, their relative
#        ordering is ambiguous. `ROW_NUMBER()` will still assign 1 and 2, but
#        which row receives `rn = 1` is NOT deterministic.
#        
#        Add another column that uniquely breaks the tie, such as:
#            updated_at DESC
#            ingestion_timestamp DESC
#            version_number DESC
#        
#        Example:
#        
#            ORDER BY
#                snapshot_date DESC,
#                updated_at DESC
#



# =============================================================================
# =============================================================================
# =============================================================================



"""
Step 7 — Top N per Group: ROW_NUMBER vs RANK vs DENSE_RANK

QUESTIONS
---------

1. What is the difference between ROW_NUMBER(), RANK(), and DENSE_RANK()?
------------------------------------------------------------------------
ROW_NUMBER() assigns a unique sequential number to every row.

RANK() gives tied rows the same rank and leaves gaps afterward.

DENSE_RANK() gives tied rows the same rank without leaving gaps.


2. When should you use ROW_NUMBER()?
------------------------------------
When you need exactly one deterministic position per row, such as selecting
one winner or exactly N rows per group.


3. When should you use RANK()?
------------------------------
When ties should share the same rank and gaps in ranking are acceptable.


4. When should you use DENSE_RANK()?
------------------------------------
When ties should share the same rank but ranking should remain consecutive.


5. What is the standard pattern for top N per group?
----------------------------------------------------
Aggregate to the desired grain first, then apply a ranking window partitioned
by the grouping key and ordered by the metric descending.


6. Why should aggregation usually happen before ranking?
--------------------------------------------------------
Because ranking raw transactional rows may rank individual transactions rather
than the business entity or metric you actually care about.

"""
# =============================================================================
# Step 7A — Create richer sales data
# =============================================================================
# 1. Create a new sales dataset with enough rows to produce multiple products
#    within each category.
#
#    Use this data:
#
#      order_id | store_id | product_id | quantity | unit_price
#
#      2001     | S01      | P001       | 2        | 10.00
#      2002     | S01      | P002       | 1        | 30.00
#      2003     | S01      | P003       | 4        | 5.00
#      2004     | S02      | P001       | 3        | 10.00
#      2005     | S02      | P002       | 2        | 30.00
#      2006     | S02      | P003       | 6        | 5.00
sales_2_schema = StructType([
    StructField('order_id',   IntegerType(),      False),
    StructField('store_id',   StringType(),       False),
    StructField('product_id', StringType(),       False),
    StructField('quantity',   IntegerType(),      False),
    StructField('unit_price', DecimalType(10, 2), False),
])
sales_2_df = spark.createDataFrame(
    [
    # -------------------------------------------------------------------------
    # Grocery
    # -------------------------------------------------------------------------

    # P001 total revenue = 100.00
    (2001, 'S01', 'P001', 4, Decimal('10.00')),
    (2002, 'S02', 'P001', 6, Decimal('10.00')),

    # P002 total revenue = 100.00
    # Deliberate tie with P001.
    (2003, 'S01', 'P002', 8,  Decimal('5.00')),
    (2004, 'S03', 'P002', 12, Decimal('5.00')),

    # P003 total revenue = 80.00
    (2005, 'S02', 'P003', 4, Decimal('8.00')),
    (2006, 'S03', 'P003', 6, Decimal('8.00')),

    # P004 total revenue = 40.00
    (2007, 'S01', 'P004', 4, Decimal('4.00')),
    (2008, 'S02', 'P004', 6, Decimal('4.00')),

    # P005 total revenue = 30.00
    (2009, 'S01', 'P005', 4, Decimal('3.00')),
    (2010, 'S03', 'P005', 6, Decimal('3.00')),


    # -------------------------------------------------------------------------
    # Electronics
    # -------------------------------------------------------------------------

    # P006 total revenue = 400.00
    (2011, 'S01', 'P006', 3, Decimal('50.00')),
    (2012, 'S02', 'P006', 5, Decimal('50.00')),

    # P007 total revenue = 400.00
    # Deliberate tie with P006.
    (2013, 'S02', 'P007', 4, Decimal('40.00')),
    (2014, 'S03', 'P007', 6, Decimal('40.00')),

    # P008 total revenue = 300.00
    (2015, 'S01', 'P008', 5, Decimal('25.00')),
    (2016, 'S03', 'P008', 7, Decimal('25.00')),

    # P009 total revenue = 200.00
    (2017, 'S01', 'P009', 4, Decimal('20.00')),
    (2018, 'S02', 'P009', 6, Decimal('20.00')),

    # P010 total revenue = 100.00
    (2019, 'S02', 'P010', 4, Decimal('10.00')),
    (2020, 'S03', 'P010', 6, Decimal('10.00')),


    # -------------------------------------------------------------------------
    # Home
    # -------------------------------------------------------------------------

    # P011 total revenue = 600.00
    (2021, 'S01', 'P011', 2, Decimal('100.00')),
    (2022, 'S02', 'P011', 4, Decimal('100.00')),

    # P012 total revenue = 500.00
    (2023, 'S01', 'P012', 4, Decimal('50.00')),
    (2024, 'S03', 'P012', 6, Decimal('50.00')),

    # P013 total revenue = 400.00
    (2025, 'S02', 'P013', 4, Decimal('40.00')),
    (2026, 'S03', 'P013', 6, Decimal('40.00')),

    # P014 total revenue = 200.00
    (2027, 'S01', 'P014', 3, Decimal('25.00')),
    (2028, 'S02', 'P014', 5, Decimal('25.00')),

    # P015 total revenue = 100.00
    (2029, 'S02', 'P015', 4, Decimal('10.00')),
    (2030, 'S03', 'P015', 6, Decimal('10.00')),
    ],
    sales_2_schema,
)
# sales_2_df.show(truncate=False)
"""
`sales_2_df`
+--------+--------+----------+--------+----------+                              
|order_id|store_id|product_id|quantity|unit_price|
+--------+--------+----------+--------+----------+
|2001    |S01     |P001      |4       |10.00     |
|2002    |S02     |P001      |6       |10.00     |
|2003    |S01     |P002      |8       |5.00      |
|2004    |S03     |P002      |12      |5.00      |
|2005    |S02     |P003      |4       |8.00      |
|2006    |S03     |P003      |6       |8.00      |
|2007    |S01     |P004      |4       |4.00      |
|2008    |S02     |P004      |6       |4.00      |
|2009    |S01     |P005      |4       |3.00      |
|2010    |S03     |P005      |6       |3.00      |
|2011    |S01     |P006      |3       |50.00     |
|2012    |S02     |P006      |5       |50.00     |
|2013    |S02     |P007      |4       |40.00     |
|2014    |S03     |P007      |6       |40.00     |
|2015    |S01     |P008      |5       |25.00     |
|2016    |S03     |P008      |7       |25.00     |
|2017    |S01     |P009      |4       |20.00     |
|2018    |S02     |P009      |6       |20.00     |
|2019    |S02     |P010      |4       |10.00     |
|2020    |S03     |P010      |6       |10.00     |
+--------+--------+----------+--------+----------+
only showing top 20 rows
"""

# 2. Create a matching product dataset:
#
#      P001 | Coffee     | Grocery
#      P002 | Headphones | Electronics
#      P003 | Tea        | Grocery
product_2_schema = StructType([
    StructField('product_id',   StringType(), False),
    StructField('product_name', StringType(), False),
    StructField('category',     StringType(), False),
])
product_2_df = spark.createDataFrame(
    [
    # Grocery
    ('P001', 'Coffee',     'Grocery'),
    ('P002', 'Tea',        'Grocery'),
    ('P003', 'Rice',       'Grocery'),
    ('P004', 'Bread',      'Grocery'),
    ('P005', 'Milk',       'Grocery'),

    # Electronics
    ('P006', 'Headphones', 'Electronics'),
    ('P007', 'Keyboard',   'Electronics'),
    ('P008', 'Mouse',      'Electronics'),
    ('P009', 'Charger',    'Electronics'),
    ('P010', 'Cable',      'Electronics'),

    # Home
    ('P011', 'Vacuum',     'Home'),
    ('P012', 'Lamp',       'Home'),
    ('P013', 'Kettle',     'Home'),
    ('P014', 'Pillow',     'Home'),
    ('P015', 'Mug',        'Home'),
    ],
    product_2_schema,
)
# product_2_df.show(truncate=False)
"""
`product_2_df`
+----------+------------+-----------+                                           
|product_id|product_name|category   |
+----------+------------+-----------+
|P001      |Coffee      |Grocery    |
|P002      |Tea         |Grocery    |
|P003      |Rice        |Grocery    |
|P004      |Bread       |Grocery    |
|P005      |Milk        |Grocery    |
|P006      |Headphones  |Electronics|
|P007      |Keyboard    |Electronics|
|P008      |Mouse       |Electronics|
|P009      |Charger     |Electronics|
|P010      |Cable       |Electronics|
|P011      |Vacuum      |Home       |
|P012      |Lamp        |Home       |
|P013      |Kettle      |Home       |
|P014      |Pillow      |Home       |
|P015      |Mug         |Home       |
+----------+------------+-----------+
"""

# =============================================================================
# Step 7B — Spark SQL: top products per category
# =============================================================================
# 3. Register both datasets as temporary views.
sales_2_df.createOrReplaceTempView('sales_2')
product_2_df.createOrReplaceTempView('product_2')

# 4. Write SQL that:
#
#      - calculates total revenue per product
#      - joins product category information
#      - ranks products within each category by total revenue DESC
#      - returns the TOP 2 products per category
#
#    Required output:
#
#      category
#      product_id
#      total_revenue
#      rn
#
#    Use ROW_NUMBER().
#
#    Hint:
#
#      CTE 1:
#          aggregate to product grain
#
#      CTE 2:
#          rank products within category
#
#      final query:
#          WHERE rn <= 2
"""
`sales_2_df`  GRAIN: one row PER order
+--------+--------+----------+--------+----------+                              
|order_id|store_id|product_id|quantity|unit_price|
+--------+--------+----------+--------+----------+
|2001    |S01     |P001      |4       |10.00     |
|2002    |S02     |P001      |6       |10.00     |
|2003    |S01     |P002      |8       |5.00      |
|2004    |S03     |P002      |12      |5.00      |
|2005    |S02     |P003      |4       |8.00      |
|2006    |S03     |P003      |6       |8.00      |
|2007    |S01     |P004      |4       |4.00      |
|2008    |S02     |P004      |6       |4.00      |
|2009    |S01     |P005      |4       |3.00      |
|2010    |S03     |P005      |6       |3.00      |
|2011    |S01     |P006      |3       |50.00     |
|2012    |S02     |P006      |5       |50.00     |
|2013    |S02     |P007      |4       |40.00     |
|2014    |S03     |P007      |6       |40.00     |
|2015    |S01     |P008      |5       |25.00     |
|2016    |S03     |P008      |7       |25.00     |
|2017    |S01     |P009      |4       |20.00     |
|2018    |S02     |P009      |6       |20.00     |
|2019    |S02     |P010      |4       |10.00     |
|2020    |S03     |P010      |6       |10.00     |
+--------+--------+----------+--------+----------+
only showing top 20 rows

`product_2_df`  GRAIN: one row PER product
+----------+------------+-----------+                                           
|product_id|product_name|category   |
+----------+------------+-----------+
|P001      |Coffee      |Grocery    |
|P002      |Tea         |Grocery    |
|P003      |Rice        |Grocery    |
|P004      |Bread       |Grocery    |
|P005      |Milk        |Grocery    |
|P006      |Headphones  |Electronics|
|P007      |Keyboard    |Electronics|
|P008      |Mouse       |Electronics|
|P009      |Charger     |Electronics|
|P010      |Cable       |Electronics|
|P011      |Vacuum      |Home       |
|P012      |Lamp        |Home       |
|P013      |Kettle      |Home       |
|P014      |Pillow      |Home       |
|P015      |Mug         |Home       |
+----------+------------+-----------+
"""
top_2_products_per_category_sql_df = spark.sql(
    """
    WITH pre_aggregates AS (
        SELECT
            p.category,
            p.product_id,
            SUM(s.quantity * s.unit_price) AS total_revenue
        FROM sales_2 AS s
        INNER JOIN product_2 AS p
            ON p.product_id = s.product_id
        GROUP BY
            p.category,
            p.product_id
    ),
    row_numbers AS (
        SELECT
            *,
            ROW_NUMBER() OVER(
                PARTITION BY
                    category
                ORDER BY
                    total_revenue DESC,
                    product_id ASC
            ) AS rn
        FROM pre_aggregates
    )
    SELECT
        category,
        product_id,
        total_revenue,
        rn
    FROM row_numbers
    WHERE rn <= 2;
    """
)
# top_2_products_per_category_sql_df.show(truncate=False)
"""
`top_2_products_per_category_sql_df`
+-----------+----------+-------------+---+                                      
|category   |product_id|total_revenue|rn |
+-----------+----------+-------------+---+
|Electronics|P006      |400.00       |1  |
|Electronics|P007      |400.00       |2  |
|Grocery    |P001      |100.00       |1  |
|Grocery    |P002      |100.00       |2  |
|Home       |P011      |600.00       |1  |
|Home       |P012      |500.00       |2  |
+-----------+----------+-------------+---+

+-----------+----------+-------------+----------+----+----------+               
|category   |product_id|total_revenue|ROW_NUMBER|RANK|DENSE_RANK|
+-----------+----------+-------------+----------+----+----------+
|Electronics|P006      |400.00       |1         |1   |1         |
|Electronics|P007      |400.00       |2         |2   |2         |
|Electronics|P008      |300.00       |3         |3   |3         |
|Electronics|P009      |200.00       |4         |4   |4         |
|Electronics|P010      |100.00       |5         |5   |5         |
|Grocery    |P001      |100.00       |1         |1   |1         |
|Grocery    |P002      |100.00       |2         |2   |2         |
|Grocery    |P003      |80.00        |3         |3   |3         |
|Grocery    |P004      |40.00        |4         |4   |4         |
|Grocery    |P005      |30.00        |5         |5   |5         |
|Home       |P011      |600.00       |1         |1   |1         |
|Home       |P012      |500.00       |2         |2   |2         |
|Home       |P013      |400.00       |3         |3   |3         |
|Home       |P014      |200.00       |4         |4   |4         |
|Home       |P015      |100.00       |5         |5   |5         |
+-----------+----------+-------------+----------+----+----------+
"""


# =============================================================================
# Step 7C — PySpark DataFrame API
# =============================================================================
# 5. Join sales to products.
#
# 6. Aggregate to:
#
#      one row per category + product_id
#
#    with:
#
#      total_revenue
#
# 7. Define a Window:
#
#      PARTITION BY category
#      ORDER BY total_revenue DESC
#
# 8. Add ROW_NUMBER().
#
# 9. Filter to rn <= 2.
#
# 10. Sort by:
#
#      category
#      rn
#
"""
`sales_2_df`  GRAIN: one row PER order
+--------+--------+----------+--------+----------+                              
|order_id|store_id|product_id|quantity|unit_price|
+--------+--------+----------+--------+----------+
|2001    |S01     |P001      |4       |10.00     |
|2002    |S02     |P001      |6       |10.00     |
|2003    |S01     |P002      |8       |5.00      |
|2004    |S03     |P002      |12      |5.00      |
|2005    |S02     |P003      |4       |8.00      |
|2006    |S03     |P003      |6       |8.00      |
|2007    |S01     |P004      |4       |4.00      |
|2008    |S02     |P004      |6       |4.00      |
|2009    |S01     |P005      |4       |3.00      |
|2010    |S03     |P005      |6       |3.00      |
|2011    |S01     |P006      |3       |50.00     |
|2012    |S02     |P006      |5       |50.00     |
|2013    |S02     |P007      |4       |40.00     |
|2014    |S03     |P007      |6       |40.00     |
|2015    |S01     |P008      |5       |25.00     |
|2016    |S03     |P008      |7       |25.00     |
|2017    |S01     |P009      |4       |20.00     |
|2018    |S02     |P009      |6       |20.00     |
|2019    |S02     |P010      |4       |10.00     |
|2020    |S03     |P010      |6       |10.00     |
+--------+--------+----------+--------+----------+
only showing top 20 rows

`product_2_df`  GRAIN: one row PER product
+----------+------------+-----------+                                           
|product_id|product_name|category   |
+----------+------------+-----------+
|P001      |Coffee      |Grocery    |
|P002      |Tea         |Grocery    |
|P003      |Rice        |Grocery    |
|P004      |Bread       |Grocery    |
|P005      |Milk        |Grocery    |
|P006      |Headphones  |Electronics|
|P007      |Keyboard    |Electronics|
|P008      |Mouse       |Electronics|
|P009      |Charger     |Electronics|
|P010      |Cable       |Electronics|
|P011      |Vacuum      |Home       |
|P012      |Lamp        |Home       |
|P013      |Kettle      |Home       |
|P014      |Pillow      |Home       |
|P015      |Mug         |Home       |
+----------+------------+-----------+
"""
window_2 = (
    Window
    .partitionBy(F.col('category'))
    .orderBy(
        F.col('total_revenue').desc(),
        F.col('product_id').asc(),
    )
)
top_2_products_per_category_df = (
    sales_2_df
    .join(
        product_2_df,
        on='product_id',
        how='inner',
    )
    .groupBy(
        F.col('category'),
        F.col('product_id'),
    )
    .agg(
        F.sum(
            F.col('quantity') * F.col('unit_price')
        ).alias('total_revenue')
    )
    .select(
        F.col('*'),
        F.row_number().over(window_2).alias('rn'),
    )
    .filter(F.col('rn') <= 2)
    .orderBy(
        F.col('category'),
        F.col('product_id'),
    )
)
# top_2_products_per_category_df.show(truncate=False)
"""
`top_2_products_per_category_df`
+-----------+----------+-------------+---+
|category   |product_id|total_revenue|rn |
+-----------+----------+-------------+---+
|Electronics|P006      |400.00       |1  |
|Electronics|P007      |400.00       |2  |
|Grocery    |P001      |100.00       |1  |
|Grocery    |P002      |100.00       |2  |
|Home       |P011      |600.00       |1  |
|Home       |P012      |500.00       |2  |
+-----------+----------+-------------+---+
"""

# =============================================================================
# Step 7D — Ranking reasoning
# =============================================================================

# 11. Suppose revenues within one category were:
#
#      100
#      100
#      80
#
#     What rankings would each function produce?
#
#     ROW_NUMBER:
#         1, 2, 3
#
#     RANK:
#         1, 1, 3
#
#     DENSE_RANK:
#         1, 1, 2
#


# 12. If the requirement says:
#
#     "Return exactly two products per category"
#
#     which ranking function is safest?
#
#     A: `ROW_NUMBER() <= 2`
#

# 13. If the requirement says:
#
#     "Return the top two revenue ranks, including ties"
#
#     which function would you consider instead?
#
#     A: `DENSE_RANK()`
#



# =============================================================================
# =============================================================================
# =============================================================================



"""
Step 8 — LAG(): Previous-Row Comparisons

QUESTIONS
---------

1. What does LAG() do?
----------------------
It returns a value from a previous row within the same window partition.


2. What is LAG() commonly used for?
-----------------------------------
Period-over-period comparisons, change detection, previous-event analysis,
and comparing sequential snapshots.


3. Why does ORDER BY matter for LAG()?
--------------------------------------
Because it defines what "previous" means.


4. Does LAG() change the grain?
-------------------------------
No. It adds information from another row while preserving the current rows.


5. What happens for the first row in a partition?
-------------------------------------------------
There is no previous row, so LAG() returns NULL unless a default is specified.

"""

# =============================================================================
# Step 8A — Seed data
# =============================================================================
# 1. Create `daily_sales_df`:
#
#      store_id | sales_date | revenue
#
#      S01      | 2026-08-20 | 100.00
#      S01      | 2026-08-21 | 130.00
#      S01      | 2026-08-22 | 110.00
#      S02      | 2026-08-20 | 200.00
#      S02      | 2026-08-21 | 250.00
#      S02      | 2026-08-22 | 225.00
#
#    Grain:
#      one row per store + sales_date
daily_sales_schema = StructType([
    StructField('store_id', StringType(), False),
    StructField('sales_date', DateType(), False),
    StructField('revenue', DecimalType(10, 2), False),
])
daily_sales_df = spark.createDataFrame(
    [
        ('S01', date(2026, 8, 20), Decimal('100.00')),
        ('S01', date(2026, 8, 21), Decimal('130.00')),
        ('S01', date(2026, 8, 22), Decimal('110.00')),
        ('S02', date(2026, 8, 20), Decimal('200.00')),
        ('S02', date(2026, 8, 21), Decimal('250.00')),
        ('S02', date(2026, 8, 22), Decimal('225.00')),
    ],
    daily_sales_schema,
)
# daily_sales_df.show(truncate=False)
"""
`daily_sales_df`
+--------+----------+-------+                                                   
|store_id|sales_date|revenue|
+--------+----------+-------+
|S01     |2026-08-20|100.00 |
|S01     |2026-08-21|130.00 |
|S01     |2026-08-22|110.00 |
|S02     |2026-08-20|200.00 |
|S02     |2026-08-21|250.00 |
|S02     |2026-08-22|225.00 |
+--------+----------+-------+
"""

# =============================================================================
# Step 8B — Spark SQL
# =============================================================================
# 2. Register the DataFrame as 'daily_sales'.
daily_sales_df.createOrReplaceTempView('daily_sales')

# 3. Return:
#
#      store_id
#      sales_date
#      revenue
#      previous_revenue
#      revenue_change
#
#    where:
#
#      previous_revenue =
#          previous day's revenue for that store
#
#      revenue_change =
#          revenue - previous_revenue
#
#    Use:
#
#      LAG(revenue) OVER (
#          PARTITION BY store_id
#          ORDER BY sales_date
#      )
#
#    Hint:
#      Use a CTE/subquery so you can reference previous_revenue when
#      calculating revenue_change.
"""
`daily_sales_df`
+--------+----------+-------+                                                   
|store_id|sales_date|revenue|
+--------+----------+-------+
|S01     |2026-08-20|100.00 |
|S01     |2026-08-21|130.00 |
|S01     |2026-08-22|110.00 |
|S02     |2026-08-20|200.00 |
|S02     |2026-08-21|250.00 |
|S02     |2026-08-22|225.00 |
+--------+----------+-------+
"""
daily_revenue_change_sql_df = spark.sql(
    """
    WITH daily_sales_with_prev_dates AS (
        SELECT
            *,
            LAG(revenue) OVER(
                PARTITION BY store_id
                ORDER BY sales_date
            ) AS previous_revenue
        FROM daily_sales
    )
    SELECT
        store_id,
        sales_date,
        revenue,
        previous_revenue,
        (revenue - previous_revenue) AS revenue_change
    FROM daily_sales_with_prev_dates
    ORDER BY
        store_id,
        sales_date
    """
)
# daily_revenue_change_sql_df.show(truncate=False)
"""
`daily_revenue_change_sql_df`
+--------+----------+-------+----------------+--------------+                   
|store_id|sales_date|revenue|previous_revenue|revenue_change|
+--------+----------+-------+----------------+--------------+
|S01     |2026-08-20|100.00 |NULL            |NULL          |
|S01     |2026-08-21|130.00 |100.00          |30.00         |
|S01     |2026-08-22|110.00 |130.00          |-20.00        |
|S02     |2026-08-20|200.00 |NULL            |NULL          |
|S02     |2026-08-21|250.00 |200.00          |50.00         |
|S02     |2026-08-22|225.00 |250.00          |-25.00        |
+--------+----------+-------+----------------+--------------+
"""


# =============================================================================
# Step 8C — PySpark DataFrame API
# =============================================================================
# 4. Define a Window:
#
#      PARTITION BY store_id
#      ORDER BY sales_date
#
# 5. Add:
#
#      previous_revenue
#
#    using F.lag(...).
#
# 6. Add:
#
#      revenue_change =
#          revenue - previous_revenue
#
# 7. Sort by:
#
#      store_id
#      sales_date
#
"""
`daily_sales_df`
+--------+----------+-------+                                                   
|store_id|sales_date|revenue|
+--------+----------+-------+
|S01     |2026-08-20|100.00 |
|S01     |2026-08-21|130.00 |
|S01     |2026-08-22|110.00 |
|S02     |2026-08-20|200.00 |
|S02     |2026-08-21|250.00 |
|S02     |2026-08-22|225.00 |
+--------+----------+-------+
"""
window_3 = Window.partitionBy('store_id').orderBy('sales_date')
daily_revenue_change_df = (
    # daily_sales_df
    # .select(
    #     '*',
    #     F.lag('revenue').over(window_3).alias('previous_revenue'),
    #     (F.col('revenue') - F.col('previous_revenue')).alias('revenue_change'),
    # )
    # .orderBy(
    #     'store_id',
    #     'sales_date',
    # )
    #
    # OR...
    #
    daily_sales_df
    .withColumn(
        'previous_revenue',
        F.lag('revenue').over(window_3),
    )
    .withColumn(
        'revenue_change',
        F.col('revenue') - F.col('previous_revenue'),
    )
    .orderBy(
        'store_id',
        'sales_date',
    )
)
# daily_revenue_change_df.show(truncate=False)
"""
`daily_revenue_change_df`
+--------+----------+-------+----------------+--------------+                   
|store_id|sales_date|revenue|previous_revenue|revenue_change|
+--------+----------+-------+----------------+--------------+
|S01     |2026-08-20|100.00 |NULL            |NULL          |
|S01     |2026-08-21|130.00 |100.00          |30.00         |
|S01     |2026-08-22|110.00 |130.00          |-20.00        |
|S02     |2026-08-20|200.00 |NULL            |NULL          |
|S02     |2026-08-21|250.00 |200.00          |50.00         |
|S02     |2026-08-22|225.00 |250.00          |-25.00        |
+--------+----------+-------+----------------+--------------+
"""

# =============================================================================
# Step 8D — Reasoning
# =============================================================================
# 8. Why should the first row for each store have revenue_change = NULL?
#
#    A: Here, NULL has business meaning: the data is simply unavailable, not 0,
#       because there simply IS no previous date for all store's first date.
#

# 9. If the dates were:
#
#      Aug 20
#      Aug 23
#
#    would LAG() mean "previous calendar day"?
#
#    Or merely "previous row according to the window ordering"?
#
#    A: The latter.
#



# =============================================================================
# =============================================================================
# =============================================================================



"""
Step 9 — Running Totals and Rolling Windows

QUESTIONS
---------

1. What is a running total?
---------------------------
A cumulative aggregate from the start of a partition through the current row.


2. What is a rolling window?
----------------------------
An aggregate over a moving subset of rows around the current row.


3. Does a running total change the grain?
-----------------------------------------
No. Each original row remains; the cumulative metric is added as a new column.


4. What does ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW mean?
-------------------------------------------------------------------
Start at the first row in the partition and include every row through the
current row.


5. What does ROWS BETWEEN 2 PRECEDING AND CURRENT ROW mean?
-----------------------------------------------------------
Use the current row plus the previous two rows: a 3-row rolling window.


6. Is a 3-row rolling window the same as a 3-day rolling window?
----------------------------------------------------------------
Not necessarily. If dates are missing, three rows may span more than three
calendar days.


7. Why should ORDER BY be specified?
------------------------------------
Because cumulative and rolling calculations depend on row sequence.

"""
# =============================================================================
# Step 9A — Use existing daily_sales_df
# =============================================================================
# Grain:
#
#     one row per store + sales_date
#
# Columns:
#
#     store_id
#     sales_date
#     revenue
#

# =============================================================================
# Step 9B — Spark SQL: running total
# =============================================================================
# 1. Return:
#
#      store_id
#      sales_date
#      revenue
#      running_revenue
#
#    where running_revenue is cumulative revenue for each store.
#
#    Use:
#
#      SUM(revenue) OVER (
#          PARTITION BY store_id
#          ORDER BY sales_date
#          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
#      )
#
"""
`daily_sales_df`
+--------+----------+-------+                                                   
|store_id|sales_date|revenue|
+--------+----------+-------+
|S01     |2026-08-20|100.00 |
|S01     |2026-08-21|130.00 |
|S01     |2026-08-22|110.00 |
|S02     |2026-08-20|200.00 |
|S02     |2026-08-21|250.00 |
|S02     |2026-08-22|225.00 |
+--------+----------+-------+
"""
running_total_sql_df = spark.sql(
    """
    SELECT
        *,
        SUM(revenue) OVER(
            PARTITION BY store_id
            ORDER BY sales_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_revenue
    FROM daily_sales
    ORDER BY
        store_id,
        sales_date;
    """
)
# running_total_sql_df.show(truncate=False)
"""
`running_total_sql_df`
+--------+----------+-------+---------------+                                   
|store_id|sales_date|revenue|running_revenue|
+--------+----------+-------+---------------+
|S01     |2026-08-20|100.00 |100.00         |
|S01     |2026-08-21|130.00 |230.00         |
|S01     |2026-08-22|110.00 |340.00         |
|S02     |2026-08-20|200.00 |200.00         |
|S02     |2026-08-21|250.00 |450.00         |
|S02     |2026-08-22|225.00 |675.00         |
+--------+----------+-------+---------------+
"""

# =============================================================================
# Step 9C — PySpark: running total
# =============================================================================
# 2. Define a Window partitioned by store_id and ordered by sales_date.
#
# 3. Add the frame:
#
#      rowsBetween(
#          Window.unboundedPreceding,
#          Window.currentRow,
#      )
#
# 4. Add running_revenue using F.sum(...).over(...).
#
"""
`daily_sales_df`
+--------+----------+-------+                                                   
|store_id|sales_date|revenue|
+--------+----------+-------+
|S01     |2026-08-20|100.00 |
|S01     |2026-08-21|130.00 |
|S01     |2026-08-22|110.00 |
|S02     |2026-08-20|200.00 |
|S02     |2026-08-21|250.00 |
|S02     |2026-08-22|225.00 |
+--------+----------+-------+
"""
window_4 = (
    Window
    .partitionBy('store_id')
    .orderBy('sales_date')
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)
running_total_df = (
    daily_sales_df
    .withColumn(
        'running_revenue',
        F.sum('revenue').over(window_4)
    )
    .orderBy(
        'store_id',
        'sales_date',
    )
)
# running_total_df.show(truncate=False)
"""
`running_total_df`
+--------+----------+-------+---------------+                                   
|store_id|sales_date|revenue|running_revenue|
+--------+----------+-------+---------------+
|S01     |2026-08-20|100.00 |100.00         |
|S01     |2026-08-21|130.00 |230.00         |
|S01     |2026-08-22|110.00 |340.00         |
|S02     |2026-08-20|200.00 |200.00         |
|S02     |2026-08-21|250.00 |450.00         |
|S02     |2026-08-22|225.00 |675.00         |
+--------+----------+-------+---------------+
"""


# =============================================================================
# Step 9D — 3-row rolling average
# =============================================================================
# 5. In SQL, calculate:
#
#      rolling_3_row_avg
#
#    using:
#
#      AVG(revenue) OVER (
#          PARTITION BY store_id
#          ORDER BY sales_date
#          ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
#      )
#
# 6. Reproduce it in PySpark using:
#
#      rowsBetween(-2, 0)
#
"""
`daily_sales_df`
+--------+----------+-------+                                                   
|store_id|sales_date|revenue|
+--------+----------+-------+
|S01     |2026-08-20|100.00 |
|S01     |2026-08-21|130.00 |
|S01     |2026-08-22|110.00 |
|S02     |2026-08-20|200.00 |
|S02     |2026-08-21|250.00 |
|S02     |2026-08-22|225.00 |
+--------+----------+-------+
"""
rolling_3_avg_sql_df = spark.sql(
    """
    SELECT
        *,
        ROUND(AVG(revenue) OVER(
            PARTITION BY store_id
            ORDER BY sales_date
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ), 2) AS rolling_3_row_avg
    FROM daily_sales
    ORDER BY
        store_id,
        sales_date;
    """
)
# rolling_3_avg_sql_df.show(truncate=False)
"""
`rolling_3_avg_sql_df`
+--------+----------+-------+-----------------+                                 
|store_id|sales_date|revenue|rolling_3_row_avg|
+--------+----------+-------+-----------------+
|S01     |2026-08-20|100.00 |100.00           |
|S01     |2026-08-21|130.00 |115.00           |
|S01     |2026-08-22|110.00 |113.33           |
|S02     |2026-08-20|200.00 |200.00           |
|S02     |2026-08-21|250.00 |225.00           |
|S02     |2026-08-22|225.00 |225.00           |
+--------+----------+-------+-----------------+
"""

window_5 = (
    Window
    .partitionBy('store_id')
    .orderBy('sales_date')
    .rowsBetween(-2, Window.currentRow)
)
rolling_3_avg_df = (
    daily_sales_df
    .withColumn(
        'rolling_3_row_avg',
        F.round(F.avg('revenue').over(window_5), 2),
    )
    .orderBy(
        'store_id',
        'sales_date',
    )
)
# rolling_3_avg_df.show(truncate=False)
"""
`rolling_3_avg_df`
+--------+----------+-------+-----------------+
|S01     |2026-08-20|100.00 |100.00           |
|S01     |2026-08-21|130.00 |115.00           |
|S01     |2026-08-22|110.00 |113.33           |
|S02     |2026-08-20|200.00 |200.00           |
|S02     |2026-08-21|250.00 |225.00           |
|S02     |2026-08-22|225.00 |225.00           |
+--------+----------+-------+-----------------+
"""


# =============================================================================
# Step 9E — Reasoning
# =============================================================================
# For revenues:
#
#      100
#      130
#      110
#
# 7. What should the running totals be?
#
#      100.00
#      230.00
#      340.00
#
# 8. What should the 3-row rolling averages be?
#
#      100.00
#      115.00
#      113.33
#

# 9. If the dates were Aug 20, Aug 23, Aug 24:
#
#    would ROWS BETWEEN 2 PRECEDING AND CURRENT ROW mean
#    "the last three calendar days"?
#
#    A: No. It would mean the last three ROWS, not calendar days.
#



# =============================================================================
# =============================================================================
# =============================================================================



"""
Step 10 — Grain-Safe Joins and Pre-Aggregation

QUESTIONS
---------

1. Why can joins cause double counting?
---------------------------------------
If both datasets contain multiple rows for the same join key, joining them can
create a many-to-many multiplication of rows.


2. What should you determine before every join?
-----------------------------------------------
The grain of each input dataset and whether the join key is unique on either
side.


3. What is pre-aggregation?
---------------------------
Aggregating a dataset to the required join grain before joining it to another
dataset.


4. Why is pre-aggregation useful?
---------------------------------
It prevents row multiplication and ensures measures are calculated at the
correct business grain.


5. Why is SELECT DISTINCT not a reliable fix for double counting?
-----------------------------------------------------------------
DISTINCT removes identical rows; it does not fix an incorrectly designed join
or guarantee correct measures.


6. What is the safest mental pattern?
-------------------------------------
Identify grain → validate key uniqueness → pre-aggregate if necessary → join.


7. What is the key engineering question before joining?
-------------------------------------------------------
"Can either side contain multiple rows for this join key?"

"""
# =============================================================================
# Step 10A — Seed data
# =============================================================================

# `orders_df`
#
# Grain:
#     one row per order
orders_2_schema = StructType([
    StructField('order_id', IntegerType(), False),
    StructField('customer_id', StringType(), False),
])
orders_2_df = spark.createDataFrame(
    [
        (3001, 'C001'),
        (3002, 'C002'),
    ],
    orders_2_schema,
)
# orders_2_df.show(truncate=False)
"""
`orders_2_df`
+--------+-----------+                                                          
|order_id|customer_id|
+--------+-----------+
|3001    |C001       |
|3002    |C002       |
+--------+-----------+
"""

# `order_items_df`
#
# Grain:
#     one row per order line
order_items_2_schema = StructType([
    StructField('order_id', IntegerType(), False),
    StructField('line_number', IntegerType(), False),
    StructField('quantity', IntegerType(), False),
    StructField('unit_price', DecimalType(10, 2), False),
])
order_items_2_df = spark.createDataFrame(
    [
        # Order 3001 has TWO lines.
        (3001, 1, 1, Decimal('10.00')),
        (3001, 2, 1, Decimal('20.00')),

        # Order 3002 has one line.
        (3002, 1, 2, Decimal('20.00')),
    ],
    order_items_2_schema,
)
# order_items_2_df.show(truncate=False)
"""
`order_items_2_df`
+--------+-----------+--------+----------+                                      
|order_id|line_number|quantity|unit_price|
+--------+-----------+--------+----------+
|3001    |1          |1       |10.00     |
|3001    |2          |1       |20.00     |
|3002    |1          |2       |20.00     |
+--------+-----------+--------+----------+
"""

# `payments_df`
#
# Grain:
#     one row per payment
payments_2_schema = StructType([
    StructField('order_id', IntegerType(), False),
    StructField('payment_id', StringType(), False),
    StructField('payment_amount', DecimalType(10, 2), False),
])
payments_2_df = spark.createDataFrame(
    [
        # Order 3001 has TWO payments.
        (3001, 'PAY001', Decimal('15.00')),
        (3001, 'PAY002', Decimal('15.00')),

        # Order 3002 has one payment.
        (3002, 'PAY003', Decimal('40.00')),
    ],
    payments_2_schema,
)
# payments_2_df.show(truncate=False)
"""
`payments_2_df`
+--------+----------+--------------+                                            
|order_id|payment_id|payment_amount|
+--------+----------+--------------+
|3001    |PAY001    |15.00         |
|3001    |PAY002    |15.00         |
|3002    |PAY003    |40.00         |
+--------+----------+--------------+
"""

# =============================================================================
# Step 10B — Observe the dangerous join
# =============================================================================
# 1. Register all three DataFrames:
#
#      orders
#      order_items
#      payments
orders_2_df.createOrReplaceTempView('orders_2')
order_items_2_df.createOrReplaceTempView('order_items_2')
payments_2_df.createOrReplaceTempView('payments_2')

# 2. Join:
#
#      orders
#        → order_items using order_id
#        → payments using order_id
#
#    Do NOT aggregate first.
#
#    Inspect the resulting rows for order 3001.
#
#    Before running it, predict:
#
#      order_items rows for 3001 = 2
#      payment rows for 3001     = 2
#
#      joined rows for 3001      = [4]
#
"""
`orders_2_df`
+--------+-----------+                                                          
|order_id|customer_id|
+--------+-----------+
|3001    |C001       |
|3002    |C002       |
+--------+-----------+

`order_items_2_df`
+--------+-----------+--------+----------+                                      
|order_id|line_number|quantity|unit_price|
+--------+-----------+--------+----------+
|3001    |1          |1       |10.00     |
|3001    |2          |1       |20.00     |
|3002    |1          |2       |20.00     |
+--------+-----------+--------+----------+

`payments_2_df`
+--------+----------+--------------+                                            
|order_id|payment_id|payment_amount|
+--------+----------+--------------+
|3001    |PAY001    |15.00         |
|3001    |PAY002    |15.00         |
|3002    |PAY003    |40.00         |
+--------+----------+--------------+
"""
incorrect_joins_sql_df = spark.sql(
    """
    SELECT *
    FROM orders_2 AS o
    INNER JOIN order_items_2 AS oi
        ON o.order_id = oi.order_id
    INNER JOIN payments_2 AS p
        ON o.order_id = p.order_id;
    """
)
#
# OR...
#
incorrect_joins_df = (
    orders_2_df.alias('o')
    .join(
        order_items_2_df.alias('oi'),
        F.col('o.order_id') == F.col('oi.order_id'),
        how='inner',
    )
    .join(
        payments_2_df.alias('p'),
        F.col('o.order_id') == F.col('p.order_id'),
        how='inner',
    )
)
# incorrect_joins_df.show(truncate=False)
"""
`incorrect_joins_df`
+--------+-----------+--------+-----------+--------+----------+--------+----------+--------------+
|order_id|customer_id|order_id|line_number|quantity|unit_price|order_id|payment_id|payment_amount|
+--------+-----------+--------+-----------+--------+----------+--------+----------+--------------+
|3001    |C001       |3001    |2          |1       |20.00     |3001    |PAY001    |15.00         |
|3001    |C001       |3001    |1          |1       |10.00     |3001    |PAY001    |15.00         |
|3001    |C001       |3001    |2          |1       |20.00     |3001    |PAY002    |15.00         |
|3001    |C001       |3001    |1          |1       |10.00     |3001    |PAY002    |15.00         |
|3002    |C002       |3002    |1          |2       |20.00     |3002    |PAY003    |40.00         |
+--------+-----------+--------+-----------+--------+----------+--------+----------+--------------+
"""

# 3. Explain why this happened.
#
#    A: This is because `order_items_2` and `payments_2` create a many-to-many
#       relationship when joined, thereby multiplying the `3001` rows.
#       This results in the creation of 2 x 2 = 4 rows for `3001`.
#


# =============================================================================
# Step 10C — Demonstrate the incorrect aggregate
# =============================================================================
# 4. Using the naive joined dataset, calculate per order:
#
#      SUM(quantity * unit_price) AS order_revenue
#      SUM(payment_amount)        AS total_paid
#
#    Compare the result for order 3001 with the correct business totals.
#
#    Question: Why are the measures overstated?
#
#    A: This is due to row multiplication from naive joins.
#       I.e., quantity ,unit_price, and payment_amount are duplicated across
#             several rows that results in incorrect aggregations.
#
# --------------------------------------
# Correct business totals:
#
# order_id | order_revenue | total_paid
# --------------------------------------
# 3001     | 30.00         | 30.00
# 3002     | 40.00         | 40.00
# --------------------------------------
"""
`orders_2_df`
+--------+-----------+                                                          
|order_id|customer_id|
+--------+-----------+
|3001    |C001       |
|3002    |C002       |
+--------+-----------+

`order_items_2_df`
+--------+-----------+--------+----------+                                      
|order_id|line_number|quantity|unit_price|
+--------+-----------+--------+----------+
|3001    |1          |1       |10.00     |
|3001    |2          |1       |20.00     |
|3002    |1          |2       |20.00     |
+--------+-----------+--------+----------+

`payments_2_df`
+--------+----------+--------------+                                            
|order_id|payment_id|payment_amount|
+--------+----------+--------------+
|3001    |PAY001    |15.00         |
|3001    |PAY002    |15.00         |
|3002    |PAY003    |40.00         |
+--------+----------+--------------+
"""
incorrect_aggregates_df = spark.sql(
    """
    SELECT
        o.order_id,
        SUM(oi.quantity * oi.unit_price) AS order_revenue,
        SUM(p.payment_amount) AS payment_amount
    FROM orders_2 AS o
    INNER JOIN order_items_2 AS oi
        ON o.order_id = oi.order_id
    INNER JOIN payments_2 AS p
        ON o.order_id = p.order_id
    GROUP BY o.order_id
    ORDER BY o.order_id;
    """
)
# incorrect_aggregates_df.show(truncate=False)
"""
`incorrect_aggregates_df`
+--------+-------------+--------------+                                         
|order_id|order_revenue|payment_amount|
+--------+-------------+--------------+
|3001    |60.00        |60.00         |
|3002    |40.00        |40.00         |
+--------+-------------+--------------+
"""


# =============================================================================
# Step 10D — Correct Spark SQL solution
# =============================================================================
# 5. Correct the problem using TWO pre-aggregation CTEs.
#
#    CTE 1:
#
#      Aggregate order_items to:
#
#          one row per order_id
#
#      producing:
#
#          order_revenue
#
#
#    CTE 2:
#
#      Aggregate payments to:
#
#          one row per order_id
#
#      producing:
#
#          total_paid
#
#
#    Final query:
#
#      Join orders to the TWO pre-aggregated datasets.
#
#    Expected:
#
#      +--------+-----------+-------------+----------+
#      |order_id|customer_id|order_revenue|total_paid|
#      +--------+-----------+-------------+----------+
#      |3001    |C001       |30.00        |30.00     |
#      |3002    |C002       |40.00        |40.00     |
#      +--------+-----------+-------------+----------+
#
"""
`orders_2_df`
+--------+-----------+                                                          
|order_id|customer_id|
+--------+-----------+
|3001    |C001       |
|3002    |C002       |
+--------+-----------+

`order_items_2_df`
+--------+-----------+--------+----------+                                      
|order_id|line_number|quantity|unit_price|
+--------+-----------+--------+----------+
|3001    |1          |1       |10.00     |
|3001    |2          |1       |20.00     |
|3002    |1          |2       |20.00     |
+--------+-----------+--------+----------+

`payments_2_df`
+--------+----------+--------------+                                            
|order_id|payment_id|payment_amount|
+--------+----------+--------------+
|3001    |PAY001    |15.00         |
|3001    |PAY002    |15.00         |
|3002    |PAY003    |40.00         |
+--------+----------+--------------+
"""
correct_aggregates_sql_df = spark.sql(
    """
    WITH oi AS (
        SELECT
            order_id,
            SUM(quantity * unit_price) AS order_revenue
        FROM order_items_2
        GROUP BY order_id
    ),
    p AS (
        SELECT
            order_id,
            SUM(payment_amount) AS total_paid
        FROM payments_2
        GROUP BY order_id
    )
    SELECT
        o.order_id,
        o.customer_id,
        oi.order_revenue,
        p.total_paid
    FROM orders_2 AS o
    INNER JOIN oi
        ON o.order_id = oi.order_id
    INNER JOIN p
        ON o.order_id = p.order_id
    ORDER BY o.order_id;
    """
)
# correct_aggregates_sql_df.show(truncate=False)
"""
`correct_aggregates_sql_df`
+--------+-----------+-------------+----------+                                 
|order_id|customer_id|order_revenue|total_paid|
+--------+-----------+-------------+----------+
|3001    |C001       |30.00        |30.00     |
|3002    |C002       |40.00        |40.00     |
+--------+-----------+-------------+----------+
"""


# =============================================================================
# Step 10E — Correct PySpark DataFrame API solution
# =============================================================================
# 6. Create `order_revenue_df`.
#
#    Grain:
#
#      one row per order_id
#
#    Columns:
#
#      order_id
#      order_revenue
#
# 7. Create `total_paid_df`.
#
#    Grain:
#
#      one row per order_id
#
#    Columns:
#
#      order_id
#      total_paid
#
# 8. Join:
#
#      orders_df
#        → order_revenue_df
#        → total_paid_df
#
#    The final grain should remain:
#
#      one row per order
#
"""
`orders_2_df`
+--------+-----------+                                                          
|order_id|customer_id|
+--------+-----------+
|3001    |C001       |
|3002    |C002       |
+--------+-----------+

`order_items_2_df`
+--------+-----------+--------+----------+                                      
|order_id|line_number|quantity|unit_price|
+--------+-----------+--------+----------+
|3001    |1          |1       |10.00     |
|3001    |2          |1       |20.00     |
|3002    |1          |2       |20.00     |
+--------+-----------+--------+----------+

`payments_2_df`
+--------+----------+--------------+                                            
|order_id|payment_id|payment_amount|
+--------+----------+--------------+
|3001    |PAY001    |15.00         |
|3001    |PAY002    |15.00         |
|3002    |PAY003    |40.00         |
+--------+----------+--------------+
"""
order_revenue_df = (
    order_items_2_df
    .groupBy('order_id')
    .agg(
        F.sum(F.col('quantity') * F.col('unit_price')).alias('order_revenue')
    )
)
total_paid_df = (
    payments_2_df
    .groupBy('order_id')
    .agg(
        F.sum(F.col('payment_amount')).alias('total_paid')
    )
)
correct_aggregates_df = (
    orders_2_df
    .join(
        order_revenue_df,
        on='order_id',
        how='inner',
    )
    .join(
        total_paid_df,
        on='order_id',
        how='inner',
    )
    .orderBy('order_id')
)
# correct_aggregates_df.show(truncate=False)
"""
`correct_aggregates_df`
+--------+-----------+-------------+----------+                                 
|order_id|customer_id|order_revenue|total_paid|
+--------+-----------+-------------+----------+
|3001    |C001       |30.00        |30.00     |
|3002    |C002       |40.00        |40.00     |
+--------+-----------+-------------+----------+
"""

# =============================================================================
# Step 10F — Interview reasoning
# =============================================================================

# 9. Complete:
#
#      order_items grain = one row PER order line
#
#      payments grain = one row PER payment
#
#      naive joined grain = payment combination within an order
#
#      pre-aggregated order_revenue_df grain = one row PER order
#
#      pre-aggregated total_paid_df grain = one row PER order
#
#      final corrected grain = one row PER order
#

# 10. Why wouldn't adding DISTINCT to the naive join be a proper solution?
#
#     A: This is because when 3 or more tables are involved in a naive join,
#        multiplied rows can contain different metrics from other tables,
#        thereby rendering the "uniqueness" of the rows meaningless.
#

# 11. What data-quality check could you perform if a dataset is EXPECTED to
#     contain exactly one row per order_id?
#
#     Hint:
#
#         groupBy('order_id')
#         count()
#         count > 1
#
#     A: Do this: `df.groupBy('order_id').count().filter(F.col('count') > 1)`
#        What we want is an empty table, which signifies that `df` contains
#        exactly one row per `order_id``.
#



# =============================================================================
# =============================================================================
# =============================================================================



"""
Step 11 — Final Mixed Interview Problem

QUESTIONS
---------

1. What should you identify before writing the query?
-----------------------------------------------------
The grain of every input, the required output grain, and the join relationships.


2. What is the safest transformation order here?
-------------------------------------------------
Join → aggregate to product grain → apply windows → filter top N.


3. Why aggregate before ranking?
--------------------------------
Because we want to rank products by their total revenue, not individual sales rows.


4. Does the category-total window change the grain?
---------------------------------------------------
No. It attaches the category total to each product-level row.


5. What should you explain while coding?
----------------------------------------
Grain changes, join assumptions, tie behavior, NULL handling, and why each
transformation is necessary.

"""
# =============================================================================
# FINAL PROBLEM
# =============================================================================
# Use:
#
#     sales_2_df
#         grain = one row per order line
#
#     product_2_df
#         grain = one row per product
#
#
# Return the TOP 2 products by total revenue within each category.
#
# Required columns:
#
#     category
#     product_id
#     total_revenue
#     category_revenue
#     revenue_share_pct
#     rn
#
#
# Requirements:
#
# 1. Join sales to products on product_id.
#
# 2. Aggregate to:
#
#        one row per category + product_id
#
#    Calculate:
#
#        total_revenue = SUM(quantity * unit_price)
#
# 3. Calculate category_revenue using a window:
#
#        SUM(total_revenue) OVER (
#            PARTITION BY category
#        )
#
# 4. Calculate:
#
#        revenue_share_pct =
#            total_revenue / category_revenue * 100
#
# 5. Rank products within each category:
#
#        ROW_NUMBER() OVER (
#            PARTITION BY category
#            ORDER BY total_revenue DESC, product_id ASC
#        )
#
# 6. Keep:
#
#        rn <= 2
#
# 7. Sort by:
#
#        category,
#        rn
#
#
# Expected category totals:
#
#     Electronics = 1400.00
#     Grocery     = 350.00
#     Home        = 1800.00
#


# =============================================================================
# Step 11A — Spark SQL
# =============================================================================
# Solve using:
#
#     CTE 1 → aggregate product revenue
#     CTE 2 → add category total + ranking
#     final SELECT → calculate share and filter rn <= 2
#
"""
`sales_2_df`  GRAIN: one row PER order line
+--------+--------+----------+--------+----------+                              
|order_id|store_id|product_id|quantity|unit_price|
+--------+--------+----------+--------+----------+
|2001    |S01     |P001      |4       |10.00     |
|2002    |S02     |P001      |6       |10.00     |
|2003    |S01     |P002      |8       |5.00      |
|2004    |S03     |P002      |12      |5.00      |
|2005    |S02     |P003      |4       |8.00      |
|2006    |S03     |P003      |6       |8.00      |
|2007    |S01     |P004      |4       |4.00      |
|2008    |S02     |P004      |6       |4.00      |
|2009    |S01     |P005      |4       |3.00      |
|2010    |S03     |P005      |6       |3.00      |
|2011    |S01     |P006      |3       |50.00     |
|2012    |S02     |P006      |5       |50.00     |
|2013    |S02     |P007      |4       |40.00     |
|2014    |S03     |P007      |6       |40.00     |
|2015    |S01     |P008      |5       |25.00     |
|2016    |S03     |P008      |7       |25.00     |
|2017    |S01     |P009      |4       |20.00     |
|2018    |S02     |P009      |6       |20.00     |
|2019    |S02     |P010      |4       |10.00     |
|2020    |S03     |P010      |6       |10.00     |
+--------+--------+----------+--------+----------+
only showing top 20 rows

`product_2_df`  GRAIN: one row PER product
+----------+------------+-----------+                                           
|product_id|product_name|category   |
+----------+------------+-----------+
|P001      |Coffee      |Grocery    |
|P002      |Tea         |Grocery    |
|P003      |Rice        |Grocery    |
|P004      |Bread       |Grocery    |
|P005      |Milk        |Grocery    |
|P006      |Headphones  |Electronics|
|P007      |Keyboard    |Electronics|
|P008      |Mouse       |Electronics|
|P009      |Charger     |Electronics|
|P010      |Cable       |Electronics|
|P011      |Vacuum      |Home       |
|P012      |Lamp        |Home       |
|P013      |Kettle      |Home       |
|P014      |Pillow      |Home       |
|P015      |Mug         |Home       |
+----------+------------+-----------+
"""
top_2_products_by_category_total_revenue_sql_df = spark.sql(
    # """
    # SELECT *
    # FROM
    # (
    #     SELECT
    #         *,
    #         SUM(total_revenue) OVER(
    #             PARTITION BY category
    #         ) AS category_revenue,
    #         ROUND(
    #             (total_revenue / category_revenue) * 100,
    #             2
    #         ) AS revenue_share_pct,
    #         ROW_NUMBER() OVER(
    #             PARTITION BY category
    #             ORDER BY
    #                 total_revenue DESC,
    #                 product_id ASC
    #         ) AS rn
    #     FROM
    #     (
    #         SELECT
    #             p.product_id,
    #             p.category,
    #             tr.total_revenue
    #         FROM product_2 as p
    #         INNER JOIN
    #         (
    #             SELECT
    #                 product_id,
    #                 SUM(quantity * unit_price) AS total_revenue
    #             FROM sales_2
    #             GROUP BY product_id
    #         ) AS tr
    #             ON p.product_id = tr.product_id
    #     )
    # )
    # WHERE rn <= 2;
    # """
    """
    WITH product_revenue AS (
        SELECT
            p.category,
            p.product_id,
            SUM(s.quantity * s.unit_price) AS total_revenue
        FROM sales_2 AS s
        INNER JOIN product_2 AS p
            ON s.product_id = p.product_id
        GROUP BY
            p.category,
            p.product_id
    ),
    category_metrics AS (
        SELECT
            *,
            SUM(total_revenue) OVER(
                PARTITION BY category
            ) AS category_revenue,
            ROW_NUMBER() OVER(
                PARTITION BY category
                ORDER BY
                    total_revenue DESC,
                    product_id ASC
            ) AS rn
        FROM product_revenue
    )
    SELECT
        category,
        product_id,
        total_revenue,
        category_revenue,
        ROUND(
            (total_revenue / category_revenue) * 100,
            2
        ) AS revenue_share_pct,
        rn
    FROM category_metrics
    WHERE rn <= 2
    ORDER BY
        category,
        rn;
    """
)
# top_2_products_by_category_total_revenue_sql_df.show(truncate=False)
"""
`top_2_products_by_category_total_revenue_sql_df`
+-----------+----------+-------------+----------------+-----------------+---+   
|category   |product_id|total_revenue|category_revenue|revenue_share_pct|rn |
+-----------+----------+-------------+----------------+-----------------+---+
|Electronics|P006      |400.00       |1400.00         |28.57            |1  |
|Electronics|P007      |400.00       |1400.00         |28.57            |2  |
|Grocery    |P001      |100.00       |350.00          |28.57            |1  |
|Grocery    |P002      |100.00       |350.00          |28.57            |2  |
|Home       |P011      |600.00       |1800.00         |33.33            |1  |
|Home       |P012      |500.00       |1800.00         |27.78            |2  |
+-----------+----------+-------------+----------------+-----------------+---+
"""


# =============================================================================
# Step 11B — PySpark DataFrame API
# =============================================================================
# Solve using:
#
#     join()
#     groupBy().agg()
#     Window.partitionBy(...)
#     F.sum().over(...)
#     F.row_number().over(...)
#     withColumn()
#     filter()
#     orderBy()
#
"""
`sales_2_df`  GRAIN: one row PER order
+--------+--------+----------+--------+----------+                              
|order_id|store_id|product_id|quantity|unit_price|
+--------+--------+----------+--------+----------+
|2001    |S01     |P001      |4       |10.00     |
|2002    |S02     |P001      |6       |10.00     |
|2003    |S01     |P002      |8       |5.00      |
|2004    |S03     |P002      |12      |5.00      |
|2005    |S02     |P003      |4       |8.00      |
|2006    |S03     |P003      |6       |8.00      |
|2007    |S01     |P004      |4       |4.00      |
|2008    |S02     |P004      |6       |4.00      |
|2009    |S01     |P005      |4       |3.00      |
|2010    |S03     |P005      |6       |3.00      |
|2011    |S01     |P006      |3       |50.00     |
|2012    |S02     |P006      |5       |50.00     |
|2013    |S02     |P007      |4       |40.00     |
|2014    |S03     |P007      |6       |40.00     |
|2015    |S01     |P008      |5       |25.00     |
|2016    |S03     |P008      |7       |25.00     |
|2017    |S01     |P009      |4       |20.00     |
|2018    |S02     |P009      |6       |20.00     |
|2019    |S02     |P010      |4       |10.00     |
|2020    |S03     |P010      |6       |10.00     |
+--------+--------+----------+--------+----------+
only showing top 20 rows

`product_2_df`  GRAIN: one row PER product
+----------+------------+-----------+                                           
|product_id|product_name|category   |
+----------+------------+-----------+
|P001      |Coffee      |Grocery    |
|P002      |Tea         |Grocery    |
|P003      |Rice        |Grocery    |
|P004      |Bread       |Grocery    |
|P005      |Milk        |Grocery    |
|P006      |Headphones  |Electronics|
|P007      |Keyboard    |Electronics|
|P008      |Mouse       |Electronics|
|P009      |Charger     |Electronics|
|P010      |Cable       |Electronics|
|P011      |Vacuum      |Home       |
|P012      |Lamp        |Home       |
|P013      |Kettle      |Home       |
|P014      |Pillow      |Home       |
|P015      |Mug         |Home       |
+----------+------------+-----------+
"""
window_final_1 = (
    Window
    .partitionBy('category')
)
window_final_2 = (
    Window
    .partitionBy('category')
    .orderBy(
        F.col('total_revenue').desc(),
        F.col('product_id').asc(),
    )
)
top_2_products_by_category_total_revenue_df = (
    sales_2_df
    .groupBy('product_id')
    .agg(
        F.sum(F.col('quantity') * F.col('unit_price')).alias('total_revenue')
    )
    .join(
        product_2_df,
        on='product_id',
        how='inner'
    )
    .withColumn(
        'category_revenue',
        F.sum('total_revenue').over(window_final_1)
    )
    .withColumn(
        'rn',
        F.row_number().over(window_final_2)
    )
    .withColumn(
        'revenue_share_pct',
        F.round((F.col('total_revenue') / F.col('category_revenue')) * 100, 2)
    )
    .filter(
        F.col('rn') <= 2
    )
    .select(
        'category',
        'product_id',
        'total_revenue',
        'category_revenue',
        'revenue_share_pct',
        'rn',
    )
    .orderBy(
        'category',
        'rn',
    )
)
# top_2_products_by_category_total_revenue_df.show(truncate=False)
"""
`top_2_products_by_category_total_revenue_df`
+-----------+----------+-------------+----------------+-----------------+---+   
|category   |product_id|total_revenue|category_revenue|revenue_share_pct|rn |
+-----------+----------+-------------+----------------+-----------------+---+
|Electronics|P006      |400.00       |1400.00         |28.57            |1  |
|Electronics|P007      |400.00       |1400.00         |28.57            |2  |
|Grocery    |P001      |100.00       |350.00          |28.57            |1  |
|Grocery    |P002      |100.00       |350.00          |28.57            |2  |
|Home       |P011      |600.00       |1800.00         |33.33            |1  |
|Home       |P012      |500.00       |1800.00         |27.78            |2  |
+-----------+----------+-------------+----------------+-----------------+---+
"""

# =============================================================================
# Step 11C — Explain aloud
# =============================================================================
# Be able to answer:
#
# 1. Input sales grain?
#
# 2. Grain immediately after the product join?
#
# 3. Grain after groupBy(category, product_id)?
#
# 4. Does either window change that grain?
#
# 5. Why use product_id as a secondary ordering key?
#
# 6. What would happen if product_2_df contained duplicate product_id values?
#