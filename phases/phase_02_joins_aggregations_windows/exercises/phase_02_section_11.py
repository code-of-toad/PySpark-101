from datetime import datetime

from pyspark.sql import Window
from pyspark.sql import functions as F

from practice_data import (
    spark,
)

"""
Phase 2, Section 11: Deduplication, `union`, `unionByName`
----------------------------------------------------------
1. `dropDuplicates()` vs. deterministic selection
2. union vs. join
3. positional `union()`
4. name-based `unionByName()`

QUESTIONS
=========
Q: Why can positional union be dangerous even if Spark raises no error?
Q: Why is `unionByName()` usually safer for pipeline batches?
Q: Does union remove duplicates automatically?
Q: When does `dropDuplicates()` remain appropriate?
"""


# =============================================================================
# 11.1 Deterministic deduplication
# =============================================================================
events_df = spark.createDataFrame(
    [
        ('E001', datetime(2026, 1, 1, 10, 0), 1),
        ('E001', datetime(2026, 1, 1, 10, 5), 2),
        ('E002', datetime(2026, 1, 1, 11, 0), 1),
    ],
    ['event_id', 'event_ts', 'payload_version'],
)
# events_df.show(truncate=False)
# =>
"""
`events_df`
+--------+-------------------+---------------+                                  
|event_id|event_ts           |payload_version|
+--------+-------------------+---------------+
|E001    |2026-01-01 10:00:00|1              |
|E001    |2026-01-01 10:05:00|2              |
|E002    |2026-01-01 11:00:00|1              |
+--------+-------------------+---------------+
"""

# Any survivor is acceptable only when the business rule truly does not care.
arbitrary_survivor_df = events_df.dropDuplicates([
    'event_id'
])
# arbitrary_survivor_df.show(truncate=False)
# =>
"""
`arbitrary_survivor_df`
+--------+-------------------+---------------+                                  
|event_id|event_ts           |payload_version|
+--------+-------------------+---------------+
|E001    |2026-01-01 10:00:00|1              |
|E002    |2026-01-01 11:00:00|1              |
+--------+-------------------+---------------+
"""

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
# latest_event_df.show(truncate=False)
# =>
"""
`latest_event_df`
+--------+-------------------+---------------+                                  
|event_id|event_ts           |payload_version|
+--------+-------------------+---------------+
|E001    |2026-01-01 10:05:00|2              |
|E002    |2026-01-01 11:00:00|1              |
+--------+-------------------+---------------+
"""


# =============================================================================
# 11.2 `union()` is positional
# =============================================================================
batch_a_df = spark.createDataFrame(
    [
        ('R001', 'READY'),
        ('R002', 'READY'),
    ],
    ['record_id', 'status'],
)
# batch_a_df.show(truncate=False)
# =>
"""
`batch_a_df`
+---------+------+                                                              
|record_id|status|
+---------+------+
|R001     |READY |
|R002     |READY |
+---------+------+
"""
batch_b_reordered_df = spark.createDataFrame(
    [
        ('READY', 'R003'),
        ('FAILED', 'R004'),
    ],
    ['status', 'record_id'],
)
# batch_b_reordered_df.show(truncate=False)
# =>
"""
`batch_b_reordered_df`
+------+---------+                                                              
|status|record_id|
+------+---------+
|READY |R003     |
|FAILED|R004     |
+------+---------+
"""
# `union()` aligns columns by POSITION.
#
# Both columns are strings, so this can run while silently putting values
# under the wrong semantic column names.
unsafe_union_df = batch_a_df.union(
    batch_b_reordered_df
)
# unsafe_union_df.show(truncate=False)
# =>
"""
`unsafe_union_df`
+---------+------+                                                              
|record_id|status|
+---------+------+
|R001     |READY |
|R002     |READY |
|READY    |R003  |
|FAILED   |R004  |
+---------+------+
"""


# =============================================================================
# 11.3 `unionByName()`
# --------------------
# INPUT GRAIN:
# OUTPUT GRAIN:
# =============================================================================
# Resolve columns by NAME instead of position.
safe_union_df = batch_a_df.unionByName(
    batch_b_reordered_df
)
# safe_union_df.show(truncate=False)
# =>
"""
`safe_union_df`
+---------+------+                                                              
|record_id|status|
+---------+------+
|R001     |READY |
|R002     |READY |
|R003     |READY |
|R004     |FAILED|
+---------+------+
"""


# =============================================================================
# 11.4 Missing columns
# --------------------
# KEY DISTINCTION:
#
# 1. JOIN combines columns based on a row relationship.
# 2. UNION stacks rows representing the same conceptual row type.
# =============================================================================
batch_c_df = spark.createDataFrame(
    [
        ('R005', 'READY', 'API'),
    ],
    ['record_id', 'status', 'source_system'],
)
# batch_c_df.show(truncate=False)
# =>
"""
`batch_c_df`
+---------+------+-------------+                                                
|record_id|status|source_system|
+---------+------+-------------+
|R005     |READY |API          |
+---------+------+-------------+
"""

# Missing columns are filled with NULL when allowMissingColumns=True.
evolving_union_df = batch_a_df.unionByName(
    batch_c_df,
    allowMissingColumns=True,
)
# evolving_union_df.show(truncate=False)
# =>
"""
`evolving_union_df`
+---------+------+-------------+                                                
|record_id|status|source_system|
+---------+------+-------------+
|R001     |READY |NULL         |
|R002     |READY |NULL         |
|R005     |READY |API          |
+---------+------+-------------+
"""
