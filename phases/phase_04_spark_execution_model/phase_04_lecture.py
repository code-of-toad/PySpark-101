#!/usr/bin/env python
'''PySpark 101 — Phase 4 lecture.

Spark application architecture, lazy execution, DAGs, lineage, partitions,
parallelism, narrow and wide dependencies, shuffles, stages, tasks, and the
conceptual role of RDDs.

This is lecture-only code. It uses small deterministic data so execution-model
claims can be reasoned about directly. Detailed Catalyst, physical-plan, AQE,
and performance-tuning analysis is intentionally deferred to later phases.
'''

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# =============================================================================
# 0. SPARK SESSION AND PHASE 4 MENTAL MODEL
# =============================================================================

spark = (
    SparkSession.builder
    .appName('phase_04_spark_execution_model')
    # Four local worker threads give Spark capacity to run several tasks at once.
    # This models task concurrency without pretending one machine is a cluster.
    .master('local[4]')
    # Fix the default shuffle partition count so downstream task reasoning is
    # deliberate rather than dependent on the environment's default setting.
    .config('spark.sql.shuffle.partitions', '4')
    # AQE can alter shuffle partitioning at runtime. It is disabled only so this
    # Phase 4 teaching example stays deterministic; AQE itself belongs to Phase 5.
    .config('spark.sql.adaptive.enabled', 'false')
    .getOrCreate()
)

# Phase 4 asks what Spark does AFTER PySpark code describes a computation.
#
#     PySpark transformations
#              |
#              v
#       lazy dependency graph
#              |
#              | action demands a result
#              v
#            job
#              |
#              | shuffle dependencies split work
#              v
#           stages
#              |
#              | one task per stage partition
#              v
#            tasks
#              |
#              v
#          executors
#
# Core questions for every important example:
#
#     What is lazy?
#     What triggers execution?
#     Which dependencies are narrow or wide?
#     Where should a shuffle occur?
#     Where should stages split?
#     How do partitions relate to tasks?
#     What work can execute in parallel?


# =============================================================================
# 1. SPARK APPLICATION ARCHITECTURE
# =============================================================================

# DRIVER
# ------
# The driver coordinates one Spark application. It runs the application's main
# control flow, owns the SparkSession/SparkContext, schedules distributed work,
# tracks task progress, and receives results explicitly returned to it.
#
# Think:
#     driver = coordinates WHAT distributed work must happen
#
# EXECUTORS
# ---------
# Executors are application-specific processes that run tasks over partitions.
# They perform the distributed computation and can hold cached application data.
#
# Think:
#     executors = perform the partition-level distributed work
#
# CLUSTER MANAGER
# ---------------
# A cluster manager allocates compute resources to Spark applications.
# Examples include Spark Standalone, YARN, and Kubernetes.
#
# Think:
#     cluster manager = allocates resources, not business transformations
#
# WORKER NODES
# ------------
# Worker nodes are machines capable of hosting executor processes.
#
# IMPORTANT DISTINCTION:
#     worker node != executor
#
# A node is a machine. An executor is an application process running on a node.
# In local mode these roles are not physically spread across multiple machines,
# but the scheduling concepts below still exist.


# =============================================================================
# 2. APPLICATION -> JOBS -> STAGES -> TASKS
# =============================================================================

# APPLICATION
# -----------
# This Python process plus its SparkSession represents one Spark application for
# the purposes of this lecture.
#
# JOB
# ---
# An action creates an execution demand. Spark computes the lineage needed to
# satisfy that demand as job work.
#
# STAGE
# -----
# A job is divided into stages around shuffle dependencies. Narrow work can
# usually pipeline together; a shuffle creates the important stage boundary.
#
# TASK
# ----
# A task is one stage's computation for one partition of that stage's data.
#
# Useful hierarchy:
#
#     Application
#         |
#         +-- Job(s)
#               |
#               +-- Stage(s)
#                      |
#                      +-- Task(s)
#
# Do NOT use these false shortcuts:
#
#     one DataFrame method = one job
#     one transformation = one stage
#     one action = exactly one visible Spark UI job in every situation
#
# The useful Phase 4 model is approximate execution reasoning, not hard-coded
# stage IDs or UI counters.


# =============================================================================
# 3. DELIBERATELY PARTITIONED SOURCE DATA
# =============================================================================

# spark.range(..., numPartitions=4) establishes exactly four input partitions
# without first calling repartition(), which would introduce an unrelated
# shuffle before the experiment even begins.
base_df = spark.range(
    start=0,
    end=12,
    step=1,
    numPartitions=4,
)

# Derive a small retail-like dataset entirely through narrow expressions.
# Alternating store IDs ensure S01 and S02 appear across several source
# partitions, so grouping by store_id will genuinely require redistribution.
sales_df = base_df.select(
    F.col('id').alias('sale_id'),
    F.when(
        (F.col('id') % 2) == 0,
        F.lit('S01'),
    )
    .otherwise(F.lit('S02'))
    .alias('store_id'),
    F.when(
        (F.col('id') % 3) == 0,
        F.lit('P001'),
    )
    .when(
        (F.col('id') % 3) == 1,
        F.lit('P002'),
    )
    .otherwise(F.lit('P003'))
    .alias('product_id'),
    (F.col('id') + F.lit(1)).cast('int').alias('quantity'),
    F.lit(10).cast('int').alias('unit_price'),
)

# getNumPartitions() inspects partition metadata rather than collecting all rows.
# RDD exposure here is intentionally minimal and only supports execution-model
# reasoning; DataFrames remain the primary structured-data abstraction.
input_partition_count = sales_df.rdd.getNumPartitions()
assert input_partition_count == 4

# spark_partition_id() makes the current execution partition visible as a
# DataFrame column. collect() is an ACTION, so this small inspection now triggers
# execution. We sort only after collection in ordinary Python so the Spark
# pipeline itself does not gain a global orderBy() shuffle merely for display.
partition_rows = (
    sales_df
    .select(
        'sale_id',
        'store_id',
        'product_id',
        F.spark_partition_id().alias('partition_id'),
    )
    .collect()
)

partition_layout = sorted(
    [
        (
            row['partition_id'],
            row['sale_id'],
            row['store_id'],
            row['product_id'],
        )
        for row in partition_rows
    ],
    key=lambda value: (value[0], value[1]),
)

# The layout is deliberately observable. Each store key should appear in more
# than one input partition, which proves a store-level group cannot finish from
# only one original partition's data.
store_partitions = {}
for partition_id, _, store_id, _ in partition_layout:
    store_partitions.setdefault(store_id, set()).add(partition_id)

assert len(store_partitions['S01']) > 1
assert len(store_partitions['S02']) > 1


# =============================================================================
# 4. LAZY EVALUATION AND ACTIONS
# =============================================================================

# Transformations describe work. This chain remains lazy after it is defined.
# Spark has not yet needed to produce the final rows for narrow_pipeline_df.
narrow_pipeline_df = (
    sales_df
    .filter(
        F.col('quantity') >= 3
    )
    .withColumn(
        'gross_sales',
        F.col('quantity') * F.col('unit_price'),
    )
    .select(
        'sale_id',
        'store_id',
        'product_id',
        'quantity',
        'gross_sales',
    )
)

# LAZY:
#     filter()
#     withColumn()
#     select()
#
# ACTION:
#     collect()
#
# collect() now demands concrete rows, so Spark must execute the lineage needed
# to produce them and return the small result to driver memory.
narrow_rows = narrow_pipeline_df.collect()

# Other common actions include:
#     count()
#     show()
#     take()
#     collect()
#     writes
#
# Warning: collect() is safe only when the result is known to be small enough for
# driver memory. Distributed computation can still end by overwhelming the
# driver if a large dataset is collected.


# =============================================================================
# 5. DAGS AND LINEAGE
# =============================================================================

# A DAG is a Directed Acyclic Graph of dependent computation.
#
# Directed:
#     upstream work feeds downstream work
#
# Acyclic:
#     dependencies do not loop back and require themselves recursively
#
# LINEAGE is the dependency history Spark can use to determine how a derived
# dataset was produced and what must be recomputed when an action demands it.
#
# Conceptual lineage for narrow_pipeline_df:
#
#     range source
#         |
#       select
#         |
#       filter
#         |
#     withColumn
#         |
#       select
#         |
#      collect
#
# Reusing the same unpersisted DataFrame in another action creates another
# execution demand. Spark may need to recompute the required lineage again.
second_narrow_row_count = narrow_pipeline_df.count()
assert second_narrow_row_count == len(narrow_rows)

# cache() / persist() are also lazy with respect to materialization: asking Spark
# to cache a DataFrame does not by itself mean every partition has already been
# computed. An action is still needed to populate the relevant cached results.
cached_narrow_df = narrow_pipeline_df.cache()
cached_narrow_count = cached_narrow_df.count()
assert cached_narrow_count == second_narrow_row_count

# Release the teaching cache after the example so later experiments do not
# depend on reused cached state accidentally.
cached_narrow_df.unpersist()


# =============================================================================
# 6. PARTITIONS AND PARALLELISM
# =============================================================================

# A partition is a logical chunk of data Spark can assign as one task's input for
# a stage. With four partitions, a ready stage exposes approximately four tasks.
#
#     Partition 0 -> Task 0
#     Partition 1 -> Task 1
#     Partition 2 -> Task 2
#     Partition 3 -> Task 3
#
# local[4] gives this local application four worker threads, so four runnable
# tasks can potentially execute concurrently.
#
# IMPORTANT:
#     partition != CPU core
#
# A partition is a unit of data/work. A core/task slot is execution capacity.
# Actual concurrency is bounded by both runnable tasks and available capacity.
#
# Rough reasoning model:
#
#     concurrent work
#     ~= min(runnable partition tasks, available task capacity)
#
# If a stage has eight partitions but only four available task slots, tasks run
# in waves. If a stage has two partitions and thirty-two available slots, only
# two tasks are exposed by that stage.

narrow_partition_count = narrow_pipeline_df.rdd.getNumPartitions()
assert narrow_partition_count == 4

# Because filter/withColumn/select are narrow here, no operation in this chain
# intentionally creates a new partitioning boundary. The four-partition source
# continues to define the available partition-level work for this narrow stage.
#
# EXECUTION PARTITIONS VS. STORAGE PARTITIONS
# -------------------------------------------
# The partitions discussed in Phase 4 are units of Spark execution work. They
# are not the same as directory-based storage partitions such as year=2026/.
# Storage/file partitioning is treated deeply in Phase 6.


# =============================================================================
# 7. NARROW DEPENDENCIES
# =============================================================================

# A dependency is conceptually narrow when downstream partition work can be
# produced from a limited/local set of parent partition data without globally
# redistributing records.
#
# Typical partition-local DataFrame operations:
#     filter
#     select / projection
#     ordinary withColumn expressions
#
# These operations can usually pipeline inside one stage:
#
#     read partition
#         |
#       filter
#         |
#    derived column
#         |
#      projection
#
# Three transformations do NOT imply three stages.

narrow_only_df = (
    sales_df
    .filter(
        F.col('quantity') >= 5
    )
    .withColumn(
        'line_value',
        F.col('quantity') * F.col('unit_price'),
    )
    .select(
        'sale_id',
        'store_id',
        'line_value',
    )
)

# collect() triggers this narrow-only lineage. The transformation chain itself
# introduces no grouping, global ordering, or explicit redistribution.
narrow_only_rows = narrow_only_df.collect()
assert len(narrow_only_rows) == 8


# =============================================================================
# 8. WIDE DEPENDENCIES AND SHUFFLES
# =============================================================================

# A wide dependency exists when downstream partitions require data from many
# parent partitions. Matching/grouping records must then be redistributed.
# Common operations that should trigger shuffle reasoning include groupBy(),
# distinct(), global orderBy(), repartition(), and many ordinary joins. The
# correct question is whether downstream output needs records from many parents.
#
# Our seed deliberately places S01 and S02 in multiple input partitions.
# Therefore a correct store-level aggregation cannot finish partition-locally.
wide_grouped_df = (
    sales_df
    .filter(
        F.col('quantity') >= 3
    )
    .withColumn(
        'line_value',
        F.col('quantity') * F.col('unit_price'),
    )
    .groupBy('store_id')
    .agg(
        F.sum('line_value').alias('store_value'),
        F.sum('quantity').alias('units'),
    )
)

# Still lazy: even groupBy()/agg() only describe the required grouped result.
# No shuffle must physically occur until an action demands this lineage.
wide_result_partition_count = wide_grouped_df.rdd.getNumPartitions()

# Because spark.sql.shuffle.partitions was deliberately set to four and AQE was
# disabled for this lesson, the post-shuffle result is planned with four shuffle
# partitions. This is an established count, not an invented assumption.
assert wide_result_partition_count == 4

# collect() is the action that now requires the grouped result.
wide_rows = wide_grouped_df.collect()

# Conceptual execution:
#
#     Stage region 1
#     --------------
#     4 input partitions
#         |
#       filter          narrow
#         |
#     withColumn        narrow
#         |
#     prepare grouped shuffle output
#         |
#     ========== SHUFFLE BOUNDARY ==========
#         |
#     Stage region 2
#     --------------
#     4 shuffle partitions
#         |
#     finish grouped aggregation
#         |
#       collect
#
# The shuffle is required because each store's rows started in several source
# partitions and must be brought together by key for the grouped totals.
#
# A shuffle is materially different from narrow partition-local work: Spark may
# partition intermediate records, write shuffle data, transfer it between
# executors/nodes, and read it again downstream. Phase 4 recognizes this cost;
# detailed shuffle engineering is deferred to later phases.

wide_result = {
    row['store_id']: (row['store_value'], row['units'])
    for row in wide_rows
}

# Deterministic correctness assertions keep the execution example grounded in
# actual data rather than vague terminology.
assert wide_result['S01'] == (350, 35)
assert wide_result['S02'] == (400, 40)


# =============================================================================
# 9. SHUFFLE BOUNDARIES AND STAGE BOUNDARIES
# =============================================================================

# Core Phase 4 rule:
#
#     narrow dependency -> can usually stay inside the same stage
#     wide dependency   -> shuffle boundary -> important stage boundary
#
# Narrow-only conceptual flow:
#
#     read -> filter -> withColumn -> select -> action
#
# One-wide-boundary conceptual flow:
#
#     read -> filter -> withColumn -> groupBy
#                                 |
#                        ===== shuffle =====
#                                 |
#                       finish aggregation -> action
#
# Stage count is NOT transformation count.

# Add a GLOBAL orderBy() after the grouped result. A global ordering generally
# requires another redistribution so the final dataset can be globally ordered.
# This deliberately creates a second wide boundary for reasoning practice.
two_shuffle_df = (
    wide_grouped_df
    .orderBy(
        F.col('store_value').desc(),
        F.col('store_id').asc(),
    )
)

# The orderBy() chain is still lazy until this action.
two_shuffle_rows = two_shuffle_df.collect()

# Conceptually:
#
#     Stage region 1
#     source + narrow work + prepare grouping shuffle
#             |
#     ===== shuffle 1 =====
#             |
#     Stage region 2
#     finish grouped aggregation + prepare global sort shuffle
#             |
#     ===== shuffle 2 =====
#             |
#     Stage region 3
#     produce globally ordered result
#
# Phase 4 uses 'expected stage regions' rather than asserting exact Spark UI
# stage IDs. Detailed physical-plan verification belongs to Phase 5.
assert [row['store_id'] for row in two_shuffle_rows] == ['S02', 'S01']


# =============================================================================
# 10. TASK EXECUTION
# =============================================================================

# Tasks are stage-specific. One task handles one partition for that stage.
#
# First stage in the grouped example:
#     established partitions = 4
#     approximate input tasks = 4
#
# Post-shuffle grouped stage:
#     established shuffle partitions = 4
#     approximate stage tasks = 4
#
# These are separate task sets even though both happen to have four partitions.
# A task is not permanently attached to an original dataset partition forever.
first_stage_partition_count = input_partition_count
post_shuffle_partition_count = wide_result_partition_count

assert first_stage_partition_count == 4
assert post_shuffle_partition_count == 4

# With local[4], up to four ready tasks can potentially run concurrently here.
# If there were eight partitions, four would run while the remaining runnable
# tasks waited for execution capacity, then another wave could run.
#
# Dependencies still constrain concurrency: downstream shuffle consumers cannot
# simply finish before their required upstream shuffle outputs exist.
#
# Tasks can also be retried after failures. Therefore distributed task code
# should not depend on unsafe one-time side effects. Robust replay/idempotency
# design is treated in later production-oriented phases.


# =============================================================================
# 11. DRIVER-SIDE VS. EXECUTOR/TASK-SIDE WORK
# =============================================================================

# Driver-side control flow includes ordinary Python logic such as these asserts,
# variable assignments, and decisions about which DataFrame transformations to
# define or which actions to invoke.
#
# Distributed task-side work includes processing the partitions required by
# DataFrame actions.
#
# collect() illustrates both sides:
#
#     executors/tasks -> compute distributed partitions
#            |
#            v
#          driver -> receives final rows in Python memory
#
# This is why collect() is not 'driver computes everything'. Executors do the
# distributed work; collect() merely brings the final result back to the driver.
collected_store_ids = [row['store_id'] for row in wide_rows]
assert set(collected_store_ids) == {'S01', 'S02'}


# =============================================================================
# 12. JOIN REASONING WITHOUT DRIFTING INTO PHASE 5
# =============================================================================

# Do not memorize the false rule:
#     every join always shuffles both sides
#
# A professional Phase 4 question is instead:
#
#     If matching rows are not already suitably distributed, would Spark need
#     to bring related rows together across partitions?
#
# The exact join strategy, operator choice, and physical plan are deliberately
# deferred to Phase 5 and later performance phases.
#
# This small dimension is intentionally defined only to support conceptual join
# reasoning. We do not inspect explain() output in this phase.
stores_df = spark.createDataFrame(
    [
        ('S01', 'Toronto Central'),
        ('S02', 'Mississauga West'),
    ],
    ['store_id', 'store_name'],
)

joined_df = sales_df.join(
    stores_df,
    on='store_id',
    how='left',
)

# The DataFrame remains lazy until an action. Which exact join strategy Spark
# chooses is not asserted here because that is a query-plan question.
joined_sample = joined_df.limit(4).collect()
assert len(joined_sample) == 4


# =============================================================================
# 13. CONCEPTUAL RDD UNDERSTANDING
# =============================================================================

# RDD = Resilient Distributed Dataset.
#
# Useful conceptual model:
#
#     immutable distributed collection
#             +
#     partitioned execution
#             +
#     dependency relationships
#             +
#     lineage-based recoverability
#
# RDD vocabulary makes Spark's lower-level execution foundations explicit:
#     partitions
#     dependencies
#     lineage
#     narrow vs. wide relationships
#
# DataFrames are generally preferred for structured data engineering because
# they provide schemas, named columns, relational operations, Spark SQL
# integration, and stronger opportunities for Spark's structured query engine.
# Detailed optimizer behavior belongs to Phase 5.

sales_rdd = sales_df.rdd
rdd_partition_count = sales_rdd.getNumPartitions()
assert rdd_partition_count == 4

# Stop here. The goal is NOT to rewrite this lecture using map(), flatMap(), or
# reduceByKey(). RDD concepts clarify execution; DataFrames remain the primary
# interface for this project.


# =============================================================================
# 14. LOCAL MODE VS. CLUSTER MODE
# =============================================================================

# LOCAL MODE IN THIS FILE
# -----------------------
# master('local[4]') means one machine provides four local task-execution
# threads. This environment can still demonstrate:
#     - lazy evaluation
#     - actions
#     - jobs
#     - stages
#     - tasks
#     - partitions
#     - shuffles
#     - task concurrency
#
# It does NOT reproduce:
#     - real multi-machine network traffic
#     - separate physical worker nodes
#     - production cluster-manager resource contention
#     - multi-node executor failure behavior
#
# CLUSTER MODE
# ------------
# Conceptually:
#
#     Driver
#       |
#     cluster manager
#       |
#     worker nodes
#       |
#     executors
#       |
#     tasks over distributed partitions
#
# The execution model transfers from local learning to cluster execution, while
# the physical cost and failure characteristics become genuinely distributed.


# =============================================================================
# 15. WORKED EXECUTION-PREDICTION CHECKLIST
# =============================================================================

# Use this checklist BEFORE observing Spark UI or detailed query plans later:
#
# 1. What action triggers execution?
# 2. What lineage must be computed for that action?
# 3. How many input partitions are actually established?
# 4. Which dependencies are narrow?
# 5. Which dependencies are wide?
# 6. Where must data shuffle?
# 7. Where should stages split?
# 8. What partition set feeds each stage?
# 9. Approximately how many tasks does that imply?
# 10. Which tasks can run concurrently given available resources?

prediction_df = (
    sales_df
    .filter(
        F.col('quantity') >= 3
    )
    .withColumn(
        'gross_sales',
        F.col('quantity') * F.col('unit_price'),
    )
    .groupBy('store_id')
    .agg(
        F.sum('gross_sales').alias('gross_sales'),
    )
)

# BEFORE running the action, the expected answer is:
#
# 1. LAZY
#    filter, withColumn, groupBy, and agg describe the computation.
#
# 2. ACTION
#    collect() below demands the grouped result.
#
# 3. INPUT PARTITIONS
#    exactly four were established with spark.range(..., numPartitions=4).
#
# 4. NARROW DEPENDENCIES
#    filter + withColumn.
#
# 5. WIDE DEPENDENCY
#    groupBy('store_id') because each store key spans source partitions.
#
# 6. SHUFFLE
#    rows must redistribute by grouping key.
#
# 7. STAGE SPLIT
#    at the grouping shuffle boundary.
#
# 8. TASKS
#    approximately one task per partition for each stage's partition set.
#
# 9. PARALLEL WORK
#    ready tasks in one stage can run concurrently, limited by local[4].
#
# 10. LIMIT OF PHASE 4
#    exact physical operators and Catalyst/AQE decisions are not asserted here.
prediction_rows = prediction_df.collect()
assert len(prediction_rows) == 2


# =============================================================================
# 16. COMMON FAILURE MODES — EXECUTABLE REMINDERS
# =============================================================================

# WRONG:
#     filtered_df = df.filter(...)
#     'Spark already processed every row.'
#
# RIGHT:
#     a transformation normally extends lazy lineage until an action needs it.
#
# WRONG:
#     three transformations = three stages
#
# RIGHT:
#     several narrow transformations can pipeline in one stage.
#
# WRONG:
#     four input partitions = four tasks everywhere forever
#
# RIGHT:
#     tasks are stage-specific and partition counts can change after shuffles.
#
# WRONG:
#     partition = CPU core
#
# RIGHT:
#     partition = unit of data/work; core/task slot = execution capacity.
#
# WRONG:
#     worker node = executor
#
# RIGHT:
#     worker node = machine; executor = Spark application process on a node.
#
# WRONG:
#     collect() means driver performs all processing
#
# RIGHT:
#     distributed tasks compute the data; collect() returns the result to driver.
#
# WRONG:
#     cache() immediately materializes all rows
#
# RIGHT:
#     cache/persist marks reuse intent; an action computes/materializes data.
#
# WRONG:
#     all joins always shuffle both sides
#
# RIGHT:
#     reason about whether matching data must move; inspect exact strategy later.


# =============================================================================
# 17. PHASE 4 MASTERY REFERENCE — NOT THE MASTERY GATE
# =============================================================================

# By the end of Phase 4, given a pipeline such as:
#
#     sales_df
#         .filter(...)
#         .withColumn(...)
#         .groupBy(...)
#         .agg(...)
#         .show()
#
# you should be able to predict approximately:
#
#     - the transformation chain is lazy;
#     - show() is the execution demand;
#     - filter/withColumn are conceptually narrow;
#     - grouping is wide when keys span partitions;
#     - grouping therefore requires redistribution;
#     - the shuffle creates the important stage boundary;
#     - each ready stage has approximately one task per stage partition;
#     - independent ready tasks can run in parallel subject to executor capacity;
#     - exact physical operators and AQE behavior belong to Phase 5.
#
# This file teaches that reasoning model. It does NOT perform or mark the Phase 4
# mastery gate, and it does not change ROADMAP.md.
