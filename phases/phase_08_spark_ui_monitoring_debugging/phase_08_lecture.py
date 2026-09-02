#!/usr/bin/env python
'''PySpark 101 — Phase 8 lecture.

Spark UI, monitoring, and debugging through runtime evidence.

This is lecture-only code. It reuses Phase 4–7 concepts instead of reteaching
them. The governing workflow is:

    symptom
        -> job
        -> stage
        -> tasks
        -> runtime evidence
        -> physical plan
        -> root cause
        -> change ONE thing
        -> rerun
        -> compare
        -> validate correctness

The central question is:

    where is the time or failure occurring, and what physical behavior caused it?

IMPORTANT:

    Spark UI observations depend on the runtime.

The code therefore makes predictions and creates controlled workloads, but it
never claims that a particular task duration, spill amount, stage count, AQE
change, or executor pattern must appear. Record what your Spark UI actually
shows.

Set PAUSE_AFTER_ACTION = True when you want the script to pause after important
actions so you can inspect the live Spark UI before continuing.
'''

from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


# =============================================================================
# 0. SPARK SESSION AND PHASE 8 MENTAL MODEL
# =============================================================================

spark = (
    SparkSession.builder
    .appName('phase_08_spark_ui_monitoring_debugging')
    # Four local worker threads keep task-level behavior visible while this
    # lecture remains runnable on one development machine.
    .master('local[4]')
    # A modest shuffle target keeps stage/task counts understandable in the UI.
    .config('spark.sql.shuffle.partitions', '12')
    # Start from a static plan so the early examples are easier to reason about.
    # AQE is enabled deliberately in its own experiment later.
    .config('spark.sql.adaptive.enabled', 'false')
    .getOrCreate()
)

spark.sparkContext.setLogLevel('WARN')

# Flip this to True when studying the Spark UI interactively.
#
# WHY:
# The live Web UI disappears when the Spark application exits. Pausing after
# actions lets you inspect completed jobs/stages/tasks while the application is
# still alive.
PAUSE_AFTER_ACTION = False

print('SPARK UI')
print(f'Live UI URL: {spark.sparkContext.uiWebUrl}')
print(
    'Use the URL reported by Spark. Do not assume that the UI is always on the '
    'same port.'
)

# Phase 8 is NOT:
#
#     slow job
#         -> open every UI tab
#         -> stare at metrics
#         -> guess
#
# It IS:
#
#     symptom
#         -> identify the action
#         -> identify the relevant job
#         -> identify the slow/failing stage
#         -> compare tasks
#         -> inspect runtime evidence
#         -> connect the evidence to the physical plan
#         -> form ONE root-cause hypothesis
#         -> change ONE thing
#         -> rerun the SAME workload
#         -> compare runtime behavior
#         -> validate correctness
#
# Core distinction:
#
#     Spark UI
#         -> WHERE did time/failure appear?
#
#     df.explain('formatted')
#         -> WHY did Spark create that physical work?
#
# Runtime symptom != physical root cause.


# =============================================================================
# 1. DIAGNOSTIC HELPERS
# =============================================================================

def print_section(title):
    '''Print a visible lecture section divider.'''

    print('\n' + '=' * 79)
    print(title)
    print('=' * 79)


def pause_for_ui(label):
    '''Optionally pause so the learner can inspect the live Spark UI.'''

    if PAUSE_AFTER_ACTION:
        input(
            f'\nInspect the Spark UI for {label}. '
            'Press Enter when you are ready to continue...'
        )


def print_runtime_checklist():
    '''Print the evidence checklist used after a materializing action.'''

    print('\nRECORD ACTUAL SPARK UI EVIDENCE')
    print('1. Which job corresponds to this action?')
    print('2. Which stage dominates runtime or failed?')
    print('3. How many tasks are in that stage?')
    print('4. Are task durations balanced or are there stragglers?')
    print('5. What are the relevant input / shuffle read / shuffle write sizes?')
    print('6. Is spill present? If yes, which tasks spill?')
    print('7. Are failures/retries concentrated in particular tasks/executors?')
    print('8. What does the SQL/DataFrame runtime plan show?')
    print('9. Which physical operator explains the expensive behavior?')


def run_action(label, action):
    '''Run one labelled action and return its result plus elapsed seconds.'''

    # Job descriptions make the action easier to locate in the Jobs view.
    spark.sparkContext.setJobDescription(label)

    started_at = perf_counter()
    result = action()
    elapsed_seconds = perf_counter() - started_at

    print(f'\nACTION: {label}')
    print(f'Elapsed seconds: {elapsed_seconds:.3f}')
    print('Treat elapsed time as supporting evidence, not proof by itself.')

    print_runtime_checklist()
    pause_for_ui(label)

    return result, elapsed_seconds


def print_partition_distribution(df, label):
    '''Summarize current row distribution by Spark execution partition.'''

    print(f'\nPARTITION DISTRIBUTION: {label}')

    # Aggregate first so diagnostic evidence stays small before reaching the
    # driver. Never collect a huge fact table merely to inspect partitions.
    (
        df
        .select(F.spark_partition_id().alias('partition_id'))
        .groupBy('partition_id')
        .agg(F.count('*').alias('row_count'))
        .orderBy('partition_id')
        .show(truncate=False)
    )


def print_key_distribution(df, key_column, label):
    '''Summarize key frequency so a suspected hot key can be verified.'''

    print(f'\nKEY DISTRIBUTION: {label}')

    (
        df
        .groupBy(key_column)
        .agg(F.count('*').alias('row_count'))
        .orderBy(
            F.col('row_count').desc(),
            F.col(key_column).asc_nulls_last(),
        )
        .show(20, truncate=False)
    )


def reconcile_scalar(left_df, right_df, measure_column, label):
    '''Reconcile one aggregate business measure between two DataFrames.'''

    left_value = (
        left_df
        .agg(F.sum(measure_column).alias('measure'))
        .first()['measure']
    )

    right_value = (
        right_df
        .agg(F.sum(measure_column).alias('measure'))
        .first()['measure']
    )

    print(f'\nRECONCILIATION: {label}')
    print(f'left:  {left_value}')
    print(f'right: {right_value}')

    assert left_value == right_value


# =============================================================================
# 2. DETERMINISTIC RETAIL DATA
# =============================================================================

print_section('DETERMINISTIC RETAIL DATA')

# Grain: one row per synthetic sale line.
#
# WHY:
# A deterministic relation lets before/after experiments use exactly the same
# business data and makes correctness reconciliation possible.
fact_sales_df = (
    spark.range(
        start=0,
        end=240000,
        step=1,
        numPartitions=12,
    )
    .select(
        F.col('id').cast('long').alias('sale_id'),
        F.date_add(
            F.lit('2026-01-01').cast('date'),
            (F.col('id') % 180).cast('int'),
        ).alias('order_date'),
        F.concat(
            F.lit('S'),
            F.lpad(
                ((F.col('id') % 40) + F.lit(1)).cast('string'),
                3,
                '0',
            ),
        ).alias('store_id'),
        F.concat(
            F.lit('P'),
            F.lpad(
                ((F.col('id') % 500) + F.lit(1)).cast('string'),
                4,
                '0',
            ),
        ).alias('product_id'),
        F.concat(
            F.lit('C'),
            F.lpad(
                ((F.col('id') % 10000) + F.lit(1)).cast('string'),
                5,
                '0',
            ),
        ).alias('customer_id'),
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
        ((F.col('id') % 5) + F.lit(1))
        .cast('int')
        .alias('quantity'),
        (
            F.lit(5.00)
            + ((F.col('id') % 75) * F.lit(0.75))
        )
        .cast(DecimalType(12, 2))
        .alias('unit_price'),
    )
    .withColumn(
        'gross_sales',
        (F.col('quantity') * F.col('unit_price'))
        .cast(DecimalType(16, 2)),
    )
    .withColumn('year', F.year('order_date'))
    .withColumn('month', F.month('order_date'))
)

# Grain: one row per store_id.
#
# WHY:
# This is a dimension-style relation whose key is unique and whose small size
# makes it a legitimate broadcast candidate in a controlled join experiment.
dim_store_df = (
    spark.range(
        start=1,
        end=41,
        step=1,
        numPartitions=2,
    )
    .select(
        F.concat(
            F.lit('S'),
            F.lpad(F.col('id').cast('string'), 3, '0'),
        ).alias('store_id'),
        F.concat(
            F.lit('Store '),
            F.col('id').cast('string'),
        ).alias('store_name'),
        F.when(F.col('id') <= 20, F.lit('ON'))
        .otherwise(F.lit('BC'))
        .alias('province'),
        F.when((F.col('id') % 2) == 0, F.lit('URBAN'))
        .otherwise(F.lit('SUBURBAN'))
        .alias('store_format'),
    )
)

# Grain: one row per product_id.
dim_product_df = (
    spark.range(
        start=1,
        end=501,
        step=1,
        numPartitions=4,
    )
    .select(
        F.concat(
            F.lit('P'),
            F.lpad(F.col('id').cast('string'), 4, '0'),
        ).alias('product_id'),
        F.concat(
            F.lit('Product '),
            F.col('id').cast('string'),
        ).alias('product_name'),
        F.concat(
            F.lit('CATEGORY_'),
            (F.col('id') % 12).cast('string'),
        ).alias('category'),
    )
)

print('fact_sales_df grain: one row per sale_id')
print('dim_store_df grain: one row per store_id')
print('dim_product_df grain: one row per product_id')
print(f'fact_sales_df partitions: {fact_sales_df.rdd.getNumPartitions()}')
print(f'dim_store_df partitions: {dim_store_df.rdd.getNumPartitions()}')
print(f'dim_product_df partitions: {dim_product_df.rdd.getNumPartitions()}')


# =============================================================================
# 3. ACTION -> JOB: FIND THE WORK THAT MATTERS
# =============================================================================

print_section('ACTION -> JOB')

# Transformations remain lazy until an action materializes them.
completed_store_sales_df = (
    fact_sales_df
    .filter(F.col('order_status') == 'COMPLETED')
    .groupBy('store_id')
    .agg(
        F.sum('gross_sales').alias('gross_sales'),
        F.count('*').alias('sale_line_count'),
    )
)

print('PHYSICAL PLAN BEFORE ACTION')
completed_store_sales_df.explain('formatted')

# ACTION 1:
# count() materializes the lineage and should create runtime evidence that can be
# located through the action description.
completed_store_count, _ = run_action(
    'PHASE 8 - ACTION 1 - completed store sales count',
    completed_store_sales_df.count,
)

print(f'completed store rows: {completed_store_count}')

# ACTION 2:
# A separate action can create additional job/query activity even though it uses
# the same logical DataFrame. This is the key reason to identify the action first.
completed_store_total, _ = run_action(
    'PHASE 8 - ACTION 2 - completed store sales total',
    lambda: (
        completed_store_sales_df
        .agg(F.sum('gross_sales').alias('gross_sales'))
        .first()['gross_sales']
    ),
)

print(f'completed gross sales: {completed_store_total}')

# UI STUDY:
#
# Compare the two labelled actions.
#
# Record:
#
#     Which jobs belong to ACTION 1?
#     Which jobs belong to ACTION 2?
#     Does the same upstream lineage appear to execute again?
#     Which stages are associated with each action?
#
# Do not assume that "one action always means exactly one job". Spark can create
# multiple jobs around query execution details, exchanges, broadcast materializing
# work, or other runtime behavior. Record what your application shows.


# =============================================================================
# 4. STAGES AND SHUFFLE BOUNDARIES
# =============================================================================

print_section('STAGES AND SHUFFLE BOUNDARIES')

province_sales_df = (
    fact_sales_df
    .filter(F.col('order_status') == 'COMPLETED')
    .select(
        'store_id',
        'gross_sales',
    )
    .join(
        dim_store_df.select(
            'store_id',
            'province',
        ),
        on='store_id',
        how='inner',
    )
    .groupBy('province')
    .agg(F.sum('gross_sales').alias('gross_sales'))
)

print('PLAN: PROVINCE SALES')
province_sales_df.explain('formatted')

province_sales_rows, _ = run_action(
    'PHASE 8 - STAGES - province sales collect',
    lambda: province_sales_df.orderBy('province').collect(),
)

print('PROVINCE SALES RESULT')
for row in province_sales_rows:
    print(row)

# UI STUDY:
#
# Start from the job associated with the labelled action.
#
# Then answer:
#
#     Which stages exist?
#     Which stage reads source/input data?
#     Which stage writes shuffle data?
#     Which stage reads shuffle data?
#     Which stage contains the aggregation?
#     Which stage takes longest?
#
# Then use the physical plan:
#
#     Exchange
#     BroadcastExchange
#     BroadcastHashJoin
#     SortMergeJoin
#     HashAggregate
#
# to explain WHY those stage boundaries or auxiliary jobs exist.
#
# The goal is not:
#
#     "Stage 3 is slow."
#
# The goal is:
#
#     "Stage 3 is where ______ physical behavior executes, and its task metrics
#     show ______."


# =============================================================================
# 5. TASKS: BALANCED WORK VS. STRAGGLERS
# =============================================================================

print_section('TASKS: BALANCED VS. SKEWED')

# -----------------------------------------------------------------------------
# 5.1 Balanced grouping workload
# -----------------------------------------------------------------------------

# Each store receives roughly the same number of sale lines.
balanced_sales_df = (
    spark.range(
        start=0,
        end=240000,
        step=1,
        numPartitions=12,
    )
    .select(
        F.col('id').alias('sale_id'),
        F.concat(
            F.lit('S'),
            F.lpad(
                ((F.col('id') % 40) + F.lit(1)).cast('string'),
                3,
                '0',
            ),
        ).alias('store_id'),
        F.lit(1).cast('long').alias('units'),
    )
)

print_key_distribution(
    balanced_sales_df,
    'store_id',
    'balanced store keys',
)

balanced_store_units_df = (
    balanced_sales_df
    .groupBy('store_id')
    .agg(F.sum('units').alias('units'))
)

print('BALANCED AGGREGATION PLAN')
balanced_store_units_df.explain('formatted')

run_action(
    'PHASE 8 - TASKS - balanced aggregation',
    balanced_store_units_df.count,
)

# Record actual task-level evidence:
#
#     task duration distribution
#     shuffle read size distribution
#     spill
#
# Prediction:
#
# Because key frequencies are intentionally similar, the reduce-side work should
# have a better chance of being balanced.
#
# This is only a prediction. Record what the UI actually shows.


# -----------------------------------------------------------------------------
# 5.2 Skewed grouping workload
# -----------------------------------------------------------------------------

# S001 receives 80% of rows.
#
# WHY:
# Hash partitioning cannot split one identical grouping key across several final
# grouping buckets. This creates a controlled hot-key workload.
skewed_sales_df = (
    spark.range(
        start=0,
        end=240000,
        step=1,
        numPartitions=12,
    )
    .select(
        F.col('id').alias('sale_id'),
        F.when(
            (F.col('id') % 100) < 80,
            F.lit('S001'),
        )
        .otherwise(
            F.concat(
                F.lit('S'),
                F.lpad(
                    (((F.col('id') % 39) + F.lit(2))).cast('string'),
                    3,
                    '0',
                ),
            )
        )
        .alias('store_id'),
        F.lit(1).cast('long').alias('units'),
    )
)

print_key_distribution(
    skewed_sales_df,
    'store_id',
    'skewed store keys',
)

skewed_store_units_df = (
    skewed_sales_df
    .groupBy('store_id')
    .agg(F.sum('units').alias('units'))
)

print('SKEWED AGGREGATION PLAN')
skewed_store_units_df.explain('formatted')

run_action(
    'PHASE 8 - TASKS - skewed aggregation',
    skewed_store_units_df.count,
)

# UI STUDY:
#
# Compare BALANCED vs. SKEWED.
#
# Look for:
#
#     one/few tasks much longer than peers
#     one/few tasks reading much more shuffle data
#     spill concentrated in the largest task(s)
#     stage tail waiting for a straggler
#
# Do NOT write:
#
#     "Skew caused one task to take 8 seconds."
#
# unless YOUR UI actually shows that.
#
# Instead record:
#
#     typical task duration:
#     maximum task duration:
#     typical shuffle read:
#     maximum shuffle read:
#     spill:
#
# Then connect that evidence to:
#
#     Exchange hashpartitioning(store_id, ...)
#
# and to the verified key-frequency distribution.


# =============================================================================
# 6. TOO FEW VS. TOO MANY TASKS
# =============================================================================

print_section('TOO FEW VS. TOO MANY TASKS')

base_task_df = spark.range(
    start=0,
    end=360000,
    step=1,
    numPartitions=12,
)

# -----------------------------------------------------------------------------
# 6.1 Deliberately too few partitions
# -----------------------------------------------------------------------------

few_partitions_df = (
    base_task_df
    # Change distribution deliberately so only two downstream partitions exist.
    .repartition(2)
    .select(
        F.col('id'),
        (F.col('id') % 1000).alias('group_id'),
    )
)

print(f'few_partitions_df partitions: {few_partitions_df.rdd.getNumPartitions()}')

run_action(
    'PHASE 8 - TASK COUNT - too few partitions',
    few_partitions_df.count,
)

# UI STUDY:
#
# Ask:
#
#     How many tasks execute in the relevant stage?
#     Are local worker threads idle because only two tasks are runnable?
#     How much data does each task process?
#
# Low utilization can be a PARTITIONING constraint, not an executor-count problem.


# -----------------------------------------------------------------------------
# 6.2 Deliberately many partitions
# -----------------------------------------------------------------------------

many_partitions_df = (
    base_task_df
    # Create many small downstream partitions for the same logical row count.
    .repartition(240)
    .select(
        F.col('id'),
        (F.col('id') % 1000).alias('group_id'),
    )
)

print(f'many_partitions_df partitions: {many_partitions_df.rdd.getNumPartitions()}')

run_action(
    'PHASE 8 - TASK COUNT - many small partitions',
    many_partitions_df.count,
)

# UI STUDY:
#
# Ask:
#
#     How many tasks execute?
#     How much data does each task process?
#     Are most tasks very short?
#     Is scheduling/coordination overhead now visible relative to useful work?
#
# Do not conclude that 240 partitions is universally "too many".
#
# The engineering question is:
#
#     Are tasks so small that coordination overhead is disproportionate for THIS
#     workload?


# =============================================================================
# 7. SHUFFLE READ / WRITE AND JOIN STRATEGY
# =============================================================================

print_section('SHUFFLE READ / WRITE AND JOIN STRATEGY')

# Business requirement:
#
#     completed gross sales by province
#
# Output grain:
#
#     one row per province
#
# Correctness precondition:
#
#     dim_store_df is unique on store_id

store_duplicate_count = (
    dim_store_df
    .groupBy('store_id')
    .count()
    .filter(F.col('count') > 1)
    .count()
)

assert store_duplicate_count == 0

# -----------------------------------------------------------------------------
# 7.1 Force a sort-merge join baseline
# -----------------------------------------------------------------------------

spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '-1')

sort_merge_province_sales_df = (
    fact_sales_df
    .filter(F.col('order_status') == 'COMPLETED')
    .select(
        'store_id',
        'gross_sales',
    )
    .hint('merge')
    .join(
        dim_store_df
        .select(
            'store_id',
            'province',
        )
        .hint('merge'),
        on='store_id',
        how='inner',
    )
    .groupBy('province')
    .agg(F.sum('gross_sales').alias('gross_sales'))
)

print('SORT-MERGE BASELINE PLAN')
sort_merge_province_sales_df.explain('formatted')

run_action(
    'PHASE 8 - JOIN - forced sort merge',
    sort_merge_province_sales_df.count,
)

# UI STUDY:
#
# Record actual evidence:
#
#     shuffle write by upstream stages
#     shuffle read by join stage
#     task count
#     task-duration distribution
#     spill if any
#
# Then map the runtime evidence to:
#
#     Exchange
#     Sort
#     SortMergeJoin


# -----------------------------------------------------------------------------
# 7.2 Change ONE thing: broadcast the validated-small dimension
# -----------------------------------------------------------------------------

broadcast_province_sales_df = (
    fact_sales_df
    .filter(F.col('order_status') == 'COMPLETED')
    .select(
        'store_id',
        'gross_sales',
    )
    .join(
        F.broadcast(
            dim_store_df.select(
                'store_id',
                'province',
            )
        ),
        on='store_id',
        how='inner',
    )
    .groupBy('province')
    .agg(F.sum('gross_sales').alias('gross_sales'))
)

print('BROADCAST PLAN')
broadcast_province_sales_df.explain('formatted')

run_action(
    'PHASE 8 - JOIN - broadcast dimension',
    broadcast_province_sales_df.count,
)

# Compare:
#
#     baseline join strategy
#     optimized join strategy
#     Exchanges
#     shuffle write
#     shuffle read
#     stage structure
#     task durations
#
# Correct explanation:
#
#     "The broadcast version removed the ordinary join-key shuffle on the large
#     fact side because the small build side was replicated."
#
# only if the physical/runtime plan confirms BroadcastHashJoin and the UI confirms
# the associated runtime behavior.
#
# Do NOT infer strategy from the source code alone.

reconcile_scalar(
    sort_merge_province_sales_df,
    broadcast_province_sales_df,
    'gross_sales',
    'sort merge vs. broadcast province sales',
)

spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '10485760')


# =============================================================================
# 8. SPILL: DIAGNOSE THE CAUSE, DO NOT INVENT IT
# =============================================================================

print_section('SPILL')

# A memory-intensive aggregation can create pressure, especially if partitions are
# large. Whether THIS local run spills depends on actual memory, JVM/runtime state,
# Spark internals, and partition sizes.
spill_candidate_df = (
    spark.range(
        start=0,
        end=600000,
        step=1,
        numPartitions=4,
    )
    .select(
        (F.col('id') % 200000).alias('group_id'),
        F.col('id').cast('double').alias('value'),
    )
    .groupBy('group_id')
    .agg(
        F.sum('value').alias('value_sum'),
        F.avg('value').alias('value_avg'),
    )
)

print('SPILL CANDIDATE PLAN')
spill_candidate_df.explain('formatted')

run_action(
    'PHASE 8 - SPILL - aggregation candidate',
    spill_candidate_df.count,
)

# RECORD:
#
#     memory spill:
#     disk spill:
#     which tasks spill:
#     task input/shuffle sizes:
#     task durations:
#
# If spill is ZERO:
#
#     That is a valid observation.
#
# Do not lower memory or invent a production-style spill problem merely to make
# the UI display a non-zero number.
#
# If spill appears only in one oversized task:
#
#     investigate skew / partition size first.
#
# If spill appears broadly across similarly sized tasks:
#
#     investigate general partition sizing, operator memory demand, cache pressure,
#     concurrency, and only then legitimate resource sizing.


# =============================================================================
# 9. EXECUTOR UTILIZATION AND TASK PLACEMENT
# =============================================================================

print_section('EXECUTOR UTILIZATION')

# In local mode the Spark UI still gives executor/driver-level evidence, but this
# is not equivalent to a real multi-node cluster. Use the pattern, not the exact
# local resource numbers, as the lesson.
executor_work_df = (
    spark.range(
        start=0,
        end=800000,
        step=1,
        numPartitions=32,
    )
    .select(
        F.col('id'),
        (F.col('id') % 10000).alias('group_id'),
    )
    .groupBy('group_id')
    .agg(F.count('*').alias('row_count'))
)

run_action(
    'PHASE 8 - EXECUTORS - parallel aggregation',
    executor_work_df.count,
)

# EXECUTOR VIEW QUESTIONS:
#
#     Are tasks being distributed across available executor capacity?
#     Are some executors idle because the current stage has too few tasks?
#     Does one executor repeatedly host slow or failed tasks?
#     Is GC time materially different across executors?
#     Is spill concentrated?
#
# Important:
#
# Near stage completion, many executors can become idle while one or two
# stragglers remain.
#
# That pattern can mean:
#
#     uneven work
#
# rather than:
#
#     insufficient executor count.


# =============================================================================
# 10. DRIVER VS. EXECUTOR PROBLEMS
# =============================================================================

print_section('DRIVER VS. EXECUTOR')

# Bad production pattern:
#
#     huge_rows = huge_df.collect()
#
# collect() asks executors to compute distributed partitions and then sends the
# entire result to the driver.
#
# A driver problem can therefore appear AFTER distributed task execution succeeds.

driver_safe_diagnostic_df = (
    fact_sales_df
    .groupBy('store_id')
    .agg(
        F.count('*').alias('row_count'),
        F.sum('gross_sales').alias('gross_sales'),
    )
)

# This collection is intentionally safe because the distributed aggregation has
# already reduced the result to approximately one row per store.
driver_safe_rows, _ = run_action(
    'PHASE 8 - DRIVER - collect small aggregated diagnostic',
    lambda: driver_safe_diagnostic_df.orderBy('store_id').collect(),
)

print(f'small rows collected to driver: {len(driver_safe_rows)}')

# Compare the mental models:
#
# SAFE:
#
#     huge distributed fact
#         -> distributed aggregation
#         -> tiny diagnostic result
#         -> collect
#
# RISKY:
#
#     huge distributed fact
#         -> collect entire fact
#         -> driver memory / Python local processing
#
# Diagnostic question:
#
#     Did Spark tasks themselves fail, or did the application become slow/fail
#     while returning/processing a large result on the driver?
#
# Increasing executor memory does not repair driver-side collect() misuse.


# =============================================================================
# 11. FAILED TASKS: LOCALIZE BEFORE FIXING
# =============================================================================

print_section('FAILED TASKS')

# Build a deterministic expression that will fail for one known row.
#
# The action is disabled by default because the purpose is to inspect an
# intentional failure interactively, not to make the full lecture script fail.
failure_demo_df = (
    spark.range(
        start=0,
        end=10000,
        step=1,
        numPartitions=8,
    )
    .select(
        F.col('id'),
        F.when(
            F.col('id') == 7777,
            F.raise_error('intentional Phase 8 failure for sale_id 7777'),
        )
        .otherwise(F.col('id').cast('string'))
        .alias('checked_value'),
    )
)

RUN_FAILURE_EXPERIMENT = False

if RUN_FAILURE_EXPERIMENT:
    try:
        run_action(
            'PHASE 8 - FAILURE - intentional deterministic task failure',
            lambda: (
                failure_demo_df
                .filter(F.col('checked_value') == 'OK')
                .count()
            ),
        )
    except Exception as error:
        print('\nEXPECTED FAILURE CAPTURED')
        print(type(error).__name__)
        print(
            'Now inspect the failed job/stage/task in the Spark UI before changing '
            'anything.'
        )
        pause_for_ui('intentional deterministic task failure')

# FAILURE INVESTIGATION:
#
# Record:
#
#     job ID:
#     stage ID:
#     failing task / partition:
#     task attempt:
#     executor:
#     exception:
#
# Ask:
#
#     Does the same logical task fail repeatedly?
#     Is the failure tied to a deterministic record/partition?
#     Do failures move across executors?
#
# Same partition repeatedly failing:
#
#     data-specific or deterministic partition-level problem is plausible.
#
# Failures moving broadly:
#
#     wider resource/infrastructure pressure may be plausible.
#
# Always preserve the failure evidence before blindly rerunning.


# =============================================================================
# 12. SQL / DATAFRAME QUERY VIEW + FORMATTED PHYSICAL PLAN
# =============================================================================

print_section('SQL / DATAFRAME QUERY VIEW + PHYSICAL PLAN')

query_bridge_df = (
    fact_sales_df
    .filter(
        (F.col('year') == 2026)
        & (F.col('month') <= 3)
        & (F.col('order_status') == 'COMPLETED')
    )
    .select(
        'store_id',
        'product_id',
        'gross_sales',
    )
    .join(
        F.broadcast(
            dim_product_df.select(
                'product_id',
                'category',
            )
        ),
        on='product_id',
        how='inner',
    )
    .groupBy(
        'store_id',
        'category',
    )
    .agg(F.sum('gross_sales').alias('gross_sales'))
)

print('FORMATTED PHYSICAL PLAN')
query_bridge_df.explain('formatted')

run_action(
    'PHASE 8 - SQL QUERY - store category sales',
    query_bridge_df.count,
)

# SQL / DATAFRAME VIEW QUESTIONS:
#
#     Which query entry corresponds to the labelled action?
#     Which jobs/stages are associated with it?
#     Which physical operators show runtime metrics?
#     How many rows reach Filter / Join / Aggregate operators?
#     How much data does an Exchange write?
#     What does BroadcastExchange report?
#
# Cross-reference:
#
#     Spark UI SQL operator
#         <->
#     df.explain('formatted') operator
#
# Example causal chain:
#
#     UI:
#         expensive shuffle stage
#
#     SQL operator:
#         Exchange
#
#     formatted plan:
#         Exchange hashpartitioning(store_id, category, ...)
#
#     explanation:
#         grouping requires rows with the same grouping keys to be colocated.
#
# The plan explains why the work exists.
# The UI explains how expensive the work became at runtime.


# =============================================================================
# 13. AQE: OBSERVE WHAT ACTUALLY CHANGES
# =============================================================================

print_section('AQE RUNTIME BEHAVIOR')

# -----------------------------------------------------------------------------
# 13.1 Baseline with AQE disabled
# -----------------------------------------------------------------------------

spark.conf.set('spark.sql.adaptive.enabled', 'false')
spark.conf.set('spark.sql.shuffle.partitions', '96')

aqe_input_df = (
    spark.range(
        start=0,
        end=240000,
        step=1,
        numPartitions=12,
    )
    .select(
        (F.col('id') % 40).alias('store_bucket'),
        F.col('id').alias('sale_id'),
    )
)

aqe_off_df = (
    aqe_input_df
    .groupBy('store_bucket')
    .agg(F.count('*').alias('row_count'))
)

print('AQE OFF PLAN')
aqe_off_df.explain('formatted')

run_action(
    'PHASE 8 - AQE - disabled',
    aqe_off_df.count,
)

# Record:
#
#     configured shuffle partition target:
#     actual stage task count:
#     runtime plan:
#
# Do not infer task count only from spark.sql.shuffle.partitions.


# -----------------------------------------------------------------------------
# 13.2 Change ONE thing: enable AQE
# -----------------------------------------------------------------------------

spark.conf.set('spark.sql.adaptive.enabled', 'true')
spark.conf.set('spark.sql.adaptive.coalescePartitions.enabled', 'true')

aqe_on_df = (
    aqe_input_df
    .groupBy('store_bucket')
    .agg(F.count('*').alias('row_count'))
)

run_action(
    'PHASE 8 - AQE - enabled',
    aqe_on_df.count,
)

print('AQE ON PLAN AFTER MATERIALIZATION')
aqe_on_df.explain('formatted')

# AQE STUDY:
#
# Record actual evidence:
#
#     Did the runtime plan become final/adaptive?
#     Did post-shuffle task count change?
#     Did Spark coalesce shuffle partitions?
#     Did elapsed time change?
#
# A valid outcome is:
#
#     AQE was enabled but this run did not make a material change.
#
# Never claim that AQE coalesced, switched joins, or split skew unless the runtime
# plan/UI actually proves it.

spark.conf.set('spark.sql.shuffle.partitions', '12')
spark.conf.set('spark.sql.adaptive.enabled', 'false')


# =============================================================================
# 14. RECOMPUTATION VS. JUSTIFIED CACHE
# =============================================================================

print_section('RECOMPUTATION VS. CACHE')

expensive_reused_df = (
    fact_sales_df
    .filter(F.col('order_status') == 'COMPLETED')
    .join(
        F.broadcast(
            dim_store_df.select(
                'store_id',
                'province',
            )
        ),
        on='store_id',
        how='inner',
    )
    .groupBy(
        'province',
        'product_id',
    )
    .agg(F.sum('gross_sales').alias('gross_sales'))
)

# Baseline: two separate actions over the same uncached lineage.
run_action(
    'PHASE 8 - REUSE - uncached action 1',
    expensive_reused_df.count,
)

run_action(
    'PHASE 8 - REUSE - uncached action 2',
    lambda: (
        expensive_reused_df
        .agg(F.sum('gross_sales').alias('gross_sales'))
        .first()['gross_sales']
    ),
)

# UI STUDY:
#
# Ask:
#
#     Does similar upstream work appear in both actions?
#     Are expensive joins/aggregations recomputed?
#
# Only after confirming meaningful reuse should caching be considered.

cached_reused_df = expensive_reused_df.cache()

# First cached action materializes the cache.
run_action(
    'PHASE 8 - REUSE - cache materialization',
    cached_reused_df.count,
)

# Second action can reuse materialized cached data.
run_action(
    'PHASE 8 - REUSE - cache reuse',
    lambda: (
        cached_reused_df
        .agg(F.sum('gross_sales').alias('gross_sales'))
        .first()['gross_sales']
    ),
)

# UI + STORAGE STUDY:
#
# Record:
#
#     How did the jobs/stages change after cache materialization?
#     Does the Storage view show the cached relation?
#     How many partitions are cached?
#     Is the cache reused by the next action?
#
# Never compare:
#
#     uncached cold run
#
# directly against:
#
#     cached reuse run
#
# without acknowledging the cache materialization cost.

cached_reused_df.unpersist()


# =============================================================================
# 15. INTEGRATED SLOW PIPELINE: DIAGNOSE ONE CHANGE AT A TIME
# =============================================================================

print_section('INTEGRATED DIAGNOSTIC PIPELINE')

# The integrated experiment uses actual Parquet I/O so the Spark UI can expose
# scan-related evidence as well as shuffle/join/aggregation evidence.
with TemporaryDirectory() as temporary_directory:
    temp_root = Path(temporary_directory)
    sales_path = str(temp_root / 'fact_sales')
    output_path = str(temp_root / 'province_category_sales')

    # Write deterministic partitioned source data.
    #
    # This setup action is not the workload under investigation.
    run_action(
        'PHASE 8 - SETUP - write partitioned fact sales',
        lambda: (
            fact_sales_df
            .write
            .mode('overwrite')
            .partitionBy(
                'year',
                'month',
            )
            .parquet(sales_path)
        ),
    )

    source_sales_df = spark.read.parquet(sales_path)

    # -------------------------------------------------------------------------
    # 15.1 Baseline
    # -------------------------------------------------------------------------

    # Business requirement:
    #
    #     completed Q1 2026 gross sales by province and category
    #
    # Output grain:
    #
    #     one row per province x category
    #
    # Deliberate baseline issue:
    #
    #     force a sort-merge join against a tiny store dimension
    #
    # Only ONE intentional optimization will be applied afterward.

    spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '-1')

    baseline_pipeline_df = (
        source_sales_df
        .filter(
            (F.col('year') == 2026)
            & (F.col('month') <= 3)
            & (F.col('order_status') == 'COMPLETED')
        )
        .select(
            'store_id',
            'product_id',
            'gross_sales',
        )
        .hint('merge')
        .join(
            dim_store_df
            .select(
                'store_id',
                'province',
            )
            .hint('merge'),
            on='store_id',
            how='inner',
        )
        .join(
            F.broadcast(
                dim_product_df.select(
                    'product_id',
                    'category',
                )
            ),
            on='product_id',
            how='inner',
        )
        .groupBy(
            'province',
            'category',
        )
        .agg(F.sum('gross_sales').alias('gross_sales'))
    )

    print('INTEGRATED BASELINE PLAN')
    baseline_pipeline_df.explain('formatted')

    _, baseline_seconds = run_action(
        'PHASE 8 - INTEGRATED - baseline write',
        lambda: (
            baseline_pipeline_df
            .write
            .mode('overwrite')
            .parquet(output_path)
        ),
    )

    # BASELINE INVESTIGATION TEMPLATE:
    #
    # ACTION
    #     Which write action triggered the relevant job/query?
    #
    # JOB
    #     Which job(s) correspond to the write?
    #
    # STAGE
    #     Which stage dominates runtime?
    #
    # TASKS
    #     Are tasks balanced?
    #     Are there stragglers?
    #
    # I/O
    #     Which storage partitions were actually read?
    #
    # SHUFFLE
    #     Which stage writes/reads the most shuffle?
    #
    # SPILL
    #     Is spill present? Where?
    #
    # EXECUTORS
    #     Is available parallelism being used?
    #
    # PLAN
    #     Which operator explains the expensive stage?
    #
    # ROOT CAUSE
    #     State ONE hypothesis supported by evidence.

    # -------------------------------------------------------------------------
    # 15.2 Change ONE thing: broadcast the validated-small store dimension
    # -------------------------------------------------------------------------

    optimized_pipeline_df = (
        source_sales_df
        .filter(
            (F.col('year') == 2026)
            & (F.col('month') <= 3)
            & (F.col('order_status') == 'COMPLETED')
        )
        .select(
            'store_id',
            'product_id',
            'gross_sales',
        )
        .join(
            F.broadcast(
                dim_store_df.select(
                    'store_id',
                    'province',
                )
            ),
            on='store_id',
            how='inner',
        )
        .join(
            F.broadcast(
                dim_product_df.select(
                    'product_id',
                    'category',
                )
            ),
            on='product_id',
            how='inner',
        )
        .groupBy(
            'province',
            'category',
        )
        .agg(F.sum('gross_sales').alias('gross_sales'))
    )

    print('INTEGRATED OPTIMIZED PLAN')
    optimized_pipeline_df.explain('formatted')

    optimized_output_path = str(temp_root / 'province_category_sales_optimized')

    _, optimized_seconds = run_action(
        'PHASE 8 - INTEGRATED - broadcast store write',
        lambda: (
            optimized_pipeline_df
            .write
            .mode('overwrite')
            .parquet(optimized_output_path)
        ),
    )

    reconcile_scalar(
        baseline_pipeline_df,
        optimized_pipeline_df,
        'gross_sales',
        'integrated baseline vs. optimized',
    )

    print('\nINTEGRATED WALL-CLOCK COMPARISON')
    print(f'baseline seconds: {baseline_seconds:.3f}')
    print(f'optimized seconds: {optimized_seconds:.3f}')
    print(
        'The stronger comparison is the plan + job/stage/task/shuffle evidence, '
        'not a single local wall-clock number.'
    )

    spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '10485760')


# =============================================================================
# 16. RUNTIME EVIDENCE PATTERN REFERENCE
# =============================================================================

print_section('RUNTIME EVIDENCE PATTERNS')

print(
    '''
Use these as investigation directions, not automatic diagnoses.

ONE / FEW TASKS MUCH SLOWER
    -> skew, oversized partition, spill, executor-specific issue

ONE / FEW TASKS READ MUCH MORE SHUFFLE DATA
    -> skewed shuffle partition / hot key

MANY LONG TASKS WITH SIMILAR SIZES
    -> large legitimate work, poor strategy, large I/O, or general partition size

MANY TINY SHORT TASKS
    -> excessive partitioning or small-file fragmentation

LARGE SHUFFLE ON BOTH JOIN SIDES
    -> inspect sort-merge / shuffle join and whether both sides must redistribute

SPILL CONCENTRATED IN STRAGGLERS
    -> inspect skew / oversized partitions before adding memory

SPILL ACROSS MANY BALANCED TASKS
    -> inspect general partition sizing / resource pressure

EXECUTORS MOSTLY IDLE DURING LONG STAGE
    -> too few runnable tasks or a small number of stragglers

SAME TASK / PARTITION FAILS REPEATEDLY
    -> deterministic partition/data problem is plausible

APPLICATION FAILS AFTER DISTRIBUTED TASKS FINISH
    -> driver collection / local processing problem is plausible

SIMILAR EXPENSIVE WORK REPEATED ACROSS ACTIONS
    -> evaluate whether persistence is justified

RUNTIME TASK COUNT DIFFERS FROM STATIC SHUFFLE TARGET
    -> AQE may have adapted/coalesced; inspect runtime plan

LARGE SCAN BEFORE SELECTIVE RESULT
    -> inspect pruning / pushdown / source layout

JOIN OUTPUT EXPLODES
    -> correctness and grain problem before performance tuning
'''
)


# =============================================================================
# 17. PHASE 8 ENGINEERING RULES
# =============================================================================

# 1. Start with the symptom, not with a Spark UI tab.
# 2. Identify the materializing action first.
# 3. Locate the job that corresponds to the workload you care about.
# 4. A job can contain several stages; find the stage that dominates or fails.
# 5. A stage is a location, not a root cause.
# 6. Task-level evidence often reveals the actual distribution problem.
# 7. Compare typical and maximum task duration; averages can hide stragglers.
# 8. Compare task input/shuffle sizes before calling a straggler "skew".
# 9. Shuffle write means data is being prepared for downstream redistribution.
# 10. Shuffle read means downstream tasks are consuming redistributed data.
# 11. Large shuffle is evidence to investigate, not proof of a bad pipeline.
# 12. Explain WHY each Exchange exists.
# 13. Partition count and partition balance are different diagnostic dimensions.
# 14. Too few tasks can leave executor capacity idle.
# 15. Too many tiny tasks can create disproportionate scheduling overhead.
# 16. A hot key matters when it creates materially uneven execution work.
# 17. Confirm hot keys with data-frequency evidence.
# 18. Spill is a symptom, not automatically an executor-memory problem.
# 19. Spill isolated to stragglers points toward skew/oversized partitions first.
# 20. Widespread spill can justify broader partition/resource investigation.
# 21. Executor utilization must be interpreted with runnable task count.
# 22. Many idle executors near stage completion can be a straggler symptom.
# 23. Slow tasks on one executor can indicate executor-specific trouble.
# 24. Driver memory and executor memory are different diagnoses.
# 25. collect() and toPandas() can turn distributed data into driver pressure.
# 26. Aggregate diagnostics in Spark before collecting small results.
# 27. Preserve failure evidence before repeatedly rerunning.
# 28. Repeated failure of one logical partition can indicate deterministic data trouble.
# 29. Use the SQL/DataFrame query view to bridge stages to physical operators.
# 30. Use df.explain('formatted') beside runtime evidence.
# 31. The plan explains why work exists; the UI shows how it behaved.
# 32. Do not infer BroadcastHashJoin from a broadcast hint alone; inspect the plan.
# 33. Do not infer SortMergeJoin cost from the plan alone; inspect runtime metrics.
# 34. AQE enabled does not mean AQE modified every query.
# 35. Record runtime AQE changes instead of assuming them.
# 36. A zero-spill experiment is a valid result.
# 37. Do not force unrealistic memory settings solely to manufacture spill.
# 38. Cache only when repeated runtime evidence justifies reuse.
# 39. Cache materialization must be counted as work.
# 40. The Storage view is useful only after persisted data is materialized.
# 41. Label actions so jobs are easy to identify during teaching/debugging.
# 42. Change ONE thing per experiment.
# 43. Compare the SAME materializing action before and after.
# 44. Control cold/warm/cache state when comparing runs.
# 45. Wall-clock time is supporting evidence, not the entire diagnosis.
# 46. Validate schema, grain, semantics, and business totals after optimization.
# 47. Fix broken join grain before performance tuning.
# 48. Fix query/data design before resource configuration.
# 49. A professional diagnosis connects symptom -> runtime evidence -> plan -> cause.
# 50. Never invent Spark UI observations.


# =============================================================================
# 18. PHASE 8 FINAL DIAGNOSTIC CHECKLIST
# =============================================================================

print_section('PHASE 8 FINAL DIAGNOSTIC CHECKLIST')

print(
    '''
ACTION
    Which action triggered execution?

JOB
    Which job corresponds to the action I care about?
    Are several actions recomputing the same lineage?

STAGE
    Which stage is slow or failing?
    What shuffle/stage boundary created it?

TASKS
    Are tasks balanced?
    Which tasks are stragglers?
    Do slow tasks process more data?
    Do they spill?
    Did they fail/retry?

SHUFFLE
    How much data is written/read?
    Why does the shuffle exist?
    Can data volume or row width be reduced safely?

PARTITIONING
    Too few tasks?
    Too many tiny tasks?
    One/few oversized partitions?
    Hot keys?

MEMORY
    Is spill isolated or widespread?
    Is cache competing for memory?
    Is the partition itself too large?

EXECUTORS
    Is available task parallelism being used?
    Is one executor repeatedly slow/failing?

DRIVER
    Is the problem outside distributed task execution?
    Is too much data being collected?

PLAN
    Which physical operator corresponds to the expensive stage?
    Why did Spark choose that strategy?

AQE
    Did AQE actually change the runtime plan?
    Did task count / join strategy / skew handling change?

CHANGE
    What ONE change has the strongest evidence behind it?

RERUN
    Did stage/task/shuffle/spill behavior improve?

CORRECTNESS
    Did schema, grain, row semantics, and business totals remain correct?
'''
)


# =============================================================================
# 19. PHASE 8 FINAL MENTAL MODEL
# =============================================================================

# A professional explanation should sound like:
#
#     Action:
#         the write action triggered the workload under investigation
#
#     Location:
#         job X / stage Y dominated runtime
#
#     Task evidence:
#         most tasks completed in a similar range, but one task consumed much
#         more shuffle data and ran substantially longer
#
#     Plan evidence:
#         an Exchange hashpartitioning(store_id, ...) fed a SortMergeJoin
#
#     Data evidence:
#         one store_id was verified as a hot key
#
#     Root cause:
#         skewed shuffle distribution created a straggler partition
#
#     First change:
#         apply the smallest skew-targeted design change justified by evidence
#
#     Rerun:
#         compare the same action's stage duration, task distribution, shuffle,
#         spill, and runtime plan
#
#     Correctness:
#         schema, output grain, and business totals still reconcile
#
# Another valid diagnosis can be:
#
#     no optimization is justified
#
# because task sizes are balanced, the required shuffle is behaving normally,
# the join strategy matches the input sizes, spill is absent, and the runtime is
# simply the cost of legitimate work.
#
# The ability to choose NOT to tune is also professional debugging.
#
# Final Phase 8 workflow:
#
#     observe
#         -> localize
#         -> explain
#         -> intervene
#         -> measure
#         -> validate


spark.stop()
