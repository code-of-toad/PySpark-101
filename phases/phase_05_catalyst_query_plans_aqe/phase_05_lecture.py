#!/usr/bin/env python
'''PySpark 101 — Phase 5 lecture.

Catalyst, parsed/analyzed/optimized logical plans, physical plans, explain
modes, core physical operators, join strategies, and Adaptive Query Execution.

This is lecture-only code. It uses small deterministic retail-style datasets so
query-plan claims can be predicted before inspection. Deep storage engineering,
performance tuning, and Spark UI diagnosis are intentionally deferred to later
phases.
'''

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


# =============================================================================
# 0. SPARK SESSION AND PHASE 5 MENTAL MODEL
# =============================================================================

spark = (
    SparkSession.builder
    .appName('phase_05_catalyst_query_plans_aqe')
    # Four local worker threads are enough to preserve the Phase 4 distributed
    # execution model while keeping this lecture runnable on one machine.
    .master('local[4]')
    # A small fixed shuffle target keeps static plan examples easy to reason
    # about. AQE examples later deliberately use a larger initial value.
    .config('spark.sql.shuffle.partitions', '4')
    .getOrCreate()
)

# Phase 5 asks HOW Spark turns structured query intent into executable operators.
#
#     PySpark DataFrame / Spark SQL
#                 |
#                 v
#       parsed logical plan
#                 |
#                 | resolve names, relations, functions, and types
#                 v
#       analyzed logical plan
#                 |
#                 | Catalyst applies semantics-preserving rewrites
#                 v
#       optimized logical plan
#                 |
#                 | physical planning chooses executable strategies
#                 v
#           physical plan
#                 |
#                 | AQE may revise eligible decisions at runtime
#                 v
#       final runtime strategy
#
# Core distinction:
#
#     logical plan  = WHAT result is required
#     physical plan = HOW Spark plans to produce it
#
# Core workflow:
#
#     PySpark code
#         -> predict logical behavior
#         -> predict physical requirements
#         -> inspect explain output
#         -> identify operators
#         -> explain WHY Spark chose them
#         -> execute when AQE evidence is required
#         -> inspect again


# =============================================================================
# 1. DETERMINISTIC RETAIL DATA
# =============================================================================

# Explicit schemas keep the examples aligned with the project's data-engineering
# emphasis and make analyzed-plan type resolution easier to discuss.
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

# DecimalType expects Decimal-compatible values. Casting after creation keeps the
# seed concise while preserving a decimal money column in the teaching schema.
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
        StructType([
            StructField('sale_id', IntegerType(), False),
            StructField('store_id', StringType(), False),
            StructField('product_id', StringType(), False),
            StructField('order_status', StringType(), False),
            StructField('quantity', IntegerType(), False),
            StructField('unit_price_raw', StringType(), False),
        ]),
    )
    .withColumn(
        'unit_price',
        F.col('unit_price_raw').cast(DecimalType(12, 2)),
    )
    .drop('unit_price_raw')
)

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

# Keep one reusable line-level relation for most static plan examples.
# gross_sales is a derived expression that should commonly appear as Project
# work in the physical plan.
sales_enriched_df = sales_df.withColumn(
    'gross_sales',
    F.col('quantity') * F.col('unit_price'),
)


# =============================================================================
# 2. PARSED -> ANALYZED -> OPTIMIZED -> PHYSICAL
# =============================================================================

# Use one simple pipeline to inspect all four required plan layers.
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

# PREDICT BEFORE INSPECTION:
#
# Logical intent:
#     keep COMPLETED sales
#     -> project four requested columns
#
# Physical requirements:
#     source read
#     -> Filter
#     -> Project
#
# No grouped key, global order, repartition, or ordinary shuffle join exists, so
# this pipeline gives us no reason to predict an Exchange.

print('\n' + '=' * 80)
print('PLAN LIFECYCLE: PARSED -> ANALYZED -> OPTIMIZED -> PHYSICAL')
print('=' * 80)

# extended mode is the direct Phase 5 view of the full planning sequence.
plan_lifecycle_df.explain('extended')

# What to inspect in the output:
#
# PARSED LOGICAL PLAN
# - earliest unresolved logical representation
# - may show unresolved attributes such as 'store_id
#
# ANALYZED LOGICAL PLAN
# - attributes are resolved and typed
# - expression IDs such as store_id#12 identify plan expressions, NOT stages
#
# OPTIMIZED LOGICAL PLAN
# - Catalyst may collapse projections, simplify expressions, or rearrange safe
#   logical operations while preserving semantics
#
# PHYSICAL PLAN
# - executable operator strategy such as Scan, Filter, and Project


# =============================================================================
# 3. EXPLAIN MODES
# =============================================================================

print('\n' + '=' * 80)
print('DEFAULT EXPLAIN: PHYSICAL PLAN')
print('=' * 80)

# Default explain() prints the physical plan only.
plan_lifecycle_df.explain()

print('\n' + '=' * 80)
print('FORMATTED EXPLAIN: PHYSICAL OUTLINE + NODE DETAILS')
print('=' * 80)

# formatted mode is especially useful when reading operator inputs/outputs.
plan_lifecycle_df.explain('formatted')

# IMPORTANT IDENTIFIER DISTINCTION:
#
#     formatted node (1), (2), (3)
#         != Job 1 / Stage 2 / Task 3 / Partition 3
#
# Formatted numbers label plan nodes for display. Runtime IDs belong to the
# execution layer studied in Phase 4 and later through the Spark UI.


# =============================================================================
# 4. CATALYST REWRITES: SOURCE CODE SHAPE != OPTIMIZED PLAN SHAPE
# =============================================================================

# This pipeline intentionally contains redundant-looking projections and a
# constant expression so Catalyst has opportunities to simplify the logical tree.
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
        # Constant folding can replace 2 + 3 with the literal value 5.
        (F.lit(2) + F.lit(3)).alias('constant_five'),
    )
    .select(
        'store_id',
        'gross_sales',
        'constant_five',
    )
)

# PREDICTION:
# - the analyzed plan may reflect more of the source-code structure;
# - the optimized plan does not need to preserve every intermediate select();
# - constant expressions may be simplified;
# - the final semantics must remain identical.

print('\n' + '=' * 80)
print('CATALYST REWRITE EXAMPLE')
print('=' * 80)
catalyst_df.explain('extended')

# Data-engineering lesson:
# Do not infer execution cost by counting Python method calls. Catalyst plans the
# structured query as a whole and can rewrite equivalent relational expressions.


# =============================================================================
# 5. CORE OPERATORS: SCAN, FILTER, PROJECT
# =============================================================================

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

# PREDICT:
#
#     Scan ExistingRDD / source scan
#         -> Filter
#         -> Project
#
# Read the physical plan from the leaves upward: source rows enter at the scan,
# then filters remove rows, then projection produces the required expressions.

print('\n' + '=' * 80)
print('SCAN / FILTER / PROJECT')
print('=' * 80)
scan_filter_project_df.explain('formatted')

# Operator meanings:
#
# Scan
#     physically reads source rows
#
# Filter
#     applies a row predicate
#
# Project
#     selects AND/OR computes output expressions; it is not merely column removal


# =============================================================================
# 6. HASHAGGREGATE + EXCHANGE: DISTRIBUTED GROUPED AGGREGATION
# =============================================================================

# Disable AQE temporarily so this section shows the static physical strategy
# without adaptive partition coalescing changing the visible structure.
spark.conf.set('spark.sql.adaptive.enabled', 'false')
spark.conf.set('spark.sql.shuffle.partitions', '4')

store_sales_df = (
    sales_enriched_df
    .filter(F.col('order_status') == 'COMPLETED')
    .groupBy('store_id')
    .agg(
        F.sum('gross_sales').alias('gross_sales'),
        F.sum('quantity').alias('units'),
    )
)

# PREDICT BEFORE INSPECTION:
#
#     Scan
#       -> Filter
#       -> partial HashAggregate by store_id
#       -> Exchange hashpartitioning(store_id, 4)
#       -> final HashAggregate by store_id
#
# WHY TWO AGGREGATES?
# The upstream aggregate reduces rows partition-locally before the shuffle. The
# downstream aggregate merges partial states after like keys are redistributed.
#
# WHY EXCHANGE?
# Final state for one store_id requires partial states that can originate in
# several upstream partitions. The downstream aggregate therefore requires rows
# distributed compatibly by store_id.

print('\n' + '=' * 80)
print('HASHAGGREGATE + EXCHANGE')
print('=' * 80)
store_sales_df.explain('formatted')

# Phase 4 bridge:
#
#     groupBy('store_id') is wide
#         -> shuffle required
#         -> stage boundary expected
#
# Phase 5 evidence:
#
#     Exchange hashpartitioning(store_id, ...)
#
# The physical plan gives the concrete operator that satisfies the redistribution
# requirement Phase 4 predicted conceptually.


# =============================================================================
# 7. BROADCASTHASHJOIN: SMALL DIMENSION AS BUILD SIDE
# =============================================================================

# Explicit F.broadcast() makes this teaching example deterministic enough to
# study BroadcastHashJoin itself instead of relying on relation-size estimates.
broadcast_join_df = sales_enriched_df.join(
    F.broadcast(stores_df),
    on='store_id',
    how='inner',
)

# PREDICT:
#
# stores_df is the explicitly broadcast side
#     -> BroadcastExchange
#     -> build hash relation
#
# sales side
#     -> streamed/probe side
#
# final operator family
#     -> BroadcastHashJoin
#
# The important physical advantage is NOT 'broadcast is always faster'. It is:
# the small build side can be replicated, allowing the streamed side to probe a
# local hash relation without two-sided hash repartition + sort preparation.

print('\n' + '=' * 80)
print('BROADCAST HASH JOIN')
print('=' * 80)
broadcast_join_df.explain('formatted')

# In the output, inspect BuildLeft / BuildRight rather than assuming which side
# Spark chose from the visual position alone. The build side is the side whose
# rows become the broadcast hash relation.


# =============================================================================
# 8. SORTMERGEJOIN: DISTRIBUTION + ORDERING REQUIREMENTS
# =============================================================================

# Disable automatic broadcast for this controlled comparison. This is a teaching
# control, not a production tuning recommendation.
spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '-1')
spark.conf.set('spark.sql.adaptive.enabled', 'false')

# The merge hint requests a sort-merge strategy where Spark supports it. Both
# inputs are still ordinary DataFrames, so Spark must satisfy the physical
# distribution and ordering requirements needed by SortMergeJoin.
sort_merge_join_df = (
    sales_enriched_df
    .hint('merge')
    .join(
        stores_df.hint('merge'),
        on='store_id',
        how='inner',
    )
)

# PREDICT:
#
# left input
#     -> Exchange hashpartitioning(store_id, 4)
#     -> Sort by store_id
#
# right input
#     -> Exchange hashpartitioning(store_id, 4)
#     -> Sort by store_id
#
# then
#     -> SortMergeJoin
#
# Exchange satisfies DISTRIBUTION.
# Sort satisfies ORDERING.
# They frequently appear together here, but they solve different requirements.

print('\n' + '=' * 80)
print('SORT MERGE JOIN')
print('=' * 80)
sort_merge_join_df.explain('formatted')


# =============================================================================
# 9. EXCHANGE != SORT
# =============================================================================

# A global orderBy() gives a separate example where ordering itself is the point.
ordered_store_sales_df = store_sales_df.orderBy(
    F.col('gross_sales').desc(),
    F.col('store_id').asc(),
)

# PREDICT:
# - the grouped aggregation already needs redistribution by store_id;
# - global ordering introduces additional physical ordering/distribution work;
# - Exchange and Sort should be explained independently rather than treated as
#   synonyms for 'expensive operation'.

print('\n' + '=' * 80)
print('EXCHANGE VS SORT')
print('=' * 80)
ordered_store_sales_df.explain('formatted')

# Use this causal reading discipline:
#
#     Exchange -> what downstream distribution must be satisfied?
#     Sort     -> what downstream ordering must be satisfied?


# =============================================================================
# 10. AQE: RUNTIME STATISTICS AND POST-SHUFFLE COALESCING
# =============================================================================

# AQE is enabled by default in Spark 4.2.0. We explicitly enable it here because
# earlier sections disabled it to keep static plans stable.
spark.conf.set('spark.sql.adaptive.enabled', 'true')
spark.conf.set('spark.sql.adaptive.coalescePartitions.enabled', 'true')

# Deliberately start with many shuffle partitions for a tiny result. This is only
# to make adaptive coalescing observable; it is NOT a general tuning prescription.
spark.conf.set('spark.sql.shuffle.partitions', '16')

# A larger deterministic fact relation makes the shuffle stage real while the
# final grouped output remains tiny enough for easy inspection.
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

print('\n' + '=' * 80)
print('AQE BEFORE EXECUTION')
print('=' * 80)

# Before an action, Spark has an initial adaptive physical plan but does not yet
# have the runtime shuffle statistics needed for adaptive decisions.
aqe_store_totals_df.explain('formatted')

# Look for an adaptive wrapper such as:
#
#     AdaptiveSparkPlan isFinalPlan=false
#
# Exact plan text can vary. The important meaning is that the physical strategy
# is under AQE and has not yet been finalized from runtime evidence.

# collect() is deliberately safe because the output has only eight store groups.
# The action executes query stages and gives AQE runtime map-output statistics.
aqe_store_totals_rows = aqe_store_totals_df.collect()
assert len(aqe_store_totals_rows) == 8

print('\n' + '=' * 80)
print('AQE AFTER EXECUTION')
print('=' * 80)

# Re-inspect the same DataFrame after execution. Depending on the environment and
# exact plan, look for a finalized adaptive plan and adaptive shuffle readers /
# coalesced partition evidence.
aqe_store_totals_df.explain('formatted')

# Core AQE lesson:
#
#     configured initial shuffle partitions
#         != guaranteed final post-shuffle runtime partition structure
#
# AQE can use runtime map-output sizes to coalesce contiguous small partitions.
# Deep partition sizing and shuffle engineering belong to Phases 6 and 7.


# =============================================================================
# 11. AQE: INITIAL SORT-MERGE PLAN MAY BECOME BROADCAST HASH JOIN
# =============================================================================

# Create a controlled case where static planning is forbidden from broadcasting,
# but AQE is separately allowed to broadcast based on runtime statistics.
# This demonstrates the difference between PLANNING-TIME and RUNTIME evidence.
spark.conf.set('spark.sql.adaptive.enabled', 'true')
spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '-1')
spark.conf.set('spark.sql.adaptive.autoBroadcastJoinThreshold', '10485760')
spark.conf.set('spark.sql.shuffle.partitions', '8')

# Fact side: 5,000 synthetic sales distributed over 20 stores.
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

# Tiny dimension side. Static auto-broadcast is disabled above, so its small size
# cannot cause an initial broadcast join through the normal threshold.
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

aqe_join_df = aqe_fact_df.join(
    aqe_dimension_df,
    on='store_key',
    how='inner',
)

print('\n' + '=' * 80)
print('AQE JOIN BEFORE EXECUTION')
print('=' * 80)

# PREDICT INITIAL STRATEGY:
# With static broadcasting disabled, an equi-join commonly starts with a
# SortMergeJoin plan requiring compatible partitioning and ordering.
aqe_join_df.explain('formatted')

# collect() executes this exact DataFrame, allowing its adaptive QueryExecution
# to be re-inspected afterward. Five thousand rows is deliberately small enough
# for a local teaching experiment and should never be generalized to large data.
aqe_join_rows = aqe_join_df.collect()
assert len(aqe_join_rows) == 5000

print('\n' + '=' * 80)
print('AQE JOIN AFTER EXECUTION')
print('=' * 80)

# Runtime statistics can reveal that the 20-row dimension is below the adaptive
# broadcast threshold. In an eligible plan, AQE can convert the initial
# SortMergeJoin to BroadcastHashJoin and avoid continuing unnecessary sort-merge
# preparation. Exact final text can vary by environment, so inspect rather than
# hard-code an assertion about the operator string.
aqe_join_df.explain('formatted')

# If the plan does not convert in a particular environment, that is still useful
# evidence: compare the actual initial/final plans and determine which assumption
# about statistics, stage materialization, or strategy eligibility did not hold.
# Phase 5 values causal explanation over memorizing one expected text dump.


# =============================================================================
# 12. WORKED PREDICTION-BEFORE-INSPECTION PIPELINE
# =============================================================================

# Return to the small reusable retail relations and use an explicit broadcast so
# the final teaching pipeline has deterministic, interpretable operator families.
spark.conf.set('spark.sql.shuffle.partitions', '4')
spark.conf.set('spark.sql.adaptive.enabled', 'true')

worked_df = (
    sales_enriched_df
    .filter(F.col('order_status') == 'COMPLETED')
    .groupBy('store_id')
    .agg(
        F.sum('gross_sales').alias('gross_sales'),
        F.sum('quantity').alias('units'),
    )
    .join(
        F.broadcast(stores_df),
        on='store_id',
        how='inner',
    )
    .select(
        'store_id',
        'store_name',
        'province',
        'gross_sales',
        'units',
    )
)

# BEFORE INSPECTION, write the prediction in ordinary engineering language:
#
# LOGICAL INTENT
# 1. Keep completed sales.
# 2. Aggregate them to one row per store_id.
# 3. Enrich store-level totals with store attributes.
# 4. Project the final analytical columns.
#
# EXPECTED PHYSICAL REQUIREMENTS
# 1. Read + filter the sales source.
# 2. Perform partition-local partial aggregation.
# 3. Exchange partial states by store_id.
# 4. Finish the store-level aggregation.
# 5. Broadcast the small stores dimension.
# 6. Probe it through BroadcastHashJoin.
# 7. Produce the final projection.
#
# EXPECTED OPERATOR FAMILY
#
#     sales side
#     Scan
#       -> Filter
#       -> HashAggregate partial
#       -> Exchange hashpartitioning(store_id, ...)
#       -> HashAggregate final
#                     \
#                      -> BroadcastHashJoin -> Project
#                     /
#     stores side
#     Scan
#       -> BroadcastExchange

print('\n' + '=' * 80)
print('WORKED PIPELINE: EXTENDED PLAN')
print('=' * 80)
worked_df.explain('extended')

print('\n' + '=' * 80)
print('WORKED PIPELINE: FORMATTED PHYSICAL PLAN')
print('=' * 80)
worked_df.explain('formatted')

# Execute only because AQE/runtime behavior is part of Phase 5. The result is tiny
# and the join should preserve the one-row-per-store grain because stores_df has
# one row per store_id.
worked_rows = worked_df.collect()
assert len(worked_rows) == 3
assert len({row['store_id'] for row in worked_rows}) == 3

print('\n' + '=' * 80)
print('WORKED PIPELINE: POST-EXECUTION ADAPTIVE PLAN')
print('=' * 80)
worked_df.explain('formatted')


# =============================================================================
# 13. PHASE 5 REASONING RULES
# =============================================================================

# Use these rules when reading any real PySpark plan:
#
# 1. Start with semantics and output grain before naming physical operators.
# 2. Separate logical WHAT from physical HOW.
# 3. Use explain('extended') for parsed/analyzed/optimized/physical evolution.
# 4. Use explain('formatted') for physical outline + node details.
# 5. Read physical plans from leaves upward, beginning at the source operators.
# 6. Explain every Exchange by the downstream distribution requirement it serves.
# 7. Explain every Sort by the downstream ordering requirement it serves.
# 8. Expect partial + final aggregation around a shuffle for grouped aggregation.
# 9. Explain join strategy from keys, join type, statistics, hints, and AQE.
# 10. Identify the BroadcastHashJoin build side rather than merely spotting it.
# 11. Never say every join shuffles both inputs.
# 12. Never say broadcast is universally better.
# 13. Distinguish planning-time estimates from runtime AQE statistics.
# 14. AdaptiveSparkPlan does not prove the strategy actually changed.
# 15. Formatted plan-node numbers are not runtime Job/Stage/Task/Partition IDs.
# 16. Do not overfit to one exact printed plan across versions/configurations.
# 17. When prediction and observed plan disagree, investigate the failed assumption.
# 18. Keep storage/file tuning in Phase 6 and deliberate performance tuning in 7.
#
# Repeatable checklist:
#
#     1. What result and grain does the pipeline require?
#     2. What does the parsed plan request?
#     3. What did analysis resolve?
#     4. What did Catalyst simplify or rearrange?
#     5. Which physical operators were selected?
#     6. Where are Scan / Filter / Project nodes?
#     7. Where are partial/final aggregates?
#     8. Where are Exchange nodes, and WHY must data move?
#     9. Where are Sort nodes, and WHY must rows be ordered?
#    10. Which join algorithm was chosen, and WHY?
#    11. What planning evidence supported that choice?
#    12. Is AQE enabled and is the adaptive plan final?
#    13. What changed after execution?
#    14. What runtime evidence explains the change?
#    15. How does this connect back to Phase 4 shuffle/stage reasoning?


# =============================================================================
# 14. PHASE 5 MASTERY REFERENCE — NOT THE MASTERY GATE
# =============================================================================

# The eventual mastery target is NOT merely recognizing operator names.
#
# Given a real pipeline, you should be able to explain:
#
#     source code
#         -> logical intent
#         -> analyzed meaning
#         -> Catalyst logical rewrites
#         -> chosen physical operators
#         -> distribution/order requirements
#         -> join/aggregation strategy
#         -> AQE runtime changes where applicable
#
# Example standard:
#
#     'The first HashAggregate computes partial store-level states locally.
#      The hash-partitioning Exchange redistributes those states so all states
#      for one store_id meet in the same downstream partition. The second
#      HashAggregate finishes each store total. The small stores relation is
#      broadcast and used as the hash build side, allowing the aggregated sales
#      side to probe it without two-sided shuffle-and-sort join preparation.
#      With AQE enabled, eligible shuffle decisions may still change after
#      runtime statistics become available.'
#
# The formal Phase 5 mastery gate is intentionally NOT performed in this file.
# ROADMAP.md must not be updated until mastery is explicitly requested and passed.


# =============================================================================
# 15. CLEANUP
# =============================================================================

# Stop the local teaching application cleanly when this file is run end-to-end.
spark.stop()
