#!/usr/bin/env python
'''PySpark 101 — Phase 7 lecture.

Performance engineering through controlled diagnosis: I/O, file sizing, joins,
shuffle reduction, skew, partition distribution, caching/persistence, AQE, and
resource-pressure concepts.

This is lecture-only code. It reuses Phase 4–6 execution concepts instead of
reteaching them. The governing workflow is:

    observe
        -> hypothesize
        -> inspect
        -> change ONE thing
        -> execute / measure
        -> compare
        -> explain

The first rule is always:

    fix query and data design before tuning configuration

Spark UI deep-dives and production runtime forensics are intentionally deferred
to Phase 8.
'''

from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


# =============================================================================
# 0. SPARK SESSION AND PHASE 7 MENTAL MODEL
# =============================================================================

spark = (
    SparkSession.builder
    .appName('phase_07_pyspark_performance_engineering')
    # Four local worker threads keep partition-level behavior visible while the
    # lecture remains runnable on one machine.
    .master('local[4]')
    # A modest static target makes Exchanges and partition counts easy to inspect.
    # AQE will be toggled deliberately in its own section.
    .config('spark.sql.shuffle.partitions', '12')
    .config('spark.sql.adaptive.enabled', 'false')
    .getOrCreate()
)

# Performance engineering is NOT:
#
#     slow job
#         -> change random Spark settings
#         -> hope
#
# It is:
#
#     required business result + required grain
#         -> observe expensive physical behavior
#         -> form ONE bottleneck hypothesis
#         -> inspect evidence
#         -> change ONE relevant design choice
#         -> execute the SAME workload
#         -> compare before/after
#         -> reconcile correctness
#         -> explain the causal improvement
#
# Priority order:
#
#     1. correctness / grain
#     2. unnecessary I/O
#     3. unnecessary data movement
#     4. bad join strategy
#     5. skew / bad distribution
#     6. unnecessary recomputation
#     7. AQE opportunities
#     8. legitimate resource pressure
#     9. configuration tuning only when evidence justifies it
#
# Core rule:
#
#     FIX QUERY AND DATA DESIGN BEFORE TUNING CONFIGURATION.


# =============================================================================
# 1. DETERMINISTIC RETAIL DATA
# =============================================================================

# Generate enough rows to make partition/file behavior visible without storing a
# large fixture in Git. The grain is one row per synthetic sale line.
fact_sales_df = (
    spark.range(
        start=0,
        end=120000,
        step=1,
        numPartitions=8,
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
                ((F.col('id') % 8000) + F.lit(1)).cast('string'),
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
        ((F.col('id') % 5) + F.lit(1)).cast('int').alias('quantity'),
        (
            F.lit(5.00)
            + ((F.col('id') % 75) * F.lit(0.75))
        )
        .cast(DecimalType(12, 2))
        .alias('unit_price'),
        F.concat(
            F.lit('PROMO_'),
            (F.col('id') % 25).cast('string'),
        ).alias('promotion_code'),
        F.concat(
            F.lit('CHANNEL_'),
            (F.col('id') % 4).cast('string'),
        ).alias('sales_channel'),
        F.concat(
            F.lit('DEVICE_'),
            (F.col('id') % 6).cast('string'),
        ).alias('device_type'),
    )
    .withColumn('year', F.year('order_date'))
    .withColumn('month', F.month('order_date'))
    .withColumn(
        'gross_sales',
        (F.col('quantity') * F.col('unit_price')).cast(DecimalType(16, 2)),
    )
)

# Grain: one row per store_id. This is a classic small dimension candidate.
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

# Grain: one row per product_id. This is another dimension-style relation.
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
        F.concat(
            F.lit('BRAND_'),
            (F.col('id') % 30).cast('string'),
        ).alias('brand'),
    )
)

# Create a deliberately skewed dataset for later sections.
# HOT_STORE owns 70% of rows; the remaining rows spread across 30 cold keys.
skewed_sales_df = (
    spark.range(
        start=0,
        end=100000,
        step=1,
        numPartitions=8,
    )
    .select(
        F.col('id').cast('long').alias('sale_id'),
        F.when(
            F.col('id') < 70000,
            F.lit('HOT_STORE'),
        )
        .otherwise(
            F.concat(
                F.lit('COLD_'),
                F.lpad(
                    ((F.col('id') % 30) + F.lit(1)).cast('string'),
                    2,
                    '0',
                ),
            )
        )
        .alias('store_id'),
        ((F.col('id') % 5) + F.lit(1)).cast('int').alias('quantity'),
        (
            F.lit(10.00)
            + (F.col('id') % 25) * F.lit(0.50)
        )
        .cast(DecimalType(12, 2))
        .alias('unit_price'),
    )
    .withColumn(
        'gross_sales',
        (F.col('quantity') * F.col('unit_price')).cast(DecimalType(16, 2)),
    )
)

# Grain declarations are part of performance work because an optimization that
# changes grain is not valid.
#
# fact_sales_df
# = one row per sale_id
#
# dim_store_df
# = one row per store_id
#
# dim_product_df
# = one row per product_id
#
# skewed_sales_df
# = one row per sale_id


# =============================================================================
# 2. SMALL DIAGNOSTIC HELPERS
# =============================================================================


def print_section(title):
    '''Print a consistent lecture section banner.'''

    print('\n' + '=' * 80)
    print(title)
    print('=' * 80)


def show_partition_summary(df, label):
    '''Print execution-partition count and row distribution.'''

    partition_count = df.rdd.getNumPartitions()

    # Aggregate before collecting so only the small diagnostic summary reaches
    # the driver. Never collect a large raw dataset merely to inspect skew.
    rows_by_partition = (
        df
        .select(F.spark_partition_id().alias('partition_id'))
        .groupBy('partition_id')
        .count()
        .orderBy('partition_id')
        .collect()
    )

    print(f'\n{label}')
    print(f'execution partitions: {partition_count}')
    for row in rows_by_partition:
        print(f'partition {row.partition_id}: {row.count} rows')


def show_key_frequency(df, key_column, label, limit=10):
    '''Print the hottest keys without collecting raw records.'''

    print(f'\n{label}')
    (
        df
        .groupBy(key_column)
        .agg(F.count('*').alias('row_count'))
        .orderBy(
            F.col('row_count').desc(),
            F.col(key_column).asc(),
        )
        .show(limit, truncate=False)
    )


def parquet_data_files(path):
    '''Return physical Parquet data files under a local teaching directory.'''

    return sorted(Path(path).rglob('*.parquet'))


def show_file_summary(path, label, sample_limit=8):
    '''Print local Parquet file count and compact size statistics.'''

    files = parquet_data_files(path)
    sizes = [file_path.stat().st_size for file_path in files]

    print(f'\n{label}')
    print(f'parquet data files: {len(files)}')

    if sizes:
        print(f'min bytes: {min(sizes)}')
        print(f'max bytes: {max(sizes)}')
        print(f'total bytes: {sum(sizes)}')

    for file_path in files[:sample_limit]:
        print(
            f'{file_path.relative_to(path)} '
            f'-> {file_path.stat().st_size} bytes'
        )

    if len(files) > sample_limit:
        print(f'... {len(files) - sample_limit} more files')


def timed_count(df, label):
    '''Materialize a DataFrame with count() and print elapsed wall time.'''

    started_at = perf_counter()
    row_count = df.count()
    elapsed_seconds = perf_counter() - started_at

    print(
        f'{label}: rows={row_count}, '
        f'elapsed={elapsed_seconds:.3f}s'
    )

    return row_count, elapsed_seconds


def reconcile_scalar(df_a, df_b, aggregation_column, label):
    '''Compare one deterministic aggregate between two logically equivalent plans.'''

    # This helper intentionally avoids exact whole-DataFrame comparison for large
    # teaching inputs. The chosen invariant should match the business requirement.
    value_a = (
        df_a
        .agg(F.sum(aggregation_column).alias('value'))
        .first()
        .value
    )
    value_b = (
        df_b
        .agg(F.sum(aggregation_column).alias('value'))
        .first()
        .value
    )

    assert value_a == value_b
    print(f'{label}: reconciled value = {value_a}')


# =============================================================================
# 3. PERFORMANCE WORKFLOW: BASELINE BEFORE OPTIMIZATION
# =============================================================================

print_section('PHASE 7 WORKFLOW')

print(
    'observe -> hypothesize -> inspect -> change ONE thing -> '
    'execute / measure -> compare -> explain'
)

# A useful performance baseline records both business meaning and physical
# evidence. Runtime by itself is too weak because local runs are noisy.
baseline_store_sales_df = (
    fact_sales_df
    .filter(F.col('order_status') == 'COMPLETED')
    .groupBy('store_id')
    .agg(F.sum('gross_sales').alias('gross_sales'))
)

print('\nBASELINE GRAIN: one row per store_id')
print('BASELINE PLAN:')
baseline_store_sales_df.explain('formatted')
print(
    'baseline execution partitions:',
    baseline_store_sales_df.rdd.getNumPartitions(),
)

# The remainder of this file applies the same reasoning discipline to one
# bottleneck class at a time.


# =============================================================================
# 4. I/O: COLUMN PRUNING, PREDICATE PUSHDOWN, PARTITION PRUNING
# =============================================================================

with TemporaryDirectory(prefix='pyspark_phase_07_') as temp_directory:
    temp = Path(temp_directory)

    unpartitioned_path = temp / 'sales_unpartitioned'
    partitioned_path = temp / 'sales_by_year_month'

    # Persist deterministic teaching sources as Parquet so scan-level evidence is
    # visible through ReadSchema, PushedFilters, and PartitionFilters.
    (
        fact_sales_df
        .write
        .mode('overwrite')
        .parquet(str(unpartitioned_path))
    )

    (
        fact_sales_df
        .write
        .mode('overwrite')
        .partitionBy('year', 'month')
        .parquet(str(partitioned_path))
    )

    # -------------------------------------------------------------------------
    # 4.1 Unnecessary wide read vs. narrow required read
    # -------------------------------------------------------------------------

    print_section('I/O: COLUMN PRUNING')

    # Business requirement:
    # February 2026 completed sales by store.
    # Final result needs store_id + gross_sales; filter logic also needs date/status.
    wide_read_df = (
        spark.read.parquet(str(unpartitioned_path))
        .filter(
            (F.col('order_date') >= F.lit('2026-02-01').cast('date'))
            & (F.col('order_date') < F.lit('2026-03-01').cast('date'))
            & (F.col('order_status') == 'COMPLETED')
        )
    )

    narrow_read_df = (
        spark.read.parquet(str(unpartitioned_path))
        .filter(
            (F.col('order_date') >= F.lit('2026-02-01').cast('date'))
            & (F.col('order_date') < F.lit('2026-03-01').cast('date'))
            & (F.col('order_status') == 'COMPLETED')
        )
        .select(
            'store_id',
            'gross_sales',
        )
    )

    # PREDICT:
    # Catalyst should prune unused columns for the narrow branch. Inspect the
    # Parquet scan's ReadSchema instead of trusting source-code appearance.
    print('\nWIDE READ PLAN')
    wide_read_df.explain('formatted')

    print('\nNARROW READ PLAN')
    narrow_read_df.explain('formatted')

    # Performance lesson:
    # If a wide scan feeds a later wide operator, row width also affects how many
    # bytes Spark must serialize, shuffle, sort, and hold in memory.

    # -------------------------------------------------------------------------
    # 4.2 Predicate pushdown
    # -------------------------------------------------------------------------

    print_section('I/O: PREDICATE PUSHDOWN')

    pushed_filter_df = (
        spark.read.parquet(str(unpartitioned_path))
        .filter(
            (F.col('order_status') == 'COMPLETED')
            & (F.col('quantity') >= 4)
        )
        .select(
            'sale_id',
            'store_id',
            'quantity',
            'gross_sales',
        )
    )

    # Inspect PushedFilters. A Spark Filter may still remain for correctness even
    # when the Parquet reader also receives eligible predicates.
    pushed_filter_df.explain('formatted')

    # -------------------------------------------------------------------------
    # 4.3 Partition pruning
    # -------------------------------------------------------------------------

    print_section('I/O: PARTITION PRUNING')

    february_partitioned_df = (
        spark.read.parquet(str(partitioned_path))
        .filter(
            (F.col('year') == 2026)
            & (F.col('month') == 2)
            & (F.col('order_status') == 'COMPLETED')
        )
        .select(
            'store_id',
            'gross_sales',
            'year',
            'month',
        )
    )

    # Inspect PartitionFilters separately from PushedFilters.
    february_partitioned_df.explain('formatted')

    # Diagnostic questions:
    #
    #     ReadSchema
    #         -> am I requesting unnecessary columns?
    #
    #     PushedFilters
    #         -> which row predicates reached Parquet?
    #
    #     PartitionFilters
    #         -> which storage directories can be excluded?
    #
    # These mechanisms reduce different kinds of physical work.

    # -------------------------------------------------------------------------
    # 4.4 Avoid unnecessary historical reads
    # -------------------------------------------------------------------------

    print_section('I/O: AVOID UNNECESSARY HISTORY')

    full_history_completed_df = (
        spark.read.parquet(str(partitioned_path))
        .filter(F.col('order_status') == 'COMPLETED')
        .select('store_id', 'gross_sales')
    )

    february_only_completed_df = (
        spark.read.parquet(str(partitioned_path))
        .filter(
            (F.col('year') == 2026)
            & (F.col('month') == 2)
            & (F.col('order_status') == 'COMPLETED')
        )
        .select('store_id', 'gross_sales')
    )

    print('FULL HISTORY PLAN')
    full_history_completed_df.explain('formatted')

    print('\nFEBRUARY-ONLY PLAN')
    february_only_completed_df.explain('formatted')

    # Do not increase executor memory to compensate for a query that needlessly
    # reads five years of history when the requirement is one day/month.


    # =========================================================================
    # 5. FILE SIZING: FIX THE WRITER, THEN RE-READ
    # =========================================================================

    print_section('FILE SIZING: TOO MANY SMALL FILES')

    fragmented_path = temp / 'fragmented_output'
    compact_path = temp / 'compact_output'

    fragmented_df = fact_sales_df.repartition(64)
    compact_df = fact_sales_df.coalesce(4)

    # Both outputs contain the same logical rows. Only the writer partitioning
    # changes for this controlled experiment.
    (
        fragmented_df
        .write
        .mode('overwrite')
        .parquet(str(fragmented_path))
    )

    (
        compact_df
        .write
        .mode('overwrite')
        .parquet(str(compact_path))
    )

    show_file_summary(
        fragmented_path,
        'FRAGMENTED WRITE FROM 64 OUTPUT PARTITIONS',
    )
    show_file_summary(
        compact_path,
        'COMPACT WRITE FROM 4 OUTPUT PARTITIONS',
    )

    fragmented_read_df = spark.read.parquet(str(fragmented_path))
    compact_read_df = spark.read.parquet(str(compact_path))

    print(
        '\nfragmented next-read input partitions:',
        fragmented_read_df.rdd.getNumPartitions(),
    )
    print(
        'compact next-read input partitions:',
        compact_read_df.rdd.getNumPartitions(),
    )

    # File-count lesson:
    #
    #     too many tiny files
    #         -> listing/open overhead + many tiny scan units
    #
    #     too few huge files
    #         -> reduced useful scan parallelism + large individual tasks
    #
    # There is no universal ideal file size. Fix pathological output layout at the
    # writer rather than permanently compensating with reader configuration.


    # =========================================================================
    # 6. JOIN STRATEGIES: SHUFFLE VS. BROADCAST
    # =========================================================================

    print_section('JOIN OPTIMIZATION: SORT-MERGE VS BROADCAST')

    # Correctness first:
    # fact_sales_df grain = one row per sale_id
    # dim_store_df grain   = one row per store_id
    # relationship         = many fact rows to one dimension row
    # output grain         = one row per sale_id

    dim_store_unique_count = dim_store_df.select('store_id').distinct().count()
    dim_store_row_count = dim_store_df.count()
    assert dim_store_unique_count == dim_store_row_count

    # -------------------------------------------------------------------------
    # 6.1 Baseline: force sort-merge only to create a controlled comparison
    # -------------------------------------------------------------------------

    spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '-1')

    shuffle_join_df = (
        fact_sales_df
        .select(
            'sale_id',
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
            how='left',
        )
    )

    print('BASELINE SHUFFLE JOIN PLAN')
    shuffle_join_df.explain('formatted')

    # Expect Exchange + Sort on the join inputs and SortMergeJoin in this
    # controlled setup. The point is not that SortMergeJoin is bad; it is correct
    # for large-large equi-joins when neither side should broadcast.

    # -------------------------------------------------------------------------
    # 6.2 Change ONE thing: broadcast the proven-small dimension
    # -------------------------------------------------------------------------

    broadcast_join_df = (
        fact_sales_df
        .select(
            'sale_id',
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
            how='left',
        )
    )

    print('\nOPTIMIZED BROADCAST JOIN PLAN')
    broadcast_join_df.explain('formatted')

    # Expect BroadcastExchange + BroadcastHashJoin. The large fact side should
    # not require the ordinary two-sided join repartitioning used by sort-merge.

    # Validate the output grain and one business invariant.
    assert shuffle_join_df.count() == fact_sales_df.count()
    assert broadcast_join_df.count() == fact_sales_df.count()

    reconcile_scalar(
        shuffle_join_df,
        broadcast_join_df,
        'gross_sales',
        'SORT-MERGE VS BROADCAST SALES TOTAL',
    )

    # Restore normal automatic-broadcast planning for later sections.
    spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '10485760')


    # =========================================================================
    # 7. JOIN KEYS, GRAIN, AND PERFORMANCE CORRECTNESS
    # =========================================================================

    print_section('JOIN CORRECTNESS BEFORE JOIN SPEED')

    # Construct a broken dimension with duplicate store_id values. Its physical
    # size is still tiny, but its grain is wrong for a many-to-one fact join.
    duplicated_store_df = dim_store_df.unionByName(
        dim_store_df.filter(F.col('store_id') == 'S001')
    )

    duplicated_key_count = (
        duplicated_store_df
        .groupBy('store_id')
        .count()
        .filter(F.col('count') > 1)
        .count()
    )

    assert duplicated_key_count == 1

    broken_join_df = fact_sales_df.join(
        F.broadcast(duplicated_store_df),
        on='store_id',
        how='left',
    )

    fact_row_count = fact_sales_df.count()
    broken_join_row_count = broken_join_df.count()

    print(f'fact rows: {fact_row_count}')
    print(f'broken joined rows: {broken_join_row_count}')
    assert broken_join_row_count > fact_row_count

    # Engineering rule:
    # A faster BroadcastHashJoin does not rescue a non-unique dimension. Validate
    # business keys and expected cardinality BEFORE judging join performance.

    # Join-key selection is equally semantic. If the real business key is
    # (store_id, product_id), joining only store_id can produce both wrong matches
    # and far more data to process.


    # =========================================================================
    # 8. REDUCE JOIN INPUTS BEFORE NECESSARY DATA MOVEMENT
    # =========================================================================

    print_section('REDUCE ROW WIDTH BEFORE A REQUIRED SHUFFLE')

    spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '-1')

    wide_join_df = (
        fact_sales_df
        .hint('merge')
        .join(
            dim_product_df.hint('merge'),
            on='product_id',
            how='inner',
        )
        .groupBy('category')
        .agg(F.sum('gross_sales').alias('gross_sales'))
    )

    narrow_join_df = (
        fact_sales_df
        .select(
            'product_id',
            'gross_sales',
        )
        .hint('merge')
        .join(
            dim_product_df
            .select(
                'product_id',
                'category',
            )
            .hint('merge'),
            on='product_id',
            how='inner',
        )
        .groupBy('category')
        .agg(F.sum('gross_sales').alias('gross_sales'))
    )

    print('WIDE JOIN PLAN')
    wide_join_df.explain('formatted')

    print('\nNARROW JOIN PLAN')
    narrow_join_df.explain('formatted')

    reconcile_scalar(
        wide_join_df,
        narrow_join_df,
        'gross_sales',
        'WIDE VS NARROW JOIN RESULT',
    )

    # Both correct plans still require redistribution in this forced large-large
    # setup. The optimization is reducing the payload moved through that required
    # shuffle rather than pretending the shuffle can always disappear.

    spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '10485760')


    # =========================================================================
    # 9. UNNECESSARY SHUFFLES AND REDUNDANT REPARTITIONING
    # =========================================================================

    print_section('UNNECESSARY SHUFFLE: REPARTITION BY THE WRONG FUTURE KEY')

    unnecessary_repartition_df = (
        fact_sales_df
        # This shuffle distributes by store_id...
        .repartition(12, 'store_id')
        # ...but the very next wide requirement groups by product_id.
        .groupBy('product_id')
        .agg(F.sum('gross_sales').alias('gross_sales'))
    )

    direct_group_df = (
        fact_sales_df
        .groupBy('product_id')
        .agg(F.sum('gross_sales').alias('gross_sales'))
    )

    print('UNNECESSARY REPARTITION PLAN')
    unnecessary_repartition_df.explain('formatted')

    print('\nDIRECT GROUP PLAN')
    direct_group_df.explain('formatted')

    reconcile_scalar(
        unnecessary_repartition_df,
        direct_group_df,
        'gross_sales',
        'REDUNDANT REPARTITION RECONCILIATION',
    )

    # Inspect Exchanges. The explicit store_id redistribution cannot satisfy the
    # later product_id grouping requirement, so it can create extra data movement.

    # Also avoid cleanup patterns such as:
    #
    #     joined_df.distinct()
    #
    # when duplicates actually come from a broken join grain. Global distinct is
    # both a shuffle and a correctness-smell masker.


    # =========================================================================
    # 10. SKEW AND HOT KEYS
    # =========================================================================

    print_section('SKEW: PROFILE THE KEY BEFORE CHANGING PARTITION COUNTS')

    show_key_frequency(
        skewed_sales_df,
        'store_id',
        'TOP STORE_ID FREQUENCIES',
    )

    skewed_by_store_df = skewed_sales_df.repartition(
        12,
        'store_id',
    )

    show_partition_summary(
        skewed_by_store_df,
        'HASH-PARTITIONED SKEWED SALES',
    )

    # Expected lesson:
    # one hot key still hashes to one bucket. Increasing the number of partitions
    # gives more buckets for the cold keys but does not split an indivisible equal
    # key across those buckets.

    # Diagnose in this order:
    #
    #     1. Is the hot key logically valid?
    #     2. Is it NULL / UNKNOWN / UNMAPPED data that belongs elsewhere?
    #     3. Can rows/columns be reduced before the wide operation?
    #     4. Can a small join side broadcast?
    #     5. Can AQE handle the skewed shuffle partition?
    #     6. Is targeted salting justified?


    # =========================================================================
    # 11. SALTING: TARGETED HOT-KEY AGGREGATION
    # =========================================================================

    print_section('SALTING CONCEPT: TWO-STAGE AGGREGATION')

    unsalted_store_sales_df = (
        skewed_sales_df
        .groupBy('store_id')
        .agg(F.sum('gross_sales').alias('gross_sales'))
    )

    # The salt must vary WITHIN the hot key. Hashing only store_id would assign
    # every HOT_STORE row the same salt and solve nothing.
    salted_input_df = skewed_sales_df.withColumn(
        'salt',
        F.pmod(
            F.xxhash64('sale_id'),
            F.lit(8),
        ),
    )

    salted_partial_df = (
        salted_input_df
        .groupBy(
            'store_id',
            'salt',
        )
        .agg(F.sum('gross_sales').alias('partial_gross_sales'))
    )

    salted_final_df = (
        salted_partial_df
        .groupBy('store_id')
        .agg(F.sum('partial_gross_sales').alias('gross_sales'))
    )

    print('UNSALTED AGGREGATION PLAN')
    unsalted_store_sales_df.explain('formatted')

    print('\nSALTED TWO-STAGE AGGREGATION PLAN')
    salted_final_df.explain('formatted')

    reconcile_scalar(
        unsalted_store_sales_df,
        salted_final_df,
        'gross_sales',
        'UNSALTED VS SALTED TOTAL',
    )

    # Salting intentionally adds complexity and often an extra aggregation stage.
    # It is justified only when a proven hot key creates a larger bottleneck than
    # that added work.
    #
    # For joins, salting is more complex because the matching side must be made
    # compatible with each salt bucket. AQE or broadcast is usually preferable
    # when either can solve the actual problem cleanly.


    # =========================================================================
    # 12. TOO FEW, TOO MANY, AND POORLY DISTRIBUTED PARTITIONS
    # =========================================================================

    print_section('PARTITIONING: COUNT AND BALANCE ARE DIFFERENT PROBLEMS')

    too_few_df = spark.range(
        start=0,
        end=100000,
        step=1,
        numPartitions=1,
    )

    too_many_df = spark.range(
        start=0,
        end=100000,
        step=1,
        numPartitions=200,
    )

    print('too few teaching partitions:', too_few_df.rdd.getNumPartitions())
    print('too many teaching partitions:', too_many_df.rdd.getNumPartitions())

    # Too few can mean:
    #     underused cores + large per-task memory demand
    #
    # Too many can mean:
    #     tiny tasks + scheduler/shuffle/file overhead
    #
    # Poor distribution can exist at ANY seemingly reasonable count.
    show_partition_summary(
        skewed_by_store_df,
        'REASONABLE COUNT, BAD DISTRIBUTION',
    )

    # repartition(count)
    #     -> pay for a new broadly distributed partitioning
    #
    # repartition(count, key)
    #     -> hash-distribute by key; useful only when downstream work benefits and
    #        key frequencies are acceptable
    #
    # coalesce(count)
    #     -> mainly reduce partitions without full rebalance
    #
    # coalesce() is NOT a skew fix because it does not globally redistribute rows.


    # =========================================================================
    # 13. CACHING AND PERSISTENCE
    # =========================================================================

    print_section('CACHE ONLY REUSED EXPENSIVE INTERMEDIATES')

    reused_enriched_df = (
        fact_sales_df
        .filter(F.col('order_status') == 'COMPLETED')
        .select(
            'sale_id',
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
            how='left',
        )
    )

    # Baseline: two independent actions can recompute the same upstream lineage.
    timed_count(
        reused_enriched_df.filter(F.col('province') == 'ON'),
        'UNCACHED ON ACTION',
    )
    timed_count(
        reused_enriched_df.filter(F.col('province') == 'BC'),
        'UNCACHED BC ACTION',
    )

    # Change ONE thing: persist the reduced, reusable intermediate rather than the
    # entire raw fact table.
    reused_enriched_df.persist(StorageLevel.MEMORY_AND_DISK)

    print('storage level after persist:', reused_enriched_df.storageLevel)

    # Caching is lazy. This action intentionally materializes the cache once.
    timed_count(
        reused_enriched_df,
        'CACHE MATERIALIZATION',
    )

    # These actions now have an opportunity to reuse cached data.
    timed_count(
        reused_enriched_df.filter(F.col('province') == 'ON'),
        'CACHED ON REUSE',
    )
    timed_count(
        reused_enriched_df.filter(F.col('province') == 'BC'),
        'CACHED BC REUSE',
    )

    print('\nCACHED PLAN')
    reused_enriched_df.explain('formatted')

    # Release cache when the reuse window ends so storage memory can support other
    # execution work.
    reused_enriched_df.unpersist()

    print('storage level after unpersist:', reused_enriched_df.storageLevel)

    # Do NOT cache when:
    #
    #     DataFrame is used once
    #     lineage is cheap to recompute
    #     dataset is so large it causes eviction/thrashing
    #     a much smaller reduced intermediate would serve the same reuse
    #     cache pressure steals memory from shuffle/sort/aggregation work
    #
    # Storage levels are a trade among memory, disk fallback, representation, and
    # recomputation risk. Phase 7 requires the tradeoff concept, not memorizing
    # every constant.


    # =========================================================================
    # 14. AQE: RUNTIME CORRECTION OF UNCERTAIN PHYSICAL DETAILS
    # =========================================================================

    print_section('AQE: PARTITION COALESCING')

    spark.conf.set('spark.sql.adaptive.enabled', 'true')
    spark.conf.set('spark.sql.adaptive.coalescePartitions.enabled', 'true')
    spark.conf.set('spark.sql.shuffle.partitions', '64')

    aqe_grouped_df = (
        fact_sales_df
        .filter(F.col('order_status') == 'COMPLETED')
        .groupBy('store_id')
        .agg(F.sum('gross_sales').alias('gross_sales'))
    )

    # Initial planning can target many shuffle partitions. AQE can coalesce small
    # runtime shuffle partitions after actual shuffle sizes become known.
    timed_count(
        aqe_grouped_df,
        'AQE GROUPED ACTION',
    )

    print(
        'post-action AQE DataFrame partitions:',
        aqe_grouped_df.rdd.getNumPartitions(),
    )
    aqe_grouped_df.explain('formatted')

    # -------------------------------------------------------------------------
    # 14.1 Runtime join changes
    # -------------------------------------------------------------------------

    print_section('AQE: RUNTIME JOIN CHANGES')

    # Keep the ordinary static threshold disabled, but allow AQE to consider a
    # small runtime build side using adaptive statistics. This is demonstration
    # setup, not a recommended blanket production configuration.
    spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '-1')
    spark.conf.set('spark.sql.adaptive.autoBroadcastJoinThreshold', '10485760')

    aqe_join_df = (
        fact_sales_df
        .select(
            'sale_id',
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
    )

    # Materialize first so the adaptive final plan can use runtime statistics.
    timed_count(
        aqe_join_df,
        'AQE JOIN ACTION',
    )
    aqe_join_df.explain('formatted')

    # Depending on the local runtime and Spark planner details, inspect the final
    # adaptive plan rather than assuming the initial join choice remained final.

    # -------------------------------------------------------------------------
    # 14.2 AQE skew handling concept
    # -------------------------------------------------------------------------

    print_section('AQE: SKEW HANDLING')

    spark.conf.set('spark.sql.adaptive.skewJoin.enabled', 'true')
    # Disable adaptive broadcasting only for this controlled skew-join example so
    # the join must create shuffle partitions that AQE can inspect for skew.
    spark.conf.set('spark.sql.adaptive.autoBroadcastJoinThreshold', '-1')

    # Build a small lookup relation for the skewed store keys.
    skewed_store_dimension_df = (
        skewed_sales_df
        .select('store_id')
        .distinct()
        .withColumn('region', F.lit('REGION_1'))
    )

    aqe_skew_join_df = (
        skewed_sales_df
        .hint('merge')
        .join(
            skewed_store_dimension_df.hint('merge'),
            on='store_id',
            how='inner',
        )
        .groupBy('region')
        .agg(F.sum('gross_sales').alias('gross_sales'))
    )

    timed_count(
        aqe_skew_join_df,
        'AQE SKEWED JOIN ACTION',
    )
    aqe_skew_join_df.explain('formatted')

    # AQE can split sufficiently skewed shuffle partitions when runtime size
    # thresholds are met. This small local example is mainly for plan inspection;
    # whether Spark actually labels/splits a partition depends on measured sizes
    # and configured thresholds.
    #
    # Crucially, AQE does NOT fix:
    #
    #     wrong join keys
    #     broken grain
    #     unnecessary historical scans
    #     pathological small-file production
    #     caching everything
    #
    # AQE complements good design; it does not replace it.

    # Restore ordinary defaults used by the rest of the lecture.
    spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '10485760')
    spark.conf.set('spark.sql.shuffle.partitions', '12')
    spark.conf.set('spark.sql.adaptive.enabled', 'false')


    # =========================================================================
    # 15. RESOURCE CONCEPTS: ONLY AFTER QUERY/DATA DESIGN
    # =========================================================================

    print_section('RESOURCE CONCEPTS AFTER QUERY AND DATA DESIGN')

    # These concepts are intentionally described, not tuned experimentally here.
    # Phase 8 will provide richer runtime evidence for task/executor diagnosis.

    print('EXECUTOR MEMORY')
    print(
        'Supports task working data, execution structures, and cached data. '
        'Pressure grows with large partitions, wide rows, sorts, aggregations, '
        'broadcasts, caches, and concurrent memory-heavy tasks.'
    )

    print('\nEXECUTOR CORES')
    print(
        'Controls task concurrency per executor. More cores can increase '
        'parallel work, but can also increase simultaneous memory pressure.'
    )

    print('\nDRIVER MEMORY')
    print(
        'Supports driver-side coordination and collected results. collect() or '
        'toPandas() on large data can turn distributed work into a driver OOM.'
    )

    print('\nGARBAGE COLLECTION')
    print(
        'Heavy GC means the JVM is spending substantial time reclaiming memory. '
        'First ask which workload pattern is creating the pressure.'
    )

    print('\nSPILL')
    print(
        'Sorts, shuffles, and aggregations may spill intermediate state to disk '
        'when memory is insufficient. Spill is a safety mechanism; excessive '
        'spill is a symptom to diagnose.'
    )

    print('\nMEMORY PRESSURE CHECKLIST')
    print('Which operator needs the memory?')
    print('Is one partition disproportionately large?')
    print('Is a broadcast side too large?')
    print('Is cache stealing execution memory?')
    print('Are too many heavy tasks concurrent?')
    print('Can rows or columns be reduced before the expensive operator?')

    # Bad reasoning:
    #
    #     OOM -> double executor memory
    #
    # Better reasoning:
    #
    #     OOM
    #         -> driver or executor?
    #         -> which operator / partition?
    #         -> skew / broadcast / cache / shuffle volume?
    #         -> reduce unnecessary work if possible
    #         -> only then resize legitimate resources if still required


    # =========================================================================
    # 16. CONTROLLED BEFORE/AFTER PERFORMANCE EXPERIMENT
    # =========================================================================

    print_section('CONTROLLED BEFORE / AFTER EXPERIMENT')

    # Business requirement:
    # completed sales by province.
    # Output grain = one row per province.
    #
    # Baseline hypothesis:
    # the small store dimension is being shuffled unnecessarily because broadcast
    # is disabled in the baseline.

    spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '-1')

    baseline_province_sales_df = (
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

    print('BASELINE PLAN')
    baseline_province_sales_df.explain('formatted')

    baseline_count, baseline_seconds = timed_count(
        baseline_province_sales_df,
        'BASELINE MATERIALIZATION',
    )

    # Change ONE thing: broadcast the validated-small dimension.
    optimized_province_sales_df = (
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

    print('\nOPTIMIZED PLAN')
    optimized_province_sales_df.explain('formatted')

    optimized_count, optimized_seconds = timed_count(
        optimized_province_sales_df,
        'OPTIMIZED MATERIALIZATION',
    )

    assert baseline_count == optimized_count

    reconcile_scalar(
        baseline_province_sales_df,
        optimized_province_sales_df,
        'gross_sales',
        'BASELINE VS OPTIMIZED BUSINESS RESULT',
    )

    print('\nCONTROLLED COMPARISON')
    print(f'baseline seconds: {baseline_seconds:.3f}')
    print(f'optimized seconds: {optimized_seconds:.3f}')
    print(
        'Do not overfit to one local wall-clock result. The stronger evidence is '
        'the physical-plan change plus correctness reconciliation.'
    )

    spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '10485760')


# =============================================================================
# 17. PHASE 7 ENGINEERING RULES
# =============================================================================

# 1. Preserve schema, grain, and business correctness before optimizing speed.
# 2. Fix query and data design before tuning Spark configuration.
# 3. State one concrete bottleneck hypothesis at a time.
# 4. Use physical plans as evidence; source-code order is not physical execution.
# 5. Column pruning reduces unnecessary column I/O and can narrow shuffle payloads.
# 6. Predicate pushdown is a source capability to INSPECT, not assume.
# 7. Partition pruning excludes irrelevant storage directories/files.
# 8. Avoid reading irrelevant history when the business requirement is selective.
# 9. Fix pathological file layouts at the writer when possible.
# 10. Do not optimize for the fewest files; preserve useful future parallelism.
# 11. Broadcast only relations that are genuinely small enough to replicate safely.
# 12. A dimension label does not guarantee broadcast safety or key uniqueness.
# 13. Validate expected join cardinality before judging join performance.
# 14. Choose join keys from business semantics, not from the cheapest-looking plan.
# 15. SortMergeJoin can be correct and scalable for two large equi-join inputs.
# 16. Remove unnecessary shuffles, not necessary distributed coordination.
# 17. Reduce rows and row width before necessary wide operators when semantics allow.
# 18. Do not repartition before every join or aggregation by habit.
# 19. Explain what each Exchange distributes and why the downstream operator needs it.
# 20. Partition count and partition balance are different diagnostic dimensions.
# 21. Profile key frequencies before repartitioning by key.
# 22. Ordinary hash repartitioning does not split one hot key across buckets.
# 23. Treat NULL / UNKNOWN hot keys as a data-quality/business question first.
# 24. Use salting only for proven hot-key bottlenecks after simpler fixes fail.
# 25. A salt must vary within the hot key; hashing only the hot key solves nothing.
# 26. Remove salt logically through the required second-stage aggregation/join logic.
# 27. Use repartition() only when paying for a new distribution has downstream value.
# 28. Use coalesce() mainly to reduce partitions without a full rebalance.
# 29. coalesce() is not a skew-remediation technique.
# 30. Cache only reused expensive intermediates.
# 31. Prefer caching a reduced reusable result over a huge raw source.
# 32. Cache materialization is an action and must be accounted for in benchmarks.
# 33. Unpersist when reuse ends.
# 34. AQE can coalesce partitions, adapt joins, and mitigate runtime skew.
# 35. AQE complements engineering; it does not repair wrong business logic.
# 36. Spill and heavy GC are symptoms whose physical cause should be identified.
# 37. Driver-memory problems and executor-memory problems are different diagnoses.
# 38. More executor cores can increase concurrent memory pressure.
# 39. Change ONE thing per performance experiment.
# 40. Use the same materializing workload before and after.
# 41. Control cache/warm-state differences when timing.
# 42. Local elapsed time is supporting evidence, not the whole diagnosis.
# 43. Reconcile business outputs after every meaningful optimization.
# 44. The best first optimization is the one that removes the most unjustified work.

# Repeatable diagnostic checklist:
#
#     1. What is the required output grain?
#     2. What invariants must remain true?
#     3. Which columns/files/rows does Spark actually read?
#     4. Are pruning and pushdown occurring?
#     5. Is file layout creating avoidable overhead?
#     6. Where are the Exchanges, and why does each exist?
#     7. What join strategy is used?
#     8. Are join keys correct and unique where expected?
#     9. Can join inputs be reduced safely?
#    10. Can one side broadcast safely?
#    11. How many execution partitions exist at important boundaries?
#    12. Are those partitions balanced?
#    13. Which keys are hottest?
#    14. Is repartitioning helping or merely adding another shuffle?
#    15. Is the same expensive lineage reused?
#    16. Would caching save more work than it costs?
#    17. What is AQE changing at runtime?
#    18. Is resource pressure caused by a query/data problem first?
#    19. What ONE change has the strongest evidence behind it?
#    20. Did physical work become cheaper?
#    21. Did the business result remain correct?


# =============================================================================
# 18. PHASE 7 FINAL MENTAL MODEL
# =============================================================================

# A professional explanation should sound like:
#
#     Bottleneck:
#         the large fact/small dimension join performs a two-sided shuffle
#
#     Evidence:
#         Exchange + Sort on both sides followed by SortMergeJoin
#
#     Change:
#         broadcast the validated-small dimension
#
#     Before -> after:
#         SortMergeJoin -> BroadcastHashJoin
#         large fact-side ordinary join shuffle removed
#
#     Why:
#         replicating the tiny build side is cheaper than redistributing the
#         large fact side by join key
#
#     Correctness:
#         dimension key is unique, output grain is unchanged, row count and
#         business sales total reconcile exactly
#
# Another valid conclusion can be:
#
#     no join-strategy change is justified
#
# because both sides are large, the SortMergeJoin is appropriate, the Exchanges
# are required, and the best first optimization is instead to prune unused
# columns before that necessary shuffle.
#
# That ability to choose NOT to tune is also performance engineering.


spark.stop()
