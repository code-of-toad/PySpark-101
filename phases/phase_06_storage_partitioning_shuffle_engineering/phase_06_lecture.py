#!/usr/bin/env python
'''PySpark 101 — Phase 6 lecture.

Storage, Parquet, file pruning, Spark partition types, repartition/coalesce,
shuffle behavior, output files, and the relationship between storage layout and
physical execution.

This is lecture-only code. It uses deterministic retail-style data and temporary
local Parquet datasets so storage and partition claims can be predicted before
inspection. Deep performance tuning, skew remediation, and Spark UI diagnosis
are intentionally deferred to Phases 7 and 8.
'''

from pathlib import Path
from tempfile import TemporaryDirectory

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


# =============================================================================
# 0. SPARK SESSION AND PHASE 6 MENTAL MODEL
# =============================================================================

spark = (
    SparkSession.builder
    .appName('phase_06_storage_partitioning_shuffle_engineering')
    # Four local worker threads preserve visible task-level parallelism while
    # keeping the lecture runnable on one machine.
    .master('local[4]')
    # Keep static shuffle examples deterministic. AQE is intentionally disabled
    # here because adaptive coalescing can change the post-shuffle partition
    # count; AQE itself was already taught in Phase 5.
    .config('spark.sql.shuffle.partitions', '8')
    .config('spark.sql.adaptive.enabled', 'false')
    .getOrCreate()
)

# Phase 6 asks how PHYSICAL STORAGE becomes DISTRIBUTED EXECUTION and how that
# execution creates the next physical storage layout.
#
#     physical files / storage directories
#                 |
#                 v
#     pruning + file-scan planning
#                 |
#                 v
#          input partitions
#                 |
#                 v
#      DataFrame transformations
#                 |
#                 | wide requirement
#                 v
#        Exchange / shuffle
#                 |
#                 v
#         shuffle partitions
#                 |
#                 v
#        output partitions
#                 |
#                 v
#       output files/directories
#
# NEVER use the word 'partition' without identifying the layer:
#
#     storage partition
#     input partition
#     current DataFrame partition
#     shuffle partition
#     output partition
#
# A Parquet row group is another physical grouping, but it is NOT a Spark
# execution partition.
#
# Core workflow:
#
#     predict partition/storage behavior
#         -> inspect
#         -> execute
#         -> compare before/after
#         -> explain WHY


# =============================================================================
# 1. DETERMINISTIC RETAIL DATA
# =============================================================================

# Explicit schemas keep the examples aligned with production data-engineering
# practice and make Parquet schema preservation easy to verify later.
sales_schema = StructType([
    StructField('sale_id', IntegerType(), False),
    StructField('order_date', DateType(), False),
    StructField('store_id', StringType(), False),
    StructField('product_id', StringType(), False),
    StructField('order_status', StringType(), False),
    StructField('quantity', IntegerType(), False),
    StructField('unit_price', DecimalType(12, 2), False),
])

# Create deterministic source rows through Spark expressions so the lecture can
# generate enough data to produce multiple physical files without storing a
# large fixture in the repository.
sales_df = (
    spark.range(
        start=0,
        end=12000,
        step=1,
        numPartitions=4,
    )
    .select(
        F.col('id').cast('int').alias('sale_id'),
        F.date_add(
            F.lit('2026-01-01').cast('date'),
            (F.col('id') % 90).cast('int'),
        ).alias('order_date'),
        F.concat(
            F.lit('S'),
            F.lpad(
                ((F.col('id') % 12) + F.lit(1)).cast('string'),
                2,
                '0',
            ),
        ).alias('store_id'),
        F.concat(
            F.lit('P'),
            F.lpad(
                ((F.col('id') % 40) + F.lit(1)).cast('string'),
                3,
                '0',
            ),
        ).alias('product_id'),
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
            + ((F.col('id') % 25) * F.lit(1.25))
        )
        .cast(DecimalType(12, 2))
        .alias('unit_price'),
    )
)

# Validate the generated schema against the intended business schema. Spark's
# generated nullability can differ from a handwritten schema, so compare field
# names/data types rather than requiring byte-for-byte schema equality.
expected_types = {
    field.name: field.dataType.simpleString()
    for field in sales_schema.fields
}
actual_types = {
    field.name: field.dataType.simpleString()
    for field in sales_df.schema.fields
}
assert actual_types == expected_types

# Derived columns support both storage-partition examples and business measures.
sales_enriched_df = (
    sales_df
    .withColumn('year', F.year('order_date'))
    .withColumn('month', F.month('order_date'))
    .withColumn(
        'gross_sales',
        (F.col('quantity') * F.col('unit_price')).cast(DecimalType(16, 2)),
    )
)

stores_df = (
    spark.range(
        start=1,
        end=13,
        step=1,
        numPartitions=2,
    )
    .select(
        F.concat(
            F.lit('S'),
            F.lpad(F.col('id').cast('string'), 2, '0'),
        ).alias('store_id'),
        F.concat(
            F.lit('Store '),
            F.col('id').cast('string'),
        ).alias('store_name'),
        F.when(F.col('id') <= 6, F.lit('ON'))
        .otherwise(F.lit('BC'))
        .alias('province'),
    )
)

# Grain remains part of every engineering explanation.
#
# sales_enriched_df
# = one row per synthetic sale line
#
# stores_df
# = one row per store_id


# =============================================================================
# 2. SMALL HELPERS FOR PARTITION AND FILE INSPECTION
# =============================================================================


def show_partition_summary(df, label):
    '''Print current execution-partition count and row distribution.'''

    # getNumPartitions() describes the current Spark execution partitioning, not
    # storage-directory partitioning.
    partition_count = df.rdd.getNumPartitions()

    # This aggregation is intentionally used only on teaching-scale DataFrames.
    # spark_partition_id() lets us see how many rows each execution partition owns.
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
        print(
            f'partition {row.partition_id}: '
            f'{row.count} rows'
        )


def parquet_data_files(path):
    '''Return physical Parquet data files under a local teaching directory.'''

    # Spark also writes metadata such as _SUCCESS. Restrict inspection to
    # physical Parquet data files so file-count reasoning stays precise.
    return sorted(Path(path).rglob('*.parquet'))


def show_parquet_files(path, label):
    '''Print Parquet file count and local file sizes for a teaching dataset.'''

    files = parquet_data_files(path)

    print(f'\n{label}')
    print(f'parquet data files: {len(files)}')

    for file_path in files:
        relative_path = file_path.relative_to(path)
        print(f'{relative_path} -> {file_path.stat().st_size} bytes')


# Baseline: range(..., numPartitions=4) created four execution partitions.
show_partition_summary(
    sales_enriched_df,
    'BASELINE SALES DATAFRAME',
)


# =============================================================================
# 3. PARQUET: COLUMNAR STORAGE, COMPRESSION, AND SCHEMA PRESERVATION
# =============================================================================

with TemporaryDirectory(prefix='pyspark_phase_06_') as temp_directory:
    temp = Path(temp_directory)

    parquet_path = temp / 'sales_unpartitioned'

    # The current DataFrame has four execution partitions. For this simple
    # unpartitioned write, expect roughly one non-empty Parquet data file per
    # writer task / output execution partition.
    print('\n' + '=' * 80)
    print('PARQUET WRITE: PREDICT FILE COUNT FROM OUTPUT PARTITIONS')
    print('=' * 80)
    print(
        'before write execution partitions:',
        sales_enriched_df.rdd.getNumPartitions(),
    )

    (
        sales_enriched_df
        .write
        .mode('overwrite')
        .parquet(str(parquet_path))
    )

    show_parquet_files(
        parquet_path,
        'UNPARTITIONED PARQUET OUTPUT',
    )

    # Spark 4.2.0 uses Snappy as the default Parquet write compression codec.
    # Compression reduces bytes stored/read; codec tuning itself belongs later.
    print(
        'Parquet compression codec:',
        spark.conf.get('spark.sql.parquet.compression.codec'),
    )
    print(
        'Parquet filter pushdown enabled:',
        spark.conf.get('spark.sql.parquet.filterPushdown'),
    )

    # Parquet preserves typed schema metadata. Reading the dataset does not need
    # CSV-style type inference to rediscover integers, dates, and decimals.
    parquet_sales_df = spark.read.parquet(str(parquet_path))

    print('\nRELOADED PARQUET SCHEMA')
    parquet_sales_df.printSchema()

    # Verify the important business types survived the round trip.
    parquet_types = {
        field.name: field.dataType.simpleString()
        for field in parquet_sales_df.schema.fields
    }
    assert parquet_types['sale_id'] == 'int'
    assert parquet_types['order_date'] == 'date'
    assert parquet_types['quantity'] == 'int'
    assert parquet_types['unit_price'] == 'decimal(12,2)'
    assert parquet_types['gross_sales'] == 'decimal(16,2)'

    # Physical lesson:
    # Parquet is columnar and compressed by default in ordinary Spark setups.
    # The important Phase 6 claim is not 'Parquet is always fastest'; it is that
    # the file format exposes typed column chunks + metadata that Spark can use to
    # avoid reading physical data irrelevant to an analytical query.


    # =========================================================================
    # 4. COLUMN PRUNING + PREDICATE PUSHDOWN
    # =========================================================================

    scan_pruning_df = (
        spark.read.parquet(str(parquet_path))
        .filter(
            (F.col('order_status') == 'COMPLETED')
            & (F.col('quantity') >= 4)
        )
        .select(
            'store_id',
            'product_id',
            'quantity',
            'gross_sales',
        )
    )

    # PREDICT BEFORE INSPECTION:
    #
    # COLUMN PRUNING
    # - Spark should need only columns required by the filter and final projection.
    # - FileScan parquet should expose a reduced ReadSchema.
    #
    # PREDICATE PUSHDOWN
    # - eligible equality/range predicates should appear in PushedFilters.
    # - a Spark Filter can still remain for correctness/residual evaluation.
    #
    # PARQUET STATISTICS
    # - pushed predicates may allow the reader to skip internal row groups whose
    #   min/max metadata proves no row can match.
    print('\n' + '=' * 80)
    print('COLUMN PRUNING + PREDICATE PUSHDOWN')
    print('=' * 80)
    scan_pruning_df.explain('formatted')

    # Inspect scan evidence, not assumptions:
    #
    #     ReadSchema
    #         -> which physical Parquet columns are requested?
    #
    #     PushedFilters
    #         -> which eligible predicates reached the data-source reader?
    #
    #     DataFilters
    #         -> which row predicates Spark associates with the file scan?
    #
    # These are separate from storage directory PartitionFilters, shown next.


    # =========================================================================
    # 5. STORAGE PARTITIONING + PARTITION PRUNING
    # =========================================================================

    partitioned_sales_path = temp / 'sales_partitioned_by_year_month'

    # partitionBy() creates directory-based STORAGE partitions. It does NOT mean
    # the resulting DataFrame now has one Spark execution partition per month.
    (
        sales_enriched_df
        .write
        .mode('overwrite')
        .partitionBy('year', 'month')
        .parquet(str(partitioned_sales_path))
    )

    print('\n' + '=' * 80)
    print('STORAGE PARTITION DIRECTORY LAYOUT')
    print('=' * 80)

    # Show only the directory values. A real data lake may contain many files in
    # each value directory.
    storage_partition_directories = sorted(
        path.relative_to(partitioned_sales_path)
        for path in partitioned_sales_path.glob('year=*/month=*')
    )
    for directory in storage_partition_directories:
        print(directory)

    # Query only February 2026 and COMPLETED rows. year/month are storage
    # partition columns, while order_status is an ordinary Parquet data column.
    february_completed_df = (
        spark.read.parquet(str(partitioned_sales_path))
        .filter(
            (F.col('year') == 2026)
            & (F.col('month') == 2)
            & (F.col('order_status') == 'COMPLETED')
        )
        .select(
            'store_id',
            'product_id',
            'order_status',
            'quantity',
            'gross_sales',
            'year',
            'month',
        )
    )

    # PREDICT:
    #
    # PartitionFilters
    # - year = 2026
    # - month = 2
    # - should let Spark exclude January/March storage directories
    #
    # PushedFilters
    # - order_status = COMPLETED may be delegated to the Parquet reader
    #
    # ReadSchema
    # - should not need every original source column
    print('\n' + '=' * 80)
    print('PARTITION PRUNING + PARQUET SCAN EVIDENCE')
    print('=' * 80)
    february_completed_df.explain('formatted')

    # Critical distinction:
    #
    #     storage partition pruning
    #         -> which directories/files are candidates?
    #
    #     predicate pushdown
    #         -> which row predicates can the Parquet reader use?
    #
    #     column pruning
    #         -> which column chunks need to be read?
    #
    # A single query can benefit from all three simultaneously.


    # =========================================================================
    # 6. STORAGE PARTITIONS != INPUT / DATAFRAME PARTITIONS
    # =========================================================================

    print('\n' + '=' * 80)
    print('STORAGE PARTITIONS VS SPARK EXECUTION PARTITIONS')
    print('=' * 80)

    print(
        'storage partition directories:',
        len(storage_partition_directories),
    )
    print(
        'full partitioned read execution partitions:',
        spark.read.parquet(str(partitioned_sales_path)).rdd.getNumPartitions(),
    )
    print(
        'filtered February DataFrame execution partitions:',
        february_completed_df.rdd.getNumPartitions(),
    )

    # There is no requirement that these counts match.
    #
    # Storage directories answer:
    #     WHERE ARE FILES ORGANIZED?
    #
    # Input/DataFrame partitions answer:
    #     HOW IS THE SELECTED PHYSICAL INPUT DIVIDED INTO TASK-SIZED WORK?
    #
    # Spark file-scan planning can split large files and pack multiple small files
    # into an input partition. Therefore:
    #
    #     one file != guaranteed one input partition
    #     one storage directory != one execution partition

    # Spark 4.2.0 file-based scan planning is influenced by settings such as:
    #
    #     spark.sql.files.maxPartitionBytes
    #     spark.sql.files.openCostInBytes
    #
    # We only inspect them here. Tuning them belongs to later performance work.
    print(
        'spark.sql.files.maxPartitionBytes:',
        spark.conf.get('spark.sql.files.maxPartitionBytes'),
    )
    print(
        'spark.sql.files.openCostInBytes:',
        spark.conf.get('spark.sql.files.openCostInBytes'),
    )


    # =========================================================================
    # 7. NARROW FILTERS CAN REMOVE ROWS WITHOUT REDUCING PARTITION COUNT
    # =========================================================================

    narrow_source_df = spark.range(
        start=0,
        end=10000,
        step=1,
        numPartitions=8,
    )

    very_small_filtered_df = narrow_source_df.filter(
        F.col('id') < 10
    )

    print('\n' + '=' * 80)
    print('ROW COUNT != PARTITION COUNT')
    print('=' * 80)
    print('source partitions:', narrow_source_df.rdd.getNumPartitions())
    print('filtered partitions:', very_small_filtered_df.rdd.getNumPartitions())
    print('filtered rows:', very_small_filtered_df.count())

    # PREDICTION:
    # filter() is narrow. It can reduce 10,000 rows to 10 rows while retaining the
    # existing eight-partition topology. Some partitions can become empty.
    #
    # Therefore:
    #
    #     less data per partition
    #         !=
    #     fewer partitions


    # =========================================================================
    # 8. REPARTITION(): NEW DISTRIBUTION + SHUFFLE
    # =========================================================================

    repartition_source_df = spark.range(
        start=0,
        end=12000,
        step=1,
        numPartitions=4,
    )

    repartitioned_df = repartition_source_df.repartition(8)

    print('\n' + '=' * 80)
    print('REPARTITION TO EXPLICIT COUNT')
    print('=' * 80)
    print(
        'before repartition:',
        repartition_source_df.rdd.getNumPartitions(),
    )
    print(
        'after repartition(8):',
        repartitioned_df.rdd.getNumPartitions(),
    )

    # repartition() requests a new distribution. Expect Exchange evidence in the
    # physical plan because rows must be redistributed.
    repartitioned_df.explain('formatted')

    show_partition_summary(
        repartitioned_df,
        'REPARTITIONED ROW DISTRIBUTION',
    )

    # Key rule:
    #
    #     repartition()
    #     = change execution partitioning through redistribution
    #
    # This is unrelated to storage writer partitionBy() unless a later write uses
    # the repartitioned DataFrame.


    # =========================================================================
    # 9. COALESCE(): SHRINK THROUGH A NARROW DEPENDENCY
    # =========================================================================

    coalesce_source_df = spark.range(
        start=0,
        end=12000,
        step=1,
        numPartitions=8,
    )

    coalesced_df = coalesce_source_df.coalesce(3)

    print('\n' + '=' * 80)
    print('COALESCE TO FEWER PARTITIONS')
    print('=' * 80)
    print('before coalesce:', coalesce_source_df.rdd.getNumPartitions())
    print('after coalesce(3):', coalesced_df.rdd.getNumPartitions())

    # coalesce() shrinking uses a narrow dependency rather than full global
    # redistribution. Inspect the plan and compare with repartition().
    coalesced_df.explain('formatted')

    show_partition_summary(
        coalesced_df,
        'COALESCED ROW DISTRIBUTION',
    )

    # Asking coalesce() for more partitions does not create new parallelism.
    coalesce_attempt_to_increase_df = coalesced_df.coalesce(10)
    print(
        'coalesce(3) then coalesce(10):',
        coalesce_attempt_to_increase_df.rdd.getNumPartitions(),
    )

    # Decision rule:
    #
    #     need more partitions / balanced new distribution?
    #         -> repartition()
    #
    #     need fewer partitions and current distribution is acceptable?
    #         -> coalesce()


    # =========================================================================
    # 10. REPARTITION BY KEY + DATA DISTRIBUTION
    # =========================================================================

    sales_by_store_df = sales_enriched_df.repartition(
        8,
        'store_id',
    )

    print('\n' + '=' * 80)
    print('REPARTITION BY STORE_ID')
    print('=' * 80)
    sales_by_store_df.explain('formatted')

    # Show which partition IDs receive each key. Because hash partitioning routes
    # equal key values to the same hash bucket, one store_id should not be spread
    # across multiple resulting partitions for this repartition expression.
    store_partition_map_df = (
        sales_by_store_df
        .select(
            'store_id',
            F.spark_partition_id().alias('partition_id'),
        )
        .distinct()
        .orderBy('store_id')
    )

    print('\nSTORE -> EXECUTION PARTITION AFTER KEY REPARTITION')
    store_partition_map_df.show(20, truncate=False)

    # IMPORTANT:
    # repartition(8, 'store_id') does not guarantee all eight buckets are non-empty.
    # With only 12 distinct store keys, hash collisions can leave some partitions
    # empty while multiple store keys share another partition.


    # =========================================================================
    # 11. KEY REPARTITIONING DOES NOT SOLVE HOT-KEY SKEW
    # =========================================================================

    # Build one deliberately skewed business key: HOT receives 80% of rows.
    skewed_df = (
        spark.range(
            start=0,
            end=10000,
            step=1,
            numPartitions=4,
        )
        .select(
            F.when(
                F.col('id') < 8000,
                F.lit('HOT'),
            )
            .otherwise(
                F.concat(
                    F.lit('COLD_'),
                    (F.col('id') % 20).cast('string'),
                )
            )
            .alias('business_key')
        )
    )

    skewed_by_key_df = skewed_df.repartition(
        8,
        'business_key',
    )

    print('\n' + '=' * 80)
    print('HOT-KEY REPARTITIONING')
    print('=' * 80)
    show_partition_summary(
        skewed_by_key_df,
        'SKEWED HASH-PARTITION DISTRIBUTION',
    )

    # Engineering lesson:
    # hash partitioning places equal keys together. If one key owns 80% of rows,
    # the partition receiving that key can still own roughly 80% of the data.
    # Phase 6 recognizes the distribution problem; Phase 7 studies remediation.


    # =========================================================================
    # 12. EXCESSIVE VS. INSUFFICIENT PARTITIONING
    # =========================================================================

    insufficient_df = spark.range(
        start=0,
        end=100000,
        step=1,
        numPartitions=1,
    )

    excessive_df = spark.range(
        start=0,
        end=100000,
        step=1,
        numPartitions=200,
    )

    print('\n' + '=' * 80)
    print('EXCESSIVE VS INSUFFICIENT PARTITIONING')
    print('=' * 80)
    print(
        'insufficient example partitions:',
        insufficient_df.rdd.getNumPartitions(),
    )
    print(
        'excessive teaching example partitions:',
        excessive_df.rdd.getNumPartitions(),
    )

    # Do not derive a universal 'correct partition count' from this example.
    # Partition count must be judged with:
    #
    #     bytes per partition
    #     rows per partition
    #     key skew
    #     available cluster parallelism
    #     downstream operation
    #
    # Too few:
    #     underused cores + large tasks
    #
    # Too many:
    #     tiny tasks + scheduler/shuffle/file overhead


    # =========================================================================
    # 13. WHY COMMON OPERATIONS SHUFFLE
    # =========================================================================

    spark.conf.set('spark.sql.shuffle.partitions', '8')
    spark.conf.set('spark.sql.adaptive.enabled', 'false')

    # -------------------------------------------------------------------------
    # 13.1 groupBy(): equal grouping keys must meet
    # -------------------------------------------------------------------------

    grouped_df = (
        sales_enriched_df
        .groupBy('store_id')
        .agg(
            F.sum('gross_sales').alias('gross_sales'),
            F.sum('quantity').alias('units'),
        )
    )

    print('\n' + '=' * 80)
    print('SHUFFLE: GROUP BY')
    print('=' * 80)
    grouped_df.explain('formatted')
    print('grouped execution partitions:', grouped_df.rdd.getNumPartitions())

    # Expected causal story:
    # partial aggregate states for S01 can originate in several input partitions;
    # the final aggregate needs those states colocated, so Spark hash-shuffles by
    # store_id into the configured shuffle partition target.

    # -------------------------------------------------------------------------
    # 13.2 distinct(): equal values must meet for global deduplication
    # -------------------------------------------------------------------------

    distinct_products_df = sales_enriched_df.select(
        'product_id'
    ).distinct()

    print('\n' + '=' * 80)
    print('SHUFFLE: DISTINCT')
    print('=' * 80)
    distinct_products_df.explain('formatted')

    # Global distinct cannot prove uniqueness independently inside each original
    # partition. Equal product_id values from different partitions must meet.

    # -------------------------------------------------------------------------
    # 13.3 orderBy(): establish global range distribution + local order
    # -------------------------------------------------------------------------

    ordered_df = sales_enriched_df.orderBy(
        F.col('gross_sales').desc(),
        F.col('sale_id').asc(),
    )

    print('\n' + '=' * 80)
    print('SHUFFLE: GLOBAL ORDER BY')
    print('=' * 80)
    ordered_df.explain('formatted')

    # Contrast global orderBy() with sortWithinPartitions(). The latter establishes
    # ordering only inside the current partitions and does not by itself create a
    # globally ordered result.
    local_sort_df = sales_enriched_df.sortWithinPartitions(
        'store_id',
        'sale_id',
    )

    print('\nLOCAL SORT WITHIN EXISTING PARTITIONS')
    local_sort_df.explain('formatted')

    # -------------------------------------------------------------------------
    # 13.4 non-broadcast join: compatible join-key distribution is required
    # -------------------------------------------------------------------------

    # Disable broadcast only to create a controlled shuffle-join example. This is
    # experiment setup, not a production recommendation.
    spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '-1')

    shuffle_join_df = (
        sales_enriched_df
        .hint('merge')
        .join(
            stores_df.hint('merge'),
            on='store_id',
            how='inner',
        )
    )

    print('\n' + '=' * 80)
    print('SHUFFLE: NON-BROADCAST EQUI-JOIN')
    print('=' * 80)
    shuffle_join_df.explain('formatted')

    # Expected Phase 6 explanation:
    # matching store_id values from both inputs need compatible downstream key
    # distribution. A sort-merge strategy commonly shows Exchange on each side
    # plus Sort to satisfy the merge operator's ordering requirement.
    #
    # Do not say 'every join shuffles'. Broadcast joins are the obvious
    # counterexample already studied in Phase 5.

    # Restore the normal threshold for later examples.
    spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '10485760')

    # -------------------------------------------------------------------------
    # 13.5 repartition(): redistribution is explicitly requested
    # -------------------------------------------------------------------------

    explicit_shuffle_df = sales_enriched_df.repartition(
        6,
        'product_id',
    )

    print('\n' + '=' * 80)
    print('SHUFFLE: EXPLICIT REPARTITION')
    print('=' * 80)
    explicit_shuffle_df.explain('formatted')

    # Here the Exchange is not an indirect consequence of aggregation/join logic.
    # The user explicitly requested a new product_id-based distribution.


    # =========================================================================
    # 14. OUTPUT PARTITIONS, OUTPUT FILES, AND THE SMALL-FILE PROBLEM
    # =========================================================================

    many_output_partitions_df = sales_enriched_df.repartition(32)
    fewer_output_partitions_df = sales_enriched_df.repartition(4)

    many_files_path = temp / 'many_output_files'
    fewer_files_path = temp / 'fewer_output_files'

    print('\n' + '=' * 80)
    print('OUTPUT PARTITIONS -> PHYSICAL FILES')
    print('=' * 80)
    print(
        'many-output execution partitions:',
        many_output_partitions_df.rdd.getNumPartitions(),
    )
    print(
        'fewer-output execution partitions:',
        fewer_output_partitions_df.rdd.getNumPartitions(),
    )

    (
        many_output_partitions_df
        .write
        .mode('overwrite')
        .parquet(str(many_files_path))
    )

    (
        fewer_output_partitions_df
        .write
        .mode('overwrite')
        .parquet(str(fewer_files_path))
    )

    show_parquet_files(
        many_files_path,
        'WRITE FROM 32 EXECUTION PARTITIONS',
    )
    show_parquet_files(
        fewer_files_path,
        'WRITE FROM 4 EXECUTION PARTITIONS',
    )

    # On this small dataset, 32 output partitions intentionally create many tiny
    # Parquet files. The point is not that '4 is correct'; it is that today's
    # execution partitioning directly influences tomorrow's file-open/listing and
    # scan-planning overhead.

    # -------------------------------------------------------------------------
    # 14.1 Storage partitionBy() complicates the one-partition-one-file shortcut
    # -------------------------------------------------------------------------

    partitioned_write_path = temp / 'partitioned_write'

    # Reduce to four execution partitions, then write by year/month storage values.
    # One writer task can encounter several storage-partition values, so it can
    # create more than one physical file across directory partitions.
    storage_write_df = sales_enriched_df.repartition(4)

    (
        storage_write_df
        .write
        .mode('overwrite')
        .partitionBy('year', 'month')
        .parquet(str(partitioned_write_path))
    )

    print(
        '\nstorage-partitioned write execution partitions:',
        storage_write_df.rdd.getNumPartitions(),
    )
    show_parquet_files(
        partitioned_write_path,
        'STORAGE-PARTITIONED WRITE FILES',
    )

    # Therefore use this only as a rough rule:
    #
    #     unpartitioned write
    #     output execution partitions
    #         -> strong clue about writer-task/data-file count
    #
    # Never turn it into:
    #
    #     one Spark partition = exactly one physical file in every write


    # =========================================================================
    # 15. MAX RECORDS PER FILE CAN SPLIT ONE WRITER'S OUTPUT
    # =========================================================================

    records_limited_path = temp / 'max_records_per_file'

    # Collapse to two execution partitions, but ask the writer to cap records per
    # file. This demonstrates another reason physical file count can exceed the
    # number of writer execution partitions.
    records_limited_df = sales_enriched_df.coalesce(2)

    (
        records_limited_df
        .write
        .mode('overwrite')
        .option('maxRecordsPerFile', 1000)
        .parquet(str(records_limited_path))
    )

    print(
        '\nrecords-limited execution partitions:',
        records_limited_df.rdd.getNumPartitions(),
    )
    show_parquet_files(
        records_limited_path,
        'MAX RECORDS PER FILE OUTPUT',
    )

    # Writer configuration can therefore change the file count independently of
    # the DataFrame partition count. Partition count is evidence about writer
    # parallelism, not a universal exact-file-count contract.


    # =========================================================================
    # 16. COMPLETE STORAGE -> EXECUTION -> STORAGE PIPELINE
    # =========================================================================

    # Reuse the year/month-partitioned source and trace one pipeline across layers.
    monthly_store_sales_df = (
        spark.read.parquet(str(partitioned_sales_path))
        .filter(
            (F.col('year') == 2026)
            & (F.col('month') == 2)
            & (F.col('order_status') == 'COMPLETED')
        )
        .select(
            'store_id',
            'quantity',
            'gross_sales',
        )
        .groupBy('store_id')
        .agg(
            F.sum('quantity').alias('units'),
            F.sum('gross_sales').alias('gross_sales'),
        )
    )

    # PREDICT BEFORE INSPECTION:
    #
    # 1. STORAGE PARTITION PRUNING
    #    year/month should exclude unrelated directories.
    #
    # 2. COLUMN PRUNING
    #    the scan should request only columns needed for filtering/aggregation.
    #
    # 3. PREDICATE PUSHDOWN
    #    eligible order_status filtering may reach the Parquet reader.
    #
    # 4. INPUT / DATAFRAME PARTITIONS
    #    remaining selected files become task-sized scan partitions; their count
    #    is not the number of storage partition directories.
    #
    # 5. SHUFFLE
    #    groupBy(store_id) requires partial states for each store to meet, so
    #    expect Exchange hashpartitioning(store_id, 8).
    #
    # 6. POST-SHUFFLE PARTITIONS
    #    with AQE disabled for this teaching example, expect the configured eight
    #    shuffle partitions to remain visible downstream.
    print('\n' + '=' * 80)
    print('COMPLETE PIPELINE: STORAGE -> SHUFFLE EXECUTION')
    print('=' * 80)
    monthly_store_sales_df.explain('formatted')

    print(
        'monthly store-sales execution partitions:',
        monthly_store_sales_df.rdd.getNumPartitions(),
    )

    # The grouped output is tiny (12 stores), so keeping eight tiny writer
    # partitions would be wasteful for this local demonstration. Reduce them
    # WITHOUT requiring a balanced global redistribution.
    compact_output_df = monthly_store_sales_df.coalesce(2)

    final_output_path = temp / 'monthly_store_sales_output'

    print(
        'after coalesce(2) output partitions:',
        compact_output_df.rdd.getNumPartitions(),
    )

    (
        compact_output_df
        .write
        .mode('overwrite')
        .parquet(str(final_output_path))
    )

    show_parquet_files(
        final_output_path,
        'FINAL MONTHLY STORE-SALES FILES',
    )

    # Complete causal story:
    #
    #     old storage layout
    #         -> prunes January/March directories
    #         -> selected Parquet files become input partitions
    #         -> narrow scan/filter/project work preserves current topology
    #         -> groupBy requires hash redistribution by store_id
    #         -> eight shuffle partitions are created under this static setup
    #         -> tiny grouped result is coalesced to two output partitions
    #         -> two writer lanes create the next unpartitioned Parquet layout
    #         -> that new layout becomes physical input for a future Spark job


# =============================================================================
# 17. PHASE 6 ENGINEERING RULES
# =============================================================================

# Use these rules when reasoning about real Spark pipelines:
#
# 1. Always qualify 'partition': storage/input/DataFrame/shuffle/output.
# 2. Parquet is columnar physical storage with typed metadata, not magic tuning.
# 3. Column pruning answers WHICH COLUMNS need to be read.
# 4. Partition pruning answers WHICH STORAGE DIRECTORY PARTITIONS can be skipped.
# 5. Predicate pushdown answers WHICH ROW PREDICATES reach the file reader.
# 6. Parquet statistics can help the reader skip internal blocks that cannot match.
# 7. One file is not guaranteed to equal one input partition.
# 8. One storage partition is not one Spark execution partition.
# 9. Narrow filters can remove most rows without changing partition count.
# 10. repartition() requests a new distribution and normally shuffles.
# 11. coalesce() is mainly for shrinking partitions without full redistribution.
# 12. coalesce() does not meaningfully increase parallelism.
# 13. Key repartitioning is useful only when downstream distribution benefits.
# 14. Hash repartitioning does not solve a hot-key skew problem.
# 15. Explain every Exchange by the downstream distribution requirement.
# 16. Explain Sort separately from Exchange: ordering != distribution.
# 17. groupBy/distinct require equal keys/values to meet globally.
# 18. non-broadcast joins commonly require compatible join-key distribution.
# 19. global orderBy() requires global ordering, not merely local sorting.
# 20. Output execution partitions are a strong clue about writer parallelism.
# 21. Storage partitionBy() and writer options can make file count differ from
#     the simple one-output-partition-one-file mental model.
# 22. Too few partitions can underuse parallelism; too many can make overhead
#     dominate. Partition counts only make sense with data size/skew/resources.
# 23. Today's output files are tomorrow's input-planning problem.
# 24. Preserve grain/correctness while changing partitioning.
# 25. Predict first; use actual plans/counts/files as evidence second.
#
# Repeatable checklist:
#
#     1. What is the DataFrame grain?
#     2. What file format is being read?
#     3. What storage partition columns exist?
#     4. Which storage directories should be pruned?
#     5. Which columns should the scan read?
#     6. Which predicates can be pushed into the source?
#     7. What input/DataFrame partition count exists before wide work?
#     8. Which transformations are narrow?
#     9. Which operation requires redistribution, and WHY?
#    10. What shuffle partitioning/count is planned?
#    11. How does repartition/coalesce change the execution partition count?
#    12. Is a key repartition useful or skew-prone?
#    13. What output partition count reaches the writer?
#    14. What physical files/directories are actually created?
#    15. How will that storage layout affect the next read?


# =============================================================================
# 18. PHASE 6 MASTERY REFERENCE — NOT THE MASTERY GATE
# =============================================================================

# The eventual Phase 6 target is NOT merely:
#
#     'Parquet is columnar.'
#     'repartition shuffles.'
#     'coalesce is cheaper.'
#
# Given a real pipeline, you should be able to explain a causal chain like:
#
#     'The source is Parquet storage partitioned by year/month. The February
#      filter becomes a storage PartitionFilter, so irrelevant month directories
#      can be pruned. The file scan requests only the columns required by the
#      order_status filter and downstream aggregation, while eligible row filters
#      are pushed into the Parquet reader. The selected files are then planned as
#      Spark input partitions; that count is unrelated to the number of storage
#      partition directories. The scan/filter/project work is narrow, but the
#      groupBy(store_id) requires partial states for equal store keys to meet, so
#      Spark inserts a hash-partitioning Exchange. Under the static eight-shuffle-
#      partition setup, the grouped result has eight downstream execution
#      partitions. Because the result is tiny, coalesce(2) reduces writer
#      parallelism without a full rebalance, producing a compact output layout.
#      Those new files become the physical scan input of the next job.'
#
# Formal mastery is intentionally NOT performed in this file.
# ROADMAP.md must remain unchanged until mastery is explicitly requested/passed.


# =============================================================================
# 19. CLEANUP
# =============================================================================

# The temporary directory and all generated Parquet teaching data are deleted
# automatically when the with-block exits. Stop the Spark application cleanly.
spark.stop()
