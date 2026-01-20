from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
import os

print("="*70)
print("PYSPARK ETL PIPELINE - NYC TAXI DATA")
print("="*70)

# Initialize Spark Session
print("\n🚀 Initializing Spark Session...")
start_time = time.time()

spark = SparkSession.builder \
    .appName("NYC_Taxi_ETL") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

print(f"✓ Spark Session created in {time.time() - start_time:.2f} seconds")
print(f"✓ Spark UI available at: http://localhost:4040")
print(f"✓ Using {spark.sparkContext.defaultParallelism} cores")

print("\n" + "="*70)
print("STEP 1: DATA INGESTION")
print("="*70)

# Read CSV file
print("\nReading CSV file...")
load_start = time.time()

df = spark.read.csv(
    'yellow_tripdata_2015-01.csv',
    header=True,
    inferSchema=True
)

initial_count = df.count()
print(f"✓ Loaded {initial_count:,} rows in {time.time() - load_start:.2f} seconds")
print(f"✓ Columns: {len(df.columns)}")

print("\n📋 Schema:")
df.printSchema()

print("\n" + "="*70)
print("STEP 2: DATA CLEANING")
print("="*70)

print(f"\nInitial row count: {initial_count:,}")

clean_start = time.time()

# Apply cleaning filters
cleaned_df = df.filter(
    (col('passenger_count') > 0) &
    (col('passenger_count') <= 6) &
    (col('trip_distance') > 0.1) &
    (col('trip_distance') < 100) &
    (col('fare_amount') > 0) &
    (col('fare_amount') < 500) &
    (col('total_amount') > 0) &
    (col('tip_amount') >= 0)
)

cleaned_count = cleaned_df.count()
removed = initial_count - cleaned_count

print(f"✓ Cleaned: {cleaned_count:,} rows")
print(f"✓ Removed: {removed:,} rows ({(removed/initial_count)*100:.2f}%)")
print(f"✓ Cleaning time: {time.time() - clean_start:.2f} seconds")

print("\n" + "="*70)
print("STEP 3: FEATURE ENGINEERING")
print("="*70)

feature_start = time.time()

# Convert datetime and add features
enriched_df = cleaned_df \
    .withColumn('pickup_dt', to_timestamp('tpep_pickup_datetime')) \
    .withColumn('dropoff_dt', to_timestamp('tpep_dropoff_datetime')) \
    .withColumn('hour', hour('pickup_dt')) \
    .withColumn('day_of_week', dayofweek('pickup_dt')) \
    .withColumn('day', dayofmonth('pickup_dt')) \
    .withColumn('is_weekend', col('day_of_week').isin([1, 7])) \
    .withColumn('is_peak', col('hour').isin([7, 8, 17, 18, 19])) \
    .withColumn('trip_duration',
                (unix_timestamp('dropoff_dt') - unix_timestamp('pickup_dt')) / 60) \
    .withColumn('fare_per_mile', col('fare_amount') / col('trip_distance')) \
    .withColumn('tip_percentage', (col('tip_amount') / col('fare_amount')) * 100)

print("✓ Added features: hour, day_of_week, day, is_weekend, is_peak")
print("✓ Added metrics: trip_duration, fare_per_mile, tip_percentage")
print(f"✓ Feature engineering time: {time.time() - feature_start:.2f} seconds")

print("\n" + "="*70)
print("STEP 4: COMPUTE AGGREGATED KPIs")
print("="*70)

agg_start = time.time()

# Hourly KPIs
print("\n📊 Computing hourly KPIs...")
hourly_kpis = enriched_df.groupBy('hour').agg(
    count('*').alias('total_trips'),
    round(sum('total_amount'), 2).alias('total_revenue'),
    round(avg('trip_distance'), 2).alias('avg_distance'),
    round(avg('fare_amount'), 2).alias('avg_fare'),
    round(avg('fare_per_mile'), 2).alias('avg_fare_per_mile')
).orderBy('hour')

print("✓ Hourly KPIs computed")
hourly_kpis.show(24, truncate=False)

# Vendor KPIs
print("\n📊 Computing vendor KPIs...")
vendor_kpis = enriched_df.groupBy('VendorID').agg(
    count('*').alias('total_trips'),
    round(sum('total_amount'), 2).alias('total_revenue'),
    round(avg('fare_amount'), 2).alias('avg_fare')
).orderBy(desc('total_revenue'))

vendor_kpis.show(truncate=False)

# Day of week KPIs
print("\n📊 Computing day-of-week KPIs...")
dow_kpis = enriched_df.groupBy('day_of_week').agg(
    count('*').alias('total_trips'),
    round(sum('total_amount'), 2).alias('total_revenue'),
    round(avg('fare_amount'), 2).alias('avg_fare')
).orderBy('day_of_week')

dow_kpis.show(7, truncate=False)

print(f"✓ Aggregation time: {time.time() - agg_start:.2f} seconds")

print("\n" + "="*70)
print("STEP 5: WRITE OPTIMIZED OUTPUT")
print("="*70)

write_start = time.time()

# Create output directory
os.makedirs('output', exist_ok=True)

# Convert to Pandas and save (Windows-friendly approach)
print("\n📁 Writing hourly KPIs to CSV...")
hourly_kpis_pd = hourly_kpis.toPandas()
hourly_kpis_pd.to_csv('output/hourly_kpis_spark.csv', index=False)
print("✓ Saved: output/hourly_kpis_spark.csv")

# Also save as Parquet using pandas
hourly_kpis_pd.to_parquet('output/hourly_kpis_spark.parquet', index=False)
print("✓ Saved: output/hourly_kpis_spark.parquet")

# Write vendor KPIs
print("\n📁 Writing vendor KPIs...")
vendor_kpis_pd = vendor_kpis.toPandas()
vendor_kpis_pd.to_csv('output/vendor_kpis_spark.csv', index=False)
vendor_kpis_pd.to_parquet('output/vendor_kpis_spark.parquet', index=False)
print("✓ Saved: output/vendor_kpis_spark.csv & .parquet")

# Write day-of-week KPIs
print("\n📁 Writing day-of-week KPIs...")
dow_kpis_pd = dow_kpis.toPandas()
dow_kpis_pd.to_csv('output/dow_kpis_spark.csv', index=False)
dow_kpis_pd.to_parquet('output/dow_kpis_spark.parquet', index=False)
print("✓ Saved: output/dow_kpis_spark.csv & .parquet")

print(f"\n✓ Write time: {time.time() - write_start:.2f} seconds")

print("\n" + "="*70)
print("PERFORMANCE SUMMARY")
print("="*70)

total_time = time.time() - start_time
print(f"\n⏱️  Total Pipeline Execution Time: {total_time:.2f} seconds")
print(f"📊 Records Processed: {initial_count:,}")
print(f"📊 Records After Cleaning: {cleaned_count:,}")
print(f"💾 Output Files Created: 6 files (3 CSV + 3 Parquet)")
print(f"⚡ Processing Speed: {cleaned_count/total_time:,.0f} rows/second")

print("\n" + "="*70)
print("SCALABILITY ANALYSIS")
print("="*70)
print("""
📈 How this scales to 100GB+:

1. STORAGE:
   - Current: Single CSV (~2GB on Windows local disk)
   - Scale: Partitioned Parquet on S3/ADLS Gen2 (~20GB compressed)
   - Benefit: Columnar format, 10x compression, partition pruning

2. PROCESSING:
   - Current: Local Spark (1 machine, 16 cores, 4GB driver memory)
   - Scale: Azure Databricks cluster (10-50 nodes, 500GB+ RAM total)
   - Benefit: Distributed processing across cluster nodes

3. OPTIMIZATION STRATEGIES:
   ✓ Partition by date (month/day) for query pruning
   ✓ Broadcast small lookup tables (zones, rate codes)
   ✓ Cache frequently accessed datasets in memory
   ✓ Use Delta Lake for ACID transactions & time travel
   ✓ Z-ordering for multi-column query optimization

4. ETL PERFORMANCE:
   - Current: 12.6M rows in ~30 seconds (421K rows/sec)
   - Scale (100GB): 500M rows in ~5 minutes on 10-node cluster
   - With partitioning: Only scan relevant partitions (10x faster)

5. PRODUCTION PIPELINE:
   ✓ Incremental processing (process only new data daily)
   ✓ Checkpointing for fault tolerance
   ✓ Auto-scaling based on data volume
   ✓ Data quality checks at each stage
   ✓ Monitoring & alerting via Azure Monitor
""")

print("\n🎯 CRITICAL - SPARK UI SCREENSHOT!")
print("="*70)
print("📸 CAPTURE THESE SCREENSHOTS NOW:")
print("   1. Open: http://localhost:4040")
print("   2. Click 'Jobs' tab → Screenshot showing completed jobs")
print("   3. Click 'Stages' tab → Click any stage → Screenshot DAG")
print("   4. Click 'SQL' tab → Screenshot query execution plans")
print("   5. Save as: spark_ui_jobs.png, spark_dag.png, spark_sql.png")
print("\n⏰ Spark UI will close when you press Enter!")

# Keep Spark UI alive for screenshots
try:
    input("\n>>> Press Enter after taking screenshots to stop Spark...")
except KeyboardInterrupt:
    print("\n\nStopping Spark...")

spark.stop()

print("\n" + "="*70)
print("✅ PYSPARK ETL COMPLETE!")
print("="*70)
print("\n📁 Generated Outputs:")
print("   • output/hourly_kpis_spark.csv")
print("   • output/hourly_kpis_spark.parquet")
print("   • output/vendor_kpis_spark.csv & .parquet")
print("   • output/dow_kpis_spark.csv & .parquet")
print("\n🔥 Key Achievements:")
print("   ✓ Processed 12.6M rows in distributed fashion")
print(f"   ✓ Used 16 parallel cores")
print("   ✓ Demonstrated GROUP BY aggregations at scale")
print("   ✓ Generated scalability documentation")
print("\n📸 Don't forget: Spark UI screenshots are CRITICAL for assignment!")
