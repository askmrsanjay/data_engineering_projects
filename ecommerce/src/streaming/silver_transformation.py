from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, current_timestamp, coalesce, lit, when
from pyspark.sql.types import TimestampType

def create_spark_session():
    """Create a Spark Session with Iceberg and AWS configurations."""
    return SparkSession.builder \
        .appName("Ecommerce-Silver-Transformation") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.demo.type", "hadoop") \
        .config("spark.sql.catalog.demo.warehouse", "s3a://ecommerce-bucket/warehouse") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://ecommerce-minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .master("local[*]") \
        .getOrCreate()

def run_silver_transformation():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Starting Silver Layer Transformation...")

    # 1. Read from Bronze Table (Incremental Stream)
    bronze_df = spark.readStream \
        .format("iceberg") \
        .load("demo.default.bronze_events")

    # 2. Transformations & Cleaning
    silver_df = bronze_df \
        .filter(col("event_id").isNotNull()) \
        .withColumn("event_timestamp", to_timestamp(col("event_timestamp"))) \
        .withColumn("price", coalesce(col("price"), lit(0.0))) \
        .withColumn("processed_at", current_timestamp()) \
        .dropDuplicates(["event_id"])

    # 3. Write to Silver Table (Iceberg)
    # Using 'toTable' ensures the table is created if it doesn't exist
    # 3. Define Micro-Batch Function
    def process_batch(batch_df, batch_id):
        print(f"Processing Batch {batch_id} with {batch_df.count()} records")
        
        # A. Write to Iceberg (Storage)
        batch_df.write \
            .format("iceberg") \
            .mode("append") \
            .save("demo.default.silver_events")
            
        # B. Write to CSV (Dashboard "Real-Time" View)
        # We overwrite this file every few seconds so the dashboard sees the latest "Pulse"
        # We filter for purchases only to keep it exciting
        sales_df = batch_df.filter(col("event_type") == "purchase")
        if sales_df.count() > 0:
            sales_df.select(col("event_timestamp").cast("string"), "category", "price", "user_id") \
                .toPandas() \
                .to_csv("/opt/bitnami/spark/app/realtime_sales_update.csv", mode='a', header=False, index=False)
                
    # 4. Start Streaming Query
    query = silver_df.writeStream \
        .foreachBatch(process_batch) \
        .trigger(processingTime="5 seconds") \
        .option("checkpointLocation", "s3a://ecommerce-bucket/checkpoints/silver_events_v2") \
        .start()

    print("Real-Time Silver Transformation Started...")
    query.awaitTermination()

if __name__ == "__main__":
    run_silver_transformation()
