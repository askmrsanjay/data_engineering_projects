from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, sum, count, approx_count_distinct

def create_spark_session():
    """Create a Spark Session with Iceberg and AWS configurations."""
    return SparkSession.builder \
        .appName("Ecommerce-Gold-Aggregations") \
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

def run_gold_aggregations():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Starting Gold Layer Aggregations...")

    # 1. Read from Silver Table (Streaming)
    silver_df = spark.readStream \
        .format("iceberg") \
        .load("demo.default.silver_events")

    # 2. Filter for Sales and Aggregate
    # We use a 10-minute window for faster feedback during development
    gold_df = silver_df \
        .filter(col("event_type") == "purchase") \
        .withWatermark("event_timestamp", "10 minutes") \
        .groupBy(
            window(col("event_timestamp"), "10 minutes"),
            col("category")
        ) \
        .agg(
            sum("price").alias("total_revenue"),
            count("event_id").alias("total_transactions"),
            approx_count_distinct("user_id").alias("unique_buyers")
        ) \
        .select(
            col("window.start").alias("hour_start"),
            col("window.end").alias("hour_end"),
            col("category"),
            col("total_revenue"),
            col("total_transactions"),
            col("unique_buyers")
        )

    # 3. Write to Gold Table (Iceberg)
    # Note: For aggregations in streaming, Iceberg supports 'complete' or 'append' 
    # based on the underlying logic. Here we append the results of each window.
    query = gold_df.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .trigger(processingTime="30 seconds") \
        .option("checkpointLocation", "s3a://ecommerce-bucket/checkpoints/gold_category_sales") \
        .toTable("demo.default.gold_category_sales")

    print("Aggregations running... Writing to demo.default.gold_category_sales")
    query.awaitTermination()

if __name__ == "__main__":
    run_gold_aggregations()
