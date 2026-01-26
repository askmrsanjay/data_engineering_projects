from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, sum, count, approx_count_distinct

def preview_gold():
    """Batch query Silver data to preview what Gold aggregates would look like."""
    spark = SparkSession.builder \
        .appName("Preview-Gold-Batch") \
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

    try:
        print("\n--- GOLD LAYER PREVIEW (Batch Query from Silver) ---")
        
        # Read from Silver table
        silver_df = spark.table("demo.default.silver_events")

        # Perform the same aggregation as the Gold job, but in batch mode
        gold_preview = silver_df \
            .filter(col("event_type") == "purchase") \
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
                col("category"),
                col("total_revenue"),
                col("total_transactions")
            ) \
            .orderBy(col("hour_start").desc(), col("total_revenue").desc())

        gold_preview.show(20, truncate=False)

    except Exception as e:
        print(f"Error previewing Gold: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    preview_gold()
