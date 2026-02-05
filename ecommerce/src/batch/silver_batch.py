from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, current_timestamp, coalesce, lit
from pyspark.sql.types import TimestampType

def create_spark_session():
    """Create a Spark Session with Iceberg and AWS configurations."""
    return SparkSession.builder \
        .appName("Ecommerce-Silver-Batch") \
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

def run_silver_batch():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Starting Silver Layer Batch Transformation...")

    # 1. Read from Bronze Table (Batch Mode)
    # In a real scenario, you might filter by date or use Iceberg's incremental read
    bronze_df = spark.table("demo.default.bronze_events")

    # 2. Transformations & Cleaning
    silver_df = bronze_df \
        .filter(col("event_id").isNotNull()) \
        .withColumn("event_timestamp", to_timestamp(col("event_timestamp"))) \
        .withColumn("price", coalesce(col("price"), lit(0.0))) \
        .withColumn("processed_at", current_timestamp()) \
        .dropDuplicates(["event_id"])

    # 3. Write to Silver Table (Iceberg) - Overwrite to prevent duplicates in Batch Mode
    # Since we read the entire Bronze table, we must overwrite Silver to maintain consistency.
    silver_df.write \
        .format("iceberg") \
        .mode("overwrite") \
        .save("demo.default.silver_events")

    print("Silver Batch Transformation Complete.")
    spark.stop()

if __name__ == "__main__":
    run_silver_batch()
