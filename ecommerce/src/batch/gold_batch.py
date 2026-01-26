from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, sum, count, approx_count_distinct, date_trunc, lit, expr

def create_spark_session():
    """Create a Spark Session with Iceberg and AWS configurations."""
    return SparkSession.builder \
        .appName("Ecommerce-Gold-Batch") \
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

def run_gold_batch():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Starting Gold Layer Batch Aggregations...")

    # 1. Read from Silver Table (Batch Mode)
    silver_df = spark.table("demo.default.silver_events")

    # 2. Filter for Sales and Aggregate
    # For batch, we can use simple group by with date_trunc
    gold_df = silver_df \
        .filter(col("event_type") == "purchase") \
        .groupBy(
            date_trunc("hour", col("event_timestamp")).alias("hour_start"),
            col("category")
        ) \
        .agg(
            sum("price").alias("total_revenue"),
            count("event_id").alias("total_transactions"),
            approx_count_distinct("user_id").alias("unique_buyers")
        ) \
        .withColumn("hour_end", expr("hour_start + interval 1 hour")) \
        .select(
            "hour_start",
            "hour_end",
            "category",
            "total_revenue",
            "total_transactions",
            "unique_buyers"
        )

    # 3. Write to Gold Table (Iceberg)
    gold_df.write \
        .format("iceberg") \
        .mode("append") \
        .save("demo.default.gold_category_sales")

    # 4. EXPORT TO CSV (For Host Dashboard)
    # We save a copy to the shared volume so Streamlit can read it without Spark
    print("Exporting Gold results to CSV for dashboard...")
    pd_gold = gold_df \
        .withColumn("hour_start", col("hour_start").cast("string")) \
        .withColumn("hour_end", col("hour_end").cast("string")) \
        .toPandas()
    pd_gold.to_csv("/opt/bitnami/spark/app/gold_sales_dashboard.csv", index=False)

    print("Gold Batch Aggregations Complete.")
    spark.stop()

if __name__ == "__main__":
    run_gold_batch()
