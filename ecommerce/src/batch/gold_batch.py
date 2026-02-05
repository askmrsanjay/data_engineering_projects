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

    # Filter for Purchases once
    purchases_df = silver_df.filter(col("event_type") == "purchase")

    # --- AGGREGATION 1: HOURLY SALES BY CATEGORY ---
    print("Processing: Hourly Category Sales...")
    category_sales_df = purchases_df \
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
        .select("hour_start", "hour_end", "category", "total_revenue", "total_transactions", "unique_buyers")

    # Write to Iceberg using SQL CTAS (Safest for Overwrite)
    category_sales_df.createOrReplaceTempView("tmp_category_sales")
    spark.sql("""
        CREATE OR REPLACE TABLE demo.default.gold_category_sales 
        USING iceberg 
        AS SELECT * FROM tmp_category_sales
    """)
    
    # Export to CSV
    category_sales_df \
        .withColumn("hour_start", col("hour_start").cast("string")) \
        .withColumn("hour_end", col("hour_end").cast("string")) \
        .toPandas() \
        .to_csv("/opt/bitnami/spark/app/gold_category_sales.csv", index=False)


    # --- AGGREGATION 2: TOP PRODUCTS ---
    print("Processing: Top Products...")
    top_products_df = purchases_df \
        .groupBy("product_id", "category") \
        .agg(
            sum("price").alias("total_revenue"),
            count("event_id").alias("units_sold")
        ) \
        .orderBy(col("total_revenue").desc())

    # Write to Iceberg using SQL CTAS (Safest for Overwrite)
    top_products_df.createOrReplaceTempView("tmp_top_products")
    spark.sql("""
        CREATE OR REPLACE TABLE demo.default.gold_top_products 
        USING iceberg 
        AS SELECT * FROM tmp_top_products
    """)

    # Export to CSV (Top 20)
    top_products_df.limit(20).toPandas() \
        .to_csv("/opt/bitnami/spark/app/gold_top_products.csv", index=False)


    # --- AGGREGATION 3: USER STATS ---
    print("Processing: User Statistics...")
    user_stats_df = purchases_df \
        .groupBy("user_id") \
        .agg(
            sum("price").alias("total_spend"),
            count("event_id").alias("purchase_count"),
            approx_count_distinct("session_id").alias("session_count")
        ) \
        .orderBy(col("total_spend").desc())

    # Write to Iceberg using SQL CTAS
    user_stats_df.createOrReplaceTempView("tmp_user_stats")
    spark.sql("""
        CREATE OR REPLACE TABLE demo.default.gold_user_stats 
        USING iceberg 
        AS SELECT * FROM tmp_user_stats
    """)

    # Export to CSV (Top 20 Spenders)
    user_stats_df.limit(20).toPandas() \
        .to_csv("/opt/bitnami/spark/app/gold_user_stats.csv", index=False)


    print("Gold Batch Aggregations and Exports Complete.")
    spark.stop()

if __name__ == "__main__":
    run_gold_batch()
