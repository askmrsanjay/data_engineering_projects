from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import time

def create_spark_session():
    """Create a Spark Session with Iceberg and AWS configurations."""
    return SparkSession.builder \
        .appName("Ecommerce-Iceberg-Governance") \
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

def run_governance():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("\n--- ICEBERG GOVERNANCE: MAINTENANCE TASKS ---")

    tables = [
        "demo.default.bronze_events",
        "demo.default.silver_events",
        "demo.default.gold_category_sales"
    ]

    for table in tables:
        print(f"\nProcessing Table: {table}")
        
        try:
            # 1. Expire Snapshots (Keep only last 24 hours of history for this demo)
            # This prevents metadata bloat and saves storage in MinIO
            print("- Expiring old snapshots...")
            timestamp_limit = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
            spark.sql(f"CALL demo.system.expire_snapshots('{table}', TIMESTAMP '{timestamp_limit}')")

            # 2. Remove Orphan Files (Cleanup files not referenced by metadata)
            print("- Removing orphan files...")
            spark.sql(f"CALL demo.system.remove_orphan_files('{table}')")

            # 3. Rewrite Data Files (Compaction)
            # Combine small files into larger ones for better query performance
            print("- Rewriting data files (Compaction)...")
            spark.sql(f"CALL demo.system.rewrite_data_files('{table}')")

            print(f"✅ Maintenance complete for {table}")
        except Exception as e:
            print(f"❌ Error maintaining {table}: {e}")

    spark.stop()

if __name__ == "__main__":
    run_governance()
