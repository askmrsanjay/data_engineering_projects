
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("CheckBronzeDist") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.demo.type", "hadoop") \
    .config("spark.sql.catalog.demo.warehouse", "s3a://ecommerce-bucket/warehouse") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

try:
    print("Checking Feb 3 Event Distribution...")
    df = spark.table("demo.default.bronze_events")
    df.filter("date(event_timestamp) = '2026-02-03'") \
      .groupBy("event_type").count().show()
except Exception as e:
    print(f"Error: {e}")
finally:
    spark.stop()
