
from pyspark.sql import SparkSession
from pyspark.sql.functions import max
import sys

spark = SparkSession.builder \
    .appName("CheckBronze") \
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
    with open("/opt/bitnami/spark/app/check_results.txt", "w") as f:
        f.write("Reading Bronze table...\n")
        df = spark.table("demo.default.bronze_events")
        
        latest = df.select(max("event_timestamp")).collect()[0][0]
        f.write(f"Latest Timestamp: {latest}\n\n")
        
        f.write("Record count by date:\n")
        counts = df.selectExpr("date(event_timestamp) as event_date").groupBy("event_date").count().orderBy("event_date").collect()
        for row in counts:
            f.write(f"{row['event_date']}: {row['count']}\n")
            
except Exception as e:
    with open("/opt/bitnami/spark/app/check_results.txt", "a") as f:
        f.write(f"\nError: {e}\n")
finally:
    spark.stop()
