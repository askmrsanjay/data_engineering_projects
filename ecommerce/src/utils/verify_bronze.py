from pyspark.sql import SparkSession

def verify_bronze():
    """Read the Bronze Iceberg table and print some stats."""
    spark = SparkSession.builder \
        .appName("Verify-Bronze") \
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
        print("Reading Bronze table...")
        # Check for both possible table names
        try:
            df = spark.table("demo.default.bronze_events")
        except:
            df = spark.table("demo.bronze_events")
        
        count = df.count()
        print(f"\nSUCCESS! Total records in Bronze layer: {count}")
        
        print("\nLast 5 events:")
        df.orderBy(df.event_timestamp.desc()).show(5, truncate=False)
        
    except Exception as e:
        print(f"Error reading table: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    verify_bronze()
