
from pyspark.sql import SparkSession
import os

spark = SparkSession.builder \
    .appName("PurgeCheckpoints") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

try:
    checkpoint_base = "s3a://ecommerce-bucket/checkpoints"
    print(f"Purging directory: {checkpoint_base}")
    
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    path = spark._jvm.org.apache.hadoop.fs.Path(checkpoint_base)
    
    if fs.exists(path):
        # Recursive delete
        res = fs.delete(path, True)
        print(f"Delete result: {res}")
    else:
        print("Path does not exist.")
        
    # Verify
    print(f"Exists after delete: {fs.exists(path)}")

except Exception as e:
    print(f"Error: {e}")
finally:
    spark.stop()
