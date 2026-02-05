
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ClearCheckpoint") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

try:
    checkpoint_path = "s3a://ecommerce-bucket/checkpoints/bronze_events"
    print(f"Deleting checkpoint: {checkpoint_path}")
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    path = spark._jvm.org.apache.hadoop.fs.Path(checkpoint_path)
    if fs.exists(path):
        fs.delete(path, True)
        print("Checkpoint deleted successfully.")
    else:
        print("Checkpoint path does not exist.")
except Exception as e:
    print(f"Error: {e}")
finally:
    spark.stop()
