from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# 1. Define the Schema to parse JSON from Kafka
schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("platform", StringType(), True)
])

def create_spark_session():
    """Create a Spark Session with Kafka and Iceberg configurations."""
    # Note: Spark 3.3.0 requires corresponding 3.3 JARs
    return SparkSession.builder \
        .appName("Ecommerce-Bronze-Streaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4") \
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

def run_bronze_streaming():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Ensure the database exists
    spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.default")

    # 2. Read from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "ecommerce-topic") \
        .option("startingOffsets", "earliest") \
        .load()

    # 3. Parse JSON data
    raw_df = kafka_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # 4. Write to Bronze Table (Iceberg Format)
    # Using toTable is the modern way for Iceberg in Spark 3.x
    query = raw_df.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .option("checkpointLocation", "s3a://ecommerce-bucket/checkpoints/bronze_events") \
        .toTable("demo.default.bronze_events")

    print("Streaming started... Writing to demo.default.bronze_events")
    query.awaitTermination()

if __name__ == "__main__":
    run_bronze_streaming()
