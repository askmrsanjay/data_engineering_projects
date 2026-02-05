from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import great_expectations as gx

def create_spark_session():
    """Create a Spark Session with Iceberg and AWS configurations."""
    return SparkSession.builder \
        .appName("Ecommerce-Silver-Quality") \
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

def run_quality_checks():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("\n--- SILVER LAYER QUALITY VALIDATION (GE 1.1.0) ---")

    try:
        # 1. Load Silver data
        silver_df = spark.table("demo.default.silver_events")
        
        # 2. Setup GE Context
        context = gx.get_context()
        
        # 3. Create a Data Source and Asset (Fluent API for GX 0.18)
        datasource_name = "spark_ds_v4"
        # Use context.sources instead of context.data_sources
        datasource = context.sources.add_spark(name=datasource_name)
        
        data_asset_name = "silver_data_asset_v4"
        asset = datasource.add_dataframe_asset(name=data_asset_name)
        
        # Build Batch Request passing the DataFrame
        batch_request = asset.build_batch_request(dataframe=silver_df)
        
        # 4. Create and Configure Expectation Suite using Validator
        suite_name = "silver_suite_v4"
        context.add_or_update_expectation_suite(expectation_suite_name=suite_name)
        
        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite_name=suite_name
        )
        
        # Add Expectations
        validator.expect_column_values_to_not_be_null(column="event_id")
        validator.expect_column_values_to_be_in_set(
            column="event_type", 
            value_set=["view_item", "add_to_cart", "purchase"]
        )
        validator.expect_column_values_to_be_between(column="price", min_value=0)
        validator.expect_column_values_to_not_be_null(column="event_timestamp")
        
        # Save suite to context
        validator.save_expectation_suite()

        # 5. Run Validation via Checkpoint
        print("Running validation via Checkpoint...")
        checkpoint = context.add_or_update_checkpoint(
            name="silver_quality_checkpoint",
            validations=[
                {
                    "batch_request": batch_request,
                    "expectation_suite_name": suite_name,
                },
            ],
        )
        
        checkpoint_result = checkpoint.run()

        # 6. Report Results
        print("\nQuality Report:")
        if checkpoint_result.success:
            print("\n🏆 SUCCESS: All quality checks passed!")
        else:
            print("\n❌ FAILED: Quality checks failed.")
            import sys
            sys.exit(1)

    except Exception as e:
        print(f"Error running quality checks: {e}")
        import traceback
        traceback.print_exc()
        import sys
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    run_quality_checks()
