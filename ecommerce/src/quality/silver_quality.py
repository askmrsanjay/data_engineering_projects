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
        
        # 3. Create a Data Source and Asset
        datasource_name = "spark_ds_v4"
        datasource = context.data_sources.add_spark(name=datasource_name)
        
        data_asset_name = "silver_data_asset_v4"
        data_asset = datasource.add_dataframe_asset(name=data_asset_name)
        
        # 4. Create a Batch Definition
        batch_definition_name = "all_silver_batch"
        batch_definition = data_asset.add_batch_definition_whole_dataframe(name=batch_definition_name)

        # 5. Create Expectation Suite
        suite_name = "silver_suite_v4"
        suite = context.suites.add(gx.ExpectationSuite(name=suite_name))
        
        # Add expectations using the modern Expectation classes
        suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="event_id"))
        suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
            column="event_type", 
            value_set=["view_item", "add_to_cart", "purchase"]
        ))
        suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="price", min_value=0))
        suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="event_timestamp"))

        # 6. Run Validation
        print("Running validation...")
        validation_definition_name = "silver_val_def_v4"
        validation_definition = context.validation_definitions.add(
            gx.ValidationDefinition(
                name=validation_definition_name,
                data=batch_definition,
                suite=suite
            )
        )
        
        # We pass the actual dataframe as a parameter to the run method
        validation_results = validation_definition.run(batch_parameters={"dataframe": silver_df})

        # 7. Report Results
        print("\nQuality Report:")
        for result in validation_results.results:
            status = "✅ PASS" if result.success else "❌ FAIL"
            expectation_type = result.expectation_config.type
            column = result.expectation_config.kwargs.get("column", "N/A")
            print(f"{status}: {expectation_type} (Column: {column})")

        if validation_results.success:
            print("\n🏆 SUCCESS: All quality checks passed!")
        else:
            print("\n⚠️ WARNING: Some quality checks failed. Check the Silver data for anomalies.")

    except Exception as e:
        print(f"Error running quality checks: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    run_quality_checks()
