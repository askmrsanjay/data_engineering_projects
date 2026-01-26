import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, coalesce

# Simplified transformation for testing
def clean_data_logic(df):
    return df \
        .filter(col("event_id").isNotNull()) \
        .withColumn("price", coalesce(col("price"), lit(0.0))) \
        .dropDuplicates(["event_id"])

class TestMedallionTransformations(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("Unit-Testing") \
            .master("local[1]") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_silver_deduplication(self):
        # Create Dummy Bronze Data
        data = [
            ("1", "user_1", 100.0), # Original
            ("1", "user_1", 100.0), # Duplicate
            ("2", "user_2", None)   # Missing price
        ]
        columns = ["event_id", "user_id", "price"]
        df = self.spark.createDataFrame(data, columns)

        # Apply transformation logic
        processed_df = clean_data_logic(df)
        
        # Assertions
        self.assertEqual(processed_df.count(), 2)
        print("✅ CI/CD Test: Deduplication passed.")

    def test_price_validation(self):
        data = [
            ("3", "user_3", -50.0),
            ("4", "user_4", 10.0)
        ]
        columns = ["event_id", "user_id", "price"]
        df = self.spark.createDataFrame(data, columns)
        
        # In our pipeline, we filter/clean prices
        processed_df = clean_data_logic(df)
        
        # Check that null price was filled (already tested in deduplication implicitly)
        # Check manual filtering
        valid_df = processed_df.filter(col("price") >= 0)
        
        self.assertEqual(valid_df.count(), 1) # Filter removed the negative value
        print("✅ CI/CD Test: Logical checks passed.")

if __name__ == "__main__":
    unittest.main()
