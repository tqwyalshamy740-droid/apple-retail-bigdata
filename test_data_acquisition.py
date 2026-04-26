"""
================================================================================
Unit Tests for Data Acquisition Module
Engineer #1: Data Engineer
================================================================================
"""

import unittest
import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src', 'acquisition'))

from data_acquisition import (
    SparkEnvironment, 
    DataAcquisitionEngineer, 
    DataSourceConfig
)


class TestSparkEnvironment(unittest.TestCase):
    """Test Spark session management"""

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkEnvironment.get_session(app_name="TestSession")

    @classmethod
    def tearDownClass(cls):
        SparkEnvironment.stop_session()

    def test_session_creation(self):
        """Test Spark session is created successfully"""
        self.assertIsNotNone(self.spark)
        self.assertFalse(self.spark.sparkContext._jsc.sc().isStopped())

    def test_session_singleton(self):
        """Test singleton pattern - same session returned"""
        spark2 = SparkEnvironment.get_session()
        self.assertEqual(self.spark, spark2)

    def test_spark_version(self):
        """Test Spark version is 3.x"""
        version = self.spark.version
        self.assertTrue(version.startswith("3."))


class TestDataAcquisitionEngineer(unittest.TestCase):
    """Test Data Acquisition functionality"""

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkEnvironment.get_session(app_name="TestAcquisition")
        cls.engineer = DataAcquisitionEngineer(base_path="tests/test_data")

        # Create test data
        os.makedirs("tests/test_data", exist_ok=True)

        # Create sample CSV
        sample_data = """sale_id,sale_date,store_id,product_id,quantity
S001,2023-01-01,ST01,P001,2
S002,2023-01-02,ST01,P002,1
S003,2023-01-03,ST02,P001,3
S001,2023-01-01,ST01,P001,2
,2023-01-04,ST03,P003,1
S005,2023-01-05,,P001,2"""

        with open("tests/test_data/test_sales.csv", "w") as f:
            f.write(sample_data)

    @classmethod
    def tearDownClass(cls):
        SparkEnvironment.stop_session()

    def test_schema_definition(self):
        """Test schema is correctly defined"""
        schema = self.engineer._get_schema("sales")
        self.assertIsNotNone(schema)
        self.assertEqual(len(schema.fields), 5)
        self.assertEqual(schema["sale_id"].dataType, StringType())
        self.assertEqual(schema["quantity"].dataType, IntegerType())

    def test_data_ingestion(self):
        """Test data ingestion from CSV"""
        config = DataSourceConfig(
            name="test_sales",
            path="tests/test_data/test_sales.csv",
            custom_schema=self.engineer._get_schema("sales")
        )

        df = self.engineer.ingest_dataset(config)
        self.assertIsNotNone(df)
        self.assertIn("test_sales", self.engineer.dataframes)

        # Should have 6 rows (including duplicates and nulls for testing)
        self.assertEqual(df.count(), 6)

    def test_basic_cleaning(self):
        """Test basic cleaning operations"""
        # Create test DataFrame
        data = [("S001", "2023-01-01", "ST01", "P001", 2),
                ("S001", "2023-01-01", "ST01", "P001", 2),  # Duplicate
                ("S002", "2023-01-02", "ST01", "P002", 1),
                (None, "2023-01-03", "ST02", "P001", 3)]     # Null ID

        columns = ["sale_id", "sale_date", "store_id", "product_id", "quantity"]
        df = self.spark.createDataFrame(data, columns)

        cleaned_df = self.engineer.perform_basic_cleaning(df, "test")

        # Should remove 1 duplicate and 1 null row = 2 rows removed
        self.assertEqual(cleaned_df.count(), 2)

        # Should have audit columns
        self.assertIn("_ingestion_timestamp", cleaned_df.columns)
        self.assertIn("_source_file", cleaned_df.columns)

    def test_data_profile(self):
        """Test data profile generation"""
        data = [("S001", "2023-01-01", "ST01", "P001", 2),
                (None, "2023-01-02", "ST01", "P002", 1)]
        columns = ["sale_id", "sale_date", "store_id", "product_id", "quantity"]
        df = self.spark.createDataFrame(data, columns)

        profile = self.engineer.generate_data_profile(df, "test")

        self.assertEqual(profile["total_records"], 2)
        self.assertEqual(profile["columns"]["sale_id"]["null_count"], 1)
        self.assertEqual(profile["columns"]["sale_id"]["null_percentage"], 50.0)


class TestDataSourceConfig(unittest.TestCase):
    """Test DataSourceConfig dataclass"""

    def test_default_values(self):
        """Test default configuration values"""
        config = DataSourceConfig(name="test", path="/test/path")
        self.assertEqual(config.file_format, "csv")
        self.assertEqual(config.delimiter, ",")
        self.assertTrue(config.header)
        self.assertFalse(config.infer_schema)

    def test_custom_values(self):
        """Test custom configuration values"""
        config = DataSourceConfig(
            name="test",
            path="/test/path",
            file_format="json",
            delimiter=";",
            header=False,
            infer_schema=True
        )
        self.assertEqual(config.file_format, "json")
        self.assertEqual(config.delimiter, ";")
        self.assertFalse(config.header)
        self.assertTrue(config.infer_schema)


if __name__ == "__main__":
    unittest.main()
