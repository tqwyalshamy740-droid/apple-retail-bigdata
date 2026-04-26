"""
================================================================================
Apple Retail Sales - Data Acquisition Module
Engineer #1: Data Engineer Role
Project: End-to-End Big Data Solution with PySpark
Dataset: Apple Retail Sales Dataset (>1M records)
Source: Kaggle (amangarg08)
================================================================================
"""

import os
import logging
from datetime import datetime
from typing import Optional, List, Dict
from dataclasses import dataclass

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DateType, DoubleType, TimestampType
)
from pyspark.sql.functions import (
    col, count, when, isnan, lit, current_timestamp,
    monotonically_increasing_id, regexp_replace, trim,
    lower, upper, to_date, year, month, dayofmonth
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/data_acquisition_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class DataSourceConfig:
    """Configuration for data sources"""
    name: str
    path: str
    file_format: str = "csv"
    delimiter: str = ","
    header: bool = True
    infer_schema: bool = False
    custom_schema: Optional[StructType] = None


class SparkEnvironment:
    """
    Singleton Pattern for Spark Session Management
    Ensures single SparkContext across the application
    """
    _instance: Optional[SparkSession] = None

    @classmethod
    def get_session(
        cls,
        app_name: str = "AppleRetailSales_BigData",
        master: str = "local[*]",
        executor_memory: str = "4g",
        driver_memory: str = "4g",
        shuffle_partitions: int = 200
    ) -> SparkSession:
        """Initialize or retrieve existing Spark session with optimized configs"""
        if cls._instance is None or cls._instance.sparkContext._jsc.sc().isStopped():
            logger.info(f"Initializing Spark Session: {app_name}")

            cls._instance = (SparkSession.builder
                .appName(app_name)
                .master(master)
                .config("spark.executor.memory", executor_memory)
                .config("spark.driver.memory", driver_memory)
                .config("spark.sql.shuffle.partitions", shuffle_partitions)
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.sql.parquet.compression.codec", "snappy")
                .config("spark.sql.files.maxPartitionBytes", "134217728")  # 128MB
                .config("spark.sql.files.openCostInBytes", "4194304")      # 4MB
                .getOrCreate())

            # Set log level
            cls._instance.sparkContext.setLogLevel("WARN")
            logger.info("Spark Session initialized successfully")

        return cls._instance

    @classmethod
    def stop_session(cls):
        """Gracefully stop Spark session"""
        if cls._instance and not cls._instance.sparkContext._jsc.sc().isStopped():
            cls._instance.stop()
            cls._instance = None
            logger.info("Spark Session stopped")


class DataAcquisitionEngineer:
    """
    Data Acquisition Engineer (Engineer #1)
    Responsible for: Data Collection, Environment Setup, Initial Ingestion, Basic Cleaning
    """

    def __init__(self, base_path: str = "data/raw"):
        self.base_path = base_path
        self.spark = SparkEnvironment.get_session()
        self.dataframes: Dict[str, DataFrame] = {}
        self.ingestion_metadata: Dict = {}

        # Ensure directories exist
        os.makedirs(base_path, exist_ok=True)
        os.makedirs("logs", exist_ok=True)
        os.makedirs("data/processed", exist_ok=True)

    def _get_schema(self, dataset_name: str) -> StructType:
        """Define explicit schemas for type safety and performance"""
        schemas = {
            "sales": StructType([
                StructField("sale_id", StringType(), False),
                StructField("sale_date", StringType(), False),
                StructField("store_id", StringType(), False),
                StructField("product_id", StringType(), False),
                StructField("quantity", IntegerType(), False)
            ]),
            "products": StructType([
                StructField("product_id", StringType(), False),
                StructField("product_name", StringType(), False),
                StructField("category_id", StringType(), False),
                StructField("launch_date", StringType(), True),
                StructField("price", DoubleType(), False)
            ]),
            "stores": StructType([
                StructField("store_id", StringType(), False),
                StructField("store_name", StringType(), False),
                StructField("city", StringType(), False),
                StructField("country", StringType(), False)
            ]),
            "category": StructType([
                StructField("category_id", StringType(), False),
                StructField("category_name", StringType(), False)
            ]),
            "warranty": StructType([
                StructField("claim_id", StringType(), False),
                StructField("claim_date", StringType(), True),
                StructField("sale_id", StringType(), False),
                StructField("repair_status", StringType(), True)
            ])
        }
        return schemas.get(dataset_name.lower(), None)

    def ingest_dataset(self, config: DataSourceConfig) -> DataFrame:
        """
        Ingest a single dataset with comprehensive logging and validation

        Args:
            config: DataSourceConfig object containing ingestion parameters

        Returns:
            Spark DataFrame
        """
        logger.info(f"Starting ingestion for: {config.name}")
        start_time = datetime.now()

        try:
            # Use custom schema if provided, otherwise infer
            schema = config.custom_schema or self._get_schema(config.name)

            reader = self.spark.read                 .format(config.file_format)                 .option("header", str(config.header).lower())                 .option("delimiter", config.delimiter)                 .option("mode", "PERMISSIVE")                 .option("columnNameOfCorruptRecord", "_corrupt_record")

            if schema and not config.infer_schema:
                reader = reader.schema(schema)
            else:
                reader = reader.option("inferSchema", "true")

            df = reader.load(config.path)

            # Capture metadata
            record_count = df.count()
            column_count = len(df.columns)

            self.ingestion_metadata[config.name] = {
                "source_path": config.path,
                "record_count": record_count,
                "column_count": column_count,
                "columns": df.columns,
                "ingestion_time": datetime.now().isoformat(),
                "duration_seconds": (datetime.now() - start_time).total_seconds()
            }

            logger.info(f"Successfully ingested {config.name}: {record_count:,} records, {column_count} columns")
            self.dataframes[config.name] = df
            return df

        except Exception as e:
            logger.error(f"Failed to ingest {config.name}: {str(e)}")
            raise

    def perform_basic_cleaning(self, df: DataFrame, dataset_name: str) -> DataFrame:
        """
        Initial cleaning operations:
        - Remove exact duplicates
        - Handle null values in critical columns
        - Standardize string formats
        - Validate data types
        """
        logger.info(f"Performing basic cleaning on: {dataset_name}")
        initial_count = df.count()

        # 1. Remove exact duplicates
        df_clean = df.dropDuplicates()
        after_dedup = df_clean.count()
        duplicates_removed = initial_count - after_dedup

        # 2. Trim whitespace from string columns
        for field in df_clean.schema.fields:
            if field.dataType == StringType():
                df_clean = df_clean.withColumn(
                    field.name, 
                    trim(col(field.name))
                )

        # 3. Standardize case for key identifier columns
        id_columns = [c for c in df_clean.columns if '_id' in c.lower()]
        for id_col in id_columns:
            df_clean = df_clean.withColumn(id_col, upper(col(id_col)))

        # 4. Remove rows with nulls in critical identifier columns
        for id_col in id_columns:
            null_count = df_clean.filter(col(id_col).isNull()).count()
            if null_count > 0:
                logger.warning(f"Found {null_count:,} null values in {id_col}")
                df_clean = df_clean.filter(col(id_col).isNotNull())

        # 5. Add audit columns
        df_clean = df_clean             .withColumn("_ingestion_timestamp", current_timestamp())             .withColumn("_source_file", lit(dataset_name))

        final_count = df_clean.count()
        logger.info(f"Cleaning complete for {dataset_name}: "
                   f"Removed {duplicates_removed:,} duplicates, "
                   f"Final count: {final_count:,}")

        return df_clean

    def generate_data_profile(self, df: DataFrame, dataset_name: str) -> Dict:
        """Generate comprehensive data quality profile"""
        logger.info(f"Generating data profile for: {dataset_name}")

        profile = {
            "dataset_name": dataset_name,
            "total_records": df.count(),
            "total_columns": len(df.columns),
            "columns": {}
        }

        for column in df.columns:
            col_profile = {
                "type": str(df.schema[column].dataType),
                "null_count": df.filter(col(column).isNull()).count(),
                "null_percentage": round(
                    (df.filter(col(column).isNull()).count() / profile["total_records"]) * 100, 2
                ) if profile["total_records"] > 0 else 0
            }

            # Add distinct count for categorical columns
            if "_id" in column.lower() or column in ["country", "city", "category_name", "repair_status"]:
                col_profile["distinct_count"] = df.select(column).distinct().count()

            profile["columns"][column] = col_profile

        return profile

    def save_processed_data(self, df: DataFrame, dataset_name: str, format: str = "parquet"):
        """Save cleaned data in optimized format for downstream processing"""
        output_path = f"data/processed/{dataset_name}"

        logger.info(f"Saving {dataset_name} to {output_path} as {format}")

        if format == "parquet":
            df.write                 .mode("overwrite")                 .option("compression", "snappy")                 .parquet(output_path)
        elif format == "delta":
            df.write                 .format("delta")                 .mode("overwrite")                 .save(output_path)

        logger.info(f"Successfully saved {dataset_name}")

    def run_acquisition_pipeline(self, data_sources: List[DataSourceConfig]) -> Dict[str, DataFrame]:
        """
        Execute complete acquisition pipeline
        """
        logger.info("=" * 80)
        logger.info("STARTING DATA ACQUISITION PIPELINE")
        logger.info("=" * 80)

        for source in data_sources:
            # Ingest
            df = self.ingest_dataset(source)

            # Profile raw data
            raw_profile = self.generate_data_profile(df, f"{source.name}_raw")

            # Clean
            df_clean = self.perform_basic_cleaning(df, source.name)

            # Profile cleaned data
            clean_profile = self.generate_data_profile(df_clean, f"{source.name}_cleaned")

            # Save
            self.save_processed_data(df_clean, source.name)

            # Store reference
            self.dataframes[source.name] = df_clean

        logger.info("=" * 80)
        logger.info("DATA ACQUISITION PIPELINE COMPLETED")
        logger.info("=" * 80)

        return self.dataframes


# ================================================================================
# EXECUTION SCRIPT
# ================================================================================

def main():
    """Main execution function for Engineer #1"""

    # Initialize engineer
    engineer = DataAcquisitionEngineer(base_path="data/raw")

    # Define data sources configuration
    data_sources = [
        DataSourceConfig(
            name="sales",
            path="data/raw/sales.csv",
            custom_schema=engineer._get_schema("sales")
        ),
        DataSourceConfig(
            name="products",
            path="data/raw/products.csv",
            custom_schema=engineer._get_schema("products")
        ),
        DataSourceConfig(
            name="stores",
            path="data/raw/stores.csv",
            custom_schema=engineer._get_schema("stores")
        ),
        DataSourceConfig(
            name="category",
            path="data/raw/category.csv",
            custom_schema=engineer._get_schema("category")
        ),
        DataSourceConfig(
            name="warranty",
            path="data/raw/warranty.csv",
            custom_schema=engineer._get_schema("warranty")
        )
    ]

    # Run pipeline
    dataframes = engineer.run_acquisition_pipeline(data_sources)

    # Print summary
    print("\n" + "=" * 80)
    print("DATA ACQUISITION SUMMARY")
    print("=" * 80)
    for name, meta in engineer.ingestion_metadata.items():
        print(f"\n{name.upper()}:")
        print(f"  Records: {meta['record_count']:,}")
        print(f"  Columns: {meta['column_count']}")
        print(f"  Duration: {meta['duration_seconds']:.2f}s")

    return dataframes


if __name__ == "__main__":
    main()
