"""
================================================================================
Sales Data Loader Module
Purpose: Load sales dataset ONCE and reuse it across all modules
================================================================================
"""

from pyspark.sql import SparkSession, DataFrame


class SalesDataLoader:
    """
    ========================================================================
    Central Data Loader for Sales Dataset
    ========================================================================
    Loads data once and provides reusable Spark DataFrame
    ========================================================================
    """

    def __init__(self, file_path: str):
        """
        Parameters:
        ----------
        file_path : str
            Path to sales dataset (processed csv)
        """
        self.file_path = file_path
        self.spark = self._create_spark()
        self.df = None

    # =========================
    # CREATE SPARK SESSION
    # =========================
    def _create_spark(self):
        spark = SparkSession.builder \
            .appName("Sales_Data_Loader") \
            .getOrCreate()
        return spark
    # =========================
    # LOAD DATA ONCE
    # =========================
    def load_data(self) -> DataFrame:
        """
        Load dataset only once and cache it
        """
        if self.df is None:
            print("Loading Sales Dataset...")

            self.df = self.spark.read.csv(
                self.file_path,
                header=True,
                inferSchema=True
            )

            self.df.cache() 
            print("Sales Dataset Loaded Successfully!")

        return self.df
    # =========================
    # GET DATAFRAME
    # =========================
    def get_df(self):
        """
        Return cached dataframe
        """
        if self.df is None:
            return self.load_data()
        return self.df

