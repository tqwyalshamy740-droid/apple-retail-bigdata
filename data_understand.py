"""
============================================================================
DATA UNDERSTANDING 
============================================================================
Purpose:
--------
This module is responsible for exploring and understanding the dataset
before any preprocessing or modeling steps.

Main Responsibilities:
----------------------
- Explore dataset structure
- Check data schema and types
- Detect missing values
- Detect duplicate rows
- Basic data quality inspection
============================================================================
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum as _sum

from sales_loader import SalesDataLoader


class DataUnderstanding:
    """
    =========================================================================
    Data Understanding Class
    =========================================================================
    This class provides basic exploratory functions to analyze dataset quality
    and structure before feature engineering or preprocessing.
    =========================================================================
    """

    def __init__(self, df: DataFrame):
        self.df = df

    # ============================================================
    # DATA OVERVIEW
    # ============================================================
    def overview(self):
        """Display dataset shape and schema"""

        print("\nDATA OVERVIEW")
        print("=" * 60)

        print(f"Rows    : {self.df.count()}")
        print(f"Columns : {len(self.df.columns)}")

        print("\nSCHEMA:")
        self.df.printSchema()

    # ============================================================
    # SAMPLE DATA
    # ============================================================
    def sample(self, n=5):
        """Show sample rows from dataset"""

        print("\nSAMPLE DATA")
        print("=" * 60)

        self.df.show(n, truncate=False)

    # ============================================================
    # MISSING VALUES CHECK
    # ============================================================
    def missing_values(self):
        """Check missing values per column"""

        print("\nMISSING VALUES")
        print("=" * 60)

        missing_df = self.df.select([
            _sum(col(c).isNull().cast("int")).alias(c)
            for c in self.df.columns
        ])

        missing_df.show(truncate=False)

    # ============================================================
    # DUPLICATES CHECK
    # ============================================================
    def duplicates(self):
        """Check duplicate rows in dataset"""

        print("\nDUPLICATES CHECK")
        print("=" * 60)

        total_rows = self.df.count()
        unique_rows = self.df.distinct().count()

        print(f"Total rows    : {total_rows}")
        print(f"Duplicate rows: {total_rows - unique_rows}")


# ============================================================
# MAIN EXECUTION
# ============================================================
if __name__ == "__main__":

    # =========================
    # LOAD DATA
    # =========================
    loader = SalesDataLoader("data/processed/sales.csv")
    df = loader.load_data()

    # =========================
    # RUN ANALYSIS
    # =========================
    du = DataUnderstanding(df)

    du.overview()
    du.sample()
    du.missing_values()
    du.duplicates()

    print("\nDATA UNDERSTANDING COMPLETED SUCCESSFULLY")