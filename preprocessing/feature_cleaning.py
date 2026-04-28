"""
===============================================================================
Feature Cleaning
Purpose: Clean feature-engineered dataset 
===============================================================================
"""

from pyspark.sql.functions import concat, lpad, lit, col
from sales_loader import SalesDataLoader


def run_cleaning(path="data/processed/sales_fe.csv"):
    """
    Runs cleaning steps and returns cleaned dataframe
    """

    loader = SalesDataLoader(path)
    df = loader.load_data()

    print("DATA LOADED SUCCESSFULLY")

    # =========================
    # DROP UNUSED COLUMNS (SAFE DROP)
    # =========================
    cols_to_drop = ["_ingestion_timestamp", "_source_file", "sale_id"]

    existing_cols = [c for c in cols_to_drop if c in df.columns]
    if existing_cols:
        df = df.drop(*existing_cols)

    # =========================
    # FIX / CREATE YEAR_MONTH
    # =========================
    if "year" in df.columns and "month" in df.columns:
        df = df.withColumn(
            "year_month",
            concat(
                col("year").cast("string"),
                lit("-"),
                lpad(col("month").cast("string"), 2, "0")
            )
        )

    # =========================
    # HANDLE MISSING VALUES
    # =========================
    df = df.na.fill(0)

    # =========================
    # FINAL CHECK
    # =========================
    print("\nCLEANED DATA SCHEMA")
    df.printSchema()

    print("\nSAMPLE DATA")
    df.show(5, truncate=False)

    print("CLEANING COMPLETED SUCCESSFULLY")

    df.toPandas().to_csv("data/processed/sales_cleaned.csv", index=False)