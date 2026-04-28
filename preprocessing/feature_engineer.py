"""
===============================================================================
Feature Engineering 
===============================================================================
"""

from pyspark.sql.functions import (
    to_date, year, month, dayofweek, weekofyear,
    when, col, concat, lit, sum as _sum, avg as _avg, count
)
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank, desc

from sales_loader import SalesDataLoader


def run_feature_engineering(path="data/processed/sales.csv"):
    """
    Runs full feature engineering pipeline and returns dataframe
    """

    loader = SalesDataLoader(path)
    df = loader.load_data()

    # =========================
    # DATE TRANSFORMATION
    # =========================
    df = df.withColumn("sale_date", to_date("sale_date", "dd-MM-yyyy"))

    df = df.withColumn("year", year("sale_date")) \
           .withColumn("month", month("sale_date")) \
           .withColumn("day_of_week", dayofweek("sale_date")) \
           .withColumn("week_of_year", weekofyear("sale_date"))

    # =========================
    # TIME FEATURES
    # =========================
    df = df.withColumn(
        "is_weekend",
        when(col("day_of_week").isin([1, 7]), 1).otherwise(0)
    )

    df = df.withColumn(
        "year_month",
        concat(col("year").cast("string"), lit("-"), col("month").cast("string"))
    )

    # =========================
    # BEHAVIOR FEATURES
    # =========================
    df = df.withColumn(
        "is_bulk_purchase",
        when(col("quantity") >= 5, 1).otherwise(0)
    )

    df = df.withColumn(
        "high_demand_transaction",
        when(col("quantity") >= 10, 1).otherwise(0)
    )

    # =========================
    # STORE FEATURES
    # =========================
    store_features = df.groupBy("store_id").agg(
        _sum("quantity").alias("store_total_sales"),
        _avg("quantity").alias("store_avg_order"),
        count("*").alias("store_transactions")
    )

    window_s = Window.orderBy(desc("store_total_sales"))

    store_features = store_features.withColumn(
        "store_rank",
        dense_rank().over(window_s)
    )

    df = df.join(store_features, "store_id", "left")

    # =========================
    # PRODUCT FEATURES
    # =========================
    product_features = df.groupBy("product_id").agg(
        _sum("quantity").alias("product_total_sales"),
        _avg("quantity").alias("product_avg_sales"),
        count("*").alias("product_frequency")
    )

    window_p = Window.orderBy(desc("product_total_sales"))

    product_features = product_features.withColumn(
        "product_rank",
        dense_rank().over(window_p)
    )

    df = df.join(product_features, "product_id", "left")

    # =========================
    # CLEAN NULLS
    # =========================
    df = df.na.fill(0)

    # =========================
    # DEBUG (OPTIONAL)
    # =========================
    print("\nFEATURE ENGINEERING COMPLETED")
    print("Rows:", df.count())
    print("Columns:", len(df.columns))

    return df.toPandas().to_csv("data/processed/sales_fe.csv", index=False)