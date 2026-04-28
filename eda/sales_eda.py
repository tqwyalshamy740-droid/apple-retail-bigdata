# """
# ===============================================================================
# EDA Analysis Module 
# ===============================================================================

# Purpose:
# --------
# This module is responsible for performing Exploratory Data Analysis (EDA)
# on the integrated master dataset.

# It helps in:
# - Understanding data distribution
# - Detecting patterns and relationships
# - Identifying outliers
# - Extracting time-based insights
# - Measuring correlation between variables

# ===============================================================================

import matplotlib.pyplot as plt
from pyspark.sql.functions import col, sum as _sum, count


# =========================
# LOAD DATA USING DATA LOADER
# =========================
from sales_loader import SalesDataLoader
loader = SalesDataLoader("data/processed/sales_fe.csv")
df = loader.load_data()
# =========================
# BASIC PREPARATION
# =========================
df = df.dropDuplicates()


# ============================================================
# SALES OVER TIME
# ============================================================
def sales_over_time():

    print("Generating Sales Over Time Plot...")

    data = df.groupBy("sale_date").agg(
        _sum("quantity").alias("total_sales")
    ).orderBy("sale_date")

    pdf = data.toPandas()

    plt.figure(figsize=(12,5))
    plt.plot(pdf["sale_date"], pdf["total_sales"])
    plt.title("Sales Over Time")
    plt.xlabel("Date")
    plt.ylabel("Total Sales")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()


# ============================================================
# TOP STORES
# ============================================================
def top_stores():

    print("Generating Top Stores Plot...")

    data = df.groupBy("store_id").agg(
        _sum("quantity").alias("total_sales")
    ).orderBy(col("total_sales").desc()).limit(10)

    pdf = data.toPandas()

    plt.figure(figsize=(10,5))
    plt.bar(pdf["store_id"], pdf["total_sales"])
    plt.title("Top 10 Stores by Sales")
    plt.xlabel("Store ID")
    plt.ylabel("Total Sales")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()


# ============================================================
# TOP PRODUCTS
# ============================================================
def top_products():

    print("Generating Top Products Plot...")

    data = df.groupBy("product_id").agg(
        _sum("quantity").alias("total_sales")
    ).orderBy(col("total_sales").desc()).limit(10)

    pdf = data.toPandas()

    plt.figure(figsize=(10,5))
    plt.bar(pdf["product_id"], pdf["total_sales"])
    plt.title("Top 10 Products by Sales")
    plt.xlabel("Product ID")
    plt.ylabel("Total Sales")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()


# ============================================================
# QUANTITY DISTRIBUTION
# ============================================================
def quantity_distribution():

    print("Generating Quantity Distribution Plot...")

    pdf = df.select("quantity").toPandas()

    plt.figure(figsize=(8,5))
    plt.hist(pdf["quantity"], bins=30)
    plt.title("Distribution of Quantity")
    plt.xlabel("Quantity")
    plt.ylabel("Frequency")
    plt.tight_layout()
    plt.show()


# ============================================================
# WEEKEND VS WEEKDAY
# ============================================================
def weekend_analysis():

    print("Generating Weekend vs Weekday Analysis...")

    data = df.groupBy("is_weekend").agg(
        _sum("quantity").alias("total_sales")
    )

    pdf = data.toPandas()

    plt.figure(figsize=(6,5))
    plt.bar(["Weekday (0)", "Weekend (1)"], pdf["total_sales"])
    plt.title("Weekend vs Weekday Sales")
    plt.ylabel("Total Sales")
    plt.tight_layout()
    plt.show()
"""
Sales significantly increase during weekends compared to weekdays,
which indicates stronger customer purchasing behavior on weekends.
This may be due to higher customer availability and shopping activity.
"""

# ============================================================
# MAIN EXECUTION
# ============================================================
if __name__ == "__main__":

    print("\n====================================")
    print("STARTING EDA VISUALIZATION")
    print("====================================\n")

    sales_over_time()
    top_stores()
    top_products()
    quantity_distribution()
    weekend_analysis()

    print("\nEDA COMPLETED SUCCESSFULLY")

