"""
================================================================================
Sales Correlation Analysis Module
Purpose: Correlation analysis for engineered sales dataset
================================================================================
"""

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from sales_loader import SalesDataLoader

# =========================
# LOAD FEATURE ENGINEERED DATA
# =========================
loader = SalesDataLoader("data/processed/sales_fe.csv")
df = loader.load_data()

print("Loading Feature Engineered Dataset...")

# =========================
# SELECT NUMERICAL COLUMNS ONLY
# =========================
selected_columns = [
    "quantity",
    "is_weekend",
    "is_bulk_purchase",
    "high_demand_transaction",
    "store_total_sales",
    "store_avg_order",
    "store_transactions",
    "store_rank",
    "product_total_sales",
    "product_avg_sales",
    "product_frequency",
    "product_rank"
]

df_selected = df.select(selected_columns)

# =========================
# CONVERT TO PANDAS
# =========================
pdf = df_selected.toPandas()

print("Generating Correlation Matrix...")

# =========================
# CORRELATION MATRIX
# =========================
corr_matrix = pdf.corr()

print(corr_matrix)

# =========================
# HEATMAP VISUALIZATION
# =========================
plt.figure(figsize=(12, 8))

sns.heatmap(
    corr_matrix,
    annot=True,
    fmt=".2f",
    cmap="coolwarm"
)

plt.title("Correlation Heatmap")
plt.tight_layout()
plt.show()

print("Correlation Analysis Completed Successfully")


"""
Key Observations:

- Bulk purchases have
 the strongest positive impact on quantity, indicating that when customers buy in bulk, 
 the number of items per transaction increases significantly.

- High demand transactions also positively affect quantity, but with a moderate
 influence compared to bulk purchases.

- Product-level features are highly important, especially product_total_sales 
and product_frequency, which are strongly correlated, showing consistent product demand patterns.

- Product ranking is strongly negatively correlated with total product sales,
 meaning better-ranked products are associated with higher sales performance.

- Store-level features show very weak correlation with quantity, indicating that
 store behavior has limited effect on transaction quantity compared to product behavior.

- Time-based features such as weekend indicator have almost no impact on quantity, 
meaning sales behavior is stable across weekdays and weekends.

- Overall, product-driven features dominate the dataset and are the main drivers of
 sales quantity prediction.

"""