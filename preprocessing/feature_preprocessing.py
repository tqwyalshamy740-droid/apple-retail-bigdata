"""
===============================================================================
Sales Preprocessing 
===============================================================================
Purpose:
- Encode categorical features
- Assemble features
- Scale features
- Prepare dataset for ML
===============================================================================
"""

from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.ml import Pipeline
from sales_loader import SalesDataLoader


def run_preprocessing(path="data/processed/sales_cleaned.csv"):
    """
    Runs full preprocessing pipeline and returns ML-ready dataframe
    """

    loader = SalesDataLoader(path)
    df = loader.load_data()

    # =========================
    # SAFE DROP
    # =========================
    if "year_month" in df.columns:
        df = df.drop("year_month")

    df = df.na.fill(0)

    # =========================
    # STRING INDEXING
    # =========================
    product_indexer = StringIndexer(
        inputCol="product_id",
        outputCol="product_index",
        handleInvalid="keep"
    )

    store_indexer = StringIndexer(
        inputCol="store_id",
        outputCol="store_index",
        handleInvalid="keep"
    )

    # =========================
    # ONE HOT ENCODING
    # =========================
    product_encoder = OneHotEncoder(
        inputCol="product_index",
        outputCol="product_vec",
        handleInvalid="keep"
    )

    store_encoder = OneHotEncoder(
        inputCol="store_index",
        outputCol="store_vec",
        handleInvalid="keep"
    )

    # =========================
    # FEATURES
    # =========================
    feature_cols = [
        "product_vec",
        "store_vec",
        "year",
        "month",
        "day_of_week",
        "week_of_year",
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

    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features",
        handleInvalid="keep"
    )

    scaler = StandardScaler(
        inputCol="features",
        outputCol="scaled_features",
        withStd=True,
        withMean=False
    )

    # =========================
    # PIPELINE
    # =========================
    pipeline = Pipeline(stages=[
        product_indexer,
        store_indexer,
        product_encoder,
        store_encoder,
        assembler,
        scaler
    ])

    model = pipeline.fit(df)
    processed_df = model.transform(df)

    print("\nDATA READY FOR MODEL")
    processed_df.printSchema()

    return processed_df.select("scaled_features", "quantity")



"""
This dataset has been transformed into a Sparse Feature Vector
 with 181 dimensions after applying feature engineering and preprocessing steps.

The representation includes both categorical and numerical features:
- Categorical variables (e.g., product_id, store_id) were encoded using StringIndexer
 and OneHotEncoder.
- Numerical features were scaled using StandardScaler to ensure consistent feature 
magnitude.

The resulting sparse format stores only non-zero values, which improves memory efficiency
 and processing performance within Spark ML pipelines.

This final feature representation is fully prepared for machine learning model training
 and is suitable for regression models to predict sales quantity.
"""