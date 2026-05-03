# 🍎 Apple Retail Sales - Big Data Project

## 📋 Project Overview

End-to-end Big Data solution for Apple Retail Sales analysis using **PySpark**, covering the complete data pipeline from acquisition to deployment.

- **Dataset**: Apple Retail Sales Dataset (>1M records) [^1^]
- **Tech Stack**: PySpark, Delta Lake, Streamlit, Pandas, Scikit-learn
- **Team Size**: 4 Engineers
- **Project Type**: Collaborative Big Data Engineering

---

## 👥 Team Structure & Responsibilities

| Engineer | Role | Primary Tasks | Bonus Tasks |
|----------|------|---------------|-------------|
| **#1** | **Data Engineer** | Data Acquisition, Environment Setup, Initial Ingestion, Basic Cleaning | Data Validation Framework |
| #2 | Data Analyst / Feature Engineer | EDA, Data Visualization, Advanced Cleaning, Feature Engineering | Automated Reporting |
| **#3** | **Machine Learning Engineer** | Model Development, Training, Comparison, Evaluation | Model Selection Interface |
| #4 | Deployment / UI Engineer | Streamlit UI, Model Integration, Dynamic Data Upload | Multi-Model Comparison UI |

---

## 🗂️ Dataset Description

The dataset contains 5 CSV files representing Apple retail operations worldwide [^2^]:

### Schema

| File | Records | Columns | Description |
|------|---------|---------|-------------|
| `sales.csv` | ~1M+ | 5 | Transaction records with sale date, quantity, foreign keys |
| `products.csv` | ~50 | 5 | Product details: name, category, launch date, price |
| `stores.csv` | ~25 | 4 | Store locations: name, city, country |
| `category.csv` | ~8 | 2 | Product categories |
| `warranty.csv` | ~50K | 4 | Warranty claims with repair status |

### Key Characteristics
- **Volume**: >1 million transaction records
- **Variety**: 5 related tables with different structures
- **Velocity**: Time-series sales data spanning multiple years
- **Veracity**: Synthetic dataset with realistic business patterns

---
```
apple-retail-bigdata/
│
├── 📁 data/
│   ├── raw/
│   ├── processed/
│   ├── external/
│
├── 📁 src/
│   ├── acquisition/      # Engineer #1: Data ingestion & understanding
│   │   ├── sales_loader.py # Engineer #2
│   │   ├── data_understanding.py # Engineer #2
│   │
│   ├── eda/              # Engineer #2: Analysis & visualization
│   │   ├── sales_eda.py
│   │   ├── sales_correlations.py
│   │
│   ├── preprocessing/    # Engineer #2 (feature_engineering , feature_cleaning , feature_preprocessing)
│   │   ├── feature_engineer.py
│   │   ├── feature_cleaning.py
│   │   ├── feature_preprocessing.py
│   │
│   ├── modeling/         # Engineer #3: Model development & evaluation
│   │   ├── Engineer3_Final_v2.ipynb
│   │
│   ├── runner.py
│
├── 📁 models/
│   ├── linear_regression/
│   ├── generalized_lr/
│   ├── decision_tree/
│   ├── random_forest/
│   ├── gbt/
│   ├── isotonic/
│   ├── evaluation_results.json
│   └── model_comparison.png
│
├── 📁 logs/
├── requirements.txt
├── README.md
├── .gitignore
```
## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- Java 8+ (for Spark)
- 8GB+ RAM recommended

### 1. Clone Repository
```bash
git clone https://github.com/your-org/apple-retail-bigdata.git
cd apple-retail-bigdata
```

### 2. Run Setup Script
```bash
chmod +x environment_setup.sh
./environment_setup.sh
```

### 3. Activate Environment
```bash
source venv/bin/activate
```

### 4. Download Dataset
```bash
kaggle datasets download -d amangarg08/apple-retail-sales-dataset -p data/raw/
cd data/raw && unzip apple-retail-sales-dataset.zip
```

### 5. Run Data Acquisition (Engineer #1)
```bash
python src/acquisition/data_acquisition.py
```

### 6. Run Feature Engineering (Engineer #2)
```bash
python runner.py
```

### 7. Run Model Training (Engineer #3)
```bash
jupyter nbconvert --to notebook --execute src/modeling/Engineer3_Final_v2.ipynb
```

---

## 📊 Pipeline Stages

### Stage 1: Data Acquisition (Engineer #1) ✅
- [x] Spark environment setup with optimized configurations
- [x] Multi-source data ingestion (CSV → Spark DataFrame)
- [x] Schema enforcement for type safety
- [x] Initial data profiling and quality assessment
- [x] Basic cleaning (deduplication, null handling, standardization)
- [x] Conversion to optimized Parquet format
- [x] Audit trail and metadata tracking

### Stage 2: EDA & Feature Engineering (Engineer #2) ✅
- [x] Statistical analysis and distributions
- [x] Correlation analysis
- [x] Time-series visualization
- [x] Advanced cleaning and outlier detection
- [x] Feature engineering (aggregations, encodings)

### Stage 3: Model Development (Engineer #3) ✅
- [x] Problem definition (regression — predict sales quantity)
- [x] Feature replication from Engineer #2 pipeline
- [x] Training 6 models with PySpark MLlib
- [x] Performance evaluation (RMSE, MAE, R2)
- [x] Model comparison and selection
- [x] Results exported for Engineer #4

### Stage 4: Deployment (Engineer #4) ⏳
- [ ] Streamlit UI development
- [ ] Model integration
- [ ] Dynamic data upload
- [ ] Multi-model selection interface
- [ ] Production deployment

---

## ⚙️ Spark Configuration

Key optimizations applied:

| Parameter | Value | Purpose |
|-----------|-------|---------|
| `spark.sql.adaptive.enabled` | true | Auto-optimize shuffle partitions |
| `spark.serializer` | Kryo | Faster serialization |
| `spark.sql.parquet.compression.codec` | snappy | Efficient compression |
| `spark.sql.files.maxPartitionBytes` | 128MB | Optimal partition size |

---

## 🧪 Testing

```bash
python -m pytest tests/

python -m pytest tests/unit/test_data_acquisition.py -v

python -m pytest --cov=src tests/
```

---

## 📈 Performance Benchmarks

| Operation | Before Optimization | After Optimization |
|-----------|-------------------|-------------------|
| Data Ingestion | ~45s | ~12s |
| Deduplication (1M rows) | ~23s | ~8s |
| Format Conversion | ~18s | ~5s |

---

## 🤝 Collaboration Guidelines

### Git Workflow
```bash
git checkout -b feature/engineer-1-acquisition

git add .
git commit -m "feat: add data acquisition pipeline

- Implemented Spark session singleton
- Added multi-source ingestion
- Created data profiling module
- Added unit tests"

git push origin feature/engineer-1-acquisition
```

### Commit Message Convention
- `feat:` New feature
- `fix:` Bug fix
- `docs:` Documentation
- `test:` Tests
- `refactor:` Code refactoring

---

## 📚 References

[^1^]: [Apple Retail Sales Dataset - Kaggle](https://www.kaggle.com/datasets/amangarg08/apple-retail-sales-dataset)
[^2^]: Dataset schema based on [Apple Retail Store Analysis by Daniel Gallo](https://medium.com/@daniel.gallo12/apple-retail-store-analysis-3f76913df5c3)

---

## 📝 License

This project is for educational purposes. Dataset is synthetic and sourced from Kaggle.

---

## 👨‍💻 Engineer #1 Deliverables

### Completed Tasks
1. ✅ **Environment Setup**: Virtual environment, dependency management, Spark configuration
2. ✅ **Data Acquisition**: Multi-source ingestion with schema enforcement
3. ✅ **Initial Cleaning**: Deduplication, null handling, standardization
4. ✅ **Data Profiling**: Comprehensive quality assessment
5. ✅ **Format Optimization**: CSV → Parquet conversion
6. ✅ **Testing**: Unit tests for all components
7. ✅ **Documentation**: Code comments, README, setup guide

### Files Created
- `src/acquisition/data_acquisition.py` - Main acquisition module
- `environment_setup.sh` - One-click environment setup
- `tests/unit/test_data_acquisition.py` - Unit tests
- `config/spark_config.yaml` - Spark configuration
- `requirements.txt` - Python dependencies

### Handoff Notes for Engineer #2
- All data saved in `data/processed/` as Parquet files
- Schemas are strictly enforced
- Audit columns (`_ingestion_timestamp`, `_source_file`) added
- Data quality profiles available in logs
- Ready for EDA and advanced feature engineering

---
## 🤖 Stage 2: EDA & Feature Engineering (Engineer #2) ✅

### Completed Tasks

1. ✅ **Data Loading**: Unified sales loader with schema validation (`sales_loader.py`)
2. ✅ **Data Understanding**: Statistical analysis and distributions (`data_understand.py`)
3. ✅ **Exploratory Data Analysis**: Time-series trends, sales patterns (`sales_eda.py`)
4. ✅ **Correlation Analysis**: Feature relationships and multicollinearity check (`sales_correlations.py`)
5. ✅ **Feature Engineering**: Date features, behavioral flags (`feature_engineer.py`)
6. ✅ **Feature Cleaning**: Null handling, column standardization (`feature_cleaning.py`)
7. ✅ **Feature Preprocessing**: Temporal split, encoding, scaling, ML-ready pipeline (`feature_preprocessing.py`)
8. ✅ **Pipeline Orchestration**: End-to-end runner (`runner.py`)

### Files Created

- `src/eda/sales_loader.py` — Data loading module
- `src/eda/data_understand.py` — Statistical profiling
- `src/eda/sales_eda.py` — EDA visualizations
- `src/eda/sales_correlations.py` — Correlation analysis
- `src/features/feature_engineer.py` — Feature engineering
- `src/features/feature_cleaning.py` — Feature cleaning
- `src/features/feature_preprocessing.py` — ML preprocessing pipeline
- `src/runner.py` — Pipeline orchestrator

### Handoff Notes for Engineer #3

- ML-ready data in `data/processed/sales_cleaned.csv`
- Train/Test split is **temporal** (80/20) 
- Features scaled via `StandardScaler` 
- Categorical encoding via `StringIndexer` + `OneHotEncoder`
- Target variable: `quantity` (regression)
- Feature vector: **181 dimensions** 
- Ready for model training
---

## 👨‍💻 Engineer #3 Deliverables

### Completed Tasks
1. ✅ **Problem Definition**: Regression task — predict `quantity` sold per transaction
2. ✅ **Feature Replication**: Full replication of Engineer #2 pipeline (feature engineering + preprocessing)
3. ✅ **Model Training**: 6 models trained using PySpark MLlib
4. ✅ **Model Evaluation**: RMSE, MAE, R2 computed for all models on held-out test set
5. ✅ **Model Comparison**: Visual comparison chart saved to `models/model_comparison.png`
6. ✅ **Results Export**: `evaluation_results.json` saved for Engineer #4
7. ✅ **Model Persistence**: All 6 models saved to `models/` directory

### Models Trained

| Model | Library | Features Used |
|-------|---------|---------------|
| LinearRegression | PySpark MLlib | scaled_features (181-dim sparse vector) |
| GeneralizedLinearRegression | PySpark MLlib | scaled_features (181-dim sparse vector) |
| DecisionTreeRegressor | PySpark MLlib | scaled_features (181-dim sparse vector) |
| RandomForestRegressor | PySpark MLlib | scaled_features (181-dim sparse vector) |
| GBTRegressor | PySpark MLlib | scaled_features (181-dim sparse vector) |
| IsotonicRegression | PySpark MLlib | scaled_features (181-dim sparse vector) |

### Evaluation Results

| Model | RMSE | MAE | R2 |
|-------|------|-----|----|
| LinearRegression | 1.2267 | 1.0038 | 0.8179 |
| GeneralizedLR | 1.2269 | 1.0047 | 0.8178 |
| RandomForest | 1.2278 | 1.0107 | 0.8176 |
| GBT | 1.2286 | 1.0079 | 0.8173 |
| DecisionTree | 1.2289 | 1.0050 | 0.8172 |
| IsotonicRegression | 2.8745 | 2.5024 | -0.0000 |

### Best Model
**LinearRegression** achieved the lowest RMSE (1.2267) and highest R2 (0.8179), explaining **81.79%** of variance in sales quantity. Linear models outperformed tree-based models because Engineer #2's preprocessing pipeline (OHE + StandardScaler) linearized the feature space, making linear models naturally more effective.

### Files Created
- `src/modeling/Engineer3_Final_v2.ipynb` - Full training and evaluation notebook
- `models/linear_regression/` - Saved LinearRegression model
- `models/generalized_lr/` - Saved GeneralizedLinearRegression model
- `models/decision_tree/` - Saved DecisionTreeRegressor model
- `models/random_forest/` - Saved RandomForestRegressor model
- `models/gbt/` - Saved GBTRegressor model
- `models/isotonic/` - Saved IsotonicRegression model
- `models/evaluation_results.json` - Full evaluation metrics for Engineer #4
- `models/model_comparison.png` - Visual comparison chart

### Handoff Notes for Engineer #4
- All 6 models saved in `models/` directory, loadable via `PipelineModel.load(path)`
- `evaluation_results.json` contains model paths, feature columns, target column, and all metrics
- Target column is `quantity`
- Feature vector column is `scaled_features` (181-dim sparse vector from Engineer #2 preprocessing)
- Best model path: `models/linear_regression`
- Input data must pass through Engineer #2 preprocessing pipeline before inference
