#!/bin/bash
# ================================================================================
# Apple Retail Sales Big Data Project - Environment Setup
# Engineer #1: Data Engineer
# ================================================================================

set -e  # Exit on error

echo "================================================================================"
echo "  Apple Retail Sales - Big Data Environment Setup"
echo "================================================================================"

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# ================================================================================
# 1. Check Python Version
# ================================================================================
print_status "Checking Python version..."
PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}')
REQUIRED_VERSION="3.8"

if [ "$(printf '%s\n' "$REQUIRED_VERSION" "$PYTHON_VERSION" | sort -V | head -n1)" = "$REQUIRED_VERSION" ]; then 
    print_status "Python version $PYTHON_VERSION is compatible ✓"
else
    print_error "Python 3.8+ required. Found: $PYTHON_VERSION"
    exit 1
fi

# ================================================================================
# 2. Create Virtual Environment
# ================================================================================
print_status "Creating virtual environment..."
if [ ! -d "venv" ]; then
    python3 -m venv venv
    print_status "Virtual environment created ✓"
else
    print_warning "Virtual environment already exists"
fi

# Activate virtual environment
source venv/bin/activate

# ================================================================================
# 3. Install Dependencies
# ================================================================================
print_status "Installing dependencies..."

# Upgrade pip
pip install --upgrade pip

# Core Big Data dependencies
pip install pyspark==3.5.0
pip install delta-spark==3.0.0

# Data processing
pip install pandas==2.1.4
pip install numpy==1.26.2

# Visualization (for EDA phase)
pip install matplotlib==3.8.2
pip install seaborn==0.13.0
pip install plotly==5.18.0

# ML libraries (for Engineer #3)
pip install scikit-learn==1.3.2

# Web framework (for Engineer #4)
pip install streamlit==1.29.0

# Utilities
pip install python-dotenv==1.0.0
pip install pyyaml==6.0.1

print_status "Dependencies installed successfully ✓"

# ================================================================================
# 4. Create Project Structure
# ================================================================================
print_status "Creating project directory structure..."

mkdir -p data/{raw,processed,external}
mkdir -p notebooks/{eda,experiments}
mkdir -p src/{acquisition,eda,modeling,deployment}
mkdir -p tests/{unit,integration}
mkdir -p config
mkdir -p logs
mkdir -p docs
mkdir -p models
mkdir -p dashboards

print_status "Directory structure created ✓"

# ================================================================================
# 5. Download Dataset from Kaggle
# ================================================================================
print_status "Setting up Kaggle dataset download..."

# Check if kaggle.json exists
if [ ! -f "~/.kaggle/kaggle.json" ]; then
    print_warning "Kaggle API credentials not found!"
    print_warning "Please place kaggle.json in ~/.kaggle/ directory"
    print_warning "Or manually download the dataset from:"
    print_warning "https://www.kaggle.com/datasets/amangarg08/apple-retail-sales-dataset"
else
    pip install kaggle
    kaggle datasets download -d amangarg08/apple-retail-sales-dataset -p data/raw/
    cd data/raw && unzip -o apple-retail-sales-dataset.zip && cd ../..
    print_status "Dataset downloaded successfully ✓"
fi

# ================================================================================
# 6. Create Configuration Files
# ================================================================================
print_status "Creating configuration files..."

# requirements.txt
cat > requirements.txt << 'EOF'
# Core Big Data
pyspark==3.5.0
delta-spark==3.0.0

# Data Processing
pandas==2.1.4
numpy==1.26.2

# Visualization
matplotlib==3.8.2
seaborn==0.13.0
plotly==5.18.0

# Machine Learning
scikit-learn==1.3.2

# Web Framework
streamlit==1.29.0

# Utilities
python-dotenv==1.0.0
pyyaml==6.0.1
kaggle==1.6.0
EOF

# .gitignore
cat > .gitignore << 'EOF'
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
venv/
env/

# Data
data/raw/*.csv
data/processed/
!data/raw/.gitkeep
!data/processed/.gitkeep

# Models
models/*.pkl
models/*.joblib

# Logs
logs/*.log

# IDE
.vscode/
.idea/
*.swp
*.swo

# OS
.DS_Store
Thumbs.db

# Kaggle
kaggle.json
EOF

# Spark configuration
cat > config/spark_config.yaml << 'EOF'
spark:
  app_name: "AppleRetailSales_BigData"
  master: "local[*]"

  executor:
    memory: "4g"
    cores: 4
    instances: 2

  driver:
    memory: "4g"
    maxResultSize: "2g"

  sql:
    shuffle_partitions: 200
    adaptive_enabled: true
    adaptive_coalescePartitions_enabled: true

  serializer: "org.apache.spark.serializer.KryoSerializer"

  storage:
    parquet_compression: "snappy"
    maxPartitionBytes: 134217728
    openCostInBytes: 4194304
EOF

print_status "Configuration files created ✓"

# ================================================================================
# 7. Initialize Git Repository
# ================================================================================
print_status "Initializing Git repository..."

if [ ! -d ".git" ]; then
    git init
    git branch -m main
    print_status "Git repository initialized ✓"
fi

# Create initial commit
if [ -z "$(git status --porcelain)" ]; then
    print_warning "Nothing to commit"
else
    git add .
    git commit -m "Initial commit: Project structure and environment setup

- Added data acquisition module
- Created project directory structure
- Added configuration files
- Set up dependency management"
    print_status "Initial commit created ✓"
fi

# ================================================================================
# 8. Validation
# ================================================================================
print_status "Running validation checks..."

python3 -c "import pyspark; print(f'PySpark version: {pyspark.__version__}')" || print_error "PySpark import failed"
python3 -c "import pandas; print(f'Pandas version: {pandas.__version__}')" || print_error "Pandas import failed"
python3 -c "import streamlit; print(f'Streamlit version: {streamlit.__version__}')" || print_error "Streamlit import failed"

print_status "Validation complete ✓"

# ================================================================================
# Setup Complete
# ================================================================================
echo ""
echo "================================================================================"
echo "  ✅ Environment Setup Complete!"
echo "================================================================================"
echo ""
echo "Next steps:"
echo "  1. Activate virtual environment: source venv/bin/activate"
echo "  2. Download dataset: kaggle datasets download -d amangarg08/apple-retail-sales-dataset"
echo "  3. Run data acquisition: python src/acquisition/data_acquisition.py"
echo "  4. Start EDA phase (Engineer #2)"
echo ""
echo "Project Structure:"
tree -L 2 -I 'venv|__pycache__|*.pyc' || find . -maxdepth 2 -not -path '*/\.*' -not -path '*/venv/*' | sort
echo ""
echo "================================================================================"
