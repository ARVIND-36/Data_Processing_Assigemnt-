# Task 1: Data Preprocessing Challenge (30%)

## Overview
This task focuses on cleaning and preprocessing raw datasets using Apache Spark or Flink, handling various data quality issues.

## Objectives
- Handle missing values
- Fix data type inconsistencies
- Remove duplicate records
- Normalize/Standardize data
- Perform feature engineering

## Files
- `data_preprocessing_spark.py` - Spark-based preprocessing implementation
- `data_preprocessing_flink.py` - Flink-based preprocessing implementation
- `requirements.txt` - Python dependencies

## Setup Instructions

### Prerequisites
```bash
pip install -r requirements.txt
```

### For Spark
```bash
# Install PySpark
pip install pyspark
```

### For Flink
```bash
# Install PyFlink
pip install apache-flink
```

## Usage

### Using Spark
```bash
python data_preprocessing_spark.py
```

### Using Flink
```bash
python data_preprocessing_flink.py
```

## Data Requirements
Place your raw dataset in a `data/` folder with the name `raw_data.csv`

## Output
Processed data will be saved to `data/processed_data.csv`

## Key Features Implemented

### 1. Missing Value Handling
- Mean imputation for numeric columns
- Mode imputation for categorical columns
- Drop rows with missing values (optional)

### 2. Data Type Consistency
- Automatic type detection
- Type conversion utilities
- Validation checks

### 3. Duplicate Removal
- Full row duplicate detection
- Subset-based duplicate removal
- Keeping first/last occurrence options

### 4. Normalization/Standardization
- StandardScaler for zero mean and unit variance
- MinMaxScaler for scaling to range [0,1]
- RobustScaler for handling outliers

### 5. Feature Engineering
- Creating derived features
- Binning continuous variables
- Encoding categorical variables
- Date/time feature extraction

## Customization
Update the following in the code based on your dataset:
- Column names in type mappings
- Columns to normalize
- Feature engineering logic
- Missing value strategies per column

## Performance Considerations
- Spark is recommended for datasets > 1GB
- Use appropriate partitioning for large datasets
- Monitor memory usage with Spark UI
