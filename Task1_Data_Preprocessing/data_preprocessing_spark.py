"""
Task 1: Data Preprocessing Challenge (30%)
Using Apache Spark for data cleaning and preprocessing
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, mean, stddev, isnan, count
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.ml.feature import StandardScaler, VectorAssembler
import sys

class DataPreprocessor:
    def __init__(self, app_name="DataPreprocessing"):
        """Initialize Spark Session"""
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.driver.memory", "4g") \
            .getOrCreate()
        
    def load_data(self, file_path, file_format="csv", header=True, infer_schema=True):
        """Load data from various sources"""
        try:
            df = self.spark.read.format(file_format) \
                .option("header", header) \
                .option("inferSchema", infer_schema) \
                .load(file_path)
            print(f"Data loaded successfully. Shape: {df.count()} rows, {len(df.columns)} columns")
            return df
        except Exception as e:
            print(f"Error loading data: {e}")
            sys.exit(1)
    
    def show_data_info(self, df):
        """Display data information"""
        print("\n=== Data Schema ===")
        df.printSchema()
        
        print("\n=== First 5 rows ===")
        df.show(5)
        
        print("\n=== Data Statistics ===")
        df.describe().show()
        
        print("\n=== Missing Values Count ===")
        df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()
    
    def handle_missing_values(self, df, strategy="mean", columns=None):
        """
        Handle missing values in the dataset
        Strategies: 'mean', 'median', 'mode', 'drop', 'fill_zero'
        """
        print(f"\n=== Handling Missing Values (Strategy: {strategy}) ===")
        
        if columns is None:
            columns = df.columns
        
        if strategy == "drop":
            df_clean = df.dropna()
        elif strategy == "fill_zero":
            df_clean = df.fillna(0)
        elif strategy == "mean":
            # Calculate mean for numeric columns
            numeric_cols = [field.name for field in df.schema.fields 
                          if isinstance(field.dataType, (DoubleType, IntegerType))]
            
            for col_name in numeric_cols:
                if col_name in columns:
                    mean_val = df.select(mean(col(col_name))).first()[0]
                    df = df.fillna({col_name: mean_val})
            df_clean = df
        else:
            df_clean = df.fillna(0)  # Default fallback
        
        print(f"Rows after handling missing values: {df_clean.count()}")
        return df_clean
    
    def fix_data_types(self, df, type_mappings):
        """
        Fix data type inconsistencies
        type_mappings: dict like {'column_name': 'integer', 'column2': 'double'}
        """
        print("\n=== Fixing Data Types ===")
        
        for col_name, dtype in type_mappings.items():
            if col_name in df.columns:
                if dtype.lower() == 'integer':
                    df = df.withColumn(col_name, col(col_name).cast(IntegerType()))
                elif dtype.lower() == 'double':
                    df = df.withColumn(col_name, col(col_name).cast(DoubleType()))
                print(f"Changed {col_name} to {dtype}")
        
        return df
    
    def remove_duplicates(self, df, subset=None):
        """Remove duplicate rows"""
        print("\n=== Removing Duplicates ===")
        initial_count = df.count()
        
        if subset:
            df_clean = df.dropDuplicates(subset)
        else:
            df_clean = df.dropDuplicates()
        
        final_count = df_clean.count()
        print(f"Removed {initial_count - final_count} duplicate rows")
        print(f"Final row count: {final_count}")
        
        return df_clean
    
    def normalize_data(self, df, columns_to_normalize):
        """
        Normalize/Standardize numeric columns using StandardScaler
        """
        print("\n=== Normalizing Data ===")
        
        # Create feature vector
        assembler = VectorAssembler(
            inputCols=columns_to_normalize,
            outputCol="features"
        )
        df_vector = assembler.transform(df)
        
        # Apply StandardScaler
        scaler = StandardScaler(
            inputCol="features",
            outputCol="scaled_features",
            withStd=True,
            withMean=True
        )
        
        scaler_model = scaler.fit(df_vector)
        df_scaled = scaler_model.transform(df_vector)
        
        print(f"Normalized columns: {columns_to_normalize}")
        return df_scaled
    
    def feature_engineering(self, df):
        """
        Create new features from existing data
        Example: date features, ratio features, polynomial features
        """
        print("\n=== Feature Engineering ===")
        
        # Example 1: Create ratio features (if applicable)
        # df = df.withColumn("feature_ratio", col("feature1") / col("feature2"))
        
        # Example 2: Create categorical encoding
        # from pyspark.ml.feature import StringIndexer
        # indexer = StringIndexer(inputCol="category", outputCol="category_index")
        # df = indexer.fit(df).transform(df)
        
        # Example 3: Create binning
        # df = df.withColumn("age_group", 
        #                   when(col("age") < 18, "minor")
        #                   .when(col("age") < 65, "adult")
        #                   .otherwise("senior"))
        
        print("Feature engineering completed (customize based on your dataset)")
        return df
    
    def save_processed_data(self, df, output_path, format="csv"):
        """Save processed data"""
        print(f"\n=== Saving Processed Data to {output_path} ===")
        
        df.write.format(format) \
            .mode("overwrite") \
            .option("header", "true") \
            .save(output_path)
        
        print("Data saved successfully!")
    
    def run_pipeline(self, input_path, output_path):
        """Run complete preprocessing pipeline"""
        print("=" * 60)
        print("STARTING DATA PREPROCESSING PIPELINE")
        print("=" * 60)
        
        # Step 1: Load data
        df = self.load_data(input_path)
        
        # Step 2: Show initial data info
        self.show_data_info(df)
        
        # Step 3: Handle missing values
        df = self.handle_missing_values(df, strategy="mean")
        
        # Step 4: Remove duplicates
        df = self.remove_duplicates(df)
        
        # Step 5: Fix data types (customize based on your dataset)
        # type_mappings = {'age': 'integer', 'income': 'double'}
        # df = self.fix_data_types(df, type_mappings)
        
        # Step 6: Feature engineering
        df = self.feature_engineering(df)
        
        # Step 7: Normalization (customize columns based on your dataset)
        # numeric_cols = [field.name for field in df.schema.fields 
        #                if isinstance(field.dataType, (DoubleType, IntegerType))]
        # if numeric_cols:
        #     df = self.normalize_data(df, numeric_cols)
        
        # Step 8: Save processed data
        self.save_processed_data(df, output_path)
        
        print("\n" + "=" * 60)
        print("PREPROCESSING PIPELINE COMPLETED")
        print("=" * 60)
        
        return df
    
    def stop(self):
        """Stop Spark session"""
        self.spark.stop()


if __name__ == "__main__":
    # Example usage
    preprocessor = DataPreprocessor()
    
    # Update these paths based on your data
    INPUT_PATH = "data/raw_data.csv"
    OUTPUT_PATH = "data/processed_data.csv"
    
    try:
        processed_df = preprocessor.run_pipeline(INPUT_PATH, OUTPUT_PATH)
    finally:
        preprocessor.stop()
