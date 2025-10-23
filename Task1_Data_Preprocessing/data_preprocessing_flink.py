"""
Task 1: Data Preprocessing Challenge (30%)
Using Apache Flink for data cleaning and preprocessing
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.expressions import col, lit
from pyflink.table.window import Tumble
import pandas as pd

class FlinkDataPreprocessor:
    def __init__(self):
        """Initialize Flink Environment"""
        # Create streaming environment
        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.env.set_parallelism(1)
        
        # Create table environment
        settings = EnvironmentSettings.new_instance() \
            .in_streaming_mode() \
            .build()
        self.t_env = StreamTableEnvironment.create(self.env, environment_settings=settings)
    
    def load_csv_data(self, file_path, table_name="input_table"):
        """Load CSV data into Flink Table"""
        print(f"Loading data from {file_path}")
        
        # Create table from CSV
        ddl = f"""
            CREATE TABLE {table_name} (
                -- Define your schema here based on your dataset
                -- Example:
                id INT,
                name STRING,
                age INT,
                salary DOUBLE,
                department STRING
            ) WITH (
                'connector' = 'filesystem',
                'path' = '{file_path}',
                'format' = 'csv'
            )
        """
        
        self.t_env.execute_sql(ddl)
        print(f"Table '{table_name}' created successfully")
        return self.t_env.from_path(table_name)
    
    def handle_missing_values(self, table, strategy="fill"):
        """
        Handle missing values in Flink Table
        Strategy: 'fill' with default values or 'drop'
        """
        print("\n=== Handling Missing Values ===")
        
        if strategy == "fill":
            # Example: Fill null values with defaults
            # Customize based on your columns
            result = table.select(
                col('id'),
                col('name').if_null(lit('Unknown')),
                col('age').if_null(lit(0)),
                col('salary').if_null(lit(0.0)),
                col('department').if_null(lit('Unassigned'))
            )
        elif strategy == "drop":
            # Filter out rows with null values
            result = table.filter(
                col('id').is_not_null &
                col('name').is_not_null &
                col('age').is_not_null
            )
        else:
            result = table
        
        print("Missing values handled")
        return result
    
    def remove_duplicates(self, table):
        """Remove duplicate rows"""
        print("\n=== Removing Duplicates ===")
        
        # Group by all columns and take first occurrence
        result = table.distinct()
        
        print("Duplicates removed")
        return result
    
    def normalize_columns(self, table, columns_to_normalize):
        """
        Normalize numeric columns (Min-Max scaling)
        Note: This is simplified; for production use ML libraries
        """
        print("\n=== Normalizing Data ===")
        
        # This is a simplified example
        # In practice, you'd calculate min/max first, then normalize
        print(f"Normalize these columns: {columns_to_normalize}")
        print("Note: Implement actual normalization logic based on your requirements")
        
        return table
    
    def feature_engineering(self, table):
        """
        Create new features from existing data
        """
        print("\n=== Feature Engineering ===")
        
        # Example: Create age groups
        result = table.add_columns(
            (col('age') / lit(10)).floor().alias('age_group')
        )
        
        # Example: Create salary categories
        # result = result.add_columns(
        #     col('salary').case(
        #         lit(50000), lit('Low')
        #     ).case(
        #         lit(100000), lit('Medium')
        #     ).else_(lit('High')).alias('salary_category')
        # )
        
        print("Feature engineering completed")
        return result
    
    def save_to_csv(self, table, output_path, table_name="output_table"):
        """Save processed data to CSV"""
        print(f"\n=== Saving Data to {output_path} ===")
        
        # Create sink table
        ddl = f"""
            CREATE TABLE {table_name} (
                -- Define schema matching your processed table
                id INT,
                name STRING,
                age INT,
                salary DOUBLE,
                department STRING,
                age_group INT
            ) WITH (
                'connector' = 'filesystem',
                'path' = '{output_path}',
                'format' = 'csv'
            )
        """
        
        self.t_env.execute_sql(ddl)
        
        # Insert data
        table.execute_insert(table_name).wait()
        print("Data saved successfully!")
    
    def run_pipeline(self, input_path, output_path):
        """Run complete preprocessing pipeline"""
        print("=" * 60)
        print("STARTING FLINK DATA PREPROCESSING PIPELINE")
        print("=" * 60)
        
        try:
            # Step 1: Load data
            table = self.load_csv_data(input_path)
            
            # Step 2: Handle missing values
            table = self.handle_missing_values(table, strategy="fill")
            
            # Step 3: Remove duplicates
            table = self.remove_duplicates(table)
            
            # Step 4: Feature engineering
            table = self.feature_engineering(table)
            
            # Step 5: Normalize data (customize based on needs)
            # numeric_cols = ['age', 'salary']
            # table = self.normalize_columns(table, numeric_cols)
            
            # Step 6: Save processed data
            self.save_to_csv(table, output_path)
            
            print("\n" + "=" * 60)
            print("FLINK PREPROCESSING PIPELINE COMPLETED")
            print("=" * 60)
            
        except Exception as e:
            print(f"Error in pipeline: {e}")
            raise


def preprocess_with_pandas(input_path, output_path):
    """
    Alternative: Pandas-based preprocessing for smaller datasets
    This can be used if Flink setup is complex
    """
    print("Using Pandas for preprocessing...")
    
    # Load data
    df = pd.read_csv(input_path)
    print(f"Loaded data: {df.shape}")
    
    # Handle missing values
    df.fillna(df.mean(numeric_only=True), inplace=True)
    df.fillna('Unknown', inplace=True)
    
    # Remove duplicates
    df.drop_duplicates(inplace=True)
    
    # Feature engineering
    if 'age' in df.columns:
        df['age_group'] = df['age'] // 10
    
    # Save
    df.to_csv(output_path, index=False)
    print(f"Saved to {output_path}")


if __name__ == "__main__":
    # Example usage
    INPUT_PATH = "data/raw_data.csv"
    OUTPUT_PATH = "data/processed_data_flink.csv"
    
    # Option 1: Use Flink (requires proper setup)
    # preprocessor = FlinkDataPreprocessor()
    # preprocessor.run_pipeline(INPUT_PATH, OUTPUT_PATH)
    
    # Option 2: Use Pandas for quick testing
    preprocess_with_pandas(INPUT_PATH, OUTPUT_PATH)
