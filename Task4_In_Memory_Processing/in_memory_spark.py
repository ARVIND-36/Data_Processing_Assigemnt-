"""
Task 4: In-Memory Data Processing with Apache Spark
Using RDDs and DataFrames for efficient in-memory analytics
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum, count, max, min, when, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.regression import LinearRegression
from pyspark.ml.classification import LogisticRegression
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class InMemoryProcessor:
    def __init__(self, app_name="InMemoryProcessing", memory="4g"):
        """
        Initialize Spark Session with in-memory optimization
        """
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.driver.memory", memory) \
            .config("spark.executor.memory", memory) \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.default.parallelism", "8") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
        
        # Set log level
        self.spark.sparkContext.setLogLevel("WARN")
        
        logger.info(f"Spark Session initialized with {memory} memory")
        logger.info(f"Spark UI available at: {self.spark.sparkContext.uiWebUrl}")
    
    def load_data(self, file_path, format="csv", cache=True):
        """
        Load data into Spark DataFrame and cache in memory
        """
        logger.info(f"Loading data from {file_path}")
        start_time = time.time()
        
        df = self.spark.read.format(format) \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(file_path)
        
        if cache:
            df.cache()  # Cache in memory for fast access
            df.count()  # Trigger caching
        
        load_time = time.time() - start_time
        logger.info(f"Data loaded in {load_time:.2f} seconds")
        logger.info(f"Rows: {df.count()}, Columns: {len(df.columns)}")
        
        return df
    
    def create_rdd_from_data(self, data_list):
        """
        Create RDD from Python list
        RDD = Resilient Distributed Dataset
        """
        logger.info("Creating RDD from data")
        rdd = self.spark.sparkContext.parallelize(data_list)
        logger.info(f"RDD created with {rdd.count()} elements")
        return rdd
    
    def rdd_operations_demo(self, rdd):
        """
        Demonstrate RDD operations for in-memory processing
        """
        logger.info("\n=== RDD Operations Demo ===")
        
        # Map operation
        mapped_rdd = rdd.map(lambda x: x * 2)
        logger.info(f"Map operation: First 5 elements: {mapped_rdd.take(5)}")
        
        # Filter operation
        filtered_rdd = rdd.filter(lambda x: x > 50)
        logger.info(f"Filter operation: Count > 50: {filtered_rdd.count()}")
        
        # Reduce operation
        total = rdd.reduce(lambda x, y: x + y)
        logger.info(f"Reduce operation: Sum: {total}")
        
        # GroupBy operation
        grouped_rdd = rdd.groupBy(lambda x: x % 2)
        logger.info(f"GroupBy operation: Groups: {grouped_rdd.count()}")
        
        return mapped_rdd
    
    def in_memory_aggregations(self, df):
        """
        Perform in-memory aggregations on DataFrame
        """
        logger.info("\n=== In-Memory Aggregations ===")
        start_time = time.time()
        
        # Basic statistics
        stats = df.describe()
        stats.show()
        
        # Group by aggregations
        numeric_cols = [field.name for field in df.schema.fields 
                       if isinstance(field.dataType, (DoubleType, IntegerType))]
        
        if len(numeric_cols) > 0:
            # Calculate multiple aggregations
            agg_result = df.agg(
                *[avg(col(c)).alias(f"{c}_avg") for c in numeric_cols[:3]],
                *[sum(col(c)).alias(f"{c}_sum") for c in numeric_cols[:3]],
                *[max(col(c)).alias(f"{c}_max") for c in numeric_cols[:3]]
            )
            
            logger.info("Aggregation results:")
            agg_result.show()
        
        exec_time = time.time() - start_time
        logger.info(f"Aggregations completed in {exec_time:.2f} seconds")
        
        return agg_result
    
    def in_memory_joins(self, df1, df2, join_key):
        """
        Perform in-memory joins
        """
        logger.info("\n=== In-Memory Join Operations ===")
        start_time = time.time()
        
        # Cache both DataFrames
        df1.cache()
        df2.cache()
        
        # Perform join
        joined_df = df1.join(df2, on=join_key, how='inner')
        
        exec_time = time.time() - start_time
        logger.info(f"Join completed in {exec_time:.2f} seconds")
        logger.info(f"Joined DataFrame rows: {joined_df.count()}")
        
        return joined_df
    
    def window_operations(self, df, partition_col, order_col):
        """
        Perform window operations for analytics
        """
        from pyspark.sql.window import Window
        from pyspark.sql.functions import row_number, rank, dense_rank
        
        logger.info("\n=== Window Operations ===")
        
        # Define window specification
        window_spec = Window.partitionBy(partition_col).orderBy(col(order_col).desc())
        
        # Apply window functions
        result = df.withColumn("row_num", row_number().over(window_spec)) \
                   .withColumn("rank", rank().over(window_spec))
        
        logger.info("Window operations applied")
        result.show(10)
        
        return result
    
    def kmeans_clustering(self, df, feature_cols, k=3):
        """
        Perform K-Means clustering in memory
        """
        logger.info(f"\n=== K-Means Clustering (k={k}) ===")
        start_time = time.time()
        
        # Prepare features
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        df_vector = assembler.transform(df)
        
        # Standardize features
        scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
        scaler_model = scaler.fit(df_vector)
        df_scaled = scaler_model.transform(df_vector)
        
        # Cache scaled data
        df_scaled.cache()
        
        # Train K-Means
        kmeans = KMeans(featuresCol="scaled_features", predictionCol="cluster", k=k)
        model = kmeans.fit(df_scaled)
        
        # Make predictions
        predictions = model.transform(df_scaled)
        
        exec_time = time.time() - start_time
        logger.info(f"Clustering completed in {exec_time:.2f} seconds")
        
        # Show cluster centers
        logger.info("Cluster Centers:")
        for i, center in enumerate(model.clusterCenters()):
            logger.info(f"Cluster {i}: {center}")
        
        # Show cluster distribution
        predictions.groupBy("cluster").count().show()
        
        return predictions, model
    
    def linear_regression_analysis(self, df, feature_cols, label_col):
        """
        Perform Linear Regression in memory
        """
        logger.info("\n=== Linear Regression Analysis ===")
        start_time = time.time()
        
        # Prepare features
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        df_vector = assembler.transform(df)
        
        # Split data
        train_df, test_df = df_vector.randomSplit([0.8, 0.2], seed=42)
        train_df.cache()
        test_df.cache()
        
        # Train model
        lr = LinearRegression(featuresCol="features", labelCol=label_col)
        lr_model = lr.fit(train_df)
        
        # Make predictions
        predictions = lr_model.transform(test_df)
        
        exec_time = time.time() - start_time
        logger.info(f"Regression completed in {exec_time:.2f} seconds")
        
        # Model statistics
        logger.info(f"Coefficients: {lr_model.coefficients}")
        logger.info(f"Intercept: {lr_model.intercept}")
        logger.info(f"RMSE: {lr_model.summary.rootMeanSquaredError}")
        logger.info(f"R2: {lr_model.summary.r2}")
        
        return predictions, lr_model
    
    def real_time_analytics_demo(self, df):
        """
        Demonstrate real-time analytics using in-memory processing
        """
        logger.info("\n=== Real-Time Analytics Demo ===")
        
        # Cache DataFrame for multiple operations
        df.cache()
        
        # Operation 1: Quick counts
        start = time.time()
        total_count = df.count()
        logger.info(f"Total records: {total_count} (Time: {time.time()-start:.3f}s)")
        
        # Operation 2: Fast filtering
        start = time.time()
        filtered = df.filter(col("value") > 50)
        filtered_count = filtered.count()
        logger.info(f"Filtered records: {filtered_count} (Time: {time.time()-start:.3f}s)")
        
        # Operation 3: Aggregation
        start = time.time()
        avg_value = df.agg(avg("value")).first()[0]
        logger.info(f"Average value: {avg_value:.2f} (Time: {time.time()-start:.3f}s)")
        
        # Show performance improvement with caching
        logger.info("\nPerformance demonstration complete - all operations used in-memory cache")
    
    def performance_comparison(self, file_path):
        """
        Compare performance with and without in-memory caching
        """
        logger.info("\n=== Performance Comparison ===")
        
        # Without caching
        logger.info("Testing WITHOUT in-memory caching...")
        df_no_cache = self.spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(file_path)
        
        start = time.time()
        count1 = df_no_cache.count()
        time_no_cache = time.time() - start
        
        # With caching
        logger.info("Testing WITH in-memory caching...")
        df_cached = self.spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(file_path)
        df_cached.cache()
        
        start = time.time()
        count2 = df_cached.count()  # First count (cache miss)
        time_first = time.time() - start
        
        start = time.time()
        count3 = df_cached.count()  # Second count (cache hit)
        time_cached = time.time() - start
        
        logger.info(f"\nResults:")
        logger.info(f"Without cache: {time_no_cache:.3f}s")
        logger.info(f"With cache (first): {time_first:.3f}s")
        logger.info(f"With cache (subsequent): {time_cached:.3f}s")
        logger.info(f"Speedup: {time_no_cache/time_cached:.2f}x faster")
    
    def stop(self):
        """Stop Spark session"""
        self.spark.stop()
        logger.info("Spark Session stopped")


def main():
    """Main function demonstrating in-memory processing"""
    
    # Initialize processor
    processor = InMemoryProcessor(app_name="InMemoryDemo", memory="4g")
    
    try:
        # Example 1: RDD Operations
        logger.info("\n" + "="*60)
        logger.info("EXAMPLE 1: RDD Operations")
        logger.info("="*60)
        data = list(range(1, 101))
        rdd = processor.create_rdd_from_data(data)
        processor.rdd_operations_demo(rdd)
        
        # Example 2: DataFrame Operations
        logger.info("\n" + "="*60)
        logger.info("EXAMPLE 2: DataFrame In-Memory Operations")
        logger.info("="*60)
        
        # Create sample DataFrame
        from pyspark.sql import Row
        sample_data = [
            Row(id=i, value=i*10, category=f"cat_{i%3}")
            for i in range(1, 1001)
        ]
        df = processor.spark.createDataFrame(sample_data)
        df.cache()
        
        # Perform operations
        processor.in_memory_aggregations(df)
        processor.real_time_analytics_demo(df)
        
        # Example 3: Machine Learning
        logger.info("\n" + "="*60)
        logger.info("EXAMPLE 3: In-Memory Machine Learning")
        logger.info("="*60)
        
        # Create sample ML dataset
        import random
        ml_data = [
            Row(feature1=random.uniform(0, 100),
                feature2=random.uniform(0, 100),
                feature3=random.uniform(0, 100),
                label=random.uniform(0, 10))
            for _ in range(1000)
        ]
        ml_df = processor.spark.createDataFrame(ml_data)
        
        # Clustering
        processor.kmeans_clustering(ml_df, ['feature1', 'feature2', 'feature3'], k=3)
        
        # Regression
        processor.linear_regression_analysis(ml_df, ['feature1', 'feature2', 'feature3'], 'label')
        
    finally:
        processor.stop()


if __name__ == "__main__":
    main()
