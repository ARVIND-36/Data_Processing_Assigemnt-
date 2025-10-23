# Task 4: In-Memory Data Processing Challenge (10%)

## Overview
Implement in-memory data processing using Apache Spark with RDDs and DataFrames for efficient real-time analytics.

## Objectives
- Utilize in-memory processing for performance
- Implement RDD operations
- Perform DataFrame transformations
- Apply machine learning in-memory
- Demonstrate performance improvements

## Files
- `in_memory_spark.py` - Spark in-memory processing implementation
- `requirements.txt` - Python dependencies

## Setup Instructions

### 1. Install Apache Spark
Download from: https://spark.apache.org/downloads.html

### 2. Set Environment Variables
```powershell
# Set SPARK_HOME
$env:SPARK_HOME = "C:\spark"
$env:PATH += ";$env:SPARK_HOME\bin"

# Set JAVA_HOME (Spark requires Java 8 or 11)
$env:JAVA_HOME = "C:\Program Files\Java\jdk-11"
```

### 3. Install Python Dependencies
```powershell
pip install -r requirements.txt
```

## Usage

### Run In-Memory Processing Demo
```powershell
python in_memory_spark.py
```

### Access Spark UI
While Spark is running, access the UI at:
```
http://localhost:4040
```

## Key Concepts

### 1. Resilient Distributed Datasets (RDDs)
RDDs are the fundamental data structure in Spark:
- **Immutable**: Once created, cannot be changed
- **Distributed**: Partitioned across cluster
- **Resilient**: Fault-tolerant with lineage tracking
- **In-Memory**: Cached for fast recomputation

### 2. DataFrame API
Higher-level abstraction over RDDs:
- **Structured**: Schema-based data
- **Optimized**: Catalyst query optimizer
- **Efficient**: Tungsten execution engine
- **SQL Support**: Query with SQL syntax

### 3. Caching Strategies

#### Memory Only
```python
df.cache()  # or df.persist(StorageLevel.MEMORY_ONLY)
```

#### Memory and Disk
```python
df.persist(StorageLevel.MEMORY_AND_DISK)
```

#### Memory Only Serialized
```python
df.persist(StorageLevel.MEMORY_ONLY_SER)
```

## RDD Operations

### Transformations (Lazy)
- `map()` - Apply function to each element
- `filter()` - Select elements matching condition
- `flatMap()` - Map then flatten
- `groupByKey()` - Group by key
- `reduceByKey()` - Aggregate by key

### Actions (Eager)
- `count()` - Count elements
- `collect()` - Return all elements
- `take(n)` - Return first n elements
- `reduce()` - Aggregate elements
- `foreach()` - Apply function to each element

## DataFrame Operations

### Basic Operations
```python
df.select("column")
df.filter(col("value") > 50)
df.groupBy("category").count()
df.orderBy("value")
```

### Aggregations
```python
df.agg(
    avg("column1"),
    sum("column2"),
    max("column3")
)
```

### Joins
```python
df1.join(df2, "key", "inner")
```

## In-Memory Machine Learning

### K-Means Clustering
```python
from pyspark.ml.clustering import KMeans

kmeans = KMeans(k=3, featuresCol="features")
model = kmeans.fit(df)
predictions = model.transform(df)
```

### Linear Regression
```python
from pyspark.ml.regression import LinearRegression

lr = LinearRegression(featuresCol="features", labelCol="label")
model = lr.fit(train_df)
predictions = model.transform(test_df)
```

### Logistic Regression
```python
from pyspark.ml.classification import LogisticRegression

lr = LogisticRegression(featuresCol="features", labelCol="label")
model = lr.fit(train_df)
```

## Performance Optimization

### 1. Partitioning
```python
df.repartition(8)  # Increase parallelism
df.coalesce(2)     # Reduce partitions
```

### 2. Broadcasting
```python
from pyspark.sql.functions import broadcast
large_df.join(broadcast(small_df), "key")
```

### 3. Caching Strategy
- Cache frequently accessed data
- Unpersist when no longer needed
- Monitor memory usage

### 4. Configuration
```python
spark = SparkSession.builder \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()
```

## Performance Metrics

### Monitor with Spark UI
- **Jobs**: Track job execution
- **Stages**: View stage details
- **Storage**: Monitor cached RDDs/DataFrames
- **Environment**: Check configuration
- **Executors**: Monitor executor metrics

### Key Metrics
- Execution time
- Memory usage
- Shuffle read/write
- GC time

## Real-Time Analytics Use Cases

### 1. Streaming Analytics
```python
# Process streaming data in-memory
stream_df = spark.readStream \
    .format("kafka") \
    .load()
```

### 2. Interactive Queries
```python
# Fast queries on cached data
df.cache()
df.createOrReplaceTempView("data")
spark.sql("SELECT * FROM data WHERE value > 100")
```

### 3. Iterative ML
```python
# Repeated access for ML training
train_df.cache()
for epoch in range(10):
    model.fit(train_df)
```

## Comparison: In-Memory vs Disk-Based

### In-Memory Advantages
- ⚡ 100x faster for iterative algorithms
- 🔄 Fast repeated access
- 📊 Real-time analytics
- 🎯 Interactive queries

### When to Use Disk
- 💾 Dataset larger than memory
- 📥 One-time batch processing
- 💰 Cost constraints

## Best Practices

1. **Cache Strategically**: Only cache frequently accessed data
2. **Unpersist**: Free memory when done
3. **Monitor Memory**: Use Spark UI to track usage
4. **Partition Appropriately**: Balance parallelism
5. **Use DataFrames**: Better optimization than RDDs
6. **Broadcast Small Tables**: Optimize joins
7. **Avoid Shuffles**: Minimize data movement

## Troubleshooting

### Out of Memory
```python
# Increase memory allocation
spark = SparkSession.builder \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .getOrCreate()
```

### Slow Performance
- Check partition count
- Verify caching is working
- Monitor shuffle operations
- Review GC time in UI

### Cache Not Working
```python
# Verify cache status
spark.catalog.isCached("table_name")

# Force cache materialization
df.cache()
df.count()  # Trigger action
```

## Additional Resources
- Apache Spark Documentation: https://spark.apache.org/docs/latest/
- Spark UI Guide: https://spark.apache.org/docs/latest/web-ui.html
- Performance Tuning: https://spark.apache.org/docs/latest/tuning.html
