from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Python Spark Example") \
    .getOrCreate()

# Example dictionary
data = {
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [25, 30, 35],
    'city': ['New York', 'Los Angeles', 'Chicago']
}

# Define schema (optional but recommended for better performance)
schema = StructType([
    StructField('name', StringType(), True),
    StructField('age', IntegerType(), True),
    StructField('city', StringType(), True)
])

# Create DataFrame
df = spark.createDataFrame(list(zip(data['name'], data['age'], data['city'])), schema=schema)

# Show the DataFrame
df.show()
