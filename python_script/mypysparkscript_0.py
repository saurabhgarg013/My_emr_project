from pyspark.sql import SparkSession



# Create SparkSession
spark = SparkSession.builder.getOrCreate()

# Specify the input file path
input_file = 's3://myemrbucket13/inputfolder/product_data.csv'

# Read CSV into a DataFrame
df = spark.read.option("header", "true").csv(input_file)


print(df.show())




# Write DataFrame as Parquet to the output folder
df.write.option("header", "true").mode("overwrite").parquet("s3://myemrbucket13/outputfolder")

# Stop the SparkSession
spark.stop()
