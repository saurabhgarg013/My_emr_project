from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Initialize SparkSession
    spark = SparkSession.builder.appName("MyPySparkJob").getOrCreate()
     
    try:
        # Your PySpark code here
        input_file = 's3://myemrbucket13/inputfolder/product_data.csv'
        df = spark.read.csv(input_file)
        df.show()
        df.write.parquet("s3://myemrbucket13/outputfolder")
        # Stop SparkSession
        spark.stop()

    except Exception as e:
        # Handle any exceptions or errors
        print("Error occurred: ", str(e))
        spark.stop()
