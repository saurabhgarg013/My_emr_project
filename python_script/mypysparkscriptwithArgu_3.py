import argparse

from pyspark.sql import SparkSession

def check_product(data_source, output_uri):
    """
   

    Argument for 
        --data_source s3://myemrbucket13/inputfolder/product_data.csv
          --output_uri s3://myemrbucket13/outputfolder
    """
    with SparkSession.builder.appName("for product").getOrCreate() as spark:
        # Load the product CSV data
        if data_source is not None:
            product_df = spark.read.option("header", "true").csv(data_source)

        # Create an in-memory DataFrame to query
        product_df.createOrReplaceTempView("product_data")

        
        product_info_df = spark.sql("""SELECT customer_id, product_id
          FROM product_data """)

        # Write the results to the specified output URI
        product_info_df.write.option("header", "true").mode("overwrite").csv(output_uri)

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    
    
    parser.add_argument(
        '--data_source', help="The URI for you CSV restaurant data, like an S3 bucket location.")
        
    parser.add_argument(
        '--output_uri', help="The URI where output is saved, like an S3 bucket location.")
    args = parser.parse_args()

    check_product(args.data_source, args.output_uri)