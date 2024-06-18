from pyspark.sql import SparkSession

def assess_data_types(account_name, account_key, container_name, blob_name, file_format='csv', delimiter=','):
    # Create a Spark session
    spark = SparkSession.builder \
        .appName("DataTypeAssessment") \
        .config("spark.jars", "path/to/hadoop-azure-<version>.jar,path/to/azure-storage-<version>.jar") \
        .getOrCreate()

    # Build the URI for Azure Storage
    azure_uri = f"wasbs://{container_name}@{account_name}.blob.core.windows.net/{blob_name}?account_key={account_key}"

    # Read the data using Spark with inferSchema option set to true
    if file_format == 'csv':
        df = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", delimiter).csv(azure_uri)
    # Add similar blocks for other formats (Parquet, Avro, etc.)

    # Show the inferred schema (data types)
    df.printSchema()

    # Stop the Spark session
    spark.stop()

# Replace 'your-account-name', 'your-account-key', 'your-container', and 'your-blob' with your actual Azure Storage details
account_name = "<ACCOUNT_NAME>"
account_key = "<ACCOUNT_KEY>"
container_name = "abluva"
blob_name = "entity_list.csv"

# Specify the file format and delimiter (for CSV)
file_format = 'csv'
delimiter = ','

assess_data_types(account_name, account_key, container_name, blob_name, file_format, delimiter)
