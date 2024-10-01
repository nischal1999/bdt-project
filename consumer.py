from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType

# Kafka and Spark configurations
KAFKA_TOPIC_NAME = 'news-articles'
KAFKA_BOOTSTRAP_SERVER = 'localhost:9092'
CSV_OUTPUT_PATH = '/Users/nischalaryal/jpmc/bdt/news-output/'  # Use an absolute path
CHECKPOINT_PATH = '/Users/nischalaryal/jpmc/bdt/news-output/checkpoint/'  # Use an absolute path

def main():
    # Create a Spark session with Kafka package
    spark = SparkSession.builder \
        .appName("KafkaSparkConsumer") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
        .getOrCreate()

    # Define the schema of the incoming JSON data
    schema = StructType([
        StructField('title', StringType(), True),
        StructField('description', StringType(), True),
        StructField('content', StringType(), True),
        StructField('published_at', StringType(), True),
        StructField('source', StringType(), True),
        StructField('url', StringType(), True)
    ])

    # Read from Kafka topic
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
        .option("subscribe", KAFKA_TOPIC_NAME) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    # Extract the JSON value from the Kafka message
    json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_data")

    # Parse the JSON data and create a DataFrame with the defined schema
    articles_df = json_df.select(from_json(col("json_data"), schema).alias("data")).select("data.*")

    # Write the output to console for debugging
    debug_query = articles_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    # Wait for some time to see console output before writing to CSV
    debug_query.awaitTermination(30)

    # Stop the console debugging query
    debug_query.stop()

    # Write the output to local CSV files (checkpoint directory required for streaming)
    csv_query = articles_df.writeStream \
        .outputMode("append") \
        .format("csv") \
        .option("path", CSV_OUTPUT_PATH) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .start()

    # Await termination of the CSV writing query
    csv_query.awaitTermination()

if __name__ == "__main__":
    main()