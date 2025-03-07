import os
import findspark
findspark.init()  # Find Spark installation

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, BooleanType
import logging

from src.spark.config import SPARK_MASTER, SPARK_APP_NAME, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, CHECKPOINT_LOCATION
from src.spark.features import add_all_features

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CryptoDataProcessor:
    def __init__(self):
        # Initialize Spark session
        self.spark = SparkSession.builder \
            .appName(SPARK_APP_NAME) \
            .master(SPARK_MASTER) \
            .config("spark.sql.streaming.schemaInference", "true") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
            .getOrCreate()
        
        # Set log level
        self.spark.sparkContext.setLogLevel("WARN")
        
        # Define schema for incoming data
        self.schema = StructType([
            StructField("symbol", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("quantity", DoubleType(), True),
            StructField("timestamp", LongType(), True),
            StructField("trade_id", LongType(), True),
            StructField("is_buyer_maker", BooleanType(), True)
        ])
        
        # Create directory for checkpoints if it doesn't exist
        os.makedirs(CHECKPOINT_LOCATION, exist_ok=True)
    
    def read_from_kafka(self):
        """Read from Kafka stream"""
        return self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", KAFKA_TOPIC) \
            .option("startingOffsets", "latest") \
            .load()
    
    def process_stream(self):
        """Process the stream data"""
        # Read from Kafka
        df = self.read_from_kafka()
        
        # Parse JSON data
        parsed_df = df \
            .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
            .select(
                col("key"),
                from_json(col("value"), self.schema).alias("data")
            ) \
            .select("key", "data.*")
        
        # Convert timestamp to timestamp type
        parsed_df = parsed_df \
            .withColumn("timestamp", to_timestamp(col("timestamp") / 1000))
        
        # Group by symbol and window for aggregation
        windowed_df = parsed_df \
            .groupBy(
                col("symbol"),
                window(col("timestamp"), "1 minute") 
            ) \
            .agg(
                {"price": "avg", "quantity": "sum"}
            ) \
            .select(
                col("symbol"),
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("avg(price)").alias("price"),
                col("sum(quantity)").alias("volume")
            )
        
        # Add technical indicators
        enriched_df = add_all_features(windowed_df)
        
        return enriched_df
    
    def start_processing(self):
        """Start the stream processing"""
        logger.info("Starting Spark Structured Streaming")
        
        processed_stream = self.process_stream()
        
        # Write to console for debugging
        console_query = processed_stream \
            .writeStream \
            .outputMode("update") \
            .format("console") \
            .option("truncate", "false") \
            .start()
        
        # Write to Kafka topic for downstream consumption (like prediction)
        kafka_query = processed_stream \
            .selectExpr("symbol AS key", "to_json(struct(*)) AS value") \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("topic", "crypto_features") \
            .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/kafka") \
            .start()
        
        # Wait for termination
        try:
            kafka_query.awaitTermination()
        except KeyboardInterrupt:
            logger.info("Stopping Spark Structured Streaming")
            kafka_query.stop()
            console_query.stop()
            self.spark.stop()

if __name__ == "__main__":
    processor = CryptoDataProcessor()
    processor.start_processing()