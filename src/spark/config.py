import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Spark configuration
SPARK_MASTER = os.getenv('SPARK_MASTER', 'spark://spark-master:7077')
SPARK_APP_NAME = os.getenv('SPARK_APP_NAME', 'CryptoPrediction')

# Kafka configuration for Spark
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'crypto_prices')

# Checkpoint location
CHECKPOINT_LOCATION = os.getenv('CHECKPOINT_LOCATION', '/tmp/spark-checkpoints')

# Output configurations
PREDICTION_TOPIC = os.getenv('PREDICTION_TOPIC', 'crypto_predictions')