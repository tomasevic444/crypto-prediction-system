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
HISTORICAL_DATA_TOPIC = os.getenv('HISTORICAL_DATA_TOPIC', 'crypto_historical_4h')

# Time configuration
PREDICTION_INTERVAL = '4h'
WINDOW_DURATION = '4 hours'
SLIDE_DURATION = '4 hours'

# Model configuration
FEATURE_COLUMNS = [
    'price', 'volume',
    'sma_3', 'sma_6', 'sma_12', 'sma_24', 'sma_72',
    'ema_3', 'ema_6', 'ema_12', 'ema_24', 'ema_72',
    'rsi_14',
    'bb_upper', 'bb_lower', 'bb_middle',
    'macd', 'macd_signal', 'macd_hist',
    'volatility_6', 'volatility_pct_6',
    'momentum_1', 'momentum_3', 'momentum_6', 'momentum_12',
    'hour_of_day', 'day_of_week', 'is_weekend'
]

TARGET_COLUMN = 'target_pct_change_1'  # 1 period (4 hours) ahead prediction

# Checkpoint location
CHECKPOINT_LOCATION = os.getenv('CHECKPOINT_LOCATION', '/tmp/spark-checkpoints')

# Output configurations
PREDICTION_TOPIC = os.getenv('PREDICTION_TOPIC', 'crypto_predictions')