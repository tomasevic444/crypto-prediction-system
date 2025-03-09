import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'crypto_prices')
HISTORICAL_DATA_TOPIC = os.getenv('HISTORICAL_DATA_TOPIC', 'crypto_historical_4h')

# Binance WebSocket configuration
BINANCE_WS_URL = 'wss://stream.binance.com:9443/ws'
SYMBOLS = ['btcusdt', 'ethusdt']  # Symbols to track

# Time configuration
KLINE_INTERVAL = '4h'  # 4-hour candlestick interval