import json
import logging
import time
import websocket
from confluent_kafka import Producer
from threading import Thread

from src.kafka.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, BINANCE_WS_URL, SYMBOLS

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BinanceKafkaProducer:
    def __init__(self):
        # Initialize Kafka producer
        self.producer = Producer({
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'client.id': 'binance-crypto-producer'
        })
        self.topic = KAFKA_TOPIC
        self.ws = None
        self.is_running = False
        
    def delivery_report(self, err, msg):
        """Callback invoked on successful or failed message delivery"""
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')
    
    def on_message(self, ws, message):
        """Handle incoming WebSocket messages"""
        data = json.loads(message)
        
        # Process only trade data
        if 'e' in data and data['e'] == 'trade':
            # Create a simplified message
            kafka_message = {
                'symbol': data['s'].lower(),
                'price': float(data['p']),
                'quantity': float(data['q']),
                'timestamp': data['T'],
                'trade_id': data['t'],
                'is_buyer_maker': data['m']
            }
            
            # Send to Kafka
            self.producer.produce(
                self.topic,
                key=kafka_message['symbol'],
                value=json.dumps(kafka_message).encode('utf-8'),
                callback=self.delivery_report
            )
            self.producer.poll(0)  # Trigger delivery reports
    
    def on_error(self, ws, error):
        """Handle WebSocket errors"""
        logger.error(f"WebSocket error: {error}")
    
    def on_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket connection close"""
        logger.info(f"WebSocket connection closed: {close_msg}")
        if self.is_running:
            logger.info("Attempting to reconnect...")
            self.connect()
    
    def on_open(self, ws):
        """Handle WebSocket connection open"""
        logger.info("WebSocket connection established")
        
        # Subscribe to trade streams for each symbol
        for symbol in SYMBOLS:
            subscription = {
                "method": "SUBSCRIBE",
                "params": [f"{symbol}@trade"],
                "id": int(time.time())
            }
            ws.send(json.dumps(subscription))
            logger.info(f"Subscribed to {symbol} trade stream")
    
    def connect(self):
        """Establish WebSocket connection to Binance"""
        websocket.enableTrace(False)
        self.ws = websocket.WebSocketApp(
            BINANCE_WS_URL,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )
        
        ws_thread = Thread(target=self.ws.run_forever)
        ws_thread.daemon = True
        ws_thread.start()
    
    def start(self):
        """Start the producer"""
        logger.info("Starting Binance Kafka Producer")
        self.is_running = True
        self.connect()
    
    def stop(self):
        """Stop the producer"""
        logger.info("Stopping Binance Kafka Producer")
        self.is_running = False
        if self.ws:
            self.ws.close()
        # Flush any remaining messages
        self.producer.flush()

if __name__ == "__main__":
    producer = BinanceKafkaProducer()
    try:
        producer.start()
        # Keep the script running
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        producer.stop()