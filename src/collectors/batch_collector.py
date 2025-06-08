import time
import json
from typing import Optional

from .base_collector import Collector
from ..utils.settings import settings
from ..utils.helper import get_kafka_producer
from ..utils.helper import get_binance_client
from ..services.binance.client import BinanceClient
from ..services.kafka.client import KafkaProducer
from ..utils.logger import get_logger

logger = get_logger(__name__)

class BatchCollector(Collector):
    
    def __init__(self, symbol: str, interval: str, start_date: str, end_date: str = None):
        """
        Args:
            symbol (str): The trading pair, e.g., 'BTCUSDT'.
            interval (str): The kline interval, e.g., '1d', '1h'.
            start_date (str): The start date for historical data, e.g., "1 Dec, 2022".
            end_date (str, optional): The end date. Defaults to the present.
        """
        self.symbol = symbol.upper()
        self.interval = interval
        self.start_date = start_date
        self.end_date = end_date
        self.topic = f"{settings.KAFKA_TOPIC_PREFIX}klines_historical_{self.symbol.lower()}_{self.interval}"
        
        logger.info(f"Initializing BinanceBatchCollector for {self.symbol} ({self.interval}).")
        logger.info(f"Data will be sent to Kafka topic: {self.topic}")

        self.binance_client: Optional[BinanceClient] = get_binance_client()
        self.kafka_producer: Optional[KafkaProducer] = get_kafka_producer()

    def collect(self):
        """
        Fetches historical data in a single batch run.
        """
        if not self.binance_client or not self.kafka_producer:
            logger.error("Binance client or Kafka producer is not available. Aborting batch collection.")
            return

        logger.info(f"Starting batch collection for {self.symbol} from {self.start_date} to {self.end_date or 'now'}...")
        
        try:
            klines = self.binance_client.get_historical_klines(
                symbol=self.symbol,
                interval=self.interval,
                start_str=self.start_date,
                end_str=self.end_date
            )

            if not klines:
                logger.warning(f"No historical kline data found for {self.symbol} in the specified range.")
                return

            total_klines = len(klines)
            logger.info(f"Found {total_klines} klines. Sending to Kafka...")

            for i, kline_data in enumerate(klines):
                message_key = f"{self.symbol}_{self.interval}_{kline_data['open_time']}"
                self.kafka_producer.produce(
                    topic=self.topic,
                    key=message_key,
                    value=kline_data
                )
                if (i + 1) % 100 == 0:
                    logger.info(f"Sent {i + 1}/{total_klines} messages...")
            
            self.kafka_producer.flush()
            logger.info(f"Batch collection complete. Successfully sent {total_klines} messages to topic '{self.topic}'.")

        except Exception as e:
            logger.error(f"An error occurred during batch collection for {self.symbol}: {e}", exc_info=True)