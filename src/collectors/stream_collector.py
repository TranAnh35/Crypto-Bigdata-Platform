import asyncio
from binance import AsyncClient, BinanceSocketManager
from typing import Optional, Dict, Any

from .base_collector import Collector
from ..services.kafka.client import KafkaProducer
from ..utils.settings import settings
from ..utils.helper import get_kafka_producer
from ..utils.logger import get_logger

logger = get_logger(__name__)

class StreamCollector(Collector):
    """
    Connects to Binance WebSocket streams to collect real-time kline data
    and sends it to Kafka.
    """
    def __init__(self, symbol: str, interval: str):
        self.symbol = symbol.upper()
        self.interval = interval
        self.kafka_producer: Optional[KafkaProducer] = get_kafka_producer()
        self.topic = f"{settings.KAFKA_TOPIC_PREFIX}klines_stream_{self.symbol.lower()}_{self.interval}"

        if not self.kafka_producer:
            raise ConnectionError("Kafka producer is not available. Stream collector cannot start.")
            
        logger.info(f"Initializing BinanceStreamCollector for {self.symbol} ({self.interval}).")
        logger.info(f"Real-time data will be sent to Kafka topic: {self.topic}")

    def _process_message(self, msg: Dict[str, Any]):
        """Callback function to process messages from the WebSocket."""
        if msg.get('e') == 'error':
            logger.error(f"WebSocket error received: {msg['m']}")
            return

        # Chỉ xử lý tin nhắn kline
        if msg.get('e') == 'kline':
            kline_raw = msg['k']
            # Chỉ gửi khi nến đã đóng
            if kline_raw['x']:
                kline_data = {
                    'open_time': int(kline_raw['t']),
                    'open': float(kline_raw['o']),
                    'high': float(kline_raw['h']),
                    'low': float(kline_raw['l']),
                    'close': float(kline_raw['c']),
                    'volume': float(kline_raw['v']),
                    'close_time': int(kline_raw['T']),
                    'quote_asset_volume': float(kline_raw['q']),
                    'number_of_trades': int(kline_raw['n']),
                    'taker_buy_base_asset_volume': float(kline_raw['V']),
                    'taker_buy_quote_asset_volume': float(kline_raw['Q']),
                    'symbol': kline_raw['s'],
                    'interval': kline_raw['i']
                }
                
                message_key = f"{kline_data['symbol']}_{kline_data['interval']}_{kline_data['open_time']}"
                logger.debug(f"Received closed kline for {self.symbol}: {kline_data['close']}")
                
                try:
                    self.kafka_producer.produce(
                        topic=self.topic,
                        key=message_key,
                        value=kline_data
                    )
                    # Flush ngay để đảm bảo tin nhắn được gửi đi nhanh chóng trong stream
                    self.kafka_producer.flush(timeout=1.0)
                except Exception as e:
                    logger.error(f"Failed to send stream message to Kafka: {e}")

    async def _run_stream(self):
        """The main async loop to run the WebSocket client."""
        client = await AsyncClient.create(settings.BINANCE_API_KEY, settings.BINANCE_SECRET_KEY)
        bm = BinanceSocketManager(client)
        stream = bm.kline_socket(symbol=self.symbol, interval=self.interval)

        logger.info(f"Starting WebSocket stream for {self.symbol}...")
        async with stream as kline_stream:
            while True:
                try:
                    res = await kline_stream.recv()
                    self._process_message(res)
                except Exception as e:
                    logger.error(f"Error in WebSocket receive loop: {e}")
                    # Cân nhắc thêm logic reconnect ở đây
                    await asyncio.sleep(5)

        await client.close_connection()
        logger.info(f"WebSocket stream for {self.symbol} has been closed.")

    def collect(self):
        """
        Starts the data collection stream. This is a blocking call.
        """
        try:
            asyncio.run(self._run_stream())
        except KeyboardInterrupt:
            logger.info(f"Stream collection for {self.symbol} stopped by user.")
        finally:
            if self.kafka_producer:
                self.kafka_producer.close()
            logger.info("Collector resources cleaned up.")