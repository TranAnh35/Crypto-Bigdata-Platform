import time
from typing import Optional

from ..services.kafka.client import KafkaProducer, KafkaConsumer
from ..services.binance.client import BinanceClient
from .settings import settings
from .logger import get_logger

logger = get_logger(__name__)


# ********************************************************************
# * Binance Client
# ********************************************************************

def get_binance_client() -> Optional[BinanceClient]:
    try:
        client = BinanceClient(
            api_key=settings.BINANCE_API_KEY,
            api_secret=settings.BINANCE_SECRET_KEY,
            max_retries=settings.MAX_RETRIES,
            retry_delay=settings.RETRY_DELAY
        )
        return client
    except Exception as e:
        logger.error(f"Could not create Binance client: {e}")
        return None


# ********************************************************************
# * Kafka Client
# ********************************************************************

def get_kafka_producer(max_retries: int = 3, **kwargs) -> Optional[KafkaProducer]:
    """
    Helper function to get a Kafka producer instance
    
    Args:
        bootstrap_servers: Comma-separated list of Kafka brokers
        max_retries: Maximum number of connection retry attempts
        **kwargs: Additional producer configuration
        
    Returns:
        KafkaProducer instance if successful, None otherwise
    """
    bootstrap_servers = settings.KAFKA_BOOTSTRAP_SERVERS
    logger.info(f"Attempting to initialize Kafka producer with bootstrap servers: {bootstrap_servers}")
    
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                **kwargs
            )
            logger.info("Kafka producer initialized successfully")
            return producer
        except Exception as e:
            logger.warning(f"Attempt {attempt}/{max_retries}: Error initializing producer: {e}")
            if attempt < max_retries:
                wait_time = 2 ** attempt
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
    
    logger.error("Failed to initialize Kafka producer after maximum retries")
    return None

def get_kafka_consumer(**kwargs) -> KafkaConsumer:
    """
    Helper function to get a Kafka consumer instance
    
    Args:
        bootstrap_servers: Comma-separated list of Kafka brokers
        group_id: Consumer group ID
        **kwargs: Additional consumer configuration
    """
    bootstrap_servers = settings.KAFKA_BOOTSTRAP_SERVERS
    group_id = settings.KAFKA_GROUP_ID
    logger.info(f"Initializing Kafka consumer with bootstrap servers: {bootstrap_servers}")
    
    consumer = None
    
    while not consumer:
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=bootstrap_servers,
                group_id=group_id,
                **kwargs
            )
        except Exception as e:
            logger.error(f"Error initializing consumer: {e}")
            time.sleep(5)
    logger.info("Kafka consumer initialized successfully")
    return consumer