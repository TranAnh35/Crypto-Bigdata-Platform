from kafka import KafkaProducer as KafkaProducerImpl, KafkaConsumer as KafkaConsumerImpl
import json
from typing import Dict, Any, Callable, List
import time

from ...utils.logger import get_logger

logger = get_logger(__name__)

class KafkaProducer:
    """
    A wrapper around kafka-python Producer with error handling and retry logic.
    """
    def __init__(self, bootstrap_servers: str, **config):
        """
        Initialize Kafka Producer
        
        Args:
            bootstrap_servers: Comma-separated list of Kafka brokers
            **config: Additional Kafka producer configuration
        """
        self.bootstrap_servers = bootstrap_servers
        self.config = {
            'bootstrap_servers': bootstrap_servers,
            'retries': 3,
            'retry_backoff_ms': 1000,
            **config
        }
        self.producer = KafkaProducerImpl(
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8'),
            **self.config
        )

    def delivery_report(self, future, key):
        """
        Callback for message delivery reports
        """
        try:
            logger.debug(f'Message with key "{key}" delivered to {record_metadata.topic} '
                         f'[{record_metadata.partition}] @ offset {record_metadata.offset}')
        except Exception as e:
            logger.error(f"Error in delivery_report callback: {e}")

    def produce(self, topic: str, key: str, value: Dict[str, Any]):
        """
        Produce a message to a Kafka topic
        
        Args:
            topic: Target topic
            key: Message key
            value: Message value (will be JSON serialized)
        """
        try:
            future = self.producer.send(topic, key=key, value=value)
            
            on_send_success = lambda metadata: self.delivery_report(metadata, key_info=str(key))
            on_send_error = lambda exc: logger.error(f'Failed to send message with key "{key}": {exc}')

            future.add_callback(on_send_success)
            future.add_errback(on_send_error)
        except Exception as e:
            logger.error(f'Failed to produce message: {e}')
            raise

    def flush(self, timeout: float = 5.0):
        """
        Wait for all messages in the producer queue to be delivered
        """
        self.producer.flush(timeout=timeout)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.flush()
        self.producer.close()


class KafkaConsumer:
    """
    A wrapper around kafka-python Consumer with error handling and message processing.
    """
    def __init__(self, bootstrap_servers: str, group_id: str, **config):
        """
        Initialize Kafka Consumer
        
        Args:
            bootstrap_servers: Comma-separated list of Kafka brokers
            group_id: Consumer group ID
            **config: Additional Kafka consumer configuration
        """
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.config = {
            'bootstrap_servers': bootstrap_servers,
            'group_id': group_id,
            'auto_offset_reset': 'earliest',
            'enable_auto_commit': False,
            'value_deserializer': lambda x: json.loads(x.decode('utf-8')),
            'key_deserializer': lambda x: x.decode('utf-8'),
            **config
        }
        self.consumer = KafkaConsumerImpl(**self.config)
        self.running = False

    def subscribe(self, topics: List[str]):
        """
        Subscribe to a list of topics
        
        Args:
            topics: List of topic names to subscribe to
        """
        self.consumer.subscribe(topics)

    def consume(self, callback: Callable[[Dict[str, Any]], None], timeout_ms: int = 1000):
        """
        Start consuming messages
        
        Args:
            callback: Function to call for each message
            timeout_ms: Polling timeout in milliseconds
        """
        self.running = True
        try:
            while self.running:
                try:
                    # Poll for messages
                    msg_pack = self.consumer.poll(timeout_ms=timeout_ms, max_records=1)
                    
                    if not msg_pack:
                        continue
                        
                    for topic_partition, messages in msg_pack.items():
                        for message in messages:
                            try:
                                callback(message.value)
                                self.consumer.commit()
                            except json.JSONDecodeError as e:
                                logger.error(f'Failed to decode message: {e}')
                            except Exception as e:
                                logger.error(f'Error processing message: {e}')
                except Exception as e:
                    logger.error(f'Error polling messages: {e}')
                    time.sleep(1)

        except KeyboardInterrupt:
            logger.info('Consumer stopped by user')
        except Exception as e:
            logger.error(f'Consumer error: {e}')
        finally:
            self.close()

    def close(self):
        """
        Close the consumer
        """
        self.running = False
        try:
            self.consumer.close()
        except Exception as e:
            logger.error(f'Error closing consumer: {e}')
        logger.info('Kafka consumer closed')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()