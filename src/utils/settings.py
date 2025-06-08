import os
from pathlib import Path
from dotenv import load_dotenv
from typing import Dict, Any, Optional

class Settings:
    def __init__(self):
        self.PROJECT_DIR = Path(__file__).parent.parent.parent
        
        # Load environment variables from .env file
        env_path = self.PROJECT_DIR / '.env'
        load_dotenv(dotenv_path=env_path)
        
        # Base directories
        self.BASE_DIR = self.PROJECT_DIR / 'src'
        self.DATA_DIR = self.PROJECT_DIR / 'data'
        
        # Create necessary directories
        self.DATA_DIR.mkdir(parents=True, exist_ok=True)
        
        # Logging configuration
        self.LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
        self.LOG_DIR = self.PROJECT_DIR / 'logs'
        self.LOG_DIR.mkdir(parents=True, exist_ok=True)
        
        # Database configuration
        self.DB_HOST = os.getenv('DB_HOST', 'localhost')
        self.DB_PORT = os.getenv('DB_PORT', '5432')
        self.DB_NAME = os.getenv('DB_NAME', 'crypto_bigdata')
        self.DB_USER = os.getenv('DB_USER', 'postgres')
        self.DB_PASSWORD = os.getenv('DB_PASSWORD', 'postgres123')
        
        # Kafka configuration
        self.KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'crypto-bigdata-group')
        self.KAFKA_AUTO_OFFSET_RESET = os.getenv('KAFKA_AUTO_OFFSET_RESET', 'latest')
        self.KAFKA_TOPIC_PREFIX = os.getenv('KAFKA_TOPIC_PREFIX', 'crypto_')
        
        # Streamlit configuration
        self.STREAMLIT_HOST = os.getenv('STREAMLIT_HOST', '0.0.0.0')
        self.STREAMLIT_PORT = os.getenv('STREAMLIT_PORT', '8501')
        
        # API configuration
        self.BINANCE_API_KEY = os.getenv('BINANCE_API_KEY', 'your_binance_api_key_here')
        self.BINANCE_SECRET_KEY = os.getenv('BINANCE_SECRET_KEY', 'your_binance_secret_key_here')
        self.API_TIMEOUT = int(os.getenv('API_TIMEOUT', '30'))
        
        # Spark configuration
        self.SPARK_APP_NAME = os.getenv('SPARK_APP_NAME', 'CryptoBigData')
        self.SPARK_MASTER = os.getenv('SPARK_MASTER', 'local[*]')
        self.SPARK_EXECUTOR_MEMORY = os.getenv('SPARK_EXECUTOR_MEMORY', '2g')
        self.SPARK_DRIVER_MEMORY = os.getenv('SPARK_DRIVER_MEMORY', '1g')

        # Data collection settings
        self.BATCH_SIZE = int(os.getenv('BATCH_SIZE', '100'))
        self.MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
        self.RETRY_DELAY = int(os.getenv('RETRY_DELAY', '5'))
        
# Create settings instance
settings = Settings()