import logging
import logging.handlers
import os
import sys
from pathlib import Path
from typing import Optional

from .settings import settings

class Logger:
    _instance = None
    
    def __new__(cls, name: Optional[str] = None):
        if cls._instance is None:
            cls._instance = super(Logger, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, name: Optional[str] = None):
        if self._initialized:
            return
            
        self._initialized = True
        self.name = name or __name__
        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(settings.LOG_LEVEL)
        
        # Prevent adding handlers multiple times
        if not self.logger.handlers:
            self._setup_handlers()
    
    def _setup_handlers(self):
        """Setup console and file handlers"""
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        
        # File handler - Rotating file based on size
        log_file = settings.LOG_DIR / 'crypto_platform.log'
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=5,
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
    
    def get_logger(self) -> logging.Logger:
        """Get the configured logger instance"""
        return self.logger

# Create a default logger instance
def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Get a configured logger instance.
    
    Args:
        name: Optional name for the logger. If not provided, uses the module name.
    
    Returns:
        Configured logger instance
    """
    return Logger(name).get_logger()