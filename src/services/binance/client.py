from binance.client import Client
from binance.exceptions import BinanceAPIException
from typing import List, Dict, Any, Optional
import time

from ...utils.settings import settings
from ...utils.logger import get_logger

logger = get_logger(__name__)

class BinanceClient:
    """
    A wrapper for the python-binance library to handle API requests,
    authentication, and error handling.
    """
    def __init__(self, api_key: str, api_secret: str, max_retries: int = 3, retry_delay: int = 5):
        self.api_key = api_key
        self.api_secret = api_secret
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        
        try:
            self._client = Client(self.api_key, self.api_secret)
            self._client.ping()
            logger.info("Successfully connected to Binance API.")
        except BinanceAPIException as e:
            logger.error(f"Failed to connect to Binance API: {e}")
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred during Binance client initialization: {e}")
            raise

    def _make_request(self, method, *args, **kwargs) -> Optional[Any]:
        """
        A helper to make requests with retry logic.
        """
        for attempt in range(self.max_retries):
            try:
                return method(*args, **kwargs)
            except BinanceAPIException as e:
                logger.warning(f"Binance API error (attempt {attempt + 1}/{self.max_retries}): {e}")
                if e.status_code == 429 or e.status_code == 418: # Rate limit
                    logger.warning("Rate limit exceeded. Waiting before retry...")
                    time.sleep(self.retry_delay * (attempt + 1)) # Exponential backoff
                else:
                    logger.error(f"Unhandled Binance API error: {e}")
                    return None
            except Exception as e:
                logger.error(f"A non-API error occurred (attempt {attempt + 1}/{self.max_retries}): {e}")
                time.sleep(self.retry_delay)
        
        logger.error(f"Failed to execute method {method.__name__} after {self.max_retries} retries.")
        return None

    def get_klines(self, symbol: str, interval: str, limit: int = 100) -> Optional[List[Dict[str, Any]]]:
        """
        Get Kline/Candlestick data for a symbol.

        Args:
            symbol (str): The trading pair, e.g., 'BTCUSDT'.
            interval (str): The kline interval, e.g., Client.KLINE_INTERVAL_1MINUTE.
            limit (int): Default 500; max 1000.

        Returns:
            A list of dictionaries, where each dictionary represents a kline.
            Returns None if the request fails.
        """
        logger.info(f"Fetching {limit} klines for {symbol} with interval {interval}...")
        klines_raw = self._make_request(self._client.get_klines, symbol=symbol, interval=interval, limit=limit)
        
        if not klines_raw:
            return None

        processed_klines = []
        for k in klines_raw:
            kline = {
                'open_time': int(k[0]),
                'open': float(k[1]),
                'high': float(k[2]),
                'low': float(k[3]),
                'close': float(k[4]),
                'volume': float(k[5]),
                'close_time': int(k[6]),
                'quote_asset_volume': float(k[7]),
                'number_of_trades': int(k[8]),
                'taker_buy_base_asset_volume': float(k[9]),
                'taker_buy_quote_asset_volume': float(k[10]),
                'symbol': symbol,
                'interval': interval
            }
            processed_klines.append(kline)
        
        logger.info(f"Successfully fetched and processed {len(processed_klines)} klines for {symbol}.")
        return processed_klines

    def get_historical_klines(self, symbol: str, interval: str, start_str: str, end_str: str = None) -> Optional[List[Dict[str, Any]]]:
        """
        Get historical klines from Binance.
        This method handles pagination automatically using a generator.

        Args:
            symbol (str): Trading pair, e.g., 'BTCUSDT'.
            interval (str): Kline interval.
            start_str (str): Start date string in UTC, e.g., "1 Jan, 2020".
            end_str (str, optional): End date string in UTC. Defaults to now.

        Returns:
            A list of kline data dictionaries, or None if the request fails.
        """
        logger.info(f"Fetching historical klines for {symbol} from {start_str} to {end_str or 'now'}")
        
        try:
            # Sử dụng generator của thư viện để tự động xử lý việc lấy dữ liệu qua nhiều trang
            kline_generator = self._client.get_historical_klines_generator(symbol, interval, start_str, end_str)
            
            processed_klines = []
            for k in kline_generator:
                kline = {
                    'open_time': int(k[0]),
                    'open': float(k[1]),
                    'high': float(k[2]),
                    'low': float(k[3]),
                    'close': float(k[4]),
                    'volume': float(k[5]),
                    'close_time': int(k[6]),
                    'quote_asset_volume': float(k[7]),
                    'number_of_trades': int(k[8]),
                    'taker_buy_base_asset_volume': float(k[9]),
                    'taker_buy_quote_asset_volume': float(k[10]),
                    'symbol': symbol,
                    'interval': interval
                }
                processed_klines.append(kline)

            logger.info(f"Successfully fetched {len(processed_klines)} historical klines for {symbol}.")
            return processed_klines
            
        except BinanceAPIException as e:
            logger.error(f"API error fetching historical klines for {symbol}: {e}")
            return None
        except Exception as e:
            logger.error(f"An unexpected error occurred fetching historical klines for {symbol}: {e}", exc_info=True)
            return None