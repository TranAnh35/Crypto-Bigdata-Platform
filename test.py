# main.py
import threading
import time
from src.collectors.batch_collector import BatchCollector
from src.collectors.stream_collector import StreamCollector
from src.utils.logger import get_logger
from binance.client import Client as BinanceLibClient

logger = get_logger(__name__)

def run_batch_job():
    """Run a one-time batch collection job."""
    logger.info("--- Starting Historical Data Batch Job ---")
    try:
        # Lấy dữ liệu nến 1 ngày của BTCUSDT từ 1/1/2023 đến 31/1/2023
        batch_collector = BatchCollector(
            symbol='BTCUSDT',
            interval=BinanceLibClient.KLINE_INTERVAL_1DAY,
            start_date="1 Jan, 2024",
            end_date="31 Jan, 2025"
        )
        batch_collector.collect()
        logger.info("--- Batch Job Finished ---")
    except Exception as e:
        logger.error(f"Batch job failed: {e}", exc_info=True)

def run_stream_job(symbol, interval):
    """Target function to run a stream collector in a thread."""
    logger.info(f"--- Starting Stream Job for {symbol} ({interval}) ---")
    try:
        stream_collector = StreamCollector(symbol=symbol, interval=interval)
        stream_collector.collect()
    except Exception as e:
        logger.error(f"Stream job for {symbol} failed: {e}", exc_info=True)
    logger.info(f"--- Stream Job for {symbol} Terminated ---")


if __name__ == "__main__":
    # --- Tùy chọn 1: Chạy một BATCH job (tác vụ hữu hạn) ---
    # Bỏ comment dòng dưới để chạy
    # logger.info("Initializing batch job...")
    # run_batch_job()
    # logger.info("Batch job finished.")
    
    # docker-compose exec kafka kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic crypto_klines_historical_btcusdt_1d --from-beginning
    
    # --- Tùy chọn 2: Chạy các STREAM job (tác vụ vô hạn) song song ---
    logger.info("Initializing stream collectors in separate threads...")

    # Tạo thread cho stream BTCUSDT 1 phút
    btc_stream_thread = threading.Thread(
        target=run_stream_job, 
        args=('BTCUSDT', BinanceLibClient.KLINE_INTERVAL_1MINUTE),
        daemon=True # Đặt là daemon để thread tự thoát khi chương trình chính kết thúc
    )

    # Tạo thread cho stream ETHUSDT 1 phút
    eth_stream_thread = threading.Thread(
        target=run_stream_job,
        args=('ETHUSDT', BinanceLibClient.KLINE_INTERVAL_1MINUTE),
        daemon=True
    )

    # Bắt đầu các thread
    btc_stream_thread.start()
    eth_stream_thread.start()

    logger.info("Stream collectors are running. Press Ctrl+C to stop.")

    try:
        # Giữ chương trình chính chạy để các aemon thread có thể hoạt động
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutdown signal received. Exiting main program.")
        
    # docker-compose exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic crypto_klines_stream_btcusdt_1m --from-beginning
    # docker-compose exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic crypto_klines_stream_ethusdt_1m --from-beginning