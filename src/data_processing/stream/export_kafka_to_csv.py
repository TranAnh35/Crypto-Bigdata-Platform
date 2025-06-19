import json
import pandas as pd
from kafka import KafkaConsumer
from datetime import datetime

# Kafka config
TOPIC = "crypto_klines_stream_btcusdt_1m"
BOOTSTRAP_SERVERS = "localhost:9092"

# Tạo consumer để đọc từ đầu
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    enable_auto_commit=False,
    group_id="csv-export-test"
)

records = []
print("Reading messages from Kafka...")

for msg in consumer:
    print(f"🔄 Nhận từ Kafka: {msg}")
    value = msg.value
    try:
        ts = datetime.utcfromtimestamp(value["open_time"] / 1000)
        close = float(value["close"])
        records.append({"timestamp": ts, "close": close})
    except Exception as e:
        print("❌ Error parsing message:", e)

    # Dừng nếu đọc quá nhiều (ví dụ 200 dòng)
    if len(records) >= 200:
        break

# Ghi ra CSV
df = pd.DataFrame(records)
df.to_csv("data/btc_klines.csv", index=False)
print("✅ Export done: data/btc_klines.csv")
