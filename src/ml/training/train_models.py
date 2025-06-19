import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from keras.models import Sequential
from keras.layers import LSTM, Dense
import joblib

# Load data
df = pd.read_csv("data/btc_klines.csv")
df["close"] = df["close"].astype(float)

# Chuẩn hóa
scaler = MinMaxScaler()
scaled_close = scaler.fit_transform(df["close"].values.reshape(-1, 1))

# Tạo tập dữ liệu cho LSTM
def create_dataset(data, seq_len=60):
    X, y = [], []
    for i in range(len(data) - seq_len):
        X.append(data[i:i+seq_len])
        y.append(data[i+seq_len])
    return np.array(X), np.array(y)

X, y = create_dataset(scaled_close)
X = X.reshape(X.shape[0], X.shape[1], 1)

# Xây mô hình LSTM đơn giản
model = Sequential()
model.add(LSTM(50, return_sequences=False, input_shape=(X.shape[1], 1)))
model.add(Dense(1))
model.compile(optimizer="adam", loss="mse")

model.fit(X, y, epochs=20, batch_size=32)

# Lưu model và scaler
model.save("src/ml/models/lstm_model.h5")
joblib.dump(scaler, "src/ml/models/lstm_scaler_public.save")
print("✅ LSTM model và scaler đã được lưu.")
