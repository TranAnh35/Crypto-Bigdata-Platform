# docker/Stream/Dockerfile
FROM python:3.12-slim

WORKDIR /app

# Copy source code
COPY ./src /app/src
COPY pyproject.toml poetry.lock /app/

# Cài Poetry và phụ thuộc
RUN pip install --upgrade pip && \
    pip install poetry && \
    poetry config virtualenvs.create false && \
    poetry install --no-root --only main --no-interaction --no-ansi


ENV PYTHONPATH=/app

# Command mặc định khi chạy container
CMD ["python", "src/collectors/stream_collector.py"]
