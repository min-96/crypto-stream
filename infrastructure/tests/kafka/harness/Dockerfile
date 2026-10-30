FROM python:3.12.7-slim
WORKDIR /harness
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY check.py .
COPY check_preprocessing.py .
COPY check_offsets.py .
COPY check_retention.py .
COPY bench_websocket_raw.py .
