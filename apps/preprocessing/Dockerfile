FROM python:3.12.7-slim
WORKDIR /app
RUN pip install --no-cache-dir aiokafka==0.11.0
COPY consumer.py offsets.py pipeline.py upbit.py bithumb.py ticker.py ./
USER 65532:65532
CMD ["python", "consumer.py", "ticker", "ticker:convert_ticker"]
