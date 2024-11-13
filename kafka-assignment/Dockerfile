FROM python:3.9-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt \
    && pip install grpcio-tools requests

# Copy our code
COPY device_data_faker.py .
COPY device_producer.py .
COPY device_producer_proto.py .
COPY device_message.proto .
COPY device_ksql.sql .
COPY run_ksql.py .

# Compile protobuf
RUN python -m grpc_tools.protoc -I. --python_out=. device_message.proto

# Environment variables langsung di Dockerfile
ENV KAFKA_HOST=dataeng-kafka
ENV KAFKA_PORT=9092
ENV KAFKA_TOPIC_NAME=device-sensors
ENV KSQL_HOST=dataeng-ksql
ENV KSQL_PORT=8088

# Default command tetap menggunakan producer original
# Bisa dioverride saat menjalankan container untuk menjalankan producer protobuf atau ksql
CMD ["python", "device_producer.py"]