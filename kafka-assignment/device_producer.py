import json
import os
import time
from kafka import KafkaProducer
from device_data_faker import DeviceDataFaker

# Kafka configuration (dari environment variables)
KAFKA_HOST = os.getenv('KAFKA_HOST', 'dataeng-kafka')
KAFKA_PORT = os.getenv('KAFKA_PORT', '9092')
TOPIC_NAME = os.getenv('KAFKA_TOPIC_NAME', 'device-sensors')

class DeviceProducer:
    def __init__(self):
        print(f"Connecting to Kafka at {KAFKA_HOST}:{KAFKA_PORT}")
        retries = 0
        while retries < 3:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=f'{KAFKA_HOST}:{KAFKA_PORT}',
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                )
                print("Successfully connected to Kafka")
                break
            except Exception as e:
                print(f"Error connecting to Kafka (attempt {retries + 1}/3): {e}")
                retries += 1
                time.sleep(5)
                if retries == 3:
                    raise Exception(f"Failed to connect to Kafka after 3 attempts")
        
        self.faker = DeviceDataFaker()
    
    def get_partition(self, device_model):
        model_partitions = {
            'nexus4': 0,
            'iphone12': 1,
            'pixel6': 2,
            'galaxy_s21': 2
        }
        return model_partitions.get(device_model, 0)
    
    def start_producing(self):
        print("Starting to produce device sensor data...")
        message_count = 0
        while True:
            try:
                data = self.faker.generate_device_data()
                partition = self.get_partition(data['Model'])
                
                future = self.producer.send(
                    topic=TOPIC_NAME,
                    value=data,
                    partition=partition
                )
                
                metadata = future.get(timeout=10)
                message_count += 1
                
                print(f"\nMessage {message_count} successfully produced:")
                print(f"Topic: {TOPIC_NAME}")
                print(f"Device: {data['Device']}")
                print(f"Partition: {partition}")
                print(f"Offset: {metadata.offset}")
                print(f"Data: {json.dumps(data, indent=2)}")
                print("-" * 50)
                
                time.sleep(5)
                
            except Exception as e:
                print(f"Error producing event: {str(e)}")
                time.sleep(5)

if __name__ == "__main__":
    producer = DeviceProducer()
    producer.start_producing()