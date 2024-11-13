import time
import device_message_pb2
from kafka import KafkaProducer
from device_data_faker import DeviceDataFaker

class DeviceProtoProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers='dataeng-kafka:9092',
            value_serializer=lambda v: v.SerializeToString()
        )
        self.faker = DeviceDataFaker()

    def create_proto_message(self, data):
        message = device_message_pb2.DeviceReading()
        message.arrival_time = data['Arrival_Time']
        message.creation_time = data['Creation_Time']
        message.device = data['Device']
        message.index = data['Index']
        message.model = data['Model']
        message.user = data['User']
        message.gt = data['gt']
        message.x = data['x']
        message.y = data['y']
        message.z = data['z']
        return message

    def start_producing(self):
        while True:
            data = self.faker.generate_device_data()
            proto_message = self.create_proto_message(data)
            
            self.producer.send('device-sensors-proto', value=proto_message)
            print(f"Sent proto message for device: {data['Device']}")
            time.sleep(5)

if __name__ == "__main__":
    producer = DeviceProtoProducer()
    producer.start_producing()