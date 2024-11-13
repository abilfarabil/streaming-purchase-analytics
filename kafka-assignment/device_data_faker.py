import random
import time
from faker import Faker
from datetime import datetime

class DeviceDataFaker:
    def __init__(self):
        self.faker = Faker()
        self.devices = ['nexus4', 'iphone12', 'pixel6', 'galaxy_s21']
        self.activities = ['stand', 'walk', 'run', 'sit']
        self.device_counter = {}  # Untuk tracking index per device
        
    def generate_device_data(self):
        # Pilih device secara random
        device_model = self.faker.random_element(self.devices)
        device_id = f"{device_model}_{self.faker.random_int(min=1, max=3)}"
        
        # Inisialisasi atau increment counter untuk device ini
        if device_id not in self.device_counter:
            self.device_counter[device_id] = 0
        self.device_counter[device_id] += 1
        
        # Generate timestamp
        current_time = int(time.time() * 1000)  # milliseconds
        creation_time = int(time.time() * 1000000000)  # nanoseconds
        
        # Generate sensor data dengan random noise
        x = random.uniform(-0.01, 0.01)
        y = random.uniform(-0.05, 0.05)
        z = random.uniform(-0.02, 0.02)
        
        return {
            "Arrival_Time": current_time,
            "Creation_Time": creation_time,
            "Device": device_id,
            "Index": self.device_counter[device_id],
            "Model": device_model,
            "User": self.faker.random_lowercase_letter(),
            "gt": self.faker.random_element(self.activities),
            "x": round(x, 10),
            "y": round(y, 10),
            "z": round(z, 10)
        }