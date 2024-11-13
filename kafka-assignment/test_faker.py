from device_data_faker import DeviceDataFaker

faker = DeviceDataFaker()

# Generate dan print beberapa contoh data
for _ in range(5):
    data = faker.generate_device_data()
    print(data)