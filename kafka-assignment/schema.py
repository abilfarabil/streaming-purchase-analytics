from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka import Producer
import avro

# Define schema untuk data sensor
DEVICE_SCHEMA = """
{
  "type": "record",
  "name": "DeviceReading",
  "fields": [
    {"name": "Arrival_Time", "type": "long"},
    {"name": "Creation_Time", "type": "long"},
    {"name": "Device", "type": "string"},
    {"name": "Index", "type": "int"},
    {"name": "Model", "type": "string"},
    {"name": "User", "type": "string"},
    {"name": "gt", "type": "string"},
    {"name": "x", "type": "double"},
    {"name": "y", "type": "double"},
    {"name": "z", "type": "double"}
  ]
}
"""