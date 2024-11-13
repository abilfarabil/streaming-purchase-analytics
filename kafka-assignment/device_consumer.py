import json
import os
from kafka import KafkaConsumer
from collections import defaultdict
import numpy as np
from datetime import datetime

class DeviceConsumer:
    def __init__(self, group_id):
        self.topic = 'device-sensors'
        self.group_id = group_id
        
        # Inisialisasi consumer
        print(f"Initializing consumer with group_id: {group_id}")
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers='dataeng-kafka:9092',
            group_id=self.group_id,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        # Untuk menyimpan rolling metrics
        self.device_metrics = defaultdict(lambda: {
            'count': 0,
            'x_values': [],
            'y_values': [],
            'z_values': [],
            'activities': defaultdict(int)
        })

    def calculate_metrics(self, device_id, data):
        metrics = self.device_metrics[device_id]
        
        # Update basic counters
        metrics['count'] += 1
        metrics['activities'][data['gt']] += 1
        
        # Update sensor values arrays (keeping last 50 values)
        max_history = 50
        metrics['x_values'].append(data['x'])
        metrics['y_values'].append(data['y'])
        metrics['z_values'].append(data['z'])
        
        if len(metrics['x_values']) > max_history:
            metrics['x_values'] = metrics['x_values'][-max_history:]
            metrics['y_values'] = metrics['y_values'][-max_history:]
            metrics['z_values'] = metrics['z_values'][-max_history:]
        
        # Calculate rolling statistics
        result = {
            'device_id': device_id,
            'total_events': metrics['count'],
            'activities_count': dict(metrics['activities']),
            'sensor_metrics': {
                'x': {
                    'mean': np.mean(metrics['x_values']),
                    'std': np.std(metrics['x_values']),
                    'min': min(metrics['x_values']),
                    'max': max(metrics['x_values'])
                },
                'y': {
                    'mean': np.mean(metrics['y_values']),
                    'std': np.std(metrics['y_values']),
                    'min': min(metrics['y_values']),
                    'max': max(metrics['y_values'])
                },
                'z': {
                    'mean': np.mean(metrics['z_values']),
                    'std': np.std(metrics['z_values']),
                    'min': min(metrics['z_values']),
                    'max': max(metrics['z_values'])
                }
            },
            'most_common_activity': max(metrics['activities'].items(), key=lambda x: x[1])[0],
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        return result

    def start_consuming(self):
        print(f"Starting to consume messages from topic: {self.topic}")
        try:
            for message in self.consumer:
                # Get the data
                data = message.value
                device_id = data['Device']
                partition = message.partition
                
                # Calculate metrics
                metrics = self.calculate_metrics(device_id, data)
                
                # Print information
                print("\n" + "="*50)
                print(f"Received message from partition {partition}")
                print(f"Device: {device_id}")
                print(f"Current Activity: {data['gt']}")
                print("\nRolling Metrics:")
                print(f"Total Events: {metrics['total_events']}")
                print(f"Most Common Activity: {metrics['most_common_activity']}")
                print("\nSensor Statistics:")
                for axis in ['x', 'y', 'z']:
                    stats = metrics['sensor_metrics'][axis]
                    print(f"\n{axis.upper()} Axis:")
                    print(f"  Mean: {stats['mean']:.6f}")
                    print(f"  Std:  {stats['std']:.6f}")
                    print(f"  Min:  {stats['min']:.6f}")
                    print(f"  Max:  {stats['max']:.6f}")
                print("="*50)

        except KeyboardInterrupt:
            print("\nStopping consumer...")
        finally:
            self.consumer.close()

if __name__ == "__main__":
    # Create Dockerfile for consumer
    consumer = DeviceConsumer(group_id="device-monitoring-group")
    consumer.start_consuming()