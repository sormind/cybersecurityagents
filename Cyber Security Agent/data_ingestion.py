import os
import json
from kafka import KafkaProducer

def load_data(file_path):
    with open(file_path, 'r') as file:
        data = file.read()
    return data

def preprocess_data(data):
    # Perform data parsing and normalization
    processed_data = data.strip().replace('\n', ' ')
    return processed_data

def send_to_kafka(data, topic):
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                             value_serializer=lambda x: json.dumps(x).encode('utf-8'))
    producer.send(topic, value=data)
    producer.flush()

def main():
    data_sources = [
        {'file_path': 'network_traffic.log', 'topic': 'network_traffic'},
        {'file_path': 'system_logs.log', 'topic': 'system_logs'},
        {'file_path': 'endpoint_devices.log', 'topic': 'endpoint_devices'},
        {'file_path': 'file_system.log', 'topic': 'file_system'},
        {'file_path': 'user_behavior.log', 'topic': 'user_behavior'}
    
    ]

    for source in data_sources:
        data = load_data(source['file_path'])
        processed_data = preprocess_data(data)
        send_to_kafka(processed_data, source['topic'])

if __name__ == '__main__':
    main()