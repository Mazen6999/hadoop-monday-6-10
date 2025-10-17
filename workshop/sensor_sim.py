from kafka import KafkaProducer
import json
import random
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda x: json.dumps(x).encode('utf-8'))

for _ in range(1000):

    data = {
        "timestamp": time.strftime('%Y-%m-%dT%H:%M:%S'),
        'sensor_id': random.choice(['temp', 'humd', 'motion']),
        'value': random.uniform(20.0, 40.0)
    }

    if random.random() < 0.1:
        data['value'] = random.uniform(100.0, 200.0)



    producer.send('sensor_data', value=data)
    print(f"Sent: {data}")
    time.sleep(1)  

producer.flush() # Ensure all messages are sent before exiting
