from kafka import KafkaProducer
import json
import time

from kafka import KafkaProducer
import json

# Configure the Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Your Kafka broker address
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize data as JSON
)

# Send a message to the 'test-topic' topic
message = {'order_id': 1234, 'product': 'laptop', 'quantity': 1}
producer.send('test-topic', value=message)

# Wait for all messages to be sent
producer.flush()

print("Message sent successfully!")

# Close the producer
producer.close()
