from kafka import KafkaConsumer
import json

# Configure the Kafka Consumer
consumer = KafkaConsumer(
    'test-topic',  # Name of the topic to subscribe to
    bootstrap_servers='localhost:9092',  # Kafka broker address
    group_id='my-group',  # Consumer group
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Deserialize the JSON message
)

# Consume messages from the topic
for message in consumer:
    print(f"Received message: {message.value}")

    # You can process the message here
    # For example, print the order info
    order = message.value
    print(f"Order ID: {order['order_id']}, Product: {order['product']}, Quantity: {order['quantity']}")

# The consumer will keep running until you stop it (Ctrl+C)
