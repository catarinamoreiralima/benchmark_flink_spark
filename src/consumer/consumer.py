from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='test-consumer',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Waiting for messages...")

try:
    for message in consumer:
        print("Received:", message.value)

except KeyboardInterrupt:
    print("\nClosing consumer...")

finally:
    consumer.close()
    print("Consumer closed.")