from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "sensor-data",
    bootstrap_servers="192.168.0.144:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    group_id="sensor-group",
)

for msg in consumer:
    print("Received:", msg.value)
