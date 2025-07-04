import time
import json

import adafruit_dht
import board
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="192.168.0.144:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

dht = adafruit_dht.DHT11(board.D4)

while True:
    try:
        payload = {
            "temperature": dht.temperature,
            "humidity": dht.humidity,
            "timestamp": time.time(),
        }
        producer.send("sensor-data", payload)
        print("Sent:", payload)
    except Exception as e:
        print("Error:", e)

    time.sleep(5)
