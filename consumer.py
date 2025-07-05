import json
import sqlite3

from kafka import KafkaConsumer


conn = sqlite3.connect("sensor_data.db")
cursor = conn.cursor()

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS sensor_readings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        temperature REAL,
        humidity REAL,
        timestamp REAL
    )
"""
)
conn.commit()


consumer = KafkaConsumer(
    "sensor-data",
    bootstrap_servers="192.168.0.144:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id="sensor-group",
)

for msg in consumer:
    print("Received:", msg.value)

    try:
        cursor.execute(
            """
              INSERT INTO sensor_readings (temperature, humidity, timestamp)
              VALUES (?, ?, ?)
              """,
            (msg.value["temperature"], msg.value["humidity"], msg.value["timestamp"]),
        )
        conn.commit()
    except Exception as e:
        print("DB error:", e)
