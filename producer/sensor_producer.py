# producer/sensor_producer.py
# Fetches real sensor data from Sensor.Community API
# and publishes to Kafka topic: sensor_raw
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import json
import time
import requests
from loguru import logger
from confluent_kafka import Producer
from config.settings import get_settings

settings = get_settings()

# ── Kafka Producer Setup ──────────────────────────────────────────────────────
producer = Producer({
    "bootstrap.servers": settings.kafka_bootstrap_servers,
    "client.id": "iot-sensor-producer"
})

def delivery_report(err, msg):
    """Called when message is delivered or fails."""
    if err:
        logger.error(f"Delivery failed: {err}")
    else:
        logger.success(f"Delivered to {msg.topic()} partition {msg.partition()}")

# ── Fetch from Sensor.Community ───────────────────────────────────────────────
def fetch_sensor_data():
    """Fetch latest sensor readings from Sensor.Community API."""
    logger.info(f"Fetching data from {settings.sensor_api_url}")
    try:
        response = requests.get(settings.sensor_api_url, timeout=60)
        response.raise_for_status()
        data = response.json()
        logger.info(f"Fetched {len(data)} sensor readings")
        return data
    except Exception as e:
        logger.error(f"Failed to fetch sensor data: {e}")
        return []

# ── Publish to Kafka ──────────────────────────────────────────────────────────
def publish_to_kafka(readings):
    """Publish each sensor reading to Kafka sensor_raw topic."""
    published = 0
    for reading in readings:
        try:
            sensor_id = str(reading.get("sensor", {}).get("id", "unknown"))
            producer.produce(
                topic=settings.kafka_topic_raw,
                key=sensor_id,
                value=json.dumps(reading),
                callback=delivery_report
            )
            published += 1
        except Exception as e:
            logger.error(f"Failed to publish reading: {e}")

    producer.flush()
    logger.info(f"Published {published} readings to {settings.kafka_topic_raw}")
    return published

# ── Main Loop ─────────────────────────────────────────────────────────────────
def run():
    logger.info("IoT Producer started...")
    logger.info(f"Kafka: {settings.kafka_bootstrap_servers}")
    logger.info(f"Topic: {settings.kafka_topic_raw}")
    logger.info(f"Fetch interval: {settings.sensor_fetch_interval}s")

    while True:
        readings = fetch_sensor_data()
        if readings:
            publish_to_kafka(readings)
        logger.info(f"Sleeping {settings.sensor_fetch_interval}s...")
        time.sleep(settings.sensor_fetch_interval)

if __name__ == "__main__":
    run()