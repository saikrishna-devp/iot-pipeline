# processor/stream_processor.py
# Consumes from sensor_raw, detects anomalies,
# publishes alerts to sensor_alerts
# publishes aggregates to sensor_aggregated

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
from datetime import datetime
from collections import defaultdict
from confluent_kafka import Consumer, Producer, KafkaError
from loguru import logger
from config.settings import get_settings

settings = get_settings()

# ── Kafka Consumer Setup ──────────────────────────────────────────────────────
consumer = Consumer({
    "bootstrap.servers": settings.kafka_bootstrap_servers,
    "group.id": settings.kafka_consumer_group,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True
})

# ── Kafka Producer Setup ──────────────────────────────────────────────────────
producer = Producer({
    "bootstrap.servers": settings.kafka_bootstrap_servers,
    "client.id": "iot-stream-processor"
})

# ── Aggregation Buffer ────────────────────────────────────────────────────────
# Stores readings per sensor for windowed aggregation
agg_buffer = defaultdict(list)
MSG_COUNT = 0
AGG_EVERY = 100  # aggregate every 100 messages

# ── Anomaly Detection ─────────────────────────────────────────────────────────
def detect_anomalies(sensor_id, values):
    """Check sensor values against thresholds. Return alert if breach found."""
    alerts = []

    pm25 = values.get("P2")
    pm10 = values.get("P1")
    noise = values.get("noise_LAeq")
    temp = values.get("temperature")
    humidity = values.get("humidity")

    if pm25 and float(pm25) > settings.pm25_threshold:
        alerts.append({
            "sensor_id": sensor_id,
            "metric": "PM2.5",
            "value": float(pm25),
            "threshold": settings.pm25_threshold,
            "severity": "WARNING",
            "message": f"PM2.5={pm25} µg/m³ exceeds threshold {settings.pm25_threshold}"
        })

    if pm10 and float(pm10) > settings.pm10_threshold:
        alerts.append({
            "sensor_id": sensor_id,
            "metric": "PM10",
            "value": float(pm10),
            "threshold": settings.pm10_threshold,
            "severity": "WARNING",
            "message": f"PM10={pm10} µg/m³ exceeds threshold {settings.pm10_threshold}"
        })

    if noise and float(noise) > settings.noise_threshold:
        alerts.append({
            "sensor_id": sensor_id,
            "metric": "Noise",
            "value": float(noise),
            "threshold": settings.noise_threshold,
            "severity": "WARNING",
            "message": f"Noise={noise} dB exceeds threshold {settings.noise_threshold}"
        })

    return alerts

# ── Parse Sensor Values ───────────────────────────────────────────────────────
def parse_values(sensordatavalues):
    """Convert sensordatavalues list into a flat dict."""
    return {item["value_type"]: item["value"] for item in sensordatavalues}

# ── Publish Alert ─────────────────────────────────────────────────────────────
def publish_alert(alert):
    alert["timestamp"] = datetime.utcnow().isoformat()
    producer.produce(
        topic=settings.kafka_topic_alerts,
        key=str(alert["sensor_id"]),
        value=json.dumps(alert)
    )
    producer.poll(0)

# ── Windowed Aggregation ──────────────────────────────────────────────────────
def aggregate_and_publish():
    """Calculate averages per sensor and publish to sensor_aggregated."""
    for sensor_id, readings in agg_buffer.items():
        if not readings:
            continue

        # Collect numeric values
        metrics = defaultdict(list)
        for r in readings:
            for k, v in r.items():
                try:
                    metrics[k].append(float(v))
                except (ValueError, TypeError):
                    pass

        # Calculate averages
        aggregated = {
            "sensor_id": sensor_id,
            "window_end": datetime.utcnow().isoformat(),
            "reading_count": len(readings),
            "averages": {k: round(sum(v)/len(v), 2) for k, v in metrics.items()}
        }

        producer.produce(
            topic=settings.kafka_topic_aggregated,
            key=str(sensor_id),
            value=json.dumps(aggregated)
        )

    producer.flush()
    logger.info(f"Aggregated {len(agg_buffer)} sensors → {settings.kafka_topic_aggregated}")
    agg_buffer.clear()

# ── Main Processing Loop ──────────────────────────────────────────────────────
def run():
    global MSG_COUNT

    consumer.subscribe([settings.kafka_topic_raw])
    logger.info("Stream Processor started...")
    logger.info(f"Consuming from: {settings.kafka_topic_raw}")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f"Kafka error: {msg.error()}")
                continue

            # Parse message
            try:
                reading = json.loads(msg.value())
                sensor_id = reading.get("sensor", {}).get("id", "unknown")
                values = parse_values(reading.get("sensordatavalues", []))

                # Anomaly detection
                alerts = detect_anomalies(sensor_id, values)
                for alert in alerts:
                    publish_alert(alert)
                    logger.warning(f"ALERT: {alert['message']}")

                # Buffer for aggregation
                agg_buffer[sensor_id].append(values)
                MSG_COUNT += 1

                # Aggregate every N messages
                if MSG_COUNT % AGG_EVERY == 0:
                    aggregate_and_publish()
                    logger.info(f"Processed {MSG_COUNT} messages total")

            except Exception as e:
                logger.error(f"Processing error: {e}")

    except KeyboardInterrupt:
        logger.info(f"Shutting down. Total processed: {MSG_COUNT}")
    finally:
        consumer.close()
        producer.flush()

if __name__ == "__main__":
    run()