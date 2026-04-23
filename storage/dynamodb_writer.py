# storage/dynamodb_writer.py
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import boto3
from datetime import datetime, timezone
from loguru import logger
from config.settings import get_settings

settings = get_settings()

dynamodb = boto3.resource("dynamodb", region_name=settings.aws_region)
table = dynamodb.Table("SensorReadings")

def utcnow():
    return datetime.now(timezone.utc)

def write_reading(sensor_id, values: dict, country: str = None):
    """Write a single sensor reading to DynamoDB."""
    try:
        item = {
            "sensor_id":  str(sensor_id),
            "timestamp":  utcnow().isoformat(),
            "country":    country or "unknown",
            "pm2_5":      str(values.get("P2", "")),
            "pm10":       str(values.get("P1", "")),
            "temperature":str(values.get("temperature", "")),
            "humidity":   str(values.get("humidity", "")),
            "pressure":   str(values.get("pressure", "")),
            "noise_laeq": str(values.get("noise_LAeq", "")),
            "ingested_at":utcnow().isoformat()
        }
        # Remove empty strings
        item = {k: v for k, v in item.items() if v != ""}
        table.put_item(Item=item)
        return True
    except Exception as e:
        logger.error(f"DynamoDB write failed: {e}")
        return False

def write_alert(sensor_id, metric, value, threshold, message):
    """Write an alert to DynamoDB."""
    try:
        item = {
            "sensor_id":  f"ALERT#{sensor_id}",
            "timestamp":  utcnow().isoformat(),
            "metric":     metric,
            "value":      str(value),
            "threshold":  str(threshold),
            "message":    message,
            "severity":   "WARNING"
        }
        table.put_item(Item=item)
        return True
    except Exception as e:
        logger.error(f"DynamoDB alert write failed: {e}")
        return False

def get_recent_readings(sensor_id, limit=10):
    """Get recent readings for a sensor."""
    try:
        from boto3.dynamodb.conditions import Key
        response = table.query(
            KeyConditionExpression=Key("sensor_id").eq(str(sensor_id)),
            ScanIndexForward=False,
            Limit=limit
        )
        return response.get("Items", [])
    except Exception as e:
        logger.error(f"DynamoDB query failed: {e}")
        return []

# ── Test ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    logger.info("Testing DynamoDB writer...")

    # Write a test reading
    success = write_reading(
        sensor_id=12345,
        values={"P2": "25.5", "P1": "45.0", "temperature": "22.5", "humidity": "65"},
        country="US"
    )
    logger.success(f"Write reading: {'✅' if success else '❌'}")

    # Write a test alert
    success = write_alert(
        sensor_id=12345,
        metric="PM2.5",
        value=77.4,
        threshold=35.4,
        message="PM2.5 exceeds threshold"
    )
    logger.success(f"Write alert: {'✅' if success else '❌'}")

    # Read back
    readings = get_recent_readings(12345)
    logger.success(f"Read back: {len(readings)} records found ✅")
    logger.info(f"Sample: {readings[0] if readings else 'none'}")