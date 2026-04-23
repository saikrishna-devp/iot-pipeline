# storage/s3_layer.py
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from datetime import datetime, timezone
from loguru import logger
from config.settings import get_settings

settings = get_settings()
s3 = boto3.client("s3", region_name=settings.aws_region)

def utcnow():
    return datetime.now(timezone.utc)

def get_partition_path(layer: str, sensor_type: str = "mixed") -> str:
    now = utcnow()
    return (
        f"{layer}/"
        f"sensor_type={sensor_type}/"
        f"year={now.year}/"
        f"month={now.month:02d}/"
        f"day={now.day:02d}/"
        f"hour={now.hour:02d}/"
    )

def write_to_s3(bucket: str, key: str, data: bytes, content_type: str):
    s3.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)

def write_bronze(readings: list):
    if not readings:
        return
    timestamp = utcnow().strftime("%Y%m%d_%H%M%S")
    partition = get_partition_path(settings.s3_bronze)
    key = f"{partition}sensor_raw_{timestamp}.json"
    data = json.dumps(readings, default=str).encode("utf-8")
    write_to_s3(settings.s3_bucket, key, data, "application/json")
    logger.success(f"Bronze: wrote {len(readings)} records → s3://{settings.s3_bucket}/{key}")

def parse_reading(reading: dict) -> dict:
    sensor = reading.get("sensor", {})
    location = reading.get("location", {})
    values = {
        item["value_type"]: item["value"]
        for item in reading.get("sensordatavalues", [])
    }
    return {
        "reading_id":  reading.get("id"),
        "sensor_id":   sensor.get("id"),
        "sensor_type": sensor.get("sensor_type", {}).get("name"),
        "timestamp":   reading.get("timestamp"),
        "latitude":    location.get("latitude"),
        "longitude":   location.get("longitude"),
        "country":     location.get("country"),
        "indoor":      bool(location.get("indoor", 0)),
        "pm2_5":       values.get("P2"),
        "pm10":        values.get("P1"),
        "temperature": values.get("temperature"),
        "humidity":    values.get("humidity"),
        "pressure":    values.get("pressure"),
        "noise_laeq":  values.get("noise_LAeq"),
        "ingested_at": utcnow().isoformat()
    }

def write_silver(readings: list):
    if not readings:
        return
    rows = [parse_reading(r) for r in readings]
    df = pd.DataFrame(rows)
    for col in ["pm2_5", "pm10", "temperature", "humidity", "pressure", "noise_laeq"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    table = pa.Table.from_pandas(df)
    buffer = BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    timestamp = utcnow().strftime("%Y%m%d_%H%M%S")
    partition = get_partition_path(settings.s3_silver)
    key = f"{partition}sensor_silver_{timestamp}.parquet"
    write_to_s3(settings.s3_bucket, key, buffer.getvalue(), "application/octet-stream")
    logger.success(f"Silver: wrote {len(rows)} records → s3://{settings.s3_bucket}/{key}")
    return df

def write_gold(df: pd.DataFrame):
    if df is None or df.empty:
        return
    numeric_cols = ["pm2_5", "pm10", "temperature", "humidity", "pressure", "noise_laeq"]
    existing = [c for c in numeric_cols if c in df.columns]
    agg = df.groupby("sensor_type")[existing].agg(["mean", "min", "max"]).round(2)
    agg.columns = ["_".join(col) for col in agg.columns]
    agg = agg.reset_index()
    agg["window_end"] = utcnow().isoformat()
    agg["reading_count"] = df.groupby("sensor_type").size().values
    table = pa.Table.from_pandas(agg)
    buffer = BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    timestamp = utcnow().strftime("%Y%m%d_%H%M%S")
    partition = get_partition_path(settings.s3_gold)
    key = f"{partition}sensor_gold_{timestamp}.parquet"
    write_to_s3(settings.s3_bucket, key, buffer.getvalue(), "application/octet-stream")
    logger.success(f"Gold: aggregated {len(agg)} sensor types → s3://{settings.s3_bucket}/{key}")

if __name__ == "__main__":
    import requests
    logger.info("Fetching sample data to test S3 layers...")
    resp = requests.get(settings.sensor_api_url, timeout=60)
    sample = resp.json()[:100]
    write_bronze(sample)
    df = write_silver(sample)
    write_gold(df)
    logger.info("All 3 layers written successfully!")