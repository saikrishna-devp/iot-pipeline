# config/settings.py
# Central config — all values come from .env file

from pydantic_settings import BaseSettings
from functools import lru_cache

class Settings(BaseSettings):
    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_raw: str = "sensor_raw"
    kafka_topic_alerts: str = "sensor_alerts"
    kafka_topic_aggregated: str = "sensor_aggregated"
    kafka_consumer_group: str = "iot-processor-group"

    # Sensor.Community
    sensor_api_url: str = "https://data.sensor.community/static/v2/data.1h.json"
    sensor_fetch_interval: int = 300  # seconds

    # AWS
    aws_region: str = "us-east-1"
    s3_bucket: str = "iot-pipeline-saikrishna"
    s3_bronze: str = "bronze"
    s3_silver: str = "silver"
    s3_gold: str = "gold"

    # Timestream
    timestream_database: str = "IoTPipelineDB"
    timestream_table: str = "SensorReadings"

    # Athena
    athena_output: str = "s3://iot-pipeline-saikrishna/athena-results"

    # Dashboard
    dash_port: int = 8050

    # Anomaly Detection
    pm25_threshold: float = 35.4
    pm10_threshold: float = 154.0
    noise_threshold: float = 70.0

    class Config:
        env_file = ".env"

@lru_cache
def get_settings():
    return Settings()