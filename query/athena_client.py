# query/athena_client.py
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
import boto3
import pandas as pd
from loguru import logger
from config.settings import get_settings

settings = get_settings()

# ── Athena Client ─────────────────────────────────────────────────────────────
athena = boto3.client("athena", region_name=settings.aws_region)
s3 = boto3.client("s3", region_name=settings.aws_region)

OUTPUT_BUCKET = "s3://iot-pipeline-saikrishna-athena/results"
DATABASE = "iot_pipeline_db"

# ── Setup ─────────────────────────────────────────────────────────────────────
def create_database():
    """Create Athena database if not exists."""
    query = f"CREATE DATABASE IF NOT EXISTS {DATABASE}"
    run_query(query, database="default")
    logger.success(f"Database '{DATABASE}' ready ✅")

def create_silver_table():
    """Create external table pointing to S3 Silver layer."""
    query = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.sensor_silver (
        reading_id    BIGINT,
        sensor_id     BIGINT,
        sensor_type   STRING,
        timestamp     STRING,
        latitude      STRING,
        longitude     STRING,
        country       STRING,
        indoor        BOOLEAN,
        pm2_5         DOUBLE,
        pm10          DOUBLE,
        temperature   DOUBLE,
        humidity      DOUBLE,
        pressure      DOUBLE,
        noise_laeq    DOUBLE,
        ingested_at   STRING
    )
    STORED AS PARQUET
    LOCATION 's3://{settings.s3_bucket}/{settings.s3_silver}/'
    TBLPROPERTIES ('parquet.compress'='SNAPPY')
    """
    run_query(query)
    logger.success("Silver table created ✅")

def create_gold_table():
    """Create external table pointing to S3 Gold layer."""
    query = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.sensor_gold (
        sensor_type     STRING,
        pm2_5_mean      DOUBLE,
        pm2_5_min       DOUBLE,
        pm2_5_max       DOUBLE,
        pm10_mean       DOUBLE,
        pm10_min        DOUBLE,
        pm10_max        DOUBLE,
        temperature_mean DOUBLE,
        humidity_mean   DOUBLE,
        window_end      STRING,
        reading_count   BIGINT
    )
    STORED AS PARQUET
    LOCATION 's3://{settings.s3_bucket}/{settings.s3_gold}/'
    TBLPROPERTIES ('parquet.compress'='SNAPPY')
    """
    run_query(query)
    logger.success("Gold table created ✅")

# ── Query Runner ──────────────────────────────────────────────────────────────
def run_query(query: str, database: str = DATABASE) -> pd.DataFrame:
    """Execute Athena query and return results as DataFrame."""
    try:
        response = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": database},
            ResultConfiguration={"OutputLocation": OUTPUT_BUCKET}
        )
        execution_id = response["QueryExecutionId"]
        logger.info(f"Query started: {execution_id}")

        # Wait for completion
        while True:
            result = athena.get_query_execution(
                QueryExecutionId=execution_id
            )
            state = result["QueryExecution"]["Status"]["State"]

            if state == "SUCCEEDED":
                logger.success(f"Query succeeded ✅")
                break
            elif state in ["FAILED", "CANCELLED"]:
                reason = result["QueryExecution"]["Status"].get(
                    "StateChangeReason", "Unknown"
                )
                logger.error(f"Query {state}: {reason}")
                return pd.DataFrame()

            time.sleep(1)

        # Fetch results
        results = athena.get_query_results(QueryExecutionId=execution_id)
        rows = results["ResultSet"]["Rows"]

        if len(rows) <= 1:
            return pd.DataFrame()

        headers = [col["VarCharValue"] for col in rows[0]["Data"]]
        data = [
            [col.get("VarCharValue", "") for col in row["Data"]]
            for row in rows[1:]
        ]
        return pd.DataFrame(data, columns=headers)

    except Exception as e:
        logger.error(f"Athena query error: {e}")
        return pd.DataFrame()

# ── Sample Queries ────────────────────────────────────────────────────────────
def query_avg_pm25_by_sensor():
    """Average PM2.5 by sensor type."""
    return run_query("""
        SELECT sensor_type,
               ROUND(AVG(pm2_5), 2) as avg_pm25,
               COUNT(*) as reading_count
        FROM sensor_silver
        WHERE pm2_5 IS NOT NULL
        GROUP BY sensor_type
        ORDER BY avg_pm25 DESC
    """)

def query_high_pollution():
    """Sensors exceeding EPA PM2.5 threshold."""
    return run_query("""
        SELECT sensor_id, sensor_type, country,
               pm2_5, pm10, timestamp
        FROM sensor_silver
        WHERE pm2_5 > 35.4
        ORDER BY pm2_5 DESC
        LIMIT 20
    """)

def query_gold_summary():
    """Summary from Gold aggregated layer."""
    return run_query("""
        SELECT sensor_type,
               pm2_5_mean,
               pm10_mean,
               temperature_mean,
               humidity_mean,
               reading_count
        FROM sensor_gold
        ORDER BY reading_count DESC
    """)

# ── Test ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    logger.info("Setting up Athena...")
    create_database()
    create_silver_table()
    create_gold_table()

    logger.info("Running sample queries...")

    logger.info("1. Average PM2.5 by sensor type:")
    df = query_avg_pm25_by_sensor()
    print(df.to_string() if not df.empty else "No data yet")

    logger.info("2. High pollution sensors:")
    df = query_high_pollution()
    print(df.to_string() if not df.empty else "No data yet")

    logger.info("3. Gold layer summary:")
    df = query_gold_summary()
    print(df.to_string() if not df.empty else "No data yet")