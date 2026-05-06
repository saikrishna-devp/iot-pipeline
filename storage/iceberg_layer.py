# storage/iceberg_layer.py
# Apache Iceberg table management using AWS Glue catalog
# Wraps S3 Gold layer with ACID table format

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pyarrow as pa
from pyiceberg.catalog.glue import GlueCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, DoubleType,
    LongType, BooleanType, TimestampType
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform, IdentityTransform
from loguru import logger
from config.settings import get_settings

settings = get_settings()

# ── Catalog Setup ─────────────────────────────────────────────────────────────
def get_catalog():
    """Connect to AWS Glue catalog."""
    return GlueCatalog(
        "iot_catalog",
        **{
            "warehouse": f"s3://{settings.s3_bucket}/iceberg-warehouse",
            "region_name": settings.aws_region,
        }
    )

# ── Iceberg Schema ────────────────────────────────────────────────────────────
SENSOR_SCHEMA = Schema(
    NestedField(1,  "sensor_id",        LongType(),   required=True),
    NestedField(2,  "sensor_type",      StringType(), required=True),
    NestedField(3,  "country",          StringType(), required=False),
    NestedField(4,  "latitude",         DoubleType(), required=False),
    NestedField(5,  "longitude",        DoubleType(), required=False),
    NestedField(6,  "pm2_5",           DoubleType(), required=False),
    NestedField(7,  "pm10",            DoubleType(), required=False),
    NestedField(8,  "temperature",      DoubleType(), required=False),
    NestedField(9,  "humidity",         DoubleType(), required=False),
    NestedField(10, "pressure",         DoubleType(), required=False),
    NestedField(11, "noise_laeq",       DoubleType(), required=False),
    NestedField(12, "reading_count",    LongType(),   required=False),
    NestedField(13, "window_end",       StringType(), required=False),
    NestedField(14, "is_anomaly",       BooleanType(),required=False),
)

# ── Table Management ──────────────────────────────────────────────────────────
def create_iceberg_table():
    """Create Iceberg table in Glue catalog if not exists."""
    try:
        catalog = get_catalog()

        # Create namespace if not exists
        try:
            catalog.create_namespace("iot_pipeline_db")
            logger.info("Namespace iot_pipeline_db created")
        except Exception:
            logger.info("Namespace iot_pipeline_db already exists")

        # Create table if not exists
        table_name = "iot_pipeline_db.sensor_readings_iceberg"
        try:
            table = catalog.load_table(table_name)
            logger.info(f"Table {table_name} already exists")
        except Exception:
            table = catalog.create_table(
                identifier=table_name,
                schema=SENSOR_SCHEMA,
                location=f"s3://{settings.s3_bucket}/iceberg-warehouse/sensor_readings",
                partition_spec=PartitionSpec(
                    PartitionField(
                        source_id=3,
                        field_id=1000,
                        transform=IdentityTransform(),
                        name="country"
                    )
                )
            )
            logger.success(f"Iceberg table created: {table_name}")

        return table

    except Exception as e:
        logger.error(f"Failed to create Iceberg table: {e}")
        return None

# ── Write to Iceberg ──────────────────────────────────────────────────────────
def write_to_iceberg(df):
    """Write a pandas DataFrame to the Iceberg table."""
    if df is None or df.empty:
        return

    try:
        table = create_iceberg_table()
        if table is None:
            return

        # Convert pandas to PyArrow
        arrow_table = pa.Table.from_pandas(df, schema=SENSOR_SCHEMA.as_arrow())
        table.append(arrow_table)

        logger.success(f"Iceberg: wrote {len(df)} records")

    except Exception as e:
        logger.error(f"Iceberg write failed: {e}")

# ── Read from Iceberg ─────────────────────────────────────────────────────────
def read_from_iceberg(filter_country=None):
    """Read data from Iceberg table."""
    try:
        catalog = get_catalog()
        table = catalog.load_table("iot_pipeline_db.sensor_readings_iceberg")

        scan = table.scan()
        arrow_table = scan.to_arrow()

        import pandas as pd
        df = arrow_table.to_pandas()
        logger.success(f"Iceberg: read {len(df)} records")
        return df

    except Exception as e:
        logger.error(f"Iceberg read failed: {e}")
        return None

# ── Table Info ────────────────────────────────────────────────────────────────
def show_table_info():
    """Show Iceberg table metadata."""
    try:
        catalog = get_catalog()
        table = catalog.load_table("iot_pipeline_db.sensor_readings_iceberg")
        logger.info(f"Schema: {table.schema()}")
        logger.info(f"Snapshots: {len(list(table.snapshots()))}")
        logger.info(f"Location: {table.location()}")
    except Exception as e:
        logger.error(f"Table info failed: {e}")

# ── Test ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    logger.info("Testing Apache Iceberg setup...")
    table = create_iceberg_table()
    if table:
        logger.success("Iceberg table ready")
        show_table_info()
    else:
        logger.error("Iceberg setup failed")