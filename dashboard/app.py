# dashboard/app.py
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import boto3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, html, dcc, callback, Output, Input
import dash_bootstrap_components as dbc
from datetime import datetime, timezone
from loguru import logger
from config.settings import get_settings

settings = get_settings()

# ── AWS Clients ───────────────────────────────────────────────────────────────
dynamodb = boto3.resource("dynamodb", region_name=settings.aws_region)
table = dynamodb.Table("SensorReadings")
s3 = boto3.client("s3", region_name=settings.aws_region)

# ── Data Fetchers ─────────────────────────────────────────────────────────────
def get_s3_gold_data():
    """Read latest Gold layer Parquet file from S3."""
    try:
        response = s3.list_objects_v2(
            Bucket=settings.s3_bucket,
            Prefix=settings.s3_gold
        )
        if "Contents" not in response:
            return pd.DataFrame()

        files = sorted(response["Contents"], key=lambda x: x["LastModified"], reverse=True)
        latest = files[0]["Key"]

        obj = s3.get_object(Bucket=settings.s3_bucket, Key=latest)
        import pyarrow.parquet as pq
        from io import BytesIO
        table_data = pq.read_table(BytesIO(obj["Body"].read()))
        return table_data.to_pandas()
    except Exception as e:
        logger.error(f"S3 read error: {e}")
        return pd.DataFrame()

def get_alerts():
    """Scan DynamoDB for alert records."""
    try:
        response = table.scan(Limit=50)
        items = response.get("Items", [])
        return [i for i in items if str(i.get("sensor_id", "")).startswith("ALERT#")]
    except Exception as e:
        logger.error(f"DynamoDB scan error: {e}")
        return []

def get_recent_readings():
    """Scan DynamoDB for recent sensor readings."""
    try:
        response = table.scan(Limit=100)
        items = response.get("Items", [])
        return [i for i in items if not str(i.get("sensor_id", "")).startswith("ALERT#")]
    except Exception as e:
        logger.error(f"DynamoDB scan error: {e}")
        return []

# ── App Layout ────────────────────────────────────────────────────────────────
app = Dash(__name__, external_stylesheets=[dbc.themes.DARKLY])

app.layout = dbc.Container([

    # Header
    dbc.Row([
        dbc.Col([
            html.H1("🌍 Real-Time IoT Pipeline", className="text-center mt-4 mb-1"),
            html.P("Live sensor data — Sensor.Community → Kafka → AWS",
                   className="text-center text-muted mb-4")
        ])
    ]),

    # Stats Row
    dbc.Row([
        dbc.Col(dbc.Card([
            dbc.CardBody([
                html.H4("0", id="total-sensors", className="text-info"),
                html.P("Sensor Types", className="text-muted mb-0")
            ])
        ]), width=3),
        dbc.Col(dbc.Card([
            dbc.CardBody([
                html.H4("0", id="total-readings", className="text-success"),
                html.P("Total Readings", className="text-muted mb-0")
            ])
        ]), width=3),
        dbc.Col(dbc.Card([
            dbc.CardBody([
                html.H4("0", id="total-alerts", className="text-warning"),
                html.P("Active Alerts", className="text-muted mb-0")
            ])
        ]), width=3),
        dbc.Col(dbc.Card([
            dbc.CardBody([
                html.H4("--", id="last-updated", className="text-secondary"),
                html.P("Last Updated", className="text-muted mb-0")
            ])
        ]), width=3),
    ], className="mb-4"),

    # Charts Row 1
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("PM2.5 by Sensor Type (µg/m³)"),
                dbc.CardBody(dcc.Graph(id="pm25-chart"))
            ])
        ], width=6),
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("PM10 by Sensor Type (µg/m³)"),
                dbc.CardBody(dcc.Graph(id="pm10-chart"))
            ])
        ], width=6),
    ], className="mb-4"),

    # Charts Row 2
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Temperature & Humidity"),
                dbc.CardBody(dcc.Graph(id="temp-humidity-chart"))
            ])
        ], width=6),
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("🚨 Recent Alerts"),
                dbc.CardBody(html.Div(id="alerts-table"))
            ])
        ], width=6),
    ], className="mb-4"),

    # Auto refresh every 30 seconds
    dcc.Interval(id="interval", interval=30000, n_intervals=0)

], fluid=True)


# ── Callbacks ─────────────────────────────────────────────────────────────────
@callback(
    Output("pm25-chart", "figure"),
    Output("pm10-chart", "figure"),
    Output("temp-humidity-chart", "figure"),
    Output("alerts-table", "children"),
    Output("total-sensors", "children"),
    Output("total-readings", "children"),
    Output("total-alerts", "children"),
    Output("last-updated", "children"),
    Input("interval", "n_intervals")
)
def update_dashboard(n):
    df = get_s3_gold_data()
    alerts = get_alerts()
    now = datetime.now(timezone.utc).strftime("%H:%M:%S")

    # Empty figure template
    def empty_fig(msg="No data yet"):
        fig = go.Figure()
        fig.update_layout(
            template="plotly_dark",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            annotations=[{"text": msg, "showarrow": False,
                          "font": {"color": "gray", "size": 14}}]
        )
        return fig

    if df.empty:
        return (
            empty_fig(), empty_fig(), empty_fig(),
            html.P("No alerts yet", className="text-muted"),
            "0", "0", str(len(alerts)), now
        )

    # ── PM2.5 Chart ───────────────────────────────────────────────────────────
    if "pm2_5_mean" in df.columns:
        pm25_df = df[df["pm2_5_mean"].notna()]
        if pm25_df.empty:
            pm25_fig = empty_fig("No PM2.5 data")
        else:
            pm25_fig = px.bar(
                pm25_df, x="sensor_type", y="pm2_5_mean",
                color="sensor_type", template="plotly_dark",
                labels={"pm2_5_mean": "PM2.5 (µg/m³)", "sensor_type": "Sensor Type"}
            )
            pm25_fig.add_hline(y=35.4, line_dash="dash",
                               line_color="red", annotation_text="EPA Threshold")
            pm25_fig.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                showlegend=False
            )
    else:
        pm25_fig = empty_fig("No PM2.5 data")

    # ── PM10 Chart ────────────────────────────────────────────────────────────
    if "pm10_mean" in df.columns:
        pm10_df = df[df["pm10_mean"].notna()]
        if pm10_df.empty:
            pm10_fig = empty_fig("No PM10 data")
        else:
            pm10_fig = px.bar(
                pm10_df, x="sensor_type", y="pm10_mean",
                color="sensor_type", template="plotly_dark",
                labels={"pm10_mean": "PM10 (µg/m³)", "sensor_type": "Sensor Type"}
            )
            pm10_fig.add_hline(y=154.0, line_dash="dash",
                               line_color="red", annotation_text="EPA Threshold")
            pm10_fig.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                showlegend=False
            )
    else:
        pm10_fig = empty_fig("No PM10 data")

    # ── Temp & Humidity Chart ─────────────────────────────────────────────────
    if "temperature_mean" in df.columns and "humidity_mean" in df.columns:
        temp_df = df[df["temperature_mean"].notna()]
        if temp_df.empty:
            temp_fig = empty_fig("No temperature data")
        else:
            temp_fig = go.Figure()
            temp_fig.add_trace(go.Bar(
                name="Temp (°C)", x=temp_df["sensor_type"],
                y=temp_df["temperature_mean"], marker_color="#00d4ff"
            ))
            temp_fig.add_trace(go.Bar(
                name="Humidity (%)", x=temp_df["sensor_type"],
                y=temp_df["humidity_mean"], marker_color="#00ffaa"
            ))
            temp_fig.update_layout(
                template="plotly_dark", barmode="group",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)"
            )
    else:
        temp_fig = empty_fig("No temperature data")

    # ── Alerts Table ──────────────────────────────────────────────────────────
    if alerts:
        alerts_display = dbc.Table([
            html.Thead(html.Tr([
                html.Th("Sensor"), html.Th("Metric"),
                html.Th("Value"), html.Th("Message")
            ])),
            html.Tbody([
                html.Tr([
                    html.Td(a.get("sensor_id", "").replace("ALERT#", "")),
                    html.Td(a.get("metric", "")),
                    html.Td(a.get("value", "")),
                    html.Td(a.get("message", "")[:40])
                ]) for a in alerts[:10]
            ])
        ], striped=True, hover=True, size="sm", color="dark")
    else:
        alerts_display = html.P("No alerts yet ✅", className="text-success")

    total_sensors = str(len(df))
    total_readings = str(int(df["reading_count"].sum()) if "reading_count" in df.columns else 0)

    return (
        pm25_fig, pm10_fig, temp_fig, alerts_display,
        total_sensors, total_readings, str(len(alerts)), now
    )


if __name__ == "__main__":
    logger.info(f"Dashboard starting at http://localhost:{settings.dash_port}")
    app.run(debug=True, host="0.0.0.0", port=settings.dash_port)