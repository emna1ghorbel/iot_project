import os
import json
import time
import logging
import threading
from datetime import datetime, timezone
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor

import boto3
from boto3.dynamodb.conditions import Key
import joblib
import numpy as np
from awscrt import mqtt
from awsiot import mqtt_connection_builder

# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Configuration (env vars with fallback)
# ─────────────────────────────────────────────
AWS_ENDPOINT = os.environ.get(
    "IOT_ENDPOINT",
    "a2b9e8rfq4xg3n-ats.iot.us-east-1.amazonaws.com",
)
CLIENT_ID    = os.environ.get("IOT_CLIENT_ID",  "AI-Monitor")
AWS_REGION   = os.environ.get("AWS_REGION",     "us-east-1")

CERT_FILE    = os.environ.get("IOT_CERT_FILE",  r"C:\Users\dell\OneDrive\Desktop\iot\certs\certificate.pem.crt")
KEY_FILE     = os.environ.get("IOT_KEY_FILE",   r"C:\Users\dell\OneDrive\Desktop\iot\certs\private.pem.key")
CA_CERT      = os.environ.get("IOT_CA_CERT",    r"C:\Users\dell\OneDrive\Desktop\iot\certs\AmazonRootCA1.pem")

MODEL_PATH   = os.environ.get("MODEL_PATH",     r"C:\Users\dell\iot_project\ai_model\model.pkl")
SCALER_PATH  = os.environ.get("SCALER_PATH",    r"C:\Users\dell\iot_project\ai_model\scaler.pkl")

SCAN_INTERVAL     = int(os.environ.get("SCAN_INTERVAL",     20))   # seconds between cycles
ALERT_COOLDOWN    = int(os.environ.get("ALERT_COOLDOWN",    60))   # seconds before re-alerting same device
PAGE_SIZE         = int(os.environ.get("PAGE_SIZE",         100))  # DynamoDB page size

# ─────────────────────────────────────────────
# Load AI model
# ─────────────────────────────────────────────
log.info("Loading AI model …")
model  = joblib.load(MODEL_PATH)
scaler = joblib.load(SCALER_PATH)
log.info("Model loaded ✅")

# ─────────────────────────────────────────────
# DynamoDB
# ─────────────────────────────────────────────
dynamodb     = boto3.resource("dynamodb", region_name=AWS_REGION)
table_data   = dynamodb.Table("smarthome_data")
table_alerts = dynamodb.Table("SmartHomeAlerts")

executor = ThreadPoolExecutor(max_workers=4)

# ─────────────────────────────────────────────
# State tracking
# ─────────────────────────────────────────────
# device_id → last known relay state ("ON" / "OFF")
device_states: dict[str, str] = {}

# device_id → timestamp of last anomaly alert (epoch seconds)
last_alert_time: dict[str, float] = {}

state_lock = threading.Lock()

# ─────────────────────────────────────────────
# MQTT
# ─────────────────────────────────────────────
def on_connection_interrupted(connection, error, **kwargs):
    log.warning("⚠️  MQTT interrupted: %s", error)

def on_connection_resumed(connection, return_code, session_present, **kwargs):
    log.info("✅  MQTT reconnected (code=%s)", return_code)

def build_connection():
    return mqtt_connection_builder.mtls_from_path(
        endpoint=AWS_ENDPOINT,
        cert_filepath=CERT_FILE,
        pri_key_filepath=KEY_FILE,
        ca_filepath=CA_CERT,
        client_id=CLIENT_ID,
        clean_session=False,
        keep_alive_secs=30,
        on_connection_interrupted=on_connection_interrupted,
        on_connection_resumed=on_connection_resumed,
    )

log.info("Connecting to AWS IoT …")
conn = build_connection()
conn.connect().result()
log.info("🤖  AI + MQTT connected ✅")

# ─────────────────────────────────────────────
# Command helpers
# ─────────────────────────────────────────────
def send_command(device_id: str, command: str) -> None:
    """
    Publish a relay command only when the device state actually changes,
    preventing continuous ON/OFF spam every cycle.
    """
    with state_lock:
        if device_states.get(device_id) == command:
            return                         # already in desired state
        device_states[device_id] = command

    payload = {"device": device_id, "command": command}
    try:
        conn.publish(
            topic="smarthome/relay",
            payload=json.dumps(payload),
            qos=mqtt.QoS.AT_LEAST_ONCE,
        )
        log.info("📡  CMD %s → %s", device_id, command)
    except Exception as exc:
        log.error("MQTT publish failed for %s: %s", device_id, exc)


def _write_alert(item: dict) -> None:
    """DynamoDB write executed in thread-pool."""
    try:
        table_alerts.put_item(Item=item)
    except Exception as exc:
        log.error("Alert write failed for %s: %s", item.get("device_id"), exc)

# ─────────────────────────────────────────────
# DynamoDB paginated scan
# ─────────────────────────────────────────────
def scan_all_items() -> list[dict]:
    """
    Use paginated scan so no records are silently skipped when the table
    exceeds the 1 MB per-page DynamoDB limit.
    """
    items = []
    kwargs: dict = {"Limit": PAGE_SIZE}

    while True:
        response = table_data.scan(**kwargs)
        items.extend(response.get("Items", []))
        last = response.get("LastEvaluatedKey")
        if not last:
            break
        kwargs["ExclusiveStartKey"] = last

    return items

# ─────────────────────────────────────────────
# AI evaluation
# ─────────────────────────────────────────────
def evaluate(item: dict) -> None:
    device_id = item["device_id"]

    try:
        voltage = float(item.get("voltage", 0))
        current = float(item.get("current", 0))
        power   = float(item.get("power",   0))
        energy  = float(item.get("energy",  0))
    except (ValueError, TypeError) as exc:
        log.warning("Bad data for %s: %s", device_id, exc)
        return

    X        = np.array([[voltage, current, power, energy]])
    X_scaled = scaler.transform(X)
    pred     = model.predict(X_scaled)[0]
    score    = model.score_samples(X_scaled)[0]

    # Use UTC timestamp, consistent with the simulator
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    if pred == -1:
        # ── ANOMALY ──────────────────────────────
        now = time.time()
        with state_lock:
            last = last_alert_time.get(device_id, 0)
            cooldown_ok = (now - last) >= ALERT_COOLDOWN
            if cooldown_ok:
                last_alert_time[device_id] = now

        if cooldown_ok:
            log.warning("🚨  ANOMALY %s | P=%.2f W | score=%.4f", device_id, power, score)

            alert_record = {
                "device_id": device_id,
                "timestamp": ts,
                "voltage":   Decimal(str(round(voltage, 4))),
                "current":   Decimal(str(round(current, 4))),
                "power":     Decimal(str(round(power,   4))),
                "energy":    Decimal(str(round(energy,  6))),
                "score":     Decimal(str(round(score,   4))),
                "status":    "ANOMALY",
            }
            executor.submit(_write_alert, alert_record)
        else:
            log.debug("ANOMALY %s — suppressed (cooldown)", device_id)

        send_command(device_id, "OFF")

    else:
        # ── NORMAL ───────────────────────────────
        log.info("✅  NORMAL %s | P=%.2f W", device_id, power)
        send_command(device_id, "ON")

# ─────────────────────────────────────────────
# Main loop
# ─────────────────────────────────────────────
log.info("🚀  AI Monitoring started (interval=%ds, cooldown=%ds) …",
         SCAN_INTERVAL, ALERT_COOLDOWN)

while True:
    cycle_start = time.time()

    try:
        items = scan_all_items()
        log.info("📊  Fetched %d records", len(items))

        for item in items:
            evaluate(item)

    except Exception as exc:
        log.error("Cycle error: %s", exc)

    elapsed = time.time() - cycle_start
    log.info("⏱  Cycle done in %.1f s", elapsed)

    sleep_for = max(0, SCAN_INTERVAL - elapsed)
    time.sleep(sleep_for)