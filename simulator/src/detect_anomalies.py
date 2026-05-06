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
# Per-device normal operating power ranges (W)
# If power is within [min, max], skip AI — it's definitively normal.
# This prevents the Isolation Forest (trained on mixed hourly data)
# from false-flagging high-power devices like clim or chauffe.
# ─────────────────────────────────────────────
DEVICE_NORMAL_RANGES = {
    "principal": (30,   700),    # base ~2.1A × 220V ≈ 460W
    "clim":      (200,  800),    # Seuil abaissé à 800W pour TEST d'anomalie (normal ~1200W)
    "chauffe":   (150, 1400),    # base ~4.1A × 220V ≈ 900W
    "lumiere":   (10,   40),     # Seuil abaissé à 40W pour TEST d'anomalie (normal ~60W)
}

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
    Publish a relay command. Always publish when called, because the AI
    monitor does not track manual overrides from the dashboard.
    """
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
# DynamoDB — fetch latest record per device
# ─────────────────────────────────────────────
KNOWN_DEVICES = ["principal", "clim", "chauffe", "lumiere"]

def fetch_latest_per_device() -> list[dict]:
    """
    Query the single most-recent record for each known device.
    Using Query (not Scan) is O(1) per device instead of O(N) over
    the entire table — much faster and cheaper on DynamoDB.
    Evaluating only the latest record avoids re-triggering anomaly
    commands on thousands of historical records every cycle.
    """
    results = []
    for device_id in KNOWN_DEVICES:
        try:
            resp = table_data.query(
                KeyConditionExpression=Key("device_id").eq(device_id),
                ScanIndexForward=False,   # descending → latest first
                Limit=1,
            )
            items = resp.get("Items", [])
            if items:
                results.append(items[0])
                log.debug("Latest %s → ts=%s", device_id,
                          items[0].get("timestamp", "?"))
            else:
                log.info("No data yet for device: %s", device_id)
        except Exception as exc:
            log.warning("Query failed for %s: %s", device_id, exc)
    return results

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

    # ── Pre-check: is the data fresh? (30s threshold)
    ts_str = item.get("timestamp", "")
    try:
        # Handle both Z and +00:00 formats
        ts_dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        age = (datetime.now(timezone.utc) - ts_dt).total_seconds()
        if age > 30:
            log.info("⏳  SKIP AI %s | Data is stale (age=%.1fs) — assuming OFF/Inactive", device_id, age)
            return
    except Exception as e:
        log.warning("Could not parse timestamp %s: %s", ts_str, e)

    # ── Pre-check: is the device off?
    relay_state = item.get("relay_state", "ON")
    if relay_state == "OFF" or power == 0:
        log.info("😴  SKIP AI %s | Device is OFF or Power=0W", device_id)
        return

    # ── Pre-check: is power within the known normal range for this device?
    # This guards against the Isolation Forest false-flagging high-power
    # devices (clim, chauffe, principal) whose wattage is perfectly normal
    # but falls outside the model's training distribution.
    normal_range = DEVICE_NORMAL_RANGES.get(device_id)
    if normal_range is not None:
        lo, hi = normal_range
        if lo <= power <= hi:
            log.info("✅  NORMAL %s | P=%.2f W (in range [%d-%d]W — skipping AI)",
                     device_id, power, lo, hi)
            return
        elif power > hi:
            log.warning("🚨  FORCED ANOMALY %s | P=%.2f W > %d W (Out of bounds)", device_id, power, hi)
            score = -0.88
            ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            
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
            send_command(device_id, "OFF")
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

# ─────────────────────────────────────────────
# Main loop
# ─────────────────────────────────────────────
log.info("🚀  AI Monitoring started (interval=%ds, cooldown=%ds) …",
         SCAN_INTERVAL, ALERT_COOLDOWN)

while True:
    cycle_start = time.time()

    try:
        items = fetch_latest_per_device()
        log.info("📊  Evaluating latest record for %d device(s)", len(items))

        for item in items:
            evaluate(item)

    except Exception as exc:
        log.error("Cycle error: %s", exc)

    elapsed = time.time() - cycle_start
    log.info("⏱  Cycle done in %.1f s", elapsed)

    sleep_for = max(0, SCAN_INTERVAL - elapsed)
    time.sleep(sleep_for)