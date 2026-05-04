import time
import json
import random
import threading
import os
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key
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
    "a2b9e8rfq4xg3n-ats.iot.us-east-1.amazonaws.com"
)
CLIENT_ID  = os.environ.get("IOT_CLIENT_ID",  "ESP32-SmartHome")
TOPIC_PUB  = os.environ.get("IOT_TOPIC_PUB",  "smarthome/energy")
TOPIC_SUB  = os.environ.get("IOT_TOPIC_SUB",  "smarthome/relay")

CERT_FILE  = os.environ.get("IOT_CERT_FILE",  r"C:\Users\dell\OneDrive\Desktop\iot\certs\certificate.pem.crt")
KEY_FILE   = os.environ.get("IOT_KEY_FILE",   r"C:\Users\dell\OneDrive\Desktop\iot\certs\private.pem.key")
CA_CERT    = os.environ.get("IOT_CA_CERT",    r"C:\Users\dell\OneDrive\Desktop\iot\certs\AmazonRootCA1.pem")

AWS_REGION   = os.environ.get("AWS_REGION",    "us-east-1")
DYNAMO_TABLE = os.environ.get("DYNAMO_TABLE",  "smarthome_data")

PUBLISH_INTERVAL = int(os.environ.get("PUBLISH_INTERVAL", 5))  # seconds

# ─────────────────────────────────────────────
# Devices
# ─────────────────────────────────────────────
DEVICES = [
    {"id": "principal", "base_current": 2.1,  "base_voltage": 220.0},
    {"id": "clim",      "base_current": 5.5,  "base_voltage": 220.0},
    {"id": "chauffe",   "base_current": 4.1,  "base_voltage": 220.0},
    {"id": "lumiere",   "base_current": 0.27, "base_voltage": 220.0},
]

# Shared state — always access under relay_lock
relay_states  = {d["id"]: "ON"  for d in DEVICES}
energy_totals = {d["id"]: 0.0   for d in DEVICES}
relay_lock    = threading.Lock()

# ─────────────────────────────────────────────
# DynamoDB
# ─────────────────────────────────────────────
dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
table    = dynamodb.Table(DYNAMO_TABLE)

# Thread-pool for non-blocking DynamoDB writes
executor = ThreadPoolExecutor(max_workers=4)


def load_energy_totals():
    """Seed energy accumulators from the latest DynamoDB record per device."""
    log.info("Loading energy totals from DynamoDB …")
    for dev in DEVICES:
        try:
            resp = table.query(
                KeyConditionExpression=Key("device_id").eq(dev["id"]),
                ScanIndexForward=False,   # latest first
                Limit=1,
            )
            if resp["Items"]:
                stored = float(resp["Items"][0].get("energy", 0))
                energy_totals[dev["id"]] = stored
                log.info("  %s → resumed at %.6f kWh", dev["id"], stored)
            else:
                log.info("  %s → no history, starting at 0", dev["id"])
        except Exception as exc:
            log.warning("  %s → could not load energy: %s", dev["id"], exc)


def save_to_dynamodb(record: dict):
    """Persist one energy record. Intended to run in a thread-pool worker."""
    try:
        table.put_item(Item={
            "device_id": record["device_id"],
            "timestamp": record["timestamp"],
            "voltage":   Decimal(str(record["voltage"])),
            "current":   Decimal(str(record["current"])),
            "power":     Decimal(str(record["power"])),
            "energy":    Decimal(str(record["energy"])),
        })
        log.info("✅ DynamoDB OK → %s @ %s", record["device_id"], record["timestamp"])
    except Exception as exc:
        log.error("❌ DynamoDB FAILED → %s | %s: %s",
                  record["device_id"], type(exc).__name__, exc)

# ─────────────────────────────────────────────
# Simulation helpers
# ─────────────────────────────────────────────

def sim_voltage(base: float) -> float:
    return round(base + random.uniform(-3, 3), 2)


def sim_current(base: float) -> float:
    return round(base + random.uniform(-base * 0.1, base * 0.2), 2)

# ─────────────────────────────────────────────
# MQTT callbacks
# ─────────────────────────────────────────────

def on_message_received(topic, payload, **kwargs):
    try:
        msg = json.loads(payload.decode())
        cmd = msg.get("command", "").upper()
        dev = msg.get("device", None)

        if cmd not in ("ON", "OFF"):
            log.warning("Unknown command '%s' — ignored", cmd)
            return

        with relay_lock:
            if dev and dev in relay_states:
                relay_states[dev] = cmd
                log.info("🔌  %s → %s", dev, cmd)
            else:
                for k in relay_states:
                    relay_states[k] = cmd
                log.info("🔌  ALL DEVICES → %s", cmd)

    except Exception as exc:
        log.error("MQTT message error: %s", exc)


def on_connection_interrupted(connection, error, **kwargs):
    log.warning("⚠️  MQTT connection interrupted: %s", error)


def on_connection_resumed(connection, return_code, session_present, **kwargs):
    log.info("✅  MQTT reconnected (return_code=%s, session_present=%s)",
             return_code, session_present)

# ─────────────────────────────────────────────
# MQTT connection
# ─────────────────────────────────────────────

def build_mqtt_connection():
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


conn = build_mqtt_connection()

log.info("Connecting to AWS IoT …")
conn.connect().result()
log.info("Connected ✅")

conn.subscribe(
    topic=TOPIC_SUB,
    qos=mqtt.QoS.AT_LEAST_ONCE,
    callback=on_message_received,
)
log.info("Subscribed to '%s'", TOPIC_SUB)

# ─────────────────────────────────────────────
# Main loop
# ─────────────────────────────────────────────
load_energy_totals()

log.info("Starting publish loop (interval=%ds) …", PUBLISH_INTERVAL)

while True:
    for dev in DEVICES:
        # Guarantee a unique timestamp per device:
        # - microsecond precision (not just ms) → different even within same loop
        # - device_id embedded → unique even if two devices hit same microsecond
        now = (datetime.now(timezone.utc)
               .strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z")

        dev_id = dev["id"]

        # Thread-safe state read
        with relay_lock:
            state = relay_states[dev_id]

        if state == "OFF":
            log.info("⏩  %s is OFF — skipping measurement", dev_id)
            continue

        v = sim_voltage(dev["base_voltage"])
        i = sim_current(dev["base_current"])
        p = round(v * i, 2)

        # Update energy accumulator under lock (kWh)
        with relay_lock:
            energy_totals[dev_id] += p * PUBLISH_INTERVAL / 3_600_000
            e = round(energy_totals[dev_id], 6)

        record = {
            "device_id": dev_id,
            "timestamp": now,
            "voltage":   v,
            "current":   i,
            "power":     p,
            "energy":    e,
        }

        # Publish to IoT Core
        try:
            conn.publish(
                topic=TOPIC_PUB,
                payload=json.dumps(record),
                qos=mqtt.QoS.AT_LEAST_ONCE,
            )
        except Exception as exc:
            log.error("MQTT publish failed for %s: %s", dev_id, exc)

        # Non-blocking DynamoDB write
        executor.submit(save_to_dynamodb, record)

        log.info("📤  %s | V=%.2f V | I=%.2f A | P=%.2f W | E=%.6f kWh",
                 dev_id, v, i, p, e)

    time.sleep(PUBLISH_INTERVAL)
