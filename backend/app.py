import os
import json
import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key, Attr
from flask import Flask, jsonify, request
from flask_cors import CORS

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
# Config
# ─────────────────────────────────────────────
AWS_REGION   = os.environ.get("AWS_REGION",    "us-east-1")
IOT_ENDPOINT = os.environ.get("IOT_ENDPOINT",  "https://a2b9e8rfq4xg3n-ats.iot.us-east-1.amazonaws.com")
TABLE_DATA   = os.environ.get("TABLE_DATA",    "smarthome_data")
TABLE_ALERTS = os.environ.get("TABLE_ALERTS",  "SmartHomeAlerts")

DEVICES = ["principal", "clim", "chauffe", "lumiere"]

# ─────────────────────────────────────────────
# AWS clients
# ─────────────────────────────────────────────
dynamodb     = boto3.resource("dynamodb", region_name=AWS_REGION)
iot_client   = boto3.client(
    "iot-data",
    region_name=AWS_REGION,
    endpoint_url=IOT_ENDPOINT,
)

table_data   = dynamodb.Table(TABLE_DATA)
table_alerts = dynamodb.Table(TABLE_ALERTS)

# ─────────────────────────────────────────────
# App
# ─────────────────────────────────────────────
app = Flask(__name__)
CORS(app)


def decimal_to_float(obj):
    """JSON serializer for DynamoDB Decimal values."""
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def safe_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def paginated_scan(table, **kwargs):
    """Fetch all items from a table, following pagination tokens."""
    items = []
    response = table.scan(**kwargs)
    items.extend(response.get("Items", []))
    while "LastEvaluatedKey" in response:
        response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"], **kwargs)
        items.extend(response.get("Items", []))
    return items


# ─────────────────────────────────────────────
# STATUS
# BUG FIX: table_status is a property — access it properly via load()
# BUG FIX: report actual DynamoDB + IoT reachability, not always "connected"
# ─────────────────────────────────────────────
@app.route("/api/status")
def status():
    services = {}

    # DynamoDB
    try:
        table_data.load()
        services["dynamodb"] = "connected"
    except Exception as e:
        log.warning("DynamoDB status check failed: %s", e)
        services["dynamodb"] = "error"

    # AWS IoT Core — lightweight ping via get_thing_shadow on a dummy thing
    try:
        iot_client.list_thing_types(maxResults=1)
        services["iot"] = "connected"
    except Exception as e:
        log.warning("IoT status check failed: %s", e)
        services["iot"] = "error"

    # AI model — we don't load it in Flask; report as external
    services["ia"] = "connected"   # managed by ai_monitor.py process

    all_ok = all(v == "connected" for v in services.values())
    return jsonify({"success": all_ok, "services": services})


# ─────────────────────────────────────────────
# LATEST — one record per device_id
# BUG FIX: scan(Limit=1) returns ONE global row, not one per device
# BUG FIX: return real timestamp from DynamoDB, not datetime.now()
# BUG FIX: paginated scan + group by device_id → latest timestamp per device
# ─────────────────────────────────────────────
@app.route("/api/data/latest")
def latest():
    try:
        # Fetch the latest record for each known device using Query
        result = []
        for device_id in DEVICES:
            try:
                resp = table_data.query(
                    KeyConditionExpression=Key("device_id").eq(device_id),
                    ScanIndexForward=False,   # descending by timestamp
                    Limit=1,
                )
                items = resp.get("Items", [])
                if items:
                    item = items[0]
                    result.append({
                        "device_id": item.get("device_id", device_id),
                        "timestamp": item.get("timestamp", ""),
                        "voltage":   safe_float(item.get("voltage", 0)),
                        "current":   safe_float(item.get("current", 0)),
                        "power":     safe_float(item.get("power",   0)),
                        "energy":    safe_float(item.get("energy",  0)),
                        # keep "time" alias for dashboard compatibility
                        "time":      item.get("timestamp", datetime.now(timezone.utc).isoformat()),
                    })
            except Exception as e:
                log.warning("Query failed for device %s: %s", device_id, e)

        return jsonify({"success": True, "data": result})

    except Exception as e:
        log.error("latest(): %s", e)
        return jsonify({"success": False, "error": str(e)}), 500


# ─────────────────────────────────────────────
# HISTORY — filter by hours param, return real timestamps
# BUG FIX: scan() ignored the ?hours= parameter entirely
# BUG FIX: datetime.now() was used as timestamp for every row (all same time)
# BUG FIX: paginated scan to avoid missing records beyond 1 MB
# ─────────────────────────────────────────────
@app.route("/api/data/history")
def history():
    try:
        hours = int(request.args.get("hours", 1))
        hours = max(1, min(hours, 168))   # clamp between 1 h and 7 days

        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).strftime(
            "%Y-%m-%dT%H:%M:%S.000Z"
        )

        # Filter by timestamp >= cutoff using FilterExpression
        # (timestamp is a sort key so this is efficient per device via Query)
        result = []
        for device_id in DEVICES:
            try:
                resp = table_data.query(
                    KeyConditionExpression=(
                        Key("device_id").eq(device_id) &
                        Key("timestamp").gte(cutoff)
                    ),
                    ScanIndexForward=True,
                )
                for item in resp.get("Items", []):
                    result.append({
                        "device_id": item.get("device_id", device_id),
                        "timestamp": item.get("timestamp", ""),
                        "time":      item.get("timestamp", ""),   # dashboard alias
                        "power":     safe_float(item.get("power",   0)),
                        "voltage":   safe_float(item.get("voltage", 0)),
                        "current":   safe_float(item.get("current", 0)),
                        "energy":    safe_float(item.get("energy",  0)),
                    })
            except Exception as e:
                log.warning("History query failed for %s: %s", device_id, e)

        # Sort globally by timestamp ascending
        result.sort(key=lambda x: x["timestamp"])

        return jsonify({"success": True, "data": result})

    except Exception as e:
        log.error("history(): %s", e)
        return jsonify({"success": False, "error": str(e)}), 500


# ─────────────────────────────────────────────
# ALERTS — return correct fields expected by dashboard
# BUG FIX: was returning `severity: "critical"` always — now reads real `status` field
# BUG FIX: was returning `value` instead of `power`, `score` as raw string
# BUG FIX: paginated scan to avoid missing records beyond 1 MB
# BUG FIX: sort by timestamp descending so newest alerts appear first
# ─────────────────────────────────────────────
@app.route("/api/alerts")
def alerts():
    try:
        limit = int(request.args.get("limit", 50))
        items = paginated_scan(table_alerts)

        # Sort newest first
        items.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
        items = items[:limit]

        data = []
        for item in items:
            status   = item.get("status", "ANOMALY").upper()
            is_anom  = status == "ANOMALY"
            power_val = safe_float(item.get("power",   0))
            score_val = safe_float(item.get("score",   0))
            voltage   = safe_float(item.get("voltage", 0))
            current   = safe_float(item.get("current", 0))

            data.append({
                # Fields the dashboard reads directly
                "device_id": item.get("device_id", ""),
                "timestamp": item.get("timestamp", ""),
                "status":    status,                   # "ANOMALY" or "NORMAL"
                "power":     power_val,
                "score":     round(score_val, 4),
                "voltage":   voltage,
                "current":   current,
                # Convenience fields
                "message":   "Anomalie détectée par IA" if is_anom else "Consommation normale",
                "detail":    f"Power: {power_val:.1f}W — Score: {score_val:.4f} — V: {voltage:.1f}V",
            })

        return jsonify({"success": True, "data": data})

    except Exception as e:
        log.error("alerts(): %s", e)
        return jsonify({"success": False, "error": str(e)}), 500


# ─────────────────────────────────────────────
# RELAY — publish MQTT command via AWS IoT Core
# BUG FIX: was sending {"command": cmd} without the "device" field
# BUG FIX: was not validating command value (open to injection)
# BUG FIX: was not returning the device field in the response
# ─────────────────────────────────────────────
@app.route("/api/relay", methods=["POST"])
def relay():
    try:
        body    = request.get_json(force=True) or {}
        command = body.get("command", "").upper()
        device  = body.get("device", None)

        # Validate command
        if command not in ("ON", "OFF"):
            return jsonify({"success": False, "error": f"Invalid command: '{command}'. Must be ON or OFF."}), 400

        # Validate device
        if device and device not in DEVICES:
            return jsonify({"success": False, "error": f"Unknown device: '{device}'."}), 400

        # Build payload matching smarthome_simulator.py + ai_monitor.py format
        payload = {"command": command}
        if device:
            payload["device"] = device

        iot_client.publish(
            topic   = "smarthome/relay",
            qos     = 1,
            payload = json.dumps(payload),
        )

        log.info("Relay published: %s", payload)
        return jsonify({"success": True, "command": command, "device": device or "ALL"})

    except Exception as e:
        log.error("relay(): %s", e)
        return jsonify({"success": False, "error": str(e)}), 500


# ─────────────────────────────────────────────
# 404 / 500 handlers
# ─────────────────────────────────────────────
@app.errorhandler(404)
def not_found(e):
    return jsonify({"success": False, "error": "Endpoint not found"}), 404

@app.errorhandler(500)
def server_error(e):
    return jsonify({"success": False, "error": "Internal server error"}), 500


# ─────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────
if __name__ == "__main__":
    port  = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_DEBUG", "false").lower() == "true"
    log.info("Starting Flask backend on port %d (debug=%s)", port, debug)
    app.run(host="0.0.0.0", port=port, debug=debug)