from s3_storage import upload_csv_content
from flask import Flask, request, jsonify, render_template, Response
from datetime import datetime
from database import init_db, save_reading, get_latest_readings, get_all_readings
from collections import defaultdict, deque
import statistics
import socket
import csv
import io
import os
import json
import boto3

app = Flask(__name__)

init_db()

AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-2")
HIGH_QUEUE_URL = os.getenv("HIGH_QUEUE_URL")
NORMAL_QUEUE_URL = os.getenv("NORMAL_QUEUE_URL")
TEMP_THRESHOLD = float(os.getenv("TEMP_THRESHOLD", "60.0"))
HUM_THRESHOLD = float(os.getenv("HUM_THRESHOLD", "85.0"))

sqs_client = boto3.client("sqs", region_name=AWS_REGION)

# Lưu cửa sổ trượt 20 giá trị gần nhất của Nhiệt độ và Độ ẩm theo từng thiết bị
temp_history = defaultdict(lambda: deque(maxlen=20))
hum_history = defaultdict(lambda: deque(maxlen=20))


def detect_anomaly(device_id, temp, hum):
    """Phát hiện bất thường đa biến (Nhiệt độ & Độ ẩm) kết hợp Ngưỡng tĩnh và Z-Score động."""
    t_hist = temp_history[device_id]
    h_hist = hum_history[device_id]

    is_anomaly = False
    reasons = []
    temp_z = 0.0
    hum_z = 0.0

    # --- 1. KIỂM TRA NHIỆT ĐỘ ---
    if temp >= TEMP_THRESHOLD:
        is_anomaly = True
        reasons.append(f"Nhiệt độ vượt ngưỡng an toàn ({temp}°C >= {TEMP_THRESHOLD}°C)")
    elif len(t_hist) >= 5:
        mean_t = statistics.mean(t_hist)
        stdev_t = statistics.pstdev(t_hist)
        if stdev_t > 0.1:
            temp_z = (temp - mean_t) / stdev_t
            if abs(temp_z) >= 3.0:
                is_anomaly = True
                reasons.append(
                    f"Biến động nhiệt độ bất thường (Z-Score = {temp_z:.2f}, "
                    f"trung bình trước đó {mean_t:.1f}°C -> hiện tại {temp}°C)"
                )

    # --- 2. KIỂM TRA ĐỘ ẨM ---
    if hum >= HUM_THRESHOLD:
        is_anomaly = True
        reasons.append(f"Độ ẩm vượt ngưỡng an toàn ({hum}% >= {HUM_THRESHOLD}%)")
    elif len(h_hist) >= 5:
        mean_h = statistics.mean(h_hist)
        stdev_h = statistics.pstdev(h_hist)
        if stdev_h > 0.5:
            hum_z = (hum - mean_h) / stdev_h
            if abs(hum_z) >= 3.0:
                is_anomaly = True
                reasons.append(
                    f"Biến động độ ẩm bất thường (Z-Score = {hum_z:.2f}, "
                    f"trung bình trước đó {mean_h:.1f}% -> hiện tại {hum}%)"
                )

    # Cập nhật giá trị mới vào lịch sử trượt
    t_hist.append(temp)
    h_hist.append(hum)

    # Lấy Z-Score có độ lệch lớn nhất để lưu log
    max_z = temp_z if abs(temp_z) >= abs(hum_z) else hum_z
    reason_str = "; ".join(reasons) if reasons else "Normal"

    return is_anomaly, reason_str, round(max_z, 2)


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


@app.route("/api/readings", methods=["POST"])
def add_reading():
    data = request.get_json(silent=True)

    if not isinstance(data, dict):
        return jsonify({"error": "Invalid JSON"}), 400

    required = ["device_id", "temperature", "humidity"]

    if any(field not in data for field in required):
        return jsonify({"error": "Missing sensor data"}), 400

    if not isinstance(data["device_id"], str) or not data["device_id"].strip():
        return jsonify({"error": "Invalid device ID"}), 400

    if any(
        isinstance(data[field], bool)
        or not isinstance(data[field], (int, float))
        for field in ["temperature", "humidity"]
    ):
        return jsonify({"error": "Invalid sensor values"}), 400

    device_id = data["device_id"]
    temp = float(data["temperature"])
    hum = float(data["humidity"])

   
    is_anomaly, anomaly_reason, z_score = detect_anomaly(device_id, temp, hum)

    reading = {
        "device_id": device_id,
        "temperature": temp,
        "humidity": hum,
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "anomaly_reason": anomaly_reason,
        "z_score": z_score
    }

    if not HIGH_QUEUE_URL or not NORMAL_QUEUE_URL:
        save_reading(reading)
        return jsonify({
            "message": "Data saved directly (SQS not configured)",
            "priority": "HIGH" if is_anomaly else "NORMAL",
            "data": reading
        }), 201

    if is_anomaly:
        target_queue = HIGH_QUEUE_URL
        priority = "HIGH"
    else:
        target_queue = NORMAL_QUEUE_URL
        priority = "NORMAL"

    try:
        sqs_client.send_message(
            QueueUrl=target_queue,
            MessageBody=json.dumps(reading)
        )
        return jsonify({
            "message": "Data queued successfully",
            "priority": priority,
            "anomaly_detected": is_anomaly,
            "reason": anomaly_reason,
            "data": reading
        }), 202
    except Exception:
        app.logger.exception("Failed to send message to SQS")
        return jsonify({"error": "Queue service unavailable"}), 500


@app.route("/api/readings/latest", methods=["GET"])
def latest_readings():
    return jsonify(get_latest_readings())


@app.route("/api/devices", methods=["GET"])
def devices():
    readings = get_latest_readings()
    return jsonify(sorted({r["device_id"] for r in readings}))


@app.route("/", methods=["GET"])
def dashboard():
    return render_template(
        "dashboard.html",
        server_id=socket.gethostname()
    )


@app.route("/api/export", methods=["GET"])
def export_csv():
    readings = get_all_readings()
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["device_id", "temperature", "humidity", "created_at"])
    for reading in readings:
        writer.writerow([
            reading["device_id"],
            reading["temperature"],
            reading["humidity"],
            reading["created_at"]
        ])
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=readings.csv"}
    )


@app.route("/api/export/s3", methods=["POST"])
def export_csv_to_s3():
    readings = get_all_readings()
    if not readings:
        return jsonify({"error": "No sensor data available"}), 400
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["device_id", "temperature", "humidity", "created_at"])
    for reading in readings:
        writer.writerow([
            reading["device_id"],
            reading["temperature"],
            reading["humidity"],
            reading["created_at"]
        ])
    try:
        result = upload_csv_content(output.getvalue())
        return jsonify({
            "message": "CSV uploaded to S3 successfully",
            "bucket": result["bucket"],
            "key": result["key"],
            "total_readings": len(readings)
        }), 201
    except Exception:
        app.logger.exception("Failed to upload CSV to S3")
        return jsonify({"error": "Failed to upload CSV to S3"}), 500


if __name__ == "__main__":
    app.run(debug=True)