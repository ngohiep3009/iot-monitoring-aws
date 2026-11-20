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

sqs_client = boto3.client("sqs", region_name=AWS_REGION)

# Lưu cửa sổ trượt 20 giá trị nhiệt độ gần nhất của từng thiết bị để tính Z-Score
device_history = defaultdict(lambda: deque(maxlen=20))


def detect_anomaly(device_id, temp):
    """Phát hiện bất thường kết hợp Ngưỡng tĩnh và Z-Score động."""
    history = device_history[device_id]
    is_anomaly = False
    reason = "Normal"
    z_score = 0.0

    # 1. Kiểm tra ngưỡng nguy hiểm tuyệt đối
    if temp >= TEMP_THRESHOLD:
        is_anomaly = True
        reason = f"Vượt ngưỡng an toàn tuyệt đối ({temp}°C >= {TEMP_THRESHOLD}°C)"

    # 2. Kiểm tra đột biến thống kê (Z-Score) khi đã có đủ ít nhất 5 mẫu dữ liệu
    elif len(history) >= 5:
        mean_val = statistics.mean(history)
        stdev_val = statistics.pstdev(history)
        if stdev_val > 0.1:  # Tránh chia cho 0 khi nhiệt độ đi ngang tuyệt đối
            z_score = (temp - mean_val) / stdev_val
            if abs(z_score) >= 3.0:
                is_anomaly = True
                reason = (
                    f"Biến động nhiệt độ bất thường (Z-Score = {z_score:.2f}, "
                    f"trung bình trước đó {mean_val:.1f}°C -> hiện tại {temp}°C)"
                )

    # Cập nhật giá trị mới vào lịch sử trượt
    history.append(temp)
    return is_anomaly, reason, round(z_score, 2)


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

    # Chạy bộ phát hiện bất thường AI/Thống kê
    is_anomaly, anomaly_reason, z_score = detect_anomaly(device_id, temp)

    reading = {
        "device_id": device_id,
        "temperature": temp,
        "humidity": data["humidity"],
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