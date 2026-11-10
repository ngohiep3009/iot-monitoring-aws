from s3_storage import upload_csv_content
from flask import Flask, request, jsonify, render_template
from datetime import datetime
from database import init_db, save_reading, get_latest_readings, get_all_readings
import socket
import csv
import io
from flask import Response

app = Flask(__name__)

init_db()

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

    reading = {
        "device_id": data["device_id"],
        "temperature": data["temperature"],
        "humidity": data["humidity"],
        "created_at": datetime.now().isoformat(timespec="seconds")
    }

    save_reading(reading)

    return jsonify({
        "message": "Data received successfully",
        "data": reading
    }), 201

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

    writer.writerow([
        "device_id",
        "temperature",
        "humidity",
        "created_at"
    ])

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
        headers={
            "Content-Disposition": "attachment; filename=readings.csv"
        }
    )



@app.route("/api/export/s3", methods=["POST"])
def export_csv_to_s3():
    readings = get_all_readings()

    if not readings:
        return jsonify({
            "error": "No sensor data available"
        }), 400

    output = io.StringIO()
    writer = csv.writer(output)

    writer.writerow([
        "device_id",
        "temperature",
        "humidity",
        "created_at"
    ])

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
        return jsonify({
            "error": "Failed to upload CSV to S3"
        }), 500

if __name__ == "__main__":
    app.run(debug=True)
