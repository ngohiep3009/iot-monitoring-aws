
import requests
import random
import time
import argparse
from concurrent.futures import ThreadPoolExecutor

parser = argparse.ArgumentParser()

parser.add_argument("--url", default="http://127.0.0.1:5000")
parser.add_argument("--devices", type=int, default=5)
parser.add_argument("--interval", type=float, default=2)
parser.add_argument("--duration", type=float, default=60)

args = parser.parse_args()

if args.devices < 1 or args.interval <= 0 or args.duration <= 0:
    parser.error("devices must be >= 1; interval and duration must be > 0")

API_URL = args.url.rstrip("/") + "/api/readings"

def send_reading(device_id):
    data = {
        "device_id": f"sensor_{device_id:02d}",
        "temperature": round(random.uniform(25, 35), 1),
        "humidity": round(random.uniform(40, 80), 1)
    }

    try:
        response = requests.post(API_URL, json=data, timeout=5)
        print(f"{data['device_id']} | HTTP: {response.status_code}")
    except requests.RequestException as e:
        print(f"{data['device_id']} | Error: {e}")

print(f"Starting {args.devices} IoT devices...")
print(f"Interval: {args.interval}s")
print(f"Duration: {args.duration}s")

start_time = time.monotonic()
next_send = start_time

with ThreadPoolExecutor(max_workers=min(args.devices, 20)) as executor:
    while time.monotonic() - start_time < args.duration:
        executor.map(send_reading, range(1, args.devices + 1))

        next_send += args.interval
        remaining = next_send - time.monotonic()

        if remaining > 0:
            time.sleep(remaining)

print("Simulation completed!")
