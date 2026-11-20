import argparse
import csv
import random
import time
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from datetime import datetime
from pathlib import Path
from statistics import mean

import requests

parser = argparse.ArgumentParser()
parser.add_argument("--url", default="http://127.0.0.1:5000")
parser.add_argument("--devices", type=int, default=5)
parser.add_argument("--interval", type=float, default=2)
parser.add_argument("--duration", type=float, default=60)
parser.add_argument("--three-levels", action="store_true")
parser.add_argument("--emergency", action="store_true", help="Send high temperature emergency alerts")
args = parser.parse_args()

if args.devices < 1 or args.interval <= 0 or args.duration <= 0:
    parser.error("Invalid devices, interval or duration")

API_URL = args.url.rstrip("/") + "/api/readings"
OUTPUT_DIR = Path("load_results")
OUTPUT_DIR.mkdir(exist_ok=True)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
detail_file = OUTPUT_DIR / f"requests_{timestamp}.csv"
summary_file = OUTPUT_DIR / f"summary_{timestamp}.csv"

MAX_WORKERS = 50
MAX_PENDING = 50
REQUEST_TIMEOUT = 3
DRAIN_SECONDS = 10

levels = (
    [
        ("low", 10, 2.0, 60),
        ("medium", 50, 1.0, 60),
        ("high", 100, 0.5, 60),
    ]
    if args.three_levels
    else [("emergency" if args.emergency else "custom", args.devices, args.interval, args.duration)]
)

DETAIL_FIELDS = [
    "level", "device_id", "success",
    "status_code", "latency_ms", "error"
]
SUMMARY_FIELDS = [
    "level", "start_time", "end_time", "devices",
    "interval_s", "configured_duration_s",
    "elapsed_s", "total_requests", "successful",
    "failed", "success_rate_percent",
    "actual_rps", "avg_latency_ms", "p95_latency_ms",
    "skipped_requests"
]


def send_reading(level, device_id):
    temp = (
        round(random.uniform(75, 95), 1)
        if args.emergency
        else round(random.uniform(25, 35), 1)
    )
    payload = {
        "device_id": f"sensor_{device_id:03d}",
        "temperature": temp,
        "humidity": round(random.uniform(40, 80), 1),
    }

    started = time.monotonic()
    status_code = ""
    error = ""

    try:
        response = requests.post(
            API_URL,
            json=payload,
            timeout=(2, REQUEST_TIMEOUT),
        )
        status_code = response.status_code
        success = 200 <= status_code < 300
    except requests.RequestException as exc:
        success = False
        error = str(exc)[:200]

    latency_ms = (time.monotonic() - started) * 1000

    return {
        "level": level,
        "device_id": payload["device_id"],
        "success": success,
        "status_code": status_code,
        "latency_ms": round(latency_ms, 2),
        "error": error,
    }


def percentile(values, p):
    if not values:
        return 0
    values = sorted(values)
    index = (len(values) - 1) * p / 100
    low = int(index)
    high = min(low + 1, len(values) - 1)
    fraction = index - low
    return values[low] * (1 - fraction) + values[high] * fraction


def save_row(path, fields, row):
    new_file = not path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        if new_file:
            writer.writeheader()
        writer.writerow(row)


def collect_done(pending, results):
    completed = {f for f in pending if f.done()}
    for future in completed:
        try:
            result = future.result()
            results.append(result)
            save_row(detail_file, DETAIL_FIELDS, result)
        except Exception as exc:
            print(f"Worker error: {exc}")
    pending.difference_update(completed)


def run_level(level, devices, interval, duration):
    print(f"\n=== {level.upper()} ===", flush=True)
    print(
        f"Devices={devices}, interval={interval}s, "
        f"duration={duration}s",
        flush=True
    )

    start_wall = datetime.now().isoformat(timespec="seconds")
    start = time.monotonic()
    deadline = start + duration
    next_send = start

    pending = set()
    results = []
    skipped = 0

    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

    try:
        while time.monotonic() < deadline:
            collect_done(pending, results)

            now = time.monotonic()
            if now < next_send:
                time.sleep(min(0.05, next_send - now))
                continue

            for device_id in range(1, devices + 1):
                if len(pending) < MAX_PENDING:
                    future = executor.submit(
                        send_reading, level, device_id
                    )
                    pending.add(future)
                else:
                    skipped += 1

            next_send += interval
            if next_send < time.monotonic():
                next_send = time.monotonic() + interval

        drain_deadline = time.monotonic() + DRAIN_SECONDS

        while pending and time.monotonic() < drain_deadline:
            wait(pending, timeout=0.2,
                 return_when=FIRST_COMPLETED)
            collect_done(pending, results)

        collect_done(pending, results)

    except KeyboardInterrupt:
        print("\nInterrupted. Saving completed results...",
              flush=True)
        collect_done(pending, results)
        raise

    finally:
        executor.shutdown(wait=False, cancel_futures=True)

        elapsed = time.monotonic() - start
        total = len(results)
        successful = sum(r["success"] for r in results)
        failed = total - successful
        latencies = [r["latency_ms"] for r in results]

        summary = {
            "level": level,
            "start_time": start_wall,
            "end_time": datetime.now().isoformat(
                timespec="seconds"
            ),
            "devices": devices,
            "interval_s": interval,
            "configured_duration_s": duration,
            "elapsed_s": round(elapsed, 2),
            "total_requests": total,
            "successful": successful,
            "failed": failed,
            "success_rate_percent": round(
                successful / total * 100, 2
            ) if total else 0,
            "actual_rps": round(total / elapsed, 2)
            if elapsed else 0,
            "avg_latency_ms": round(mean(latencies), 2)
            if latencies else 0,
            "p95_latency_ms": round(
                percentile(latencies, 95), 2
            ),
            "skipped_requests": skipped,
        }

        save_row(summary_file, SUMMARY_FIELDS, summary)

        print(
            f"Completed={total}, Successful={successful}, "
            f"Failed={failed}, Skipped={skipped}",
            flush=True
        )
        print(
            f"Avg={summary['avg_latency_ms']} ms, "
            f"P95={summary['p95_latency_ms']} ms",
            flush=True
        )
        print(f"Results saved: {detail_file}", flush=True)


try:
    for level_config in levels:
        run_level(*level_config)
except KeyboardInterrupt:
    print("\nSimulation stopped by user.", flush=True)
finally:
    print(f"\nDetails: {detail_file}")
    print(f"Summary: {summary_file}")