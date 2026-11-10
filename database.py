
import sqlite3

DB_NAME = "sensors.db"

def init_db():
    with sqlite3.connect(DB_NAME) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS readings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id TEXT NOT NULL,
                temperature REAL NOT NULL,
                humidity REAL NOT NULL,
                created_at TEXT NOT NULL
            )
        """)

def save_reading(reading):
    with sqlite3.connect(DB_NAME) as conn:
        conn.execute("""
            INSERT INTO readings
            (device_id, temperature, humidity, created_at)
            VALUES (?, ?, ?, ?)
        """, (
            reading["device_id"],
            reading["temperature"],
            reading["humidity"],
            reading["created_at"]
        ))

def get_latest_readings(limit=50):
    with sqlite3.connect(DB_NAME) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""
            SELECT device_id, temperature, humidity, created_at
            FROM readings
            ORDER BY id DESC
            LIMIT ?
        """, (limit,)).fetchall()

    return [dict(row) for row in reversed(rows)]


def get_all_readings():
    with sqlite3.connect(DB_NAME) as conn:
        conn.row_factory = sqlite3.Row

        rows = conn.execute("""
            SELECT device_id, temperature, humidity, created_at
            FROM readings
            ORDER BY id ASC
        """).fetchall()

    return [dict(row) for row in rows]

