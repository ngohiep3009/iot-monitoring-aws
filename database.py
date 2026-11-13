
import os
import sqlite3
from contextlib import contextmanager

import pymysql
from pymysql.cursors import DictCursor


DB_TYPE = os.getenv("DB_TYPE", "sqlite").lower()
DB_NAME = "sensors.db"


@contextmanager
def get_connection():
    """Create a connection to SQLite or MySQL."""
    if DB_TYPE == "sqlite":
        conn = sqlite3.connect(DB_NAME)
        conn.row_factory = sqlite3.Row

    elif DB_TYPE == "mysql":
        conn = pymysql.connect(
            host=os.environ["DB_HOST"],
            port=int(os.getenv("DB_PORT", "3306")),
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"],
            database=os.environ["DB_NAME"],
            charset="utf8mb4",
            cursorclass=DictCursor,
            connect_timeout=10
        )

    else:
        raise ValueError(
            f"Unsupported DB_TYPE: {DB_TYPE}"
        )

    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    if DB_TYPE == "mysql":
        sql = """
            CREATE TABLE IF NOT EXISTS readings (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                device_id VARCHAR(100) NOT NULL,
                temperature DOUBLE NOT NULL,
                humidity DOUBLE NOT NULL,
                created_at VARCHAR(50) NOT NULL
            )
        """
    else:
        sql = """
            CREATE TABLE IF NOT EXISTS readings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id TEXT NOT NULL,
                temperature REAL NOT NULL,
                humidity REAL NOT NULL,
                created_at TEXT NOT NULL
            )
        """

    with get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
        finally:
            cursor.close()


def save_reading(reading):
    placeholder = "%s" if DB_TYPE == "mysql" else "?"

    sql = f"""
        INSERT INTO readings
        (device_id, temperature, humidity, created_at)
        VALUES ({", ".join([placeholder] * 4)})
    """

    values = (
        reading["device_id"],
        reading["temperature"],
        reading["humidity"],
        reading["created_at"]
    )

    with get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, values)
        finally:
            cursor.close()


def get_latest_readings(limit=50):
    limit = int(limit)

    if limit < 1:
        raise ValueError("Limit must be positive")

    placeholder = "%s" if DB_TYPE == "mysql" else "?"

    sql = f"""
        SELECT device_id, temperature, humidity, created_at
        FROM readings
        ORDER BY id DESC
        LIMIT {placeholder}
    """

    with get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (limit,))
            rows = cursor.fetchall()
        finally:
            cursor.close()

    return [dict(row) for row in reversed(rows)]


def get_all_readings():
    sql = """
        SELECT device_id, temperature, humidity, created_at
        FROM readings
        ORDER BY id ASC
    """

    with get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            rows = cursor.fetchall()
        finally:
            cursor.close()

    return [dict(row) for row in rows]
