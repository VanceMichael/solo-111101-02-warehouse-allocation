import sqlite3
from pathlib import Path


class RecordStore:
    def __init__(self, path):
        self.path = Path(path)

    def connect(self):
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    def initialize(self):
        with self.connect() as connection:
            connection.execute("""CREATE TABLE IF NOT EXISTS source_records (
                id INTEGER PRIMARY KEY,
                source TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                fingerprint TEXT NOT NULL UNIQUE,
                payload TEXT NOT NULL,
                received_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            )""")

    def count(self):
        with self.connect() as connection:
            row = connection.execute("SELECT count(*) AS total FROM source_records").fetchone()
            return row["total"]


class RunStore:
    """--run 工作流的持久状态。

    所有事实表只追加、按业务键去重；投影每次从这些表重建，
    因此进程重启、重复投递、重抓基准快照都不会重复扣加库存。
    """

    def __init__(self, path):
        self.path = Path(path)

    def connect(self):
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    def initialize(self):
        with self.connect() as connection:
            connection.execute("""CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT PRIMARY KEY,
                name TEXT,
                created_at TEXT NOT NULL
            )""")
            connection.execute("""CREATE TABLE IF NOT EXISTS fetches (
                id INTEGER PRIMARY KEY,
                run_id TEXT NOT NULL,
                source TEXT NOT NULL,
                attempt INTEGER NOT NULL,
                status INTEGER NOT NULL,
                etag TEXT,
                body_sha256 TEXT,
                decoded_rows INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                UNIQUE(run_id, source, attempt)
            )""")
            connection.execute("""CREATE TABLE IF NOT EXISTS snapshot_rows (
                id INTEGER PRIMARY KEY,
                run_id TEXT NOT NULL,
                warehouse TEXT NOT NULL,
                sku TEXT NOT NULL,
                watermark INTEGER NOT NULL,
                physical INTEGER NOT NULL,
                reserved INTEGER NOT NULL,
                source TEXT NOT NULL,
                content_sha TEXT NOT NULL,
                UNIQUE(run_id, warehouse, sku, watermark)
            )""")
            connection.execute("""CREATE TABLE IF NOT EXISTS channel_rows (
                id INTEGER PRIMARY KEY,
                run_id TEXT NOT NULL,
                channel TEXT NOT NULL,
                channel_sku TEXT NOT NULL,
                internal_sku TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                watermark INTEGER NOT NULL,
                source TEXT NOT NULL,
                content_sha TEXT NOT NULL,
                UNIQUE(run_id, channel, channel_sku, watermark)
            )""")
            connection.execute("""CREATE TABLE IF NOT EXISTS delta_events (
                id INTEGER PRIMARY KEY,
                run_id TEXT NOT NULL,
                event_id TEXT NOT NULL,
                warehouse TEXT NOT NULL,
                sku TEXT NOT NULL,
                base_watermark INTEGER NOT NULL,
                event_watermark INTEGER NOT NULL,
                delta_physical INTEGER NOT NULL,
                delta_reserved INTEGER NOT NULL,
                source TEXT NOT NULL,
                content_sha TEXT NOT NULL,
                state TEXT NOT NULL,
                UNIQUE(run_id, event_id)
            )""")
            connection.execute("""CREATE TABLE IF NOT EXISTS findings (
                id INTEGER PRIMARY KEY,
                run_id TEXT NOT NULL,
                bucket TEXT NOT NULL,
                origin TEXT NOT NULL,
                reason TEXT NOT NULL,
                sku TEXT,
                source TEXT,
                watermark INTEGER,
                raw TEXT,
                detail TEXT,
                dedupe_key TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL
            )""")
            connection.execute("""CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY,
                run_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                reason TEXT NOT NULL,
                sku TEXT,
                detail TEXT,
                dedupe_key TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL
            )""")
            for table in ("fetches", "snapshot_rows", "channel_rows", "delta_events", "findings", "notifications"):
                connection.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_{table}_run ON {table}(run_id)"
                )
