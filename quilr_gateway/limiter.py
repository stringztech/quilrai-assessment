"""Atomic per-tenant rolling admission budget backed by SQLite."""

import asyncio
import sqlite3
import time
from contextlib import closing
from pathlib import Path


class TokenWindow:
    def __init__(self, path: Path, limit: int = 50_000, seconds: int = 60):
        self.path = path
        self.limit = limit
        self.seconds = seconds

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(sqlite3.connect(self.path, timeout=5)) as db, db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("""
                CREATE TABLE IF NOT EXISTS admissions (
                    tenant TEXT NOT NULL,
                    at REAL NOT NULL,
                    tokens INTEGER NOT NULL CHECK (tokens > 0)
                )
            """)
            db.execute("CREATE INDEX IF NOT EXISTS admissions_at ON admissions(at)")
            db.execute("CREATE INDEX IF NOT EXISTS admissions_tenant ON admissions(tenant, at)")

    def _reserve(self, tenant: str, tokens: int) -> bool:
        if not 0 < tokens <= self.limit:
            return False
        db = sqlite3.connect(self.path, timeout=5)
        try:
            with db:
                # Acquire the writer lock before reading the current total.
                db.execute("BEGIN IMMEDIATE")
                now = time.time()
                db.execute("DELETE FROM admissions WHERE at <= ?", (now - self.seconds,))
                used = db.execute(
                    "SELECT COALESCE(SUM(tokens), 0) FROM admissions WHERE tenant = ?",
                    (tenant,),
                ).fetchone()[0]
                if used + tokens > self.limit:
                    return False
                db.execute("INSERT INTO admissions VALUES (?, ?, ?)", (tenant, now, tokens))
                return True
        finally:
            db.close()

    async def reserve(self, tenant: str, tokens: int) -> bool:
        return await asyncio.to_thread(self._reserve, tenant, tokens)
