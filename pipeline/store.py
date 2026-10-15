"""SQLite job queue with leases and fencing; one host, independent workers."""

import hashlib
import json
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4


class Conflict(Exception):
    pass


class Store:
    def __init__(self, path, clock=time.time):
        self.path = str(path)
        self.clock = clock
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY, tenant TEXT NOT NULL, request_key TEXT NOT NULL,
                    digest TEXT NOT NULL, source TEXT NOT NULL,
                    status TEXT NOT NULL CHECK(status IN ('queued','running','succeeded','failed','cancelled')),
                    attempts INTEGER NOT NULL DEFAULT 0, created_at REAL NOT NULL,
                    next_run_at REAL NOT NULL, lease_token TEXT, lease_until REAL,
                    result TEXT, error TEXT, UNIQUE(tenant,request_key)
                );
                CREATE INDEX IF NOT EXISTS jobs_claim ON jobs(status,next_run_at,lease_until);
                CREATE INDEX IF NOT EXISTS jobs_tenant ON jobs(tenant,created_at);
            """)

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        try:
            with db:
                yield db
        finally:
            db.close()

    def submit(self, tenant, key, source):
        digest = hashlib.sha256(source.encode()).hexdigest()
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM jobs WHERE tenant=? AND request_key=?", (tenant, key)).fetchone()
            if row:
                if row["digest"] != digest:
                    raise Conflict("Key used for a different dataset")
                return self.public(row), True
            identifier, now = str(uuid4()), self.clock()
            db.execute(
                "INSERT INTO jobs(id,tenant,request_key,digest,source,status,created_at,next_run_at) VALUES(?,?,?,?,?,'queued',?,?)",
                (identifier, tenant, key, digest, source, now, now),
            )
            return self.public(db.execute("SELECT * FROM jobs WHERE id=?", (identifier,)).fetchone()), False

    @staticmethod
    def public(row):
        return {
            "id": row["id"],
            "status": row["status"],
            "attempts": row["attempts"],
            "created_at": row["created_at"],
            "error": row["error"],
            "result": json.loads(row["result"]) if row["result"] else None,
        }

    def get(self, tenant, identifier):
        with self.connect() as db:
            row = db.execute("SELECT * FROM jobs WHERE tenant=? AND id=?", (tenant, identifier)).fetchone()
        return self.public(row) if row else None

    def list(self, tenant, limit=20, offset=0):
        with self.connect() as db:
            rows = db.execute(
                "SELECT * FROM jobs WHERE tenant=? ORDER BY created_at DESC,id DESC LIMIT ? OFFSET ?",
                (tenant, limit, offset),
            ).fetchall()
        # Lists carry metadata only; detailed reports are retrieved individually.
        return [{k: v for k, v in self.public(row).items() if k != "result"} for row in rows]

    def cancel(self, tenant, identifier):
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT status FROM jobs WHERE tenant=? AND id=?", (tenant, identifier)).fetchone()
            if not row:
                return False
            if row["status"] not in ("queued", "running", "cancelled"):
                raise Conflict("Completed jobs cannot be cancelled")
            db.execute(
                "UPDATE jobs SET status='cancelled',lease_token=NULL,lease_until=NULL,source='' WHERE tenant=? AND id=?",
                (tenant, identifier),
            )
        return True

    def claim(self, lease_seconds=120):
        now = self.clock()
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "UPDATE jobs SET status='failed',error='Retry limit reached after worker lease expired',source='' WHERE status='running' AND lease_until<=? AND attempts>=3",
                (now,),
            )
            row = db.execute(
                "SELECT * FROM jobs WHERE attempts<3 AND ((status='queued' AND next_run_at<=?) OR (status='running' AND lease_until<=?)) ORDER BY created_at,id LIMIT 1",
                (now, now),
            ).fetchone()
            if not row:
                return None
            token = uuid4().hex
            db.execute(
                "UPDATE jobs SET status='running',attempts=attempts+1,lease_token=?,lease_until=?,error=NULL WHERE id=?",
                (token, now + lease_seconds, row["id"]),
            )
            return {
                "id": row["id"],
                "source": row["source"],
                "token": token,
                "attempt": row["attempts"] + 1,
            }

    def finish(self, job, result):
        with self.connect() as db:
            changed = db.execute(
                "UPDATE jobs SET status='succeeded',result=?,source='',lease_token=NULL,lease_until=NULL WHERE id=? AND status='running' AND lease_token=? AND lease_until>?",
                (
                    json.dumps(result, allow_nan=False),
                    job["id"],
                    job["token"],
                    self.clock(),
                ),
            ).rowcount
        return changed == 1

    def fail(self, job, error, retryable):
        retry = retryable and job["attempt"] < 3
        with self.connect() as db:
            changed = db.execute(
                "UPDATE jobs SET status=?,error=?,next_run_at=?,lease_token=NULL,lease_until=NULL,source=CASE WHEN ? THEN source ELSE '' END WHERE id=? AND status='running' AND lease_token=? AND lease_until>?",
                (
                    "queued" if retry else "failed",
                    error,
                    self.clock() + 2 ** job["attempt"],
                    retry,
                    job["id"],
                    job["token"],
                    self.clock(),
                ),
            ).rowcount
        return changed == 1
