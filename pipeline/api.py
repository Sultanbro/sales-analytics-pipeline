import hashlib
import hmac
import json
import os
import re

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Response
from fastapi.responses import JSONResponse
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, ConfigDict, Field

from pipeline.store import Conflict, Store


class Upload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    csv_data: str = Field(min_length=1, max_length=1_000_000)


class Job(BaseModel):
    id: str
    status: str
    attempts: int
    created_at: float
    error: str | None
    result: dict | None


class BodyLimit:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        chunks, size = [], 0
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            size += len(message.get("body", b""))
            if size > 1_100_000:
                return await JSONResponse({"detail": "Body exceeds 1.1 MB"}, 413)(scope, receive, send)
            chunks.append(message.get("body", b""))
            if not message.get("more_body", False):
                break

        async def buffered():
            nonlocal chunks
            if chunks is not None:
                body, chunks = b"".join(chunks), None
                return {"type": "http.request", "body": body, "more_body": False}
            return await receive()

        await self.app(scope, buffered, send)


def create_app(db_path=None, tenant_keys=None):
    keys = tenant_keys if tenant_keys is not None else json.loads(os.getenv("TENANT_KEYS_JSON", "{}"))
    if (
        not isinstance(keys, dict)
        or not keys
        or any(
            not isinstance(k, str)
            or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", k)
            or not isinstance(v, str)
            or len(v) < 32
            for k, v in keys.items()
        )
        or len(set(keys.values())) != len(keys)
    ):
        raise RuntimeError("TENANT_KEYS_JSON must map distinct tenants to distinct random keys (32+ characters)")
    hashed = {tenant: hashlib.sha256(key.encode()).digest() for tenant, key in keys.items()}
    store = Store(db_path or os.getenv("DATABASE_PATH", "data/jobs.db"))
    app = FastAPI(
        title="Sales Analytics Pipeline",
        version="1.0.0",
        description="Submit bounded sales CSV datasets to a durable background worker. API keys identify tenants.",
    )
    app.state.store = store
    app.add_middleware(BodyLimit)
    key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

    def tenant(key: str | None = Depends(key_header)):
        digest = hashlib.sha256((key or "").encode()).digest()
        for name, expected in hashed.items():
            if hmac.compare_digest(digest, expected):
                return name
        raise HTTPException(401, "Invalid API key")

    @app.get("/health")
    def health():
        with store.connect() as db:
            db.execute("SELECT 1")
        return {"status": "ok"}

    @app.post("/v1/jobs", status_code=202, response_model=Job)
    def submit(
        body: Upload,
        response: Response,
        company: str = Depends(tenant),
        idempotency_key: str = Header(min_length=1, max_length=100, pattern=r"^[A-Za-z0-9_-]+$"),
    ):
        try:
            job, replay = store.submit(company, idempotency_key, body.csv_data)
        except Conflict as error:
            raise HTTPException(409, str(error)) from error
        if replay:
            response.status_code = 200
        response.headers["Location"] = "/v1/jobs/" + job["id"]
        return job

    @app.get("/v1/jobs")
    def list_jobs(
        company: str = Depends(tenant),
        limit: int = Query(20, ge=1, le=50),
        offset: int = Query(0, ge=0),
    ):
        return {"jobs": store.list(company, limit, offset)}

    @app.get("/v1/jobs/{identifier}", response_model=Job)
    def get_job(identifier: str, company: str = Depends(tenant)):
        job = store.get(company, identifier)
        if job is None:
            raise HTTPException(404, "Job not found")
        return job

    @app.post("/v1/jobs/{identifier}/cancel", response_model=Job)
    def cancel(identifier: str, company: str = Depends(tenant)):
        try:
            found = store.cancel(company, identifier)
        except Conflict as error:
            raise HTTPException(409, str(error)) from error
        if not found:
            raise HTTPException(404, "Job not found")
        return store.get(company, identifier)

    return app
