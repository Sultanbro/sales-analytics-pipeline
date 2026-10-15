from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from pipeline.analysis import DataError, analyze
from pipeline.api import create_app
from pipeline.store import Conflict, Store
from pipeline.worker import run_once

HEADER = "sale_id,date,category,amount,currency\n"
SOURCE = (
    HEADER
    + "s1,2026-01-01,books,0.10,USD\ns2,2026-01-01,books,0.20,USD\ns3,2026-01-02,games,5,EUR\ns4,2026-01-02,books,-0.10,USD\n"
)
KEYS = {"acme": "a" * 32, "globex": "b" * 32}


def test_exact_currency_totals_and_groups():
    result = analyze(SOURCE)
    assert result["accepted_rows"] == 4
    assert result["totals"] == [
        {"currency": "EUR", "revenue_cents": 500, "sales": 1},
        {"currency": "USD", "revenue_cents": 20, "sales": 3},
    ]
    assert {row["date"] for row in result["daily"]} == {"2026-01-01", "2026-01-02"}
    assert result["rejected_rows"] == 0


def test_duplicates_are_all_excluded_and_bad_rows_reported():
    result = analyze(
        HEADER
        + "dup,2026-01-01,books,10,USD\ndup,2026-01-02,games,30,USD\nbad,2026-02-30,books,1,USD\nvalid,2026-01-01,books,3,USD\n"
    )
    assert result["accepted_rows"] == 1
    assert result["rejected_rows"] == 3
    assert result["issues"] == [
        {"row": 2, "code": "duplicate_id"},
        {"row": 3, "code": "duplicate_id"},
        {"row": 4, "code": "invalid_date"},
    ]
    assert result["totals"][0]["revenue_cents"] == 300


@pytest.mark.parametrize(
    "row,code",
    [
        ("x,2026-01-01,books,NaN,USD", "invalid_amount"),
        ("x,2026-01-01,books,1.001,USD", "invalid_amount"),
        ("x,2026-01-01,books,1,usd", "invalid_currency"),
        ("x,20260101,books,1,USD", "invalid_date"),
        ("x,2026-01-01,,1,USD", "invalid_category"),
        ("=x,2026-01-01,books,1,USD", "invalid_id"),
        ("x,2026-01-01,books,1", "column_count"),
    ],
)
def test_invalid_rows_do_not_enter_aggregates(row, code):
    result = analyze(HEADER + row)
    assert result["accepted_rows"] == 0
    assert result["totals"] == []
    assert result["issues"][0]["code"] == code


def test_limits_empty_input_and_headers():
    for source in [HEADER, "bad", HEADER + '"unterminated', HEADER + "x,2026-01-01,b,1,USD\n" * 10001]:
        with pytest.raises(DataError):
            analyze(source)


def test_iqr_signal_is_currency_specific_and_deterministic():
    source = (
        HEADER
        + "".join(
            f"a{i},2026-01-01,b,{amount},USD\n" for i, amount in enumerate([10, 11, 12, 10, 11, 12, 10, 11, 1000])
        )
        + "eur,2026-01-01,b,10000,EUR\n"
    )
    result = analyze(source)
    assert result["outliers"] == [{"sale_id": "a8", "currency": "USD", "amount_cents": 100000}]
    assert result == analyze(source)


@pytest.fixture
def store(tmp_path):
    return Store(tmp_path / "jobs.db")


def test_idempotency_is_per_tenant_and_persistent(store):
    first, replay = store.submit("acme", "key", SOURCE)
    assert not replay
    assert Store(store.path).submit("acme", "key", SOURCE) == (first, True)
    second, replay = store.submit("globex", "key", SOURCE)
    assert not replay and first["id"] != second["id"]
    with pytest.raises(Conflict):
        store.submit("acme", "key", SOURCE + "extra")
    assert store.get("globex", first["id"]) is None


def test_workers_claim_each_job_once(store):
    for i in range(25):
        store.submit("acme", str(i), SOURCE)

    def claim(_):
        return Store(store.path).claim()

    with ThreadPoolExecutor(max_workers=10) as pool:
        jobs = [job for job in pool.map(claim, range(40)) if job]
    assert len(jobs) == len({job["id"] for job in jobs}) == 25


def test_expired_lease_is_recovered_and_stale_worker_is_fenced(tmp_path):
    now = [1000]
    store = Store(tmp_path / "jobs.db", clock=lambda: now[0])
    original, _ = store.submit("acme", "a", SOURCE)
    stale = store.claim(lease_seconds=10)
    now[0] = 1010
    fresh = store.claim(lease_seconds=10)
    assert stale["id"] == fresh["id"] and stale["token"] != fresh["token"]
    assert not store.finish(stale, {"wrong": True})
    assert not store.fail(stale, "wrong", retryable=False)
    assert store.finish(fresh, {"right": True})
    assert store.get("acme", original["id"])["result"] == {"right": True}


def test_cancellation_fences_running_worker(store):
    original, _ = store.submit("acme", "a", SOURCE)
    job = store.claim()
    assert not store.cancel("globex", job["id"])
    assert store.cancel("acme", job["id"])
    assert store.cancel("acme", job["id"])
    assert not store.finish(job, {"late": True})
    assert store.get("acme", original["id"])["status"] == "cancelled"
    assert store.claim() is None


def test_retry_backoff_and_limit(tmp_path):
    now = [1000]
    store = Store(tmp_path / "jobs.db", clock=lambda: now[0])
    original, _ = store.submit("acme", "a", SOURCE)

    def crash(_):
        raise RuntimeError("private internal detail")

    for attempt in range(1, 4):
        assert run_once(store, analyzer=crash)
        job = store.get("acme", original["id"])
        assert job["attempts"] == attempt
        assert "private" not in job["error"]
        assert store.claim() is None
        now[0] += 2**attempt
    assert job["status"] == "failed"


def test_three_worker_crashes_exhaust_lease_attempts(tmp_path):
    now = [0]
    store = Store(tmp_path / "jobs.db", clock=lambda: now[0])
    original, _ = store.submit("acme", "a", SOURCE)
    for _ in range(3):
        assert store.claim(lease_seconds=10)
        now[0] += 10
    assert store.claim() is None
    assert store.get("acme", original["id"])["status"] == "failed"


def test_worker_success_and_permanent_validation_failure(store):
    good, _ = store.submit("acme", "good", SOURCE)
    bad, _ = store.submit("acme", "bad", "invalid")
    assert run_once(store)
    assert run_once(store)
    assert not run_once(store)
    assert store.get("acme", good["id"])["result"]["accepted_rows"] == 4
    assert store.get("acme", bad["id"])["status"] == "failed"
    assert store.get("acme", bad["id"])["attempts"] == 1
    with pytest.raises(Conflict):
        store.cancel("acme", good["id"])


def test_malformed_header_is_not_retried(store):
    job, _ = store.submit("acme", "malformed", '"unterminated')
    assert run_once(store)
    failed = store.get("acme", job["id"])
    assert failed["status"] == "failed"
    assert failed["attempts"] == 1


def test_api_tenant_isolation_auth_lifecycle_and_limits(tmp_path):
    app = create_app(tmp_path / "api.db", KEYS)
    with TestClient(app) as client:
        assert client.post("/v1/jobs", json={"csv_data": SOURCE}).status_code == 401
        client.headers["X-API-Key"] = KEYS["acme"]
        assert client.post("/v1/jobs", json={"csv_data": SOURCE}).status_code == 422
        assert client.post("/v1/jobs", content=b"x" * 1_100_001).status_code == 413
        response = client.post("/v1/jobs", json={"csv_data": SOURCE}, headers={"Idempotency-Key": "first"})
        assert response.status_code == 202
        path = response.headers["Location"]
        assert response.json()["status"] == "queued"
        assert (
            client.post("/v1/jobs", json={"csv_data": SOURCE}, headers={"Idempotency-Key": "first"}).status_code == 200
        )
        assert (
            client.post("/v1/jobs", json={"csv_data": "different"}, headers={"Idempotency-Key": "first"}).status_code
            == 409
        )
        client.headers["X-API-Key"] = KEYS["globex"]
        assert client.get(path).status_code == 404
        assert client.post(path + "/cancel").status_code == 404
        assert client.get("/v1/jobs").json() == {"jobs": []}
        client.headers["X-API-Key"] = KEYS["acme"]
        assert run_once(app.state.store)
        assert client.get(path).json()["status"] == "succeeded"
        assert len(client.get("/v1/jobs").json()["jobs"]) == 1
        assert client.post(path + "/cancel").status_code == 409
        assert client.get("/v1/jobs?limit=100").status_code == 422


def test_duplicate_tenant_keys_fail_startup(tmp_path):
    with pytest.raises(RuntimeError):
        create_app(tmp_path / "bad.db", {"a": "x" * 32, "b": "x" * 32})
