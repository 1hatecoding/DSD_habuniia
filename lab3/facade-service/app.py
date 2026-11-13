import asyncio
import itertools
import os
import random
import time
from contextlib import asynccontextmanager
from typing import Optional
import httpx
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict, Field, field_validator

PORT = int(os.getenv("PORT", "8000"))
RAW_LOGGING_URLS = os.getenv(
    "LOGGING_SERVICE_URLS",
    "http://127.0.0.1:8001,http://127.0.0.1:8003,http://127.0.0.1:8004"
)
LOGGING_SERVICE_URLS = [
    url.strip().rstrip("/")
    for url in RAW_LOGGING_URLS.split(",")
    if url.strip()
]
COUNTER_SERVICE_URL = os.getenv("COUNTER_SERVICE_URL", "http://127.0.0.1:8002").rstrip("/")

_tx_seq = itertools.count(1)


def _initial_metrics() -> dict:
    return {
        "logging_time_sec": 0.0,
        "counter_time_sec": 0.0,
        "logging_calls": 0,
        "counter_calls": 0,
    }


@asynccontextmanager
async def lifespan(fastapi_app: FastAPI):
    limits = httpx.Limits(max_connections=300, max_keepalive_connections=150)
    fastapi_app.state.http_client = httpx.AsyncClient(limits=limits, timeout=30.0)
    fastapi_app.state.metrics = _initial_metrics()
    try:
        yield
    finally:
        await fastapi_app.state.http_client.aclose()


app = FastAPI(title="facade-service", lifespan=lifespan)


class ClientTransaction(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    user_Id: str = Field(validation_alias="user_Id")
    amount: int | float

    @field_validator("amount", mode="before")
    @classmethod
    def reject_bool(cls, value: object) -> object:
        if isinstance(value, bool):
            raise ValueError("'amount' must be a number, not boolean")
        return value


async def _call_logging_service_with_failover(
    method: str,
    path: str,
    json_body: Optional[dict] = None,
) -> tuple[int, dict, str]:
    """Randomly selects an instance of logging-service.
    If the chosen instance is unavailable, tries the next one, and so on.
    Returns (status_code, response_json, used_instance_url).
    """
    client: httpx.AsyncClient = app.state.http_client
    metrics: dict = app.state.metrics

    # Randomly shuffle instances
    candidates = list(LOGGING_SERVICE_URLS)
    random.shuffle(candidates)

    last_error = None
    for target_url in candidates:
        full_url = f"{target_url}{path}"
        start = time.perf_counter()
        try:
            resp = await client.request(method, full_url, json=json_body, timeout=5.0)
            elapsed = time.perf_counter() - start
            metrics["logging_time_sec"] += elapsed
            metrics["logging_calls"] += 1

            if resp.status_code < 500:
                # Valid response (including 200 OK or client errors)
                return resp.status_code, resp.json(), target_url
            else:
                last_error = f"HTTP {resp.status_code}: {resp.text}"
                print(f"[facade-service] Logging instance {target_url} returned {resp.status_code}, trying next...", flush=True)
        except Exception as exc:
            elapsed = time.perf_counter() - start
            metrics["logging_time_sec"] += elapsed
            last_error = str(exc)
            print(f"[facade-service] Logging instance {target_url} unreachable ({exc}), trying next candidate...", flush=True)

    raise HTTPException(
        status_code=502,
        detail=f"All logging-service instances are unavailable. Last error: {last_error}",
    )


async def _call_counter_service(
    method: str,
    path: str,
    json_body: Optional[dict] = None,
) -> tuple[int, dict]:
    client: httpx.AsyncClient = app.state.http_client
    metrics: dict = app.state.metrics

    full_url = f"{COUNTER_SERVICE_URL}{path}"
    start = time.perf_counter()
    try:
        resp = await client.request(method, full_url, json=json_body, timeout=10.0)
    finally:
        elapsed = time.perf_counter() - start
        metrics["counter_time_sec"] += elapsed
        metrics["counter_calls"] += 1

    return resp.status_code, resp.json()


@app.get("/health")
async def health() -> dict:
    return {
        "status": "ok",
        "logging_instances": LOGGING_SERVICE_URLS,
        "counter_url": COUNTER_SERVICE_URL,
    }


@app.post("/")
@app.post("/transaction")
async def create_transaction(payload: ClientTransaction) -> dict:
    timestamp = time.time_ns()
    tx_id = f"{timestamp}-{next(_tx_seq)}"
    tx_payload = {
        "transaction_ID": tx_id,
        "timestamp": timestamp,
        "user_Id": payload.user_Id,
        "amount": payload.amount,
    }

    # Parallel call to logging-service (random with failover) and counter-service
    log_task = _call_logging_service_with_failover("POST", "/transactions", tx_payload)
    cnt_task = _call_counter_service("POST", "/update", tx_payload)

    (log_status, log_data, used_logging_instance), (cnt_status, cnt_data) = await asyncio.gather(
        log_task, cnt_task
    )

    if log_status >= 400 or cnt_status >= 400:
        raise HTTPException(
            status_code=502,
            detail={"error": "Downstream service error", "logging": log_data, "counter": cnt_data},
        )

    return {
        "transaction_ID": tx_id,
        "balance": cnt_data["balance"],
        "logging_instance": used_logging_instance,
    }


@app.get("/user/{user_Id}")
async def get_user_info(user_Id: str) -> dict:
    cnt_task = _call_counter_service("GET", f"/user/{user_Id}")
    log_task = _call_logging_service_with_failover("GET", f"/user/{user_Id}")

    (cnt_status, cnt_data), (log_status, log_data, used_logging_instance) = await asyncio.gather(
        cnt_task, log_task
    )

    if cnt_status >= 400 or log_status >= 400:
        raise HTTPException(status_code=502, detail="Downstream service failure")

    return {
        "user_Id": user_Id,
        "balance": cnt_data["balance"],
        "transactions": log_data.get("transactions", []),
        "logging_instance": used_logging_instance,
    }


@app.get("/accounts")
async def get_accounts() -> dict:
    cnt_status, cnt_data = await _call_counter_service("GET", "/accounts")
    if cnt_status >= 400:
        raise HTTPException(status_code=502, detail="Counter-service failure")

    return {"balances": cnt_data.get("balances", {})}


@app.get("/transactions")
async def get_transactions() -> dict:
    log_status, log_data, used_instance = await _call_logging_service_with_failover("GET", "/transactions")
    if log_status >= 400:
        raise HTTPException(status_code=502, detail="Logging-service failure")

    return {
        "transactions": log_data.get("transactions", []),
        "logging_instance": used_instance,
    }


@app.get("/metrics")
async def get_metrics() -> dict:
    m = app.state.metrics
    return {
        "logging_time_sec": round(m["logging_time_sec"], 6),
        "counter_time_sec": round(m["counter_time_sec"], 6),
        "logging_calls": m["logging_calls"],
        "counter_calls": m["counter_calls"],
        "logging_instances": LOGGING_SERVICE_URLS,
    }


@app.post("/metrics/reset")
async def reset_metrics() -> dict:
    app.state.metrics = _initial_metrics()
    return {"status": "metrics_reset"}


@app.post("/reset")
async def reset_all() -> dict:
    client: httpx.AsyncClient = app.state.http_client

    # Reset counter-service
    await client.post(f"{COUNTER_SERVICE_URL}/reset")

    # Reset all reachable logging-service instances
    for url in LOGGING_SERVICE_URLS:
        try:
            await client.post(f"{url}/reset", timeout=3.0)
        except Exception:
            pass

    app.state.metrics = _initial_metrics()
    return {"status": "all_reset"}


if __name__ == "__main__":
    print(
        f"[facade-service] Starting on port {PORT}\n"
        f"  Logging instances: {LOGGING_SERVICE_URLS}\n"
        f"  Counter instance: {COUNTER_SERVICE_URL}",
        flush=True,
    )
    uvicorn.run(app, host="0.0.0.0", port=PORT, access_log=False, log_level="warning")

