import asyncio
import itertools
import json
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
CONFIG_SERVER_URL = os.getenv("CONFIG_SERVER_URL", "http://config-server:8888").rstrip("/")

# Fallback default URLs in case config-server is starting up
DEFAULT_LOGGING_URLS = [
    url.strip().rstrip("/")
    for url in os.getenv(
        "LOGGING_SERVICE_URLS",
        "http://logging-service-1:8001,http://logging-service-2:8001,http://logging-service-3:8001",
    ).split(",")
    if url.strip()
]
DEFAULT_COUNTER_URL = os.getenv("COUNTER_SERVICE_URL", "http://counter-service:8002").rstrip("/")

HAZELCAST_MEMBERS = os.getenv(
    "HAZELCAST_CLUSTER_MEMBERS",
    "hazelcast-1:5701,hazelcast-2:5701,hazelcast-3:5701"
)
HAZELCAST_CLUSTER_NAME = os.getenv("HAZELCAST_CLUSTER_NAME", "dev")
QUEUE_NAME = "counter_queue"

_tx_seq = itertools.count(1)

try:
    import hazelcast
    HAS_HAZELCAST = True
except ImportError:
    HAS_HAZELCAST = False


class QueueProducer:
    def __init__(self):
        self.hz_client = None
        self.queue = None
        self.connected = False

    async def init(self):
        if HAS_HAZELCAST:
            members = [m.strip() for m in HAZELCAST_MEMBERS.split(",") if m.strip()]
            for attempt in range(1, 16):
                try:
                    print(f"[facade-service] Connecting to Hazelcast MQ ({HAZELCAST_CLUSTER_NAME}) attempt {attempt}/15...", flush=True)
                    self.hz_client = await asyncio.to_thread(
                        hazelcast.HazelcastClient,
                        cluster_name=HAZELCAST_CLUSTER_NAME,
                        cluster_members=members,
                        client_name="facade-service-producer",
                        connection_timeout=5.0,
                    )
                    self.queue = self.hz_client.get_queue(QUEUE_NAME).blocking()
                    self.connected = True
                    print(f"[facade-service] Connected to Hazelcast Queue '{QUEUE_NAME}'!", flush=True)
                    return
                except Exception as exc:
                    print(f"[facade-service] Hazelcast connect failed: {exc}", flush=True)
                    await asyncio.sleep(2.0)
            print("[facade-service] Warning: Could not connect to Hazelcast Queue", flush=True)

    async def push_transaction(self, record: dict) -> bool:
        payload_str = json.dumps(record)
        if self.connected and self.queue:
            return await asyncio.to_thread(self.queue.offer, payload_str)
        return False

    async def close(self):
        if self.hz_client:
            await asyncio.to_thread(self.hz_client.shutdown)


queue_producer = QueueProducer()


@asynccontextmanager
async def lifespan(fastapi_app: FastAPI):
    limits = httpx.Limits(max_connections=300, max_keepalive_connections=150)
    fastapi_app.state.http_client = httpx.AsyncClient(limits=limits, timeout=5.0)
    await queue_producer.init()
    try:
        yield
    finally:
        await queue_producer.close()
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


async def get_service_instances(service_name: str) -> list[str]:
    """Queries config-server for registered instances of a service."""
    client: httpx.AsyncClient = app.state.http_client
    try:
        resp = await client.get(f"{CONFIG_SERVER_URL}/services/{service_name}", timeout=2.0)
        if resp.status_code == 200:
            instances = resp.json().get("instances", [])
            if instances:
                return instances
    except Exception as e:
        print(f"[facade-service] Config-server lookup for {service_name} failed: {e}", flush=True)

    # Fallback to configured defaults if config-server has no registrations yet
    if service_name == "logging-service":
        return list(DEFAULT_LOGGING_URLS)
    elif service_name == "counter-service":
        return [DEFAULT_COUNTER_URL]
    return []


async def _call_logging_service_with_failover(
    method: str,
    path: str,
    json_body: Optional[dict] = None,
) -> tuple[int, dict, str]:
    """Dynamically discovers logging-service instances from config-server,
    randomly shuffles them, and fails over if an instance is unavailable.
    """
    client: httpx.AsyncClient = app.state.http_client

    candidates = await get_service_instances("logging-service")
    if not candidates:
        candidates = list(DEFAULT_LOGGING_URLS)
    random.shuffle(candidates)

    last_error = None
    for target_url in candidates:
        full_url = f"{target_url}{path}"
        try:
            resp = await client.request(method, full_url, json=json_body, timeout=5.0)
            if resp.status_code < 500:
                return resp.status_code, resp.json(), target_url
            else:
                last_error = f"HTTP {resp.status_code}: {resp.text}"
        except Exception as exc:
            last_error = str(exc)
            print(f"[facade-service] Logging instance {target_url} unreachable ({exc}), trying next...", flush=True)

    raise HTTPException(
        status_code=502,
        detail=f"All logging-service instances failed. Last error: {last_error}",
    )


async def _call_counter_service(
    method: str,
    path: str,
    json_body: Optional[dict] = None,
) -> tuple[Optional[int], Optional[dict]]:
    """Calls counter-service (dynamically discovered via config-server).
    If counter-service is unavailable (e.g. docker pause), returns (None, None).
    """
    client: httpx.AsyncClient = app.state.http_client
    instances = await get_service_instances("counter-service")
    target_url = instances[0] if instances else DEFAULT_COUNTER_URL
    full_url = f"{target_url}{path}"

    try:
        resp = await client.request(method, full_url, json=json_body, timeout=3.0)
        return resp.status_code, resp.json()
    except Exception as exc:
        print(f"[facade-service] counter-service at {full_url} unreachable: {exc}", flush=True)
        return None, None


@app.get("/health")
async def health() -> dict:
    log_instances = await get_service_instances("logging-service")
    cnt_instances = await get_service_instances("counter-service")
    return {
        "status": "ok",
        "mq_connected": queue_producer.connected,
        "config_server": CONFIG_SERVER_URL,
        "logging_instances": log_instances,
        "counter_instances": cnt_instances,
    }


@app.post("/")
@app.post("/transaction")
async def create_transaction(payload: ClientTransaction) -> dict:
    """Asynchronous transaction flow via Messaging Queue:
    1. Creates transaction_ID and timestamp.
    2. Sends transaction to logging-service (replicated, random selection with failover).
    3. Pushes transaction to Hazelcast Message Queue (counter_queue) for counter-service.
    4. Returns immediately without waiting for disk DB updates!
    """
    timestamp = time.time_ns()
    tx_id = f"{timestamp}-{next(_tx_seq)}"
    tx_payload = {
        "transaction_ID": tx_id,
        "timestamp": timestamp,
        "user_Id": payload.user_Id,
        "amount": payload.amount,
    }

    # Parallel: log in logging-service and enqueue in MQ
    log_task = _call_logging_service_with_failover("POST", "/transactions", tx_payload)
    mq_task = queue_producer.push_transaction(tx_payload)

    (log_status, log_data, used_logging_instance), mq_success = await asyncio.gather(
        log_task, mq_task
    )

    if log_status >= 400:
        raise HTTPException(status_code=502, detail="Failed to log transaction")

    return {
        "transaction_ID": tx_id,
        "status": "accepted",
        "queued": mq_success,
        "logging_instance": used_logging_instance,
    }


@app.get("/user/{user_Id}")
async def get_user_info(user_Id: str) -> dict:
    """Reads user balance from counter-service and transaction history from logging-service.
    If counter-service is down/paused, balance is returned as null.
    """
    cnt_task = _call_counter_service("GET", f"/user/{user_Id}")
    log_task = _call_logging_service_with_failover("GET", f"/user/{user_Id}")

    (cnt_status, cnt_data), (log_status, log_data, used_logging_instance) = await asyncio.gather(
        cnt_task, log_task
    )

    balance = None
    counter_available = False
    if cnt_status == 200 and cnt_data:
        balance = cnt_data.get("balance")
        counter_available = True

    return {
        "user_Id": user_Id,
        "balance": balance,
        "counter_available": counter_available,
        "transactions": log_data.get("transactions", []),
        "logging_instance": used_logging_instance,
    }


@app.get("/accounts")
async def get_accounts() -> dict:
    cnt_status, cnt_data = await _call_counter_service("GET", "/accounts")
    if cnt_status == 200 and cnt_data:
        return {"balances": cnt_data.get("balances", {})}
    return {"balances": None, "error": "counter-service unavailable"}


@app.get("/transactions")
async def get_transactions() -> dict:
    log_status, log_data, used_instance = await _call_logging_service_with_failover("GET", "/transactions")
    return {
        "transactions": log_data.get("transactions", []),
        "logging_instance": used_instance,
    }


@app.post("/reset")
async def reset_all() -> dict:
    client: httpx.AsyncClient = app.state.http_client

    # Reset counter-service
    await _call_counter_service("POST", "/reset")

    # Reset all logging instances
    log_instances = await get_service_instances("logging-service")
    for url in log_instances:
        try:
            await client.post(f"{url}/reset", timeout=3.0)
        except Exception:
            pass

    return {"status": "all_reset"}


if __name__ == "__main__":
    print(f"[facade-service] Starting on port {PORT}", flush=True)
    uvicorn.run(app, host="0.0.0.0", port=PORT, access_log=False, log_level="warning")
