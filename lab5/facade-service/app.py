import asyncio
import itertools
import json
import os
import random
import time
from contextlib import asynccontextmanager
from typing import Optional, List
import httpx
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict, Field, field_validator

PORT = int(os.getenv("PORT", "8000"))

# Kubernetes Discovery & Config Server Settings
K8S_NAMESPACE = os.getenv("K8S_NAMESPACE", "default")
K8S_LOGGING_SERVICE_NAME = os.getenv("K8S_LOGGING_SERVICE_NAME", "logging-service")
K8S_COUNTER_SERVICE_NAME = os.getenv("K8S_COUNTER_SERVICE_NAME", "counter-service")

# In-cluster Kubernetes API defaults
KUBERNETES_SERVICE_HOST = os.getenv("KUBERNETES_SERVICE_HOST")
KUBERNETES_SERVICE_PORT = os.getenv("KUBERNETES_SERVICE_PORT", "443")
K8S_SA_TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"
K8S_SA_CA_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"

# Fallback URLs if running outside Kubernetes without direct API access
DEFAULT_LOGGING_URLS = [
    url.strip().rstrip("/")
    for url in os.getenv(
        "LOGGING_SERVICE_URLS",
        "http://logging-service:8001,http://logging-service-1:8001,http://logging-service-2:8001,http://logging-service-3:8001",
    ).split(",")
    if url.strip()
]
DEFAULT_COUNTER_URL = os.getenv("COUNTER_SERVICE_URL", "http://counter-service:8002").rstrip("/")

# Key/Value Config from K8s ConfigMap (or env vars)
HAZELCAST_MEMBERS = os.getenv(
    "HAZELCAST_CLUSTER_MEMBERS",
    "hazelcast-service:5701,hazelcast-1:5701,hazelcast-2:5701,hazelcast-3:5701"
)
HAZELCAST_CLUSTER_NAME = os.getenv("HAZELCAST_CLUSTER_NAME", "dev")
QUEUE_NAME = os.getenv("HAZELCAST_QUEUE_NAME", "counter_queue")

_tx_seq = itertools.count(1)

# Metric timers for benchmark evaluation
_metrics_lock = asyncio.Lock()
_total_logging_time: float = 0.0
_total_counter_time: float = 0.0

try:
    import hazelcast
    HAS_HAZELCAST = True
except ImportError:
    HAS_HAZELCAST = False


class K8sServiceDiscovery:
    """Dynamic Service Discovery client for Kubernetes.
    Discovers healthy Pod instances by querying Kubernetes Endpoints API.
    Falls back to Kubernetes CoreDNS service names if API is not accessible.
    """
    def __init__(self, namespace: str = "default"):
        self.namespace = namespace
        self.in_cluster = bool(KUBERNETES_SERVICE_HOST and os.path.exists(K8S_SA_TOKEN_PATH))
        self.client: Optional[httpx.AsyncClient] = None

    async def init(self):
        if self.in_cluster:
            try:
                with open(K8S_SA_TOKEN_PATH, "r") as f:
                    token = f.read().strip()
                headers = {"Authorization": f"Bearer {token}"}
                verify = K8S_SA_CA_PATH if os.path.exists(K8S_SA_CA_PATH) else False
                self.client = httpx.AsyncClient(
                    base_url=f"https://{KUBERNETES_SERVICE_HOST}:{KUBERNETES_SERVICE_PORT}",
                    headers=headers,
                    verify=verify,
                    timeout=3.0,
                )
                print(f"[facade-service] K8s API client initialized in namespace '{self.namespace}'", flush=True)
            except Exception as e:
                print(f"[facade-service] Failed to initialize K8s in-cluster client: {e}", flush=True)
                self.in_cluster = False

    async def get_endpoints(self, service_name: str, fallback_urls: List[str]) -> List[str]:
        """Queries Kubernetes Endpoints API for the given service.
        Returns a list of URLs pointing to ready Pod IPs and ports.
        """
        if self.in_cluster and self.client:
            url = f"/api/v1/namespaces/{self.namespace}/endpoints/{service_name}"
            try:
                resp = await self.client.get(url)
                if resp.status_code == 200:
                    data = resp.json()
                    pod_urls = []
                    subsets = data.get("subsets", [])
                    for subset in subsets:
                        ports = subset.get("ports", [])
                        port_num = ports[0]["port"] if ports else 8000
                        addresses = subset.get("addresses", [])
                        for addr in addresses:
                            ip = addr.get("ip")
                            if ip:
                                pod_urls.append(f"http://{ip}:{port_num}")
                    if pod_urls:
                        return pod_urls
            except Exception as exc:
                print(f"[facade-service] K8s API query error for {service_name}: {exc}", flush=True)

        # Fallback to configured DNS / URLs
        return fallback_urls

    async def close(self):
        if self.client:
            await self.client.aclose()


k8s_discovery = K8sServiceDiscovery(namespace=K8S_NAMESPACE)


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
                        client_name="facade-service-k8s-producer",
                        connection_timeout=5.0,
                    )
                    self.queue = self.hz_client.get_queue(QUEUE_NAME).blocking()
                    self.connected = True
                    print(f"[facade-service] Connected to Hazelcast Queue '{QUEUE_NAME}' in K8s!", flush=True)
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
    await k8s_discovery.init()
    await queue_producer.init()
    try:
        yield
    finally:
        await queue_producer.close()
        await k8s_discovery.close()
        await fastapi_app.state.http_client.aclose()


app = FastAPI(title="facade-service (Kubernetes)", lifespan=lifespan)


class ClientTransaction(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_Id: str = Field(..., min_length=1)
    amount: int | float

    @field_validator("amount", mode="before")
    @classmethod
    def reject_bool(cls, value: object) -> object:
        if isinstance(value, bool):
            raise ValueError("'amount' must be a number, not boolean")
        return value


async def get_logging_service_instances() -> List[str]:
    return await k8s_discovery.get_endpoints(K8S_LOGGING_SERVICE_NAME, DEFAULT_LOGGING_URLS)


async def get_counter_service_instances() -> List[str]:
    return await k8s_discovery.get_endpoints(K8S_COUNTER_SERVICE_NAME, [DEFAULT_COUNTER_URL])


async def _call_logging_service_with_failover(
    method: str,
    path: str,
    json_body: Optional[dict] = None,
) -> tuple[int, dict, str]:
    """Dynamically discovers logging-service instances from Kubernetes Endpoints,
    randomly balances requests among them, and automatically fails over if an instance is unhealthy.
    """
    global _total_logging_time
    client: httpx.AsyncClient = app.state.http_client

    candidates = await get_logging_service_instances()
    if not candidates:
        candidates = list(DEFAULT_LOGGING_URLS)
    random.shuffle(candidates)

    last_error = None
    t0 = time.perf_counter()
    for target_url in candidates:
        full_url = f"{target_url}{path}"
        try:
            resp = await client.request(method, full_url, json=json_body, timeout=5.0)
            elapsed = time.perf_counter() - t0
            async with _metrics_lock:
                _total_logging_time += elapsed

            if resp.status_code < 500:
                return resp.status_code, resp.json(), target_url
            else:
                last_error = f"HTTP {resp.status_code}: {resp.text}"
        except Exception as exc:
            last_error = str(exc)
            print(f"[facade-service] Logging Pod {target_url} unreachable ({exc}), failing over to next...", flush=True)

    elapsed = time.perf_counter() - t0
    async with _metrics_lock:
        _total_logging_time += elapsed

    raise HTTPException(
        status_code=502,
        detail=f"All logging-service instances failed in K8s. Last error: {last_error}",
    )


async def _call_counter_service(
    method: str,
    path: str,
    json_body: Optional[dict] = None,
) -> tuple[Optional[int], Optional[dict]]:
    """Calls counter-service (dynamically discovered via Kubernetes Endpoints).
    If counter-service is unavailable (e.g. scaled to 0 pods), returns (None, None).
    """
    client: httpx.AsyncClient = app.state.http_client
    instances = await get_counter_service_instances()
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
    log_instances = await get_logging_service_instances()
    cnt_instances = await get_counter_service_instances()
    return {
        "status": "ok",
        "service": "facade-service",
        "environment": "kubernetes" if k8s_discovery.in_cluster else "standalone",
        "k8s_namespace": K8S_NAMESPACE,
        "mq_connected": queue_producer.connected,
        "logging_instances": log_instances,
        "counter_instances": cnt_instances,
    }


@app.get("/discovery")
async def get_discovery_info() -> dict:
    log_instances = await get_logging_service_instances()
    cnt_instances = await get_counter_service_instances()
    return {
        "service_registry": "Kubernetes Endpoints API (K8s Service Discovery)",
        "config_server": "Kubernetes ConfigMap (app-config)",
        "in_cluster_api_connected": k8s_discovery.in_cluster,
        "discovered_logging_pods": log_instances,
        "logging_replicas_count": len(log_instances),
        "discovered_counter_pods": cnt_instances,
        "counter_replicas_count": len(cnt_instances),
        "hazelcast_cluster": HAZELCAST_CLUSTER_NAME,
        "hazelcast_queue": QUEUE_NAME,
    }


@app.post("/")
@app.post("/transaction")
async def create_transaction(payload: ClientTransaction) -> dict:
    """Asynchronous transaction flow via Kubernetes Service Discovery & MQ:
    1. Generates unique transaction_ID and timestamp.
    2. Sends transaction to dynamically discovered logging-service Pod with random balancing.
    3. Pushes transaction to Hazelcast Message Queue (counter_queue) for counter-service.
    4. Returns immediately without waiting for disk DB updates!
    """
    global _total_counter_time
    timestamp = time.time_ns()
    tx_id = f"{timestamp}-{next(_tx_seq)}"
    tx_payload = {
        "transaction_ID": tx_id,
        "timestamp": timestamp,
        "user_Id": payload.user_Id,
        "amount": payload.amount,
    }

    t0_cnt = time.perf_counter()
    mq_task = queue_producer.push_transaction(tx_payload)
    log_task = _call_logging_service_with_failover("POST", "/transactions", tx_payload)

    (log_status, log_data, used_logging_instance), mq_success = await asyncio.gather(
        log_task, mq_task
    )
    elapsed_cnt = time.perf_counter() - t0_cnt
    async with _metrics_lock:
        _total_counter_time += elapsed_cnt

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
    If counter-service is down/scaled to 0 in K8s, balance is returned as null.
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


@app.get("/metrics")
async def get_metrics() -> dict:
    """Returns cumulative downstream execution times for benchmark analysis."""
    async with _metrics_lock:
        return {
            "logging_time_sec": _total_logging_time,
            "counter_time_sec": _total_counter_time,
        }


@app.post("/metrics/reset")
async def reset_metrics() -> dict:
    global _total_logging_time, _total_counter_time
    async with _metrics_lock:
        _total_logging_time = 0.0
        _total_counter_time = 0.0
    return {"status": "metrics_reset"}


@app.post("/reset")
async def reset_all() -> dict:
    """Resets databases, queues, and maps across all services."""
    client: httpx.AsyncClient = app.state.http_client

    # Reset metrics
    await reset_metrics()

    # Reset counter-service
    await _call_counter_service("POST", "/reset")

    # Reset logging instances
    log_instances = await get_logging_service_instances()
    for url in log_instances:
        try:
            await client.post(f"{url}/reset", timeout=3.0)
        except Exception:
            pass

    return {"status": "all_reset"}


if __name__ == "__main__":
    print(f"[facade-service] Starting on port {PORT} in Kubernetes mode...", flush=True)
    uvicorn.run(app, host="0.0.0.0", port=PORT, access_log=False, log_level="warning")
