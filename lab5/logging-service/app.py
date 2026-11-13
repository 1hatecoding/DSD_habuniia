import asyncio
import json
import os
import socket
import sqlite3
from contextlib import asynccontextmanager
from typing import Optional
import httpx
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

PORT = int(os.getenv("PORT", "8001"))
POD_NAME = os.getenv("HOSTNAME", socket.gethostname())
INSTANCE_NAME = os.getenv("INSTANCE_NAME", POD_NAME)

# Key/Value Config read from Kubernetes ConfigMap (app-config)
HAZELCAST_CLUSTER_NAME = os.getenv("HAZELCAST_CLUSTER_NAME", "dev")
HAZELCAST_CLUSTER_MEMBERS = os.getenv(
    "HAZELCAST_CLUSTER_MEMBERS",
    "hazelcast-service:5701,hazelcast-1:5701,hazelcast-2:5701,hazelcast-3:5701"
)
LOG_TRANSACTIONS = os.getenv("LOG_TRANSACTIONS", "1").lower() in ("1", "true", "yes")

try:
    import hazelcast
    HAS_HAZELCAST = True
except ImportError:
    HAS_HAZELCAST = False


class DistributedStorage:
    def __init__(self):
        self.backend = "memory"
        self.hz_client = None
        self.tx_map = None
        self.sqlite_conn = None
        self.lock = asyncio.Lock()

    async def init(self):
        members = [m.strip() for m in HAZELCAST_CLUSTER_MEMBERS.split(",") if m.strip()]

        if HAS_HAZELCAST:
            for attempt in range(1, 16):
                try:
                    print(
                        f"[{INSTANCE_NAME}] Connecting to Hazelcast ({HAZELCAST_CLUSTER_NAME}) "
                        f"via members {members} (attempt {attempt}/15)...",
                        flush=True,
                    )
                    self.hz_client = await asyncio.to_thread(
                        hazelcast.HazelcastClient,
                        cluster_name=HAZELCAST_CLUSTER_NAME,
                        cluster_members=members,
                        client_name=f"{INSTANCE_NAME}-client",
                        connection_timeout=5.0,
                    )
                    self.tx_map = self.hz_client.get_map("transactions").blocking()
                    self.backend = "hazelcast"
                    print(f"[{INSTANCE_NAME}] Successfully connected to Hazelcast cluster!", flush=True)
                    return
                except Exception as exc:
                    print(f"[{INSTANCE_NAME}] Hazelcast connect failed: {exc}", flush=True)
                    await asyncio.sleep(2.0)

        # Fallback to local SQLite if Hazelcast is not yet reachable
        self.backend = "sqlite_fallback"
        db_path = os.getenv("FALLBACK_DB_PATH", "shared_hazelcast_fallback.db")
        self.sqlite_conn = sqlite3.connect(db_path, check_same_thread=False)
        self.sqlite_conn.row_factory = sqlite3.Row
        with self.sqlite_conn:
            self.sqlite_conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS hz_transactions (
                    transaction_id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    timestamp INTEGER
                );
                """
            )
        print(f"[{INSTANCE_NAME}] Using fallback storage: {db_path}", flush=True)

    async def put_transaction(self, record: dict):
        tx_id = record["transaction_ID"]
        user_id = record["user_Id"]
        payload_str = json.dumps(record)

        if self.backend == "hazelcast":
            await asyncio.to_thread(self.tx_map.put, tx_id, payload_str)
        else:
            async with self.lock:
                cursor = self.sqlite_conn.cursor()
                cursor.execute(
                    """
                    INSERT INTO hz_transactions (transaction_id, user_id, payload, timestamp)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(transaction_id) DO UPDATE SET
                        user_id = excluded.user_id,
                        payload = excluded.payload,
                        timestamp = excluded.timestamp;
                    """,
                    (tx_id, user_id, payload_str, record.get("timestamp")),
                )
                self.sqlite_conn.commit()

    async def get_user_transactions(self, user_id: str) -> list[dict]:
        if self.backend == "hazelcast":
            all_entries = await asyncio.to_thread(self.tx_map.entry_set)
            txs = []
            for _, raw_val in all_entries:
                try:
                    data = json.loads(raw_val)
                    if data.get("user_Id") == user_id:
                        txs.append(data)
                except Exception:
                    pass
            txs.sort(key=lambda x: x.get("timestamp", 0))
            return txs
        else:
            async with self.lock:
                cursor = self.sqlite_conn.cursor()
                cursor.execute(
                    "SELECT payload FROM hz_transactions WHERE user_id = ? ORDER BY timestamp ASC;",
                    (user_id,),
                )
                rows = cursor.fetchall()
                return [json.loads(r["payload"]) for r in rows]

    async def get_all_transactions(self) -> list[dict]:
        if self.backend == "hazelcast":
            all_entries = await asyncio.to_thread(self.tx_map.entry_set)
            txs = []
            for _, raw_val in all_entries:
                try:
                    txs.append(json.loads(raw_val))
                except Exception:
                    pass
            txs.sort(key=lambda x: x.get("timestamp", 0))
            return txs
        else:
            async with self.lock:
                cursor = self.sqlite_conn.cursor()
                cursor.execute("SELECT payload FROM hz_transactions ORDER BY timestamp ASC;")
                rows = cursor.fetchall()
                return [json.loads(r["payload"]) for r in rows]

    async def count(self) -> int:
        if self.backend == "hazelcast":
            return await asyncio.to_thread(self.tx_map.size)
        else:
            async with self.lock:
                cursor = self.sqlite_conn.cursor()
                cursor.execute("SELECT count(*) FROM hz_transactions;")
                return cursor.fetchone()[0]

    async def reset(self):
        if self.backend == "hazelcast":
            await asyncio.to_thread(self.tx_map.clear)
            print(f"[{INSTANCE_NAME}] Transactions map cleared in Hazelcast", flush=True)
        else:
            async with self.lock:
                with self.sqlite_conn:
                    self.sqlite_conn.execute("DELETE FROM hz_transactions;")

    async def close(self):
        if self.hz_client:
            await asyncio.to_thread(self.hz_client.shutdown)
        if self.sqlite_conn:
            self.sqlite_conn.close()


storage = DistributedStorage()


@asynccontextmanager
async def lifespan(fastapi_app: FastAPI):
    await storage.init()
    try:
        yield
    finally:
        await storage.close()


app = FastAPI(title=f"logging-service ({INSTANCE_NAME})", lifespan=lifespan)


class TransactionIn(BaseModel):
    transaction_ID: str
    user_Id: str
    amount: int | float
    timestamp: Optional[int] = None


@app.get("/health")
async def health() -> dict:
    count = await storage.count()
    return {
        "status": "ok",
        "service": "logging-service",
        "pod_name": POD_NAME,
        "instance": INSTANCE_NAME,
        "storage": storage.backend,
        "hazelcast_cluster": HAZELCAST_CLUSTER_NAME,
        "transactions_count": count,
    }


@app.post("/")
@app.post("/transactions")
async def save_transaction(payload: TransactionIn) -> dict:
    record = payload.model_dump(exclude_none=True)
    tx_id = payload.transaction_ID

    print(f"[{INSTANCE_NAME}] Received transaction: {record}", flush=True)
    await storage.put_transaction(record)

    if LOG_TRANSACTIONS:
        print(f"[{INSTANCE_NAME}] Stored transaction {tx_id} in Hazelcast Distributed Map", flush=True)

    return {
        "status": "ok",
        "transaction_ID": tx_id,
        "instance": INSTANCE_NAME,
        "pod_name": POD_NAME,
        "storage": storage.backend,
    }


@app.get("/user/{user_Id}")
async def get_user_transactions(user_Id: str) -> dict:
    if LOG_TRANSACTIONS:
        print(f"[{INSTANCE_NAME}] Serving GET transactions for user_Id={user_Id}", flush=True)
    txs = await storage.get_user_transactions(user_Id)
    return {
        "user_Id": user_Id,
        "transactions": txs,
        "instance": INSTANCE_NAME,
        "pod_name": POD_NAME,
    }


@app.get("/transactions")
async def get_all_transactions() -> dict:
    if LOG_TRANSACTIONS:
        print(f"[{INSTANCE_NAME}] Serving GET all transactions", flush=True)
    txs = await storage.get_all_transactions()
    return {
        "transactions": txs,
        "instance": INSTANCE_NAME,
        "pod_name": POD_NAME,
    }


@app.post("/reset")
async def reset_transactions() -> dict:
    await storage.reset()
    return {"status": "reset", "instance": INSTANCE_NAME}


if __name__ == "__main__":
    print(f"[{INSTANCE_NAME}] Starting on port {PORT} in Kubernetes...", flush=True)
    uvicorn.run(app, host="0.0.0.0", port=PORT, access_log=False, log_level="warning")
