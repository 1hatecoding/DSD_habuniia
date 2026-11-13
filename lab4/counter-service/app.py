import asyncio
import json
import os
import sqlite3
from contextlib import asynccontextmanager
from typing import Optional
import httpx
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, field_validator

PORT = int(os.getenv("PORT", "8002"))
MY_URL = os.getenv("MY_URL", f"http://counter-service:{PORT}")
CONFIG_SERVER_URL = os.getenv("CONFIG_SERVER_URL", "http://config-server:8888").rstrip("/")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgrespassword@localhost:5432/bank")
LOG_TRANSACTIONS = os.getenv("LOG_TRANSACTIONS", "1").lower() in ("1", "true", "yes")

HAZELCAST_MEMBERS = os.getenv(
    "HAZELCAST_CLUSTER_MEMBERS",
    "hazelcast-1:5701,hazelcast-2:5701,hazelcast-3:5701"
)
HAZELCAST_CLUSTER_NAME = os.getenv("HAZELCAST_CLUSTER_NAME", "dev")
QUEUE_NAME = "counter_queue"

try:
    import asyncpg
    HAS_ASYNCPG = True
except ImportError:
    HAS_ASYNCPG = False

try:
    import hazelcast
    HAS_HAZELCAST = True
except ImportError:
    HAS_HAZELCAST = False


class DBStore:
    def __init__(self, db_url: str):
        self.db_url = db_url
        self.backend: str = "sqlite"
        self.pg_pool: Optional[asyncpg.Pool] = None
        self.sqlite_conn: Optional[sqlite3.Connection] = None
        self.lock = asyncio.Lock()

    async def init(self):
        if self.db_url.startswith("postgres") and HAS_ASYNCPG:
            for attempt in range(1, 16):
                try:
                    self.pg_pool = await asyncpg.create_pool(
                        dsn=self.db_url,
                        min_size=2,
                        max_size=20,
                        timeout=5.0,
                    )
                    self.backend = "postgres"
                    print(f"[counter-service] Connected to PostgreSQL on attempt {attempt}", flush=True)
                    await self._init_postgres_schema()
                    return
                except Exception as exc:
                    print(f"[counter-service] Waiting for PostgreSQL ({attempt}/15): {exc}", flush=True)
                    await asyncio.sleep(2.0)
            print("[counter-service] PostgreSQL failed, falling back to SQLite", flush=True)

        self.backend = "sqlite"
        db_path = "counter.db"
        if self.db_url.startswith("sqlite:///"):
            db_path = self.db_url.replace("sqlite:///", "")
        self.sqlite_conn = sqlite3.connect(db_path, check_same_thread=False)
        self.sqlite_conn.row_factory = sqlite3.Row
        self._init_sqlite_schema()
        print(f"[counter-service] Using SQLite database: {db_path}", flush=True)

    async def _init_postgres_schema(self):
        async with self.pg_pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS accounts (
                    user_id VARCHAR(255) PRIMARY KEY,
                    balance DOUBLE PRECISION NOT NULL DEFAULT 0.0
                );
                CREATE TABLE IF NOT EXISTS transactions (
                    transaction_id VARCHAR(255) PRIMARY KEY,
                    user_id VARCHAR(255) NOT NULL,
                    amount DOUBLE PRECISION NOT NULL,
                    timestamp BIGINT
                );
                """
            )

    def _init_sqlite_schema(self):
        with self.sqlite_conn:
            self.sqlite_conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS accounts (
                    user_id TEXT PRIMARY KEY,
                    balance REAL NOT NULL DEFAULT 0.0
                );
                CREATE TABLE IF NOT EXISTS transactions (
                    transaction_id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    amount REAL NOT NULL,
                    timestamp INTEGER
                );
                """
            )

    async def update_balance(self, user_id: str, amount: float, tx_id: Optional[str] = None, timestamp: Optional[int] = None) -> float:
        if self.backend == "postgres":
            async with self.pg_pool.acquire() as conn:
                async with conn.transaction():
                    row = await conn.fetchrow(
                        """
                        INSERT INTO accounts (user_id, balance)
                        VALUES ($1, $2)
                        ON CONFLICT (user_id)
                        DO UPDATE SET balance = accounts.balance + EXCLUDED.balance
                        RETURNING balance;
                        """,
                        user_id,
                        float(amount),
                    )
                    new_balance = float(row["balance"])
                    if tx_id:
                        await conn.execute(
                            """
                            INSERT INTO transactions (transaction_id, user_id, amount, timestamp)
                            VALUES ($1, $2, $3, $4)
                            ON CONFLICT (transaction_id) DO NOTHING;
                            """,
                            tx_id,
                            user_id,
                            float(amount),
                            timestamp,
                        )
                    return new_balance
        else:
            async with self.lock:
                cursor = self.sqlite_conn.cursor()
                cursor.execute(
                    """
                    INSERT INTO accounts (user_id, balance)
                    VALUES (?, ?)
                    ON CONFLICT (user_id)
                    DO UPDATE SET balance = accounts.balance + excluded.balance
                    RETURNING balance;
                    """,
                    (user_id, float(amount)),
                )
                row = cursor.fetchone()
                new_balance = float(row[0])
                if tx_id:
                    cursor.execute(
                        """
                        INSERT OR IGNORE INTO transactions (transaction_id, user_id, amount, timestamp)
                        VALUES (?, ?, ?, ?);
                        """,
                        (tx_id, user_id, float(amount), timestamp),
                    )
                self.sqlite_conn.commit()
                return new_balance

    async def get_user_balance(self, user_id: str) -> float:
        if self.backend == "postgres":
            async with self.pg_pool.acquire() as conn:
                row = await conn.fetchrow("SELECT balance FROM accounts WHERE user_id = $1;", user_id)
                return float(row["balance"]) if row else 0.0
        else:
            async with self.lock:
                cursor = self.sqlite_conn.cursor()
                cursor.execute("SELECT balance FROM accounts WHERE user_id = ?;", (user_id,))
                row = cursor.fetchone()
                return float(row[0]) if row else 0.0

    async def get_all_accounts(self) -> dict[str, float]:
        if self.backend == "postgres":
            async with self.pg_pool.acquire() as conn:
                rows = await conn.fetch("SELECT user_id, balance FROM accounts;")
                return {r["user_id"]: float(r["balance"]) for r in rows}
        else:
            async with self.lock:
                cursor = self.sqlite_conn.cursor()
                cursor.execute("SELECT user_id, balance FROM accounts;")
                rows = cursor.fetchall()
                return {r[0]: float(r[1]) for r in rows}

    async def reset(self):
        if self.backend == "postgres":
            async with self.pg_pool.acquire() as conn:
                await conn.execute("DELETE FROM accounts; DELETE FROM transactions;")
        else:
            async with self.lock:
                with self.sqlite_conn:
                    self.sqlite_conn.execute("DELETE FROM accounts;")
                    self.sqlite_conn.execute("DELETE FROM transactions;")

    async def close(self):
        if self.pg_pool:
            await self.pg_pool.close()
        if self.sqlite_conn:
            self.sqlite_conn.close()


db_store = DBStore(DATABASE_URL)


class QueueConsumer:
    def __init__(self):
        self.hz_client = None
        self.queue = None
        self.running = False
        self.worker_task = None

    async def init(self):
        if HAS_HAZELCAST:
            members = [m.strip() for m in HAZELCAST_MEMBERS.split(",") if m.strip()]
            for attempt in range(1, 16):
                try:
                    print(f"[counter-service] Connecting to Hazelcast MQ ({HAZELCAST_CLUSTER_NAME}) attempt {attempt}/15...", flush=True)
                    self.hz_client = await asyncio.to_thread(
                        hazelcast.HazelcastClient,
                        cluster_name=HAZELCAST_CLUSTER_NAME,
                        cluster_members=members,
                        client_name="counter-service-mq-client",
                        connection_timeout=5.0,
                    )
                    self.queue = self.hz_client.get_queue(QUEUE_NAME).blocking()
                    print(f"[counter-service] Connected to Hazelcast Queue '{QUEUE_NAME}'!", flush=True)
                    self.running = True
                    self.worker_task = asyncio.create_task(self._consume_loop())
                    return
                except Exception as exc:
                    print(f"[counter-service] Hazelcast connect failed: {exc}", flush=True)
                    await asyncio.sleep(2.0)
            print("[counter-service] Warning: Hazelcast MQ connection failed, queue worker not started", flush=True)

    async def _consume_loop(self):
        """Asynchronously consumes messages from Hazelcast Distributed Queue."""
        print(f"[counter-service] Started MQ Consumer loop on '{QUEUE_NAME}'", flush=True)
        while self.running:
            try:
                # poll with 1.0 second timeout to allow graceful shutdown
                item = await asyncio.to_thread(self.queue.poll, 1.0)
                if item is not None:
                    try:
                        record = json.loads(item)
                        user_id = record["user_Id"]
                        amount = float(record["amount"])
                        tx_id = record.get("transaction_ID")
                        timestamp = record.get("timestamp")

                        new_balance = await db_store.update_balance(user_id, amount, tx_id, timestamp)
                        if LOG_TRANSACTIONS:
                            print(
                                f"[counter-service] [MQ Consumer] Processed tx={tx_id} -> "
                                f"user={user_id} amount={amount:+} balance={new_balance}",
                                flush=True,
                            )
                    except Exception as err:
                        print(f"[counter-service] Error processing message from MQ: {err}", flush=True)
            except Exception as e:
                if self.running:
                    await asyncio.sleep(1.0)

    async def stop(self):
        self.running = False
        if self.worker_task:
            self.worker_task.cancel()
        if self.hz_client:
            await asyncio.to_thread(self.hz_client.shutdown)


queue_consumer = QueueConsumer()


async def register_to_config_server():
    reg_payload = {"service_name": "counter-service", "instance_url": MY_URL}
    for attempt in range(1, 16):
        try:
            async with httpx.AsyncClient(timeout=3.0) as client:
                resp = await client.post(f"{CONFIG_SERVER_URL}/register", json=reg_payload)
                if resp.status_code == 200:
                    print(f"[counter-service] Registered at {CONFIG_SERVER_URL} as {MY_URL}", flush=True)
                    return
        except Exception:
            await asyncio.sleep(2.0)
    print(f"[counter-service] Warning: Failed to register with config-server at {CONFIG_SERVER_URL}", flush=True)


@asynccontextmanager
async def lifespan(fastapi_app: FastAPI):
    await db_store.init()
    await queue_consumer.init()
    asyncio.create_task(register_to_config_server())
    try:
        yield
    finally:
        await queue_consumer.stop()
        await db_store.close()


app = FastAPI(title="counter-service", lifespan=lifespan)


class CounterUpdate(BaseModel):
    user_Id: str
    amount: int | float
    transaction_ID: Optional[str] = None
    timestamp: Optional[int] = None

    @field_validator("amount", mode="before")
    @classmethod
    def reject_bool(cls, value: object) -> object:
        if isinstance(value, bool):
            raise ValueError("'amount' must be a number, not boolean")
        return value


@app.get("/health")
async def health() -> dict:
    return {
        "status": "ok",
        "backend": db_store.backend,
        "queue_consumer": queue_consumer.running,
    }


@app.post("/")
@app.post("/update")
async def update_balance(payload: CounterUpdate) -> dict:
    """Direct HTTP fallback endpoint (used when not going through MQ)."""
    new_balance = await db_store.update_balance(
        user_id=payload.user_Id,
        amount=payload.amount,
        tx_id=payload.transaction_ID,
        timestamp=payload.timestamp,
    )
    return {"user_Id": payload.user_Id, "balance": new_balance}


@app.get("/user/{user_Id}")
async def get_user_balance(user_Id: str) -> dict:
    balance = await db_store.get_user_balance(user_Id)
    return {"user_Id": user_Id, "balance": balance}


@app.get("/accounts")
async def get_all_accounts() -> dict:
    balances = await db_store.get_all_accounts()
    return {"balances": balances}


@app.post("/reset")
async def reset_store() -> dict:
    await db_store.reset()
    if queue_consumer.queue:
        await asyncio.to_thread(queue_consumer.queue.clear)
    print(f"[counter-service] Store and Queue cleared", flush=True)
    return {"status": "reset", "backend": db_store.backend}


if __name__ == "__main__":
    print(f"[counter-service] Starting on port {PORT}", flush=True)
    uvicorn.run(app, host="0.0.0.0", port=PORT, access_log=False, log_level="warning")
