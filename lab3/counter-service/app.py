import asyncio
import os
import sqlite3
from contextlib import asynccontextmanager
from typing import Optional
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, field_validator

PORT = int(os.getenv("PORT", "8002"))
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgrespassword@localhost:5432/bank")
LOG_TRANSACTIONS = os.getenv("LOG_TRANSACTIONS", "1").lower() in ("1", "true", "yes")

# Try asyncpg for high-performance async PostgreSQL
try:
    import asyncpg
    HAS_ASYNCPG = True
except ImportError:
    HAS_ASYNCPG = False


class DBStore:
    def __init__(self, db_url: str):
        self.db_url = db_url
        self.backend: str = "sqlite"
        self.pg_pool: Optional[asyncpg.Pool] = None
        self.sqlite_conn: Optional[sqlite3.Connection] = None
        self.lock = asyncio.Lock()

    async def init(self):
        if self.db_url.startswith("postgres") and HAS_ASYNCPG:
            # Attempt to connect to PostgreSQL with retries (for Docker startup order)
            for attempt in range(1, 11):
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
                    print(f"[counter-service] Waiting for PostgreSQL (attempt {attempt}/10): {exc}", flush=True)
                    await asyncio.sleep(2.0)
            print("[counter-service] PostgreSQL connection failed, falling back to SQLite storage", flush=True)

        # Fallback to local SQLite DB
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


@asynccontextmanager
async def lifespan(fastapi_app: FastAPI):
    await db_store.init()
    try:
        yield
    finally:
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
        "database_url": DATABASE_URL.split("@")[-1] if "@" in DATABASE_URL else DATABASE_URL,
    }


@app.post("/")
@app.post("/update")
async def update_balance(payload: CounterUpdate) -> dict:
    new_balance = await db_store.update_balance(
        user_id=payload.user_Id,
        amount=payload.amount,
        tx_id=payload.transaction_ID,
        timestamp=payload.timestamp,
    )
    if LOG_TRANSACTIONS:
        print(
            f"[counter-service] user_Id={payload.user_Id} amount={payload.amount:+} -> balance={new_balance} (DB: {db_store.backend})",
            flush=True,
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
    print(f"[counter-service] DB store cleared ({db_store.backend})", flush=True)
    return {"status": "reset", "backend": db_store.backend}


if __name__ == "__main__":
    print(f"[counter-service] Starting on port {PORT} with DB: {DATABASE_URL}", flush=True)
    uvicorn.run(app, host="0.0.0.0", port=PORT, access_log=False, log_level="warning")

