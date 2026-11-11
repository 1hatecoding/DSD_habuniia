import asyncio
import itertools
import os
import time
from contextlib import asynccontextmanager
import httpx
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict, Field, field_validator

# Адреси сусідніх мікросервісів (у Docker Compose передаються через змінні середовища)
LOGGING_SERVICE_URL = os.getenv("LOGGING_SERVICE_URL", "http://127.0.0.1:8001").rstrip("/")
COUNTER_SERVICE_URL = os.getenv("COUNTER_SERVICE_URL", "http://127.0.0.1:8002").rstrip("/")

# Послідовний лічильник гарантує унікальність transaction_ID навіть при запитах в одну наносекунду
_tx_seq = itertools.count(1)


def _initial_metrics() -> dict:
    # Змінні для накопичення сумарного часу віддалених викликів до кожного сервісу
    return {
        "logging_time_sec": 0.0,
        "counter_time_sec": 0.0,
        "logging_calls": 0,
        "counter_calls": 0,
    }


@asynccontextmanager
async def lifespan(fastapi_app: FastAPI):
    """Керування спільним пулом HTTP-з'єднань протягом життєвого циклу сервісу."""
    # Спільний AsyncClient уникає вичерпання TCP-портів під час навантаження (100K запитів)
    limits = httpx.Limits(max_connections=200, max_keepalive_connections=100)
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

    # Відхиляємо bool, оскільки у Python bool є підкласом int (True == 1)
    @field_validator("amount", mode="before")
    @classmethod
    def reject_bool(cls, value: object) -> object:
        if isinstance(value, bool):
            raise ValueError("'amount' must be a number, not boolean")
        return value


async def _timed_request(
    service_key: str,
    method: str,
    url: str,
    json_body: dict | None = None,
) -> tuple[int, dict]:
    """Виконує HTTP-запит до мікросервісу та додає час виконання до відповідного лічильника."""
    client: httpx.AsyncClient = app.state.http_client
    metrics: dict = app.state.metrics

    # Заміряємо час саме віддаленого виклику до logging-service або counter-service
    start = time.perf_counter()
    resp = await client.request(method, url, json=json_body)
    elapsed = time.perf_counter() - start

    metrics[f"{service_key}_time_sec"] += elapsed
    metrics[f"{service_key}_calls"] += 1
    return resp.status_code, resp.json()


@app.get("/health")
async def health() -> dict:
    """Перевірка доступності сервісу."""
    return {"status": "ok"}


@app.post("/")
@app.post("/transaction")
async def create_transaction(payload: ClientTransaction) -> dict:
    """Приймає {user_Id, amount}, генерує transaction_ID і паралельно відправляє дані в обидва сервіси."""
    # Формуємо унікальний ідентифікатор транзакції на основі timestamp та монотонного лічильника
    timestamp = time.time_ns()
    tx_id = f"{timestamp}-{next(_tx_seq)}"
    tx_payload = {
        "transaction_ID": tx_id,
        "timestamp": timestamp,
        "user_Id": payload.user_Id,
        "amount": payload.amount,
    }

    # Одночасна паралельна відправка POST-запитів до logging-service та counter-service
    (log_status, log_data), (cnt_status, cnt_data) = await asyncio.gather(
        _timed_request(
            "logging",
            "POST",
            f"{LOGGING_SERVICE_URL}/transactions",
            tx_payload,
        ),
        _timed_request(
            "counter",
            "POST",
            f"{COUNTER_SERVICE_URL}/update",
            tx_payload,
        ),
    )

    if log_status >= 400 or cnt_status >= 400:
        raise HTTPException(
            status_code=502,
            detail={"error": "Downstream service failure", "logging": log_data, "counter": cnt_data},
        )

    # Повертаємо клієнту transaction_ID та оновлений баланс після завершення обох викликів
    return {"transaction_ID": tx_id, "balance": cnt_data["balance"]}


@app.get("/user/{user_Id}")
async def get_user_info(user_Id: str) -> dict:
    """Агрегує поточний баланс (з counter-service) та список транзакцій клієнта (з logging-service)."""
    (cnt_status, cnt_data), (log_status, log_data) = await asyncio.gather(
        _timed_request("counter", "GET", f"{COUNTER_SERVICE_URL}/user/{user_Id}"),
        _timed_request("logging", "GET", f"{LOGGING_SERVICE_URL}/user/{user_Id}"),
    )

    if cnt_status >= 400 or log_status >= 400:
        raise HTTPException(status_code=502, detail="Downstream service failure")

    return {
        "balance": cnt_data["balance"],
        "transactions": log_data["transactions"],
    }


@app.get("/accounts")
async def get_accounts() -> dict:
    """Повертає баланси всіх клієнтів, отримані з counter-service."""
    cnt_status, cnt_data = await _timed_request(
        "counter", "GET", f"{COUNTER_SERVICE_URL}/accounts"
    )
    if cnt_status >= 400:
        raise HTTPException(status_code=502, detail="Downstream service failure")

    return {"balances": cnt_data["balances"]}


@app.get("/metrics")
async def get_metrics() -> dict:
    """Повертає накопичений час запитів окремо для logging-service та counter-service."""
    m = app.state.metrics
    return {
        "logging_time_sec": round(m["logging_time_sec"], 6),
        "counter_time_sec": round(m["counter_time_sec"], 6),
        "logging_calls": m["logging_calls"],
        "counter_calls": m["counter_calls"],
    }


@app.post("/metrics/reset")
async def reset_metrics() -> dict:
    """Скидає накопичені лічильники часу перед новим тестом продуктивності."""
    app.state.metrics = _initial_metrics()
    return {"status": "metrics_reset"}


@app.post("/reset")
async def reset_all() -> dict:
    """Очищує метрики фасаду та in-memory сховища обох сервісів між сценаріями тестування."""
    client: httpx.AsyncClient = app.state.http_client
    await asyncio.gather(
        client.post(f"{LOGGING_SERVICE_URL}/reset"),
        client.post(f"{COUNTER_SERVICE_URL}/reset"),
    )
    app.state.metrics = _initial_metrics()
    return {"status": "all_reset"}


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    print(
        f"[facade-service] Starting FastAPI on port {port} -> "
        f"logging={LOGGING_SERVICE_URL}, counter={COUNTER_SERVICE_URL}",
        flush=True,
    )
    uvicorn.run(app, host="0.0.0.0", port=port, access_log=False, log_level="warning")
