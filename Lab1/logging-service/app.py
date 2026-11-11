import os
from collections import defaultdict
import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI(title="logging-service")

# Основна хеш-таблиця в оперативній пам'яті: ключ — transaction_ID, значення — дані транзакції
transactions: dict[str, dict] = {}
# Додатковий індекс за user_Id для швидкого отримання всіх транзакцій конкретного клієнта
user_transactions: dict[str, list[dict]] = defaultdict(list)

# Вмикається змінною середовища LOG_TRANSACTIONS=1 для виводу транзакцій у консоль під час відлагодження
LOG_TRANSACTIONS = os.getenv("LOG_TRANSACTIONS", "0").lower() in ("1", "true", "yes")


class TransactionIn(BaseModel):
    transaction_ID: str
    user_Id: str
    amount: int | float
    timestamp: int | None = None


@app.get("/health")
async def health() -> dict:
    """Повертає стан сервісу та кількість збережених транзакцій."""
    return {"status": "ok", "count": len(transactions)}


@app.post("/")
@app.post("/transactions")
async def save_transaction(payload: TransactionIn) -> dict:
    """Зберігає вхідну транзакцію в локальній хеш-таблиці за ключем transaction_ID."""
    record = payload.model_dump(exclude_none=True)
    tx_id = payload.transaction_ID

    # Додаємо до індексу користувача лише один раз, щоб уникнути дублювання при повторних запитах
    if tx_id not in transactions:
        user_transactions[payload.user_Id].append(record)
    transactions[tx_id] = record

    if LOG_TRANSACTIONS:
        print(f"[logging-service] Stored transaction: {record}", flush=True)

    return {"status": "ok", "transaction_ID": tx_id}


@app.get("/user/{user_Id}")
async def get_user_transactions(user_Id: str) -> dict:
    """Повертає список усіх транзакцій для вказаного user_Id."""
    return {
        "user_Id": user_Id,
        "transactions": user_transactions.get(user_Id, []),
    }


@app.get("/transactions")
async def get_all_transactions() -> dict:
    """Повертає всі транзакції, збережені у пам'яті."""
    return {"transactions": list(transactions.values())}


@app.post("/reset")
async def reset_store() -> dict:
    """Очищує сховище транзакцій у пам'яті (використовується між тестами)."""
    transactions.clear()
    user_transactions.clear()
    print("[logging-service] In-memory store cleared", flush=True)
    return {"status": "reset"}


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8001"))
    print(
        f"[logging-service] Starting FastAPI on port {port} (LOG_TRANSACTIONS={LOG_TRANSACTIONS})",
        flush=True,
    )
    uvicorn.run(app, host="0.0.0.0", port=port, access_log=False, log_level="warning")
