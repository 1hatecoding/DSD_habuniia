import os
import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel, field_validator

app = FastAPI(title="counter-service")

# Хеш-таблиця балансів у оперативній пам'яті: ключ — user_Id, значення — поточний баланс
balances: dict[str, int | float] = {}

# Вмикається змінною середовища LOG_TRANSACTIONS=1 для діагностичного виводу в консоль
LOG_TRANSACTIONS = os.getenv("LOG_TRANSACTIONS", "0").lower() in ("1", "true", "yes")


class CounterUpdate(BaseModel):
    user_Id: str
    amount: int | float
    transaction_ID: str | None = None

    # Забороняємо булеві значення для суми транзакції
    @field_validator("amount", mode="before")
    @classmethod
    def reject_bool(cls, value: object) -> object:
        if isinstance(value, bool):
            raise ValueError("'amount' must be a number, not boolean")
        return value


@app.get("/health")
async def health() -> dict:
    """Повертає стан сервісу та кількість активних рахунків."""
    return {"status": "ok", "accounts": len(balances)}


@app.post("/")
@app.post("/update")
async def update_balance(payload: CounterUpdate) -> dict:
    """Оновлює баланс клієнта (поповнення або списання) та повертає актуальне значення."""
    # Операція читання-зміни-запису не містить await, тому виконується атомарно в event loop
    # Це гарантує відсутність втрачених оновлень у сценарії 10 клієнтів на 1 спільний рахунок (100K)
    new_balance = balances.get(payload.user_Id, 0) + payload.amount
    balances[payload.user_Id] = new_balance

    if LOG_TRANSACTIONS:
        print(
            f"[counter-service] user_Id={payload.user_Id} amount={payload.amount:+} -> balance={new_balance}",
            flush=True,
        )

    return {"user_Id": payload.user_Id, "balance": new_balance}


@app.get("/user/{user_Id}")
async def get_user_balance(user_Id: str) -> dict:
    """Повертає поточний баланс вказаного клієнта (0, якщо транзакцій ще не було)."""
    return {"user_Id": user_Id, "balance": balances.get(user_Id, 0)}


@app.get("/accounts")
async def get_all_accounts() -> dict:
    """Повертає словник балансів для всіх клієнтів."""
    return {"balances": dict(balances)}


@app.post("/reset")
async def reset_store() -> dict:
    """Очищує всі баланси у пам'яті між тестовими сценаріями."""
    balances.clear()
    print("[counter-service] In-memory balances cleared", flush=True)
    return {"status": "reset"}


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8002"))
    print(
        f"[counter-service] Starting FastAPI on port {port} (LOG_TRANSACTIONS={LOG_TRANSACTIONS})",
        flush=True,
    )
    uvicorn.run(app, host="0.0.0.0", port=port, access_log=False, log_level="warning")
