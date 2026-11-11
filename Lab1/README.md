# Завдання 1 — Базова архітектура мікросервісів (Python + FastAPI)

Реалізація мікросервісної архітектури банківської системи, описаної в [Task_1-Microservices_Basics.md](Task_1-Microservices_Basics.md), за допомогою **Python**, **FastAPI**, **Uvicorn** та **httpx**.

## Структура проєкту

```text
├── docker-compose.yml
├── facade-service/
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
├── logging-service/
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
├── counter-service/
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
└── load-test/
    ├── benchmark.py
    └── requirements.txt
```

## Запуск через Docker Compose

Запуск усіх трьох сервісів із увімкненим виведенням транзакцій у консоль (зручно для невеликих демонстрацій і скріншотів):

Bash:
```bash
LOG_TRANSACTIONS=1 docker compose up --build
```

PowerShell:
```powershell
$env:LOG_TRANSACTIONS="1"; docker compose up --build
```

Для тестів продуктивності запускайте з `LOG_TRANSACTIONS=0` (значення за замовчуванням), щоб уникнути сповільнення через вивід у `stdout`:

```bash
docker compose up --build
```

## Локальний запуск (Python 3.13+)

Встановіть залежності один раз:

```bash
pip install fastapi uvicorn httpx aiohttp
```

Запустіть кожен сервіс в окремому терміналі:

```bash
python logging-service/app.py
python counter-service/app.py
python facade-service/app.py
```

Порти за замовчуванням:
- `facade-service`: `8000` (Swagger UI доступний за адресою `http://localhost:8000/docs`)
- `logging-service`: `8001`
- `counter-service`: `8002`

## Ендпоїнти API (`facade-service`)

- `POST /` — створити транзакцію: `{"user_Id": "alice", "amount": 100}` -> `{"transaction_ID": "...", "balance": 100}`
- `GET /user/{user_Id}` — отримати баланс користувача та історію транзакцій: `{"balance": ..., "transactions": [...]}`
- `GET /accounts` — отримати баланси всіх рахунків: `{"balances": {...}}`
- `GET /metrics` — отримати накопичений час викликів до внутрішніх сервісів (`logging_time_sec`, `counter_time_sec`)
- `POST /metrics/reset` — скинути лічильники часу
- `POST /reset` — скинути метрики та сховища в пам'яті в усіх трьох сервісах

Приклади команд `curl`:

```bash
curl -X POST http://localhost:8000/ -H "Content-Type: application/json" -d '{"user_Id":"alice","amount":100}'
curl -X POST http://localhost:8000/ -H "Content-Type: application/json" -d '{"user_Id":"alice","amount":-25}'
curl http://localhost:8000/user/alice
curl http://localhost:8000/accounts
```

## Запуск функціональних перевірок і тестів продуктивності

```bash
python load-test/benchmark.py
```

Скрипт виконує:
1. Базові транзакції `+`/`-` та перевірку через `GET` (`Rodionov_Andrii`, `Habuniia_Anton`).
2. Сценарій 1: 10 одночасних клієнтів × 10 000 транзакцій `+1` на 10 окремих рахунках (по `10 000` на кожному).
3. Сценарій 2: 10 одночасних клієнтів × 10 000 транзакцій `+1` на 1 спільному рахунку (`100 000` разом).
4. Виводить пропускну здатність (запитів/с), внесок за часом `logging-service` та `counter-service`, а також підсумкову Markdown-таблицю.

## Вимога щодо гілки GitHub

Перед відправкою на GitHub закомітьте весь код у гілку `micro_basics`:

```bash
git init
git checkout -b micro_basics
git add .
git commit -m "Implement Task 1 microservices architecture with FastAPI"
```
