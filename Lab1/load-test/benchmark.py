import argparse
import asyncio
import json
import time
import aiohttp


async def run_basic_demo(base_url: str) -> None:
    """Демонстрація базової функціональності: транзакції +/- та перевірка через GET."""
    print("=== Basic Functionality Demo ===")
    async with aiohttp.ClientSession() as session:
        async with session.post(f"{base_url}/reset") as resp:
            resp.raise_for_status()

        # Кілька тестових транзакцій поповнення та списання для різних клієнтів
        sample_txs = [
            {"user_Id": "Rodionov_Andrii", "amount": 100},
            {"user_Id": "Habuniia_Anton", "amount": 50},
            {"user_Id": "Rodionov_Andrii", "amount": -30},
            {"user_Id": "Habuniia_Anton", "amount": -10},
        ]

        for tx in sample_txs:
            async with session.post(f"{base_url}/", json=tx) as resp:
                resp.raise_for_status()
                body = await resp.json()
                print(f"POST {tx} -> {json.dumps(body)}")

        for user_id in ("Rodionov_Andrii", "Habuniia_Anton"):
            async with session.get(f"{base_url}/user/{user_id}") as resp:
                resp.raise_for_status()
                body = await resp.json()
                print(f"GET /user/{user_id} -> {json.dumps(body)}")

        async with session.get(f"{base_url}/accounts") as resp:
            resp.raise_for_status()
            accounts = await resp.json()
            print(f"GET /accounts -> {json.dumps(accounts)}")

        assert accounts["balances"]["Rodionov_Andrii"] == 70
        assert accounts["balances"]["Habuniia_Anton"] == 40
    print("Basic functionality checks PASSED.\n")


async def _client_worker(
    base_url: str, user_id: str, num_requests: int
) -> None:
    """Імітує одного незалежного HTTP-клієнта, що виконує задану кількість запитів +1."""
    # Окрема сесія на кожного клієнта моделює реальну роботу 10 паралельних клієнтів
    connector = aiohttp.TCPConnector(limit=1)
    timeout = aiohttp.ClientTimeout(total=60)
    payload = {"user_Id": user_id, "amount": 1}

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        for _ in range(num_requests):
            async with session.post(f"{base_url}/", json=payload) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    raise RuntimeError(f"Request failed ({resp.status}): {text}")
                await resp.read()


async def run_scenario(
    base_url: str,
    title: str,
    user_ids: list[str],
    requests_per_client: int,
) -> dict:
    """Запускає навантажувальний сценарій, заміряє RPS і перевіряє коректність результатів."""
    print(f"=== {title} ===")
    async with aiohttp.ClientSession() as session:
        async with session.post(f"{base_url}/reset") as resp:
            resp.raise_for_status()

    total_requests = len(user_ids) * requests_per_client

    # Заміряємо сумарний час виконання всіх одночасних клієнтських запитів
    start = time.perf_counter()
    await asyncio.gather(
        *(
            _client_worker(base_url, uid, requests_per_client)
            for uid in user_ids
        )
    )
    elapsed = time.perf_counter() - start
    # Пропускна здатність системи: кількість запитів за секунду (count / time)
    rps = total_requests / elapsed

    # Зчитуємо накопичений час звернень до logging-service та counter-service з фасаду
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{base_url}/metrics") as resp:
            resp.raise_for_status()
            metrics = await resp.json()

        async with session.get(f"{base_url}/accounts") as resp:
            resp.raise_for_status()
            balances = (await resp.json())["balances"]

    # Перевіряємо очікувані баланси (10K на кожному з 10 рахунків або 100K на 1 рахунку)
    unique_users = set(user_ids)
    expected_per_user = (
        requests_per_client
        if len(unique_users) == len(user_ids)
        else total_requests
    )
    for uid in unique_users:
        actual = balances.get(uid)
        assert actual == expected_per_user, (
            f"Expected {uid} balance={expected_per_user}, got {actual}"
        )

    # Перевіряємо, що logging-service зберіг усі транзакції без втрат чи колізій ID
    sample_uid = next(iter(unique_users))
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{base_url}/user/{sample_uid}") as resp:
            resp.raise_for_status()
            user_data = await resp.json()
            tx_count = len(user_data["transactions"])
            assert tx_count == expected_per_user, (
                f"Expected {sample_uid} logged tx={expected_per_user}, got {tx_count}"
            )

    log_time = metrics["logging_time_sec"]
    cnt_time = metrics["counter_time_sec"]
    total_downstream = log_time + cnt_time
    log_pct = (log_time / total_downstream * 100) if total_downstream else 0.0
    cnt_pct = (cnt_time / total_downstream * 100) if total_downstream else 0.0

    print(f"Total requests: {total_requests}")
    print(f"Total time:     {elapsed:.3f} s")
    print(f"Throughput:     {rps:.2f} req/s")
    print(
        f"logging-service cumulative call time: {log_time:.3f} s ({log_pct:.1f}% of downstream wait)"
    )
    print(
        f"counter-service cumulative call time: {cnt_time:.3f} s ({cnt_pct:.1f}% of downstream wait)"
    )
    print(f"Verified balances: {len(unique_users)} account(s) = {expected_per_user} each.\n")

    return {
        "elapsed": elapsed,
        "rps": rps,
        "log_time": log_time,
        "cnt_time": cnt_time,
        "log_pct": log_pct,
        "cnt_pct": cnt_pct,
    }


async def main() -> None:
    parser = argparse.ArgumentParser(description="Task 1 Microservices Benchmark Client")
    parser.add_argument("--url", default="http://127.0.0.1:8000", help="facade-service base URL")
    parser.add_argument("--clients", type=int, default=10, help="Number of concurrent clients")
    parser.add_argument(
        "--requests",
        type=int,
        default=10000,
        help="Transactions per client (default: 10000)",
    )
    parser.add_argument("--skip-basic", action="store_true", help="Skip basic demo checks")
    args = parser.parse_args()

    base_url = args.url.rstrip("/")

    if not args.skip_basic:
        await run_basic_demo(base_url)

    # Сценарій 1: 10 клієнтів роблять по 10K транзакцій кожен на свій окремий рахунок
    s1 = await run_scenario(
        base_url,
        f"Scenario 1: {args.clients} accounts ({args.clients} x {args.requests} tx)",
        [f"user_{i}" for i in range(1, args.clients + 1)],
        args.requests,
    )

    # Сценарій 2: 10 клієнтів одночасно роблять по 10K транзакцій на один спільний рахунок
    s2 = await run_scenario(
        base_url,
        f"Scenario 2: 1 shared account ({args.clients} x {args.requests} tx)",
        ["shared_account"] * args.clients,
        args.requests,
    )

    # Виводимо заповнену таблицю результатів для протоколу
    print("=== Summary Table (Task 1 in-mem) ===")
    print("| Test scenarios | Task 1 (in-mem) | Task 3 (DB) | Task 5 (final) |")
    print("| :--- | :--- | :--- | :--- |")
    print(
        f"| 10 accounts | Total time: {s1['elapsed']:.3f} s ({s1['rps']:.1f} req/s) | Total time: | Total time: |"
    )
    print(
        f"| | logging-service contribution: {s1['log_time']:.3f} s ({s1['log_pct']:.1f}%) | logging-service contribution: | logging-service contribution: |"
    )
    print(
        f"| | counter-service contribution: {s1['cnt_time']:.3f} s ({s1['cnt_pct']:.1f}%) | counter-service contribution: | counter-service contribution: |"
    )
    print(
        f"| 1 account | Total time: {s2['elapsed']:.3f} s ({s2['rps']:.1f} req/s) | Total time: | Total time: |"
    )
    print(
        f"| | logging-service contribution: {s2['log_time']:.3f} s ({s2['log_pct']:.1f}%) | logging-service contribution: | logging-service contribution: |"
    )
    print(
        f"| | counter-service contribution: {s2['cnt_time']:.3f} s ({s2['cnt_pct']:.1f}%) | counter-service contribution: | counter-service contribution: |"
    )


if __name__ == "__main__":
    asyncio.run(main())
