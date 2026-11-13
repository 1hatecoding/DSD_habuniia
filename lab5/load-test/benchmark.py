import argparse
import asyncio
import json
import time
import aiohttp


async def run_basic_demo(base_url: str) -> None:
    print("=== Basic Functionality Demo ===")
    async with aiohttp.ClientSession() as session:
        async with session.post(f"{base_url}/reset") as resp:
            resp.raise_for_status()

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

        # Allow consumer a moment to apply queue
        await asyncio.sleep(2.0)

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


async def _client_worker(base_url: str, user_id: str, num_requests: int) -> None:
    connector = aiohttp.TCPConnector(limit=1)
    timeout = aiohttp.ClientTimeout(total=180)
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
    print(f"=== {title} ===")
    async with aiohttp.ClientSession() as session:
        async with session.post(f"{base_url}/reset") as resp:
            resp.raise_for_status()

    total_requests = len(user_ids) * requests_per_client

    start = time.perf_counter()
    await asyncio.gather(
        *(
            _client_worker(base_url, uid, requests_per_client)
            for uid in user_ids
        )
    )
    elapsed = time.perf_counter() - start
    rps = total_requests / elapsed

    # Wait for queue consumer to drain all messages
    print("Waiting for counter-service MQ consumer to drain queue...")
    await asyncio.sleep(3.0)

    async with aiohttp.ClientSession() as session:
        async with session.get(f"{base_url}/metrics") as resp:
            resp.raise_for_status()
            metrics = await resp.json()

        async with session.get(f"{base_url}/accounts") as resp:
            resp.raise_for_status()
            balances = (await resp.json())["balances"]

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

    log_time = metrics.get("logging_time_sec", 0.0)
    cnt_time = metrics.get("counter_time_sec", 0.0)
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
    parser = argparse.ArgumentParser(description="Lab 5 Kubernetes Benchmark Client")
    parser.add_argument("--url", default="http://127.0.0.1:8000", help="facade-service base URL")
    parser.add_argument("--clients", type=int, default=10, help="Number of concurrent clients (default: 10)")
    parser.add_argument(
        "--requests",
        type=int,
        default=1000,
        help="Transactions per client (default: 1000, use 10000 for full load)",
    )
    parser.add_argument("--skip-basic", action="store_true", help="Skip basic demo checks")
    args = parser.parse_args()

    base_url = args.url.rstrip("/")

    if not args.skip_basic:
        await run_basic_demo(base_url)

    # Scenario 1: 10 parallel clients, each with own account
    users_10 = [f"client_{i:02d}" for i in range(args.clients)]
    res_10 = await run_scenario(
        base_url=base_url,
        title=f"Scenario 1: {args.clients} accounts ({args.clients} clients x {args.requests} requests)",
        user_ids=users_10,
        requests_per_client=args.requests,
    )

    # Scenario 2: 10 parallel clients, all updating 1 shared account
    users_1 = ["shared_account"] * args.clients
    res_1 = await run_scenario(
        base_url=base_url,
        title=f"Scenario 2: 1 account ({args.clients} clients x {args.requests} requests)",
        user_ids=users_1,
        requests_per_client=args.requests,
    )

    print("=" * 70)
    print(" LAB 5 BENCHMARK COMPARISON TABLE")
    print("=" * 70)
    print(f"{'Metric':<30} | {'Scenario 1 (10 accounts)':<20} | {'Scenario 2 (1 account)':<20}")
    print("-" * 76)
    print(f"{'Total time (s)':<30} | {res_10['elapsed']:<20.3f} | {res_1['elapsed']:<20.3f}")
    print(f"{'Throughput (req/s)':<30} | {res_10['rps']:<20.2f} | {res_1['rps']:<20.2f}")
    print(f"{'logging-service time (s)':<30} | {res_10['log_time']:<20.3f} | {res_1['log_time']:<20.3f}")
    print(f"{'logging-service %':<30} | {res_10['log_pct']:<19.1f}% | {res_1['log_pct']:<19.1f}%")
    print(f"{'counter-service time (s)':<30} | {res_10['cnt_time']:<20.3f} | {res_1['cnt_time']:<20.3f}")
    print(f"{'counter-service %':<30} | {res_10['cnt_pct']:<19.1f}% | {res_1['cnt_pct']:<19.1f}%")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
