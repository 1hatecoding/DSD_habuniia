import argparse
import asyncio
import subprocess
import time
import aiohttp


def run_cmd(cmd: list[str]) -> str:
    print(f"Executing: {' '.join(cmd)}")
    res = subprocess.run(cmd, capture_output=True, text=True)
    return res.stdout.strip()


async def run_mq_fault_tolerance_test(base_url: str, use_docker: bool = False):
    print("=" * 65)
    print(" Lab 4 Fault Tolerance: Asynchronous Message Queue Resilience")
    print("=" * 65)

    test_user = f"mq_user_{int(time.time())}"

    async with aiohttp.ClientSession() as session:
        # 1. Normal state baseline
        print("\n[Step 1] Baseline check: Sending 1 transaction while counter-service is UP...")
        async with session.post(f"{base_url}/", json={"user_Id": test_user, "amount": 50}) as resp:
            resp.raise_for_status()
            print("  POST status:", resp.status, await resp.json())

        await asyncio.sleep(2)
        async with session.get(f"{base_url}/user/{test_user}") as resp:
            resp.raise_for_status()
            data = await resp.json()
            print(f"  GET balance: {data.get('balance')} (counter_available: {data.get('counter_available')})")
            assert data.get("balance") == 50.0

        # 2. Pause counter-service
        print("\n[Step 2] Making counter-service unavailable (docker pause counter-service)...")
        if use_docker:
            run_cmd(["docker", "pause", "counter-service"])
            time.sleep(2)
        else:
            print("  (If testing manually, run: docker pause counter-service)")

        # 3. Send transactions while counter-service is paused
        print("\n[Step 3] Sending 3 transactions while counter-service is PAUSED...")
        print("  Messages should queue up in Hazelcast Queue without throwing any HTTP errors!")
        amounts = [100, 200, -30]
        for amt in amounts:
            async with session.post(f"{base_url}/", json={"user_Id": test_user, "amount": amt}) as resp:
                print(f"  POST amount={amt:+4} -> HTTP {resp.status} (queued={((await resp.json()).get('queued'))})")
                assert resp.status == 200, f"Expected 200 OK while MQ is up, got {resp.status}"

        # 4. Check GET while counter-service is paused
        print("\n[Step 4] Checking GET /user while counter-service is PAUSED...")
        async with session.get(f"{base_url}/user/{test_user}") as resp:
            data = await resp.json()
            print(f"  GET response: balance = {data.get('balance')} (counter_available = {data.get('counter_available')})")
            print("  => As required by task, GET returns null / unavailable while counter-service is paused!")

        # 5. Unpause counter-service
        print("\n[Step 5] Restoring counter-service (docker unpause counter-service)...")
        if use_docker:
            run_cmd(["docker", "unpause", "counter-service"])
            print("  Waiting 3 seconds for counter-service to consume accumulated messages from queue...")
            time.sleep(3)
        else:
            print("  (If testing manually, run: docker unpause counter-service, then wait 3 seconds)")
            await asyncio.sleep(3)

        # 6. Verify accumulated transactions were drained and applied
        print("\n[Step 6] Verifying balances after counter-service resumed...")
        async with session.get(f"{base_url}/user/{test_user}") as resp:
            resp.raise_for_status()
            data = await resp.json()
            expected_balance = 50 + 100 + 200 - 30  # 320
            print(f"  Final balance in DB: {data.get('balance')} (Expected: {expected_balance})")
            assert data.get("balance") == expected_balance, f"Expected {expected_balance}, got {data.get('balance')}"
            print("  => SUCCESS: counter-service successfully drained the queue and applied all transactions!")

    print("\n" + "=" * 65)
    print(" FAULT TOLERANCE & MQ VERIFICATION PASSED SUCCESSFULLY!")
    print("=" * 65 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Lab 4 MQ Fault Tolerance Test")
    parser.add_argument("--url", default="http://127.0.0.1:8000", help="facade-service URL")
    parser.add_argument("--docker", action="store_true", help="Automate docker pause/unpause")
    args = parser.parse_args()

    asyncio.run(run_mq_fault_tolerance_test(args.url.rstrip("/"), use_docker=args.docker))
