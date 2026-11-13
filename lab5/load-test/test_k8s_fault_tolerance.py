import argparse
import asyncio
import subprocess
import time
import aiohttp


def run_cmd(cmd: list[str]) -> str:
    print(f"Executing: {' '.join(cmd)}")
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.stdout:
        print(f"  Stdout: {res.stdout.strip()}")
    if res.stderr:
        print(f"  Stderr: {res.stderr.strip()}")
    return res.stdout.strip()


async def run_k8s_fault_tolerance_test(base_url: str, mode: str = "manual"):
    print("=" * 70)
    print(" Lab 5 Fault Tolerance: Kubernetes Scaling, Pod Resiliency & MQ Queue")
    print("=" * 70)

    test_user = f"k8s_user_{int(time.time())}"

    async with aiohttp.ClientSession() as session:
        # -------------------------------------------------------------
        # Part A: Test logging-service Pod failover
        # -------------------------------------------------------------
        print("\n--- Part A: Logging-service Replica Failover & Dynamic Discovery ---")
        print("[Step 1] Baseline check: Sending 3 transactions across 3 replicas...")
        for i in range(3):
            async with session.post(f"{base_url}/", json={"user_Id": test_user, "amount": 10}) as resp:
                resp.raise_for_status()
                data = await resp.json()
                print(f"  Tx {i+1} handled by: {data.get('logging_instance')}")

        if mode == "k8s":
            print("\n[Step 2] Simulating Pod failure: Scaling logging-service down to 1 replica...")
            run_cmd(["kubectl", "-n", "bank-system", "scale", "deployment", "logging-service", "--replicas=1"])
            print("  Waiting 5 seconds for Kubernetes Endpoints to update...")
            time.sleep(5)
        elif mode == "docker":
            print("\n[Step 2] Simulating Pod failure: Stopping logging-service-3...")
            run_cmd(["docker", "stop", "logging-service-3"])
            time.sleep(2)
        else:
            print("\n[Step 2] (Manual mode: Scale/stop one logging-service instance now if desired)")

        print("\n[Step 3] Sending 3 transactions while logging replica is down (Auto Failover)...")
        for i in range(3):
            async with session.post(f"{base_url}/", json={"user_Id": test_user, "amount": 10}) as resp:
                resp.raise_for_status()
                data = await resp.json()
                print(f"  Tx {i+4} handled by surviving instance: {data.get('logging_instance')}")

        if mode == "k8s":
            print("\n[Step 4] Restoring logging-service back to 3 replicas...")
            run_cmd(["kubectl", "-n", "bank-system", "scale", "deployment", "logging-service", "--replicas=3"])
            time.sleep(5)
        elif mode == "docker":
            print("\n[Step 4] Restoring logging-service-3...")
            run_cmd(["docker", "start", "logging-service-3"])
            time.sleep(2)

        # -------------------------------------------------------------
        # Part B: Test counter-service unavailability (MQ Queuing)
        # -------------------------------------------------------------
        print("\n--- Part B: Counter-service Unavailability & MQ Asynchronous Queuing ---")
        print("\n[Step 5] Baseline check: Verifying counter-service is UP...")
        await asyncio.sleep(2)
        async with session.get(f"{base_url}/user/{test_user}") as resp:
            resp.raise_for_status()
            data = await resp.json()
            print(f"  Baseline balance: {data.get('balance')} (counter_available: {data.get('counter_available')})")
            assert data.get("balance") == 60.0

        print("\n[Step 6] Making counter-service unavailable...")
        if mode == "k8s":
            run_cmd(["kubectl", "-n", "bank-system", "scale", "deployment", "counter-service", "--replicas=0"])
            print("  Waiting 4 seconds for Pod termination...")
            time.sleep(4)
        elif mode == "docker":
            run_cmd(["docker", "pause", "counter-service"])
            time.sleep(2)
        else:
            print("  (If testing manually, run: kubectl scale deployment counter-service --replicas=0 or docker pause counter-service)")

        print("\n[Step 7] Sending 3 transactions while counter-service is DOWN...")
        print("  Messages should queue up in Hazelcast Queue without throwing any HTTP errors!")
        amounts = [100, 200, -40]
        for amt in amounts:
            async with session.post(f"{base_url}/", json={"user_Id": test_user, "amount": amt}) as resp:
                res_json = await resp.json()
                print(f"  POST amount={amt:+4} -> HTTP {resp.status} (queued={res_json.get('queued')})")
                assert resp.status == 200, f"Expected 200 OK while MQ is up, got {resp.status}"

        print("\n[Step 8] Checking GET /user while counter-service is DOWN...")
        async with session.get(f"{base_url}/user/{test_user}") as resp:
            data = await resp.json()
            print(f"  GET response: balance = {data.get('balance')} (counter_available = {data.get('counter_available')})")
            assert data.get("balance") is None, "Balance must be null while counter-service is unavailable"
            assert data.get("counter_available") is False
            print("  => As required by Lab 5, GET returns null / unavailable while counter-service is down!")

        print("\n[Step 9] Restoring counter-service...")
        if mode == "k8s":
            run_cmd(["kubectl", "-n", "bank-system", "scale", "deployment", "counter-service", "--replicas=1"])
            print("  Waiting 10 seconds for Pod to start and drain queue...")
            time.sleep(10)
        elif mode == "docker":
            run_cmd(["docker", "unpause", "counter-service"])
            print("  Waiting 3 seconds for counter-service to consume accumulated messages...")
            time.sleep(3)
        else:
            print("  (If testing manually, restore counter-service and wait 5 seconds)")
            await asyncio.sleep(5)

        print("\n[Step 10] Verifying balances after counter-service resumed...")
        async with session.get(f"{base_url}/user/{test_user}") as resp:
            resp.raise_for_status()
            data = await resp.json()
            expected_balance = 60 + 100 + 200 - 40  # 320
            print(f"  Final balance in DB: {data.get('balance')} (Expected: {expected_balance})")
            assert data.get("balance") == expected_balance, f"Expected {expected_balance}, got {data.get('balance')}"
            print("  => SUCCESS: counter-service successfully drained the queue and applied all transactions!")

    print("\n" + "=" * 70)
    print(" ALL KUBERNETES FAULT TOLERANCE TESTS PASSED SUCCESSFULLY!")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Lab 5 K8s Fault Tolerance Test")
    parser.add_argument("--url", default="http://127.0.0.1:8000", help="facade-service URL")
    parser.add_argument("--mode", choices=["k8s", "docker", "manual"], default="manual", help="Execution mode")
    args = parser.parse_args()

    asyncio.run(run_k8s_fault_tolerance_test(args.url.rstrip("/"), mode=args.mode))
