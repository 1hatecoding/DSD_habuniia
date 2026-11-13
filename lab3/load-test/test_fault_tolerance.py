import argparse
import asyncio
import subprocess
import time
import aiohttp


def run_cmd(cmd: list[str]) -> str:
    print(f"Executing: {' '.join(cmd)}")
    res = subprocess.run(cmd, capture_output=True, text=True)
    return res.stdout.strip()


async def check_read_write(base_url: str, test_label: str) -> bool:
    print(f"\n--- Testing Read & Write: [{test_label}] ---")
    async with aiohttp.ClientSession() as session:
        # Write
        payload = {"user_Id": f"ft_user_{int(time.time())}", "amount": 77}
        try:
            async with session.post(f"{base_url}/", json=payload, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    print(f"  [FAIL] POST / failed with status {resp.status}: {text}")
                    return False
                post_data = await resp.json()
                tx_id = post_data["transaction_ID"]
                handled_by = post_data.get("logging_instance", "unknown")
                print(f"  [OK] POST succeeded! tx_id={tx_id}, balance={post_data['balance']}, handled by: {handled_by}")
        except Exception as e:
            print(f"  [FAIL] POST request failed: {e}")
            return False

        # Read
        try:
            async with session.get(f"{base_url}/user/{payload['user_Id']}", timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    print(f"  [FAIL] GET /user/{payload['user_Id']} failed with status {resp.status}: {text}")
                    return False
                get_data = await resp.json()
                print(f"  [OK] GET /user succeeded! balance={get_data['balance']}, transactions count={len(get_data['transactions'])}")
        except Exception as e:
            print(f"  [FAIL] GET request failed: {e}")
            return False

        # Read all transactions
        try:
            async with session.get(f"{base_url}/transactions", timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    print(f"  [FAIL] GET /transactions failed with status {resp.status}: {text}")
                    return False
                all_data = await resp.json()
                print(f"  [OK] GET /transactions succeeded! Total stored={len(all_data['transactions'])}")
        except Exception as e:
            print(f"  [FAIL] GET /transactions failed: {e}")
            return False

    return True


async def main():
    parser = argparse.ArgumentParser(description="Lab 3 Fault Tolerance Test")
    parser.add_argument("--url", default="http://127.0.0.1:8000", help="facade-service URL")
    parser.add_argument("--docker", action="store_true", help="Automatically execute docker stop/start commands")
    args = parser.parse_args()

    base_url = args.url.rstrip("/")

    print("=" * 65)
    print(" Lab 3 Fault Tolerance & Failover Verification")
    print("=" * 65)

    # 1. Normal state
    ok = await check_read_write(base_url, "Normal: All 3 logging instances & 3 Hazelcast nodes active")
    if not ok:
        print("Initial sanity check failed. Make sure services are running.")
        return

    if args.docker:
        # Test 1: Stop 1 logging-service instance
        print("\n>>> Scenario A: Stopping logging-service-1 (1 instance down)...")
        run_cmd(["docker", "stop", "logging-service-1"])
        time.sleep(2)
        await check_read_write(base_url, "1 logging-service down (logging-service-2 & 3 alive)")

        # Test 2: Stop 2nd logging-service instance
        print("\n>>> Scenario B: Stopping logging-service-2 (2 instances down)...")
        run_cmd(["docker", "stop", "logging-service-2"])
        time.sleep(2)
        await check_read_write(base_url, "2 logging-services down (only logging-service-3 alive)")

        # Restore logging services
        print("\n>>> Restoring logging-service-1 and logging-service-2...")
        run_cmd(["docker", "start", "logging-service-1", "logging-service-2"])
        time.sleep(3)

        # Test 3: Stop 1 Hazelcast node
        print("\n>>> Scenario C: Stopping hazelcast-1 (1 node down)...")
        run_cmd(["docker", "stop", "hazelcast-1"])
        time.sleep(2)
        await check_read_write(base_url, "1 Hazelcast node down (hazelcast-2 & 3 alive in cluster)")

        # Test 4: Stop 2nd Hazelcast node
        print("\n>>> Scenario D: Stopping hazelcast-2 (2 nodes down)...")
        run_cmd(["docker", "stop", "hazelcast-2"])
        time.sleep(2)
        await check_read_write(base_url, "2 Hazelcast nodes down (hazelcast-3 alive in cluster)")

        # Restore Hazelcast nodes
        print("\n>>> Restoring hazelcast-1 and hazelcast-2...")
        run_cmd(["docker", "start", "hazelcast-1", "hazelcast-2"])
        time.sleep(5)
        await check_read_write(base_url, "All services restored to normal operation")
    else:
        print("\n[Manual Testing Guide]")
        print("To test fault tolerance with Docker containers, execute:")
        print("  1. Disable 1 logging-service instance:")
        print("     docker stop logging-service-1")
        print("     python load-test/demo_10_transactions.py")
        print("     (Facade will randomly pick and automatically fail over to instance 2 or 3)")
        print("\n  2. Disable 2 logging-service instances:")
        print("     docker stop logging-service-2")
        print("     python load-test/demo_10_transactions.py")
        print("     (Facade will fail over to logging-service-3, all transactions succeed)")
        print("\n  3. Disable 1 or 2 Hazelcast nodes:")
        print("     docker stop hazelcast-1")
        print("     docker stop hazelcast-2")
        print("     python load-test/demo_10_transactions.py")
        print("     (Hazelcast cluster preserves data across partition backups and surviving nodes)")
        print("\nTo automate these container stop/start checks, run:")
        print("  python load-test/test_fault_tolerance.py --docker")

    print("\n" + "=" * 65)
    print(" Fault tolerance verification complete.")
    print("=" * 65)


if __name__ == "__main__":
    asyncio.run(main())

