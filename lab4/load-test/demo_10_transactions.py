import argparse
import asyncio
import json
import time
from collections import Counter
import aiohttp


async def run_10_transactions_demo(base_url: str):
    print("=" * 65)
    print(" Lab 4: 10 Transactions Demo with Messaging Queue & Config-Server")
    print("=" * 65)

    async with aiohttp.ClientSession() as session:
        print("\n[Step 1] Resetting system state...")
        try:
            async with session.post(f"{base_url}/reset") as resp:
                print("System reset response:", await resp.json())
        except Exception as e:
            print("Reset note:", e)

        transactions = [
            {"msg_id": "msg1",  "user_Id": "alice", "amount": 100},
            {"msg_id": "msg2",  "user_Id": "bob",   "amount": 250},
            {"msg_id": "msg3",  "user_Id": "alice", "amount": -40},
            {"msg_id": "msg4",  "user_Id": "carol", "amount": 500},
            {"msg_id": "msg5",  "user_Id": "bob",   "amount": -50},
            {"msg_id": "msg6",  "user_Id": "alice", "amount": 15},
            {"msg_id": "msg7",  "user_Id": "carol", "amount": -120},
            {"msg_id": "msg8",  "user_Id": "bob",   "amount": 30},
            {"msg_id": "msg9",  "user_Id": "alice", "amount": 25},
            {"msg_id": "msg10", "user_Id": "carol", "amount": -80},
        ]

        instance_counter = Counter()

        print("\n[Step 2] Sending 10 transactions through facade-service (Asynchronous via MQ):")
        print("-" * 65)
        for tx in transactions:
            payload = {"user_Id": tx["user_Id"], "amount": tx["amount"]}
            async with session.post(f"{base_url}/", json=payload) as resp:
                resp.raise_for_status()
                result = await resp.json()
                used_instance = result.get("logging_instance", "unknown")
                instance_counter[used_instance] += 1
                print(
                    f"[{tx['msg_id']}] POST user={tx['user_Id']:<6} amount={tx['amount']:+4} "
                    f"-> status={result.get('status')} queued={result.get('queued')} | "
                    f"tx_id={result.get('transaction_ID')} | handled by: {used_instance}"
                )

        print("\n[Step 3] Distribution among logging-service instances:")
        print("-" * 65)
        for inst, count in sorted(instance_counter.items()):
            print(f"  * {inst}: received {count} transaction(s)")

        # Allow consumer a moment to drain the queue
        print("\n[Step 4] Waiting 2 seconds for MQ Consumer to drain queue...")
        await asyncio.sleep(2.0)

        print("\n[Step 5] Checking account balances via GET /accounts:")
        print("-" * 65)
        async with session.get(f"{base_url}/accounts") as resp:
            resp.raise_for_status()
            balances_data = await resp.json()
            balances = balances_data.get("balances", {})
            print("Current account balances in counter-service:")
            for user, bal in sorted(balances.items()):
                print(f"  * {user}: balance = {bal}")

            # alice: 100 - 40 + 15 + 25 = 100
            # bob: 250 - 50 + 30 = 230
            # carol: 500 - 120 - 80 = 300
            assert balances.get("alice") == 100, f"Expected 100, got {balances.get('alice')}"
            assert balances.get("bob") == 230, f"Expected 230, got {balances.get('bob')}"
            assert balances.get("carol") == 300, f"Expected 300, got {balances.get('carol')}"
            print("  => Balances correctly verified in PostgreSQL!")

        print("\n[Step 6] Checking user transactions via GET /user/{user_Id}:")
        print("-" * 65)
        for user_id in ("alice", "bob", "carol"):
            async with session.get(f"{base_url}/user/{user_id}") as resp:
                resp.raise_for_status()
                u_info = await resp.json()
                print(
                    f"  * user '{user_id}': balance={u_info.get('balance')}, "
                    f"logged transactions={len(u_info.get('transactions', []))}"
                )

        print("\n[Step 7] Checking total transactions via GET /transactions:")
        print("-" * 65)
        async with session.get(f"{base_url}/transactions") as resp:
            resp.raise_for_status()
            all_txs = (await resp.json()).get("transactions", [])
            print(f"Total transactions stored in Hazelcast Distributed Map: {len(all_txs)}")
            assert len(all_txs) == 10, f"Expected 10, got {len(all_txs)}"
            print("  => All 10 transactions verified in Hazelcast Distributed Map!")

    print("\n" + "=" * 65)
    print(" ALL LAB 4 CHECKS PASSED SUCCESSFULLY!")
    print("=" * 65 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Lab 4 10 Transactions Demo")
    parser.add_argument("--url", default="http://127.0.0.1:8000", help="facade-service URL")
    args = parser.parse_args()

    asyncio.run(run_10_transactions_demo(args.url.rstrip("/")))
