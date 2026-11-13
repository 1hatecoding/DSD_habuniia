import argparse
import asyncio
import json
from collections import Counter
import aiohttp


async def run_10_transactions_demo(base_url: str):
    print("=" * 60)
    print(" Lab 3: 10 Transactions Demo (msg1 - msg10)")
    print("=" * 60)

    async with aiohttp.ClientSession() as session:
        # 1. Reset state
        print("\n[Step 1] Resetting system state...")
        async with session.post(f"{base_url}/reset") as resp:
            resp.raise_for_status()
            print("System reset successful:", await resp.json())

        # 2. Define 10 distinct transactions (msg1 - msg10)
        transactions = [
            {"msg_id": "msg1",  "user_Id": "client_A", "amount": 100},
            {"msg_id": "msg2",  "user_Id": "client_B", "amount": 250},
            {"msg_id": "msg3",  "user_Id": "client_A", "amount": -40},
            {"msg_id": "msg4",  "user_Id": "client_C", "amount": 500},
            {"msg_id": "msg5",  "user_Id": "client_B", "amount": -50},
            {"msg_id": "msg6",  "user_Id": "client_A", "amount": 15},
            {"msg_id": "msg7",  "user_Id": "client_C", "amount": -120},
            {"msg_id": "msg8",  "user_Id": "client_B", "amount": 30},
            {"msg_id": "msg9",  "user_Id": "client_A", "amount": 25},
            {"msg_id": "msg10", "user_Id": "client_C", "amount": -80},
        ]

        instance_counter = Counter()

        # 3. Send 10 POST transactions through facade-service
        print("\n[Step 2] Sending 10 transactions through facade-service:")
        print("-" * 60)
        for tx in transactions:
            payload = {"user_Id": tx["user_Id"], "amount": tx["amount"]}
            async with session.post(f"{base_url}/", json=payload) as resp:
                resp.raise_for_status()
                result = await resp.json()
                used_instance = result.get("logging_instance", "unknown")
                instance_counter[used_instance] += 1
                print(
                    f"[{tx['msg_id']}] POST user={tx['user_Id']:<8} amount={tx['amount']:+4} "
                    f"-> balance={result['balance']:<4} | tx_id={result['transaction_ID']} "
                    f"| handled by: {used_instance}"
                )

        # 4. Summary of random distribution
        print("\n[Step 3] Distribution of transactions among logging-service instances:")
        print("-" * 60)
        for inst, count in sorted(instance_counter.items()):
            print(f"  * {inst}: received {count} transaction(s)")

        # 5. Verify counter-service balances via GET /accounts
        print("\n[Step 4] Checking account balances via GET /accounts:")
        print("-" * 60)
        async with session.get(f"{base_url}/accounts") as resp:
            resp.raise_for_status()
            balances_data = await resp.json()
            balances = balances_data.get("balances", {})
            print("Current account balances in counter-service:")
            for user, bal in sorted(balances.items()):
                print(f"  * {user}: balance = {bal}")

            # Verification of expected balances
            # client_A: 100 - 40 + 15 + 25 = 100
            # client_B: 250 - 50 + 30 = 230
            # client_C: 500 - 120 - 80 = 300
            assert balances.get("client_A") == 100, f"Expected 100, got {balances.get('client_A')}"
            assert balances.get("client_B") == 230, f"Expected 230, got {balances.get('client_B')}"
            assert balances.get("client_C") == 300, f"Expected 300, got {balances.get('client_C')}"
            print("  => Balance checks PASSED!")

        # 6. Verify logging-service transactions via GET /user/{user_Id}
        print("\n[Step 5] Checking user transactions via GET /user/{user_Id}:")
        print("-" * 60)
        for user_id in ("client_A", "client_B", "client_C"):
            async with session.get(f"{base_url}/user/{user_id}") as resp:
                resp.raise_for_status()
                user_info = await resp.json()
                tx_list = user_info.get("transactions", [])
                handled_by = user_info.get("logging_instance", "unknown")
                print(
                    f"  * user '{user_id}': balance={user_info.get('balance')}, "
                    f"transactions_count={len(tx_list)} (served by {handled_by})"
                )

        # 7. Verify all transactions via GET /transactions
        print("\n[Step 6] Checking all transactions via GET /transactions:")
        print("-" * 60)
        async with session.get(f"{base_url}/transactions") as resp:
            resp.raise_for_status()
            all_txs_data = await resp.json()
            all_txs = all_txs_data.get("transactions", [])
            print(f"Total transactions stored in Hazelcast Distributed Map: {len(all_txs)}")
            assert len(all_txs) == 10, f"Expected 10 transactions, found {len(all_txs)}"
            print("  => All 10 transactions verified in Hazelcast Distributed Map!")

    print("\n" + "=" * 60)
    print(" ALL 10 TRANSACTIONS DEMO CHECKS PASSED SUCCESSFULLY!")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Lab 3 - 10 Transactions Demo")
    parser.add_argument("--url", default="http://127.0.0.1:8000", help="facade-service base URL")
    args = parser.parse_args()

    asyncio.run(run_10_transactions_demo(args.url.rstrip("/")))

