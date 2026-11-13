import os
import subprocess
import sys
import time
import httpx

def wait_for_service(url: str, timeout: float = 10.0) -> bool:
    start = time.time()
    while time.time() - start < timeout:
        try:
            r = httpx.get(f"{url}/health", timeout=1.0)
            if r.status_code == 200:
                return True
        except Exception:
            pass
        time.sleep(0.3)
    return False

def main():
    print("=== Starting microservices integration test ===")
    env_base = os.environ.copy()

    # Processes list to terminate at exit
    procs = []
    try:
        # 1. Start counter-service (8002)
        print("[Launcher] Starting counter-service on port 8002...")
        c_env = env_base.copy()
        c_env["PORT"] = "8002"
        c_env["DATABASE_URL"] = "sqlite:///test_counter.db"
        c_proc = subprocess.Popen([sys.executable, "counter-service/app.py"], env=c_env)
        procs.append(("counter-service", c_proc))

        # 2. Start logging-service-1 (8001)
        print("[Launcher] Starting logging-service-1 on port 8001...")
        l1_env = env_base.copy()
        l1_env["PORT"] = "8001"
        l1_env["INSTANCE_NAME"] = "logging-service-1"
        l1_env["FALLBACK_DB_PATH"] = "test_shared_hz.db"
        l1_proc = subprocess.Popen([sys.executable, "logging-service/app.py"], env=l1_env)
        procs.append(("logging-service-1", l1_proc))

        # 3. Start logging-service-2 (8003)
        print("[Launcher] Starting logging-service-2 on port 8003...")
        l2_env = env_base.copy()
        l2_env["PORT"] = "8003"
        l2_env["INSTANCE_NAME"] = "logging-service-2"
        l2_env["FALLBACK_DB_PATH"] = "test_shared_hz.db"
        l2_proc = subprocess.Popen([sys.executable, "logging-service/app.py"], env=l2_env)
        procs.append(("logging-service-2", l2_proc))

        # 4. Start logging-service-3 (8004)
        print("[Launcher] Starting logging-service-3 on port 8004...")
        l3_env = env_base.copy()
        l3_env["PORT"] = "8004"
        l3_env["INSTANCE_NAME"] = "logging-service-3"
        l3_env["FALLBACK_DB_PATH"] = "test_shared_hz.db"
        l3_proc = subprocess.Popen([sys.executable, "logging-service/app.py"], env=l3_env)
        procs.append(("logging-service-3", l3_proc))

        # Wait for all downstream services to be healthy
        for name, port in [("counter-service", 8002), ("logging-1", 8001), ("logging-2", 8003), ("logging-3", 8004)]:
            url = f"http://127.0.0.1:{port}"
            if not wait_for_service(url, timeout=10.0):
                raise RuntimeError(f"Service {name} at {url} failed to start")
            print(f"  [OK] {name} is up at {url}")

        # 5. Start facade-service (8000)
        print("[Launcher] Starting facade-service on port 8000...")
        f_env = env_base.copy()
        f_env["PORT"] = "8000"
        f_env["LOGGING_SERVICE_URLS"] = "http://127.0.0.1:8001,http://127.0.0.1:8003,http://127.0.0.1:8004"
        f_env["COUNTER_SERVICE_URL"] = "http://127.0.0.1:8002"
        f_proc = subprocess.Popen([sys.executable, "facade-service/app.py"], env=f_env)
        procs.append(("facade-service", f_proc))

        if not wait_for_service("http://127.0.0.1:8000", timeout=10.0):
            raise RuntimeError("facade-service failed to start")
        print("  [OK] facade-service is up at http://127.0.0.1:8000")

        # 6. Run 10 Transactions Demo
        print("\n" + "#" * 60)
        print(" RUNNING 10 TRANSACTIONS DEMO")
        print("#" * 60)
        demo_res = subprocess.run([sys.executable, "load-test/demo_10_transactions.py", "--url", "http://127.0.0.1:8000"])
        if demo_res.returncode != 0:
            raise RuntimeError("demo_10_transactions.py failed")

        # 7. Run Fault Tolerance Failover Test
        print("\n" + "#" * 60)
        print(" RUNNING FAULT TOLERANCE FAILOVER TEST")
        print("#" * 60)

        # Test A: Terminate logging-service-1
        print(">>> Terminating logging-service-1...")
        l1_proc.terminate()
        l1_proc.wait()
        time.sleep(1)

        # Test write & read with 1 instance down
        r1 = httpx.post("http://127.0.0.1:8000/", json={"user_Id": "failover_user_1", "amount": 75})
        print(f"Write with 1 instance down -> status: {r1.status_code}, data: {r1.json()}")
        assert r1.status_code == 200, "Failover with 1 instance down failed"

        r1_get = httpx.get("http://127.0.0.1:8000/user/failover_user_1")
        print(f"Read with 1 instance down -> status: {r1_get.status_code}, data: {r1_get.json()}")
        assert r1_get.status_code == 200, "Read with 1 instance down failed"

        # Test B: Terminate logging-service-2 as well (2 instances down, only logging-service-3 alive)
        print("\n>>> Terminating logging-service-2 (only logging-service-3 alive)...")
        l2_proc.terminate()
        l2_proc.wait()
        time.sleep(1)

        r2 = httpx.post("http://127.0.0.1:8000/", json={"user_Id": "failover_user_2", "amount": 120})
        print(f"Write with 2 instances down -> status: {r2.status_code}, data: {r2.json()}")
        assert r2.status_code == 200, "Failover with 2 instances down failed"
        assert r2.json()["logging_instance"] == "http://127.0.0.1:8004", "Expected logging-service-3 to handle request"

        r2_get = httpx.get("http://127.0.0.1:8000/user/failover_user_2")
        print(f"Read with 2 instances down -> status: {r2_get.status_code}, data: {r2_get.json()}")
        assert r2_get.status_code == 200, "Read with 2 instances down failed"
        print("  => Fault tolerance failover checks PASSED!")

        # Restart logging-service-1 and logging-service-2 for benchmark
        print("\n>>> Restarting logging-service-1 and logging-service-2 for benchmark...")
        l1_proc = subprocess.Popen([sys.executable, "logging-service/app.py"], env=l1_env)
        procs[1] = ("logging-service-1", l1_proc)
        l2_proc = subprocess.Popen([sys.executable, "logging-service/app.py"], env=l2_env)
        procs[2] = ("logging-service-2", l2_proc)
        wait_for_service("http://127.0.0.1:8001")
        wait_for_service("http://127.0.0.1:8003")

        # 8. Run Benchmark
        print("\n" + "#" * 60)
        print(" RUNNING BENCHMARK (1,000 requests per client)")
        print("#" * 60)
        bench_res = subprocess.run([
            sys.executable,
            "load-test/benchmark.py",
            "--url", "http://127.0.0.1:8000",
            "--clients", "10",
            "--requests", "1000",
            "--skip-basic"
        ])
        if bench_res.returncode != 0:
            raise RuntimeError("benchmark.py failed")

        print("\n" + "=" * 60)
        print(" ALL INTEGRATION TESTS PASSED SUCCESSFULLY!")
        print("=" * 60)

    finally:
        print("\n[Cleanup] Shutting down services...")
        for name, proc in procs:
            try:
                proc.terminate()
                proc.wait(timeout=2)
            except Exception:
                proc.kill()
        # Clean up test databases
        for f in ["test_counter.db", "test_shared_hz.db"]:
            if os.path.exists(f):
                try:
                    os.remove(f)
                except Exception:
                    pass
        print("[Cleanup] Done.")

if __name__ == "__main__":
    main()

