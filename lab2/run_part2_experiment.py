"""Part II orchestrator: launches 3 counter services with interactive screenshot pauses."""

import argparse
import os
import subprocess
import sys
import threading
import time
from datetime import datetime
from typing import Dict, List, Optional

import hazelcast

CLUSTER_NAME = "lab2-cluster"
CLUSTER_MEMBERS = ["127.0.0.1:5701", "127.0.0.1:5702", "127.0.0.1:5703"]
TARGET = 100_000
CHECKPOINT_1 = 30_000
CHECKPOINT_2 = 65_000


def ts() -> str:
    """Return formatted local timestamp with milliseconds."""
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


def pause(prompt_text: str, enabled: bool = True) -> None:
    """Pause execution and wait for user to press Enter."""
    if not enabled:
        return
    print("\n" + "=" * 70, flush=True)
    print(prompt_text.strip(), flush=True)
    input(">>> Натисніть [ENTER] для продовження... ")
    print("=" * 70 + "\n", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Hazelcast Part II Distributed Lock Demonstration")
    parser.add_argument(
        "--no-pause",
        action="store_true",
        help="Run without interactive Enter pauses",
    )
    args = parser.parse_args()
    interactive = not args.no_pause

    os.makedirs("logs", exist_ok=True)
    logs: List[str] = []
    lock = threading.Lock()
    state: Dict[str, Optional[str]] = {"holder": None}

    def record(line: str) -> None:
        with lock:
            print(line, flush=True)
            logs.append(line)

    print("\n[*] Підключення до кластера Hazelcast для ініціалізації стану...", flush=True)
    monitor = hazelcast.HazelcastClient(
        cluster_name=CLUSTER_NAME,
        cluster_members=CLUSTER_MEMBERS,
        smart_routing=False,
    )
    counter_map = monitor.get_map("counter-map").blocking()
    iter_atomic = monitor.cp_subsystem.get_atomic_long("iteration").blocking()
    max_allowed = monitor.cp_subsystem.get_atomic_long("max_allowed").blocking()

    counter_map.set("counter", 0)
    iter_atomic.set(0)
    max_allowed.set(CHECKPOINT_1 if interactive else TARGET)
    record(
        f"[{ts()}] [orchestrator] Ініціалізовано: counter-map['counter']=0, "
        f"AtomicLong('iteration')=0, checkpoint={max_allowed.get()}, target={TARGET}"
    )

    service_ids = ["service-1", "service-2", "service-3"]
    procs: Dict[str, subprocess.Popen] = {}

    def stream_reader(sid: str, proc: subprocess.Popen) -> None:
        assert proc.stdout is not None
        for raw in iter(proc.stdout.readline, ""):
            line = raw.rstrip("\r\n")
            if line:
                record(line)
                if "ЗАХОПЛЕНО FencedLock" in line:
                    with lock:
                        state["holder"] = sid

    threads: List[threading.Thread] = []
    try:
        record(f"[{ts()}] [orchestrator] Запуск 3-х паралельних процесів-сервісів: {service_ids}")
        for sid in service_ids:
            cmd = [
                sys.executable,
                "-u",
                "part2_counter_service.py",
                "--service-id",
                sid,
                "--target",
                str(TARGET),
                "--log-every",
                "5000",
            ]
            if interactive:
                cmd.append("--use-checkpoints")
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
            procs[sid] = proc
            t = threading.Thread(target=stream_reader, args=(sid, proc), daemon=True)
            t.start()
            threads.append(t)

        # -------------------------------------------------------------
        # STAGE 1: First holder counts to CHECKPOINT_1 (30 000)
        # -------------------------------------------------------------
        killed: List[str] = []
        while True:
            with lock:
                holder1 = state["holder"]
            cur_iter = iter_atomic.get()
            if holder1 and cur_iter >= (CHECKPOINT_1 if interactive else CHECKPOINT_1):
                break
            time.sleep(0.25)

        time.sleep(1.0)
        pause(
            f""">>> [ПАУЗА ДЛЯ СКРІНШОТА 6]
>>> Запуск 3-х паралельних інстансів сервісу-каунтера.
>>> Сервіс [{holder1}] першим захопив FencedLock і почав підрахунок.
>>> Інші два сервіси перебувають у стані очікування блокування.
>>> Поточне значення досягло відмітки: {cur_iter} ітерацій.
>>>
>>> ДІЯ: Зробіть Скріншот 6 (вивід у терміналі та/або Management Center CP Subsystem).
>>> Після натискання [ENTER] перший сервіс ({holder1}) буде аварійно вбито (kill -9).""",
            interactive,
        )

        # Crash first service
        if interactive:
            max_allowed.set(CHECKPOINT_2)
        kill_time1 = time.time()
        procs[holder1].kill()
        procs[holder1].wait()
        killed.append(holder1)
        record(
            f"[{ts()}] [orchestrator] [!] АВАРІЙНО ЗУПИНЕНО {holder1} (1-й crash, kill -9) "
            f"на iteration={iter_atomic.get()}, counter={counter_map.get('counter')}"
        )
        record(f"[{ts()}] [orchestrator] Очікування автоматичного зняття FencedLock після таймауту CP-сесії (~10 сек)...")

        # -------------------------------------------------------------
        # STAGE 2: Second holder takes over and counts to CHECKPOINT_2 (65 000)
        # -------------------------------------------------------------
        holder2 = None
        while True:
            with lock:
                current = state["holder"]
            if current and current not in killed:
                holder2 = current
                failover1_duration = time.time() - kill_time1
                record(
                    f"[{ts()}] [orchestrator] [✓] Перехоплення 1 успішне! "
                    f"Затрачено {failover1_duration:.2f}с -> FencedLock захоплено сервісом [{holder2}]"
                )
                break
            time.sleep(0.25)

        while True:
            cur_iter = iter_atomic.get()
            if cur_iter >= (CHECKPOINT_2 if interactive else CHECKPOINT_2):
                break
            time.sleep(0.25)

        time.sleep(1.0)
        record(
            f"[{ts()}] [orchestrator] Сервіс [{holder2}] досяг {cur_iter} ітерацій. "
            f"Готовність до аварійної зупинки другого сервісу."
        )

        # Crash second service
        if interactive:
            max_allowed.set(TARGET)
        kill_time2 = time.time()
        procs[holder2].kill()
        procs[holder2].wait()
        killed.append(holder2)
        record(
            f"[{ts()}] [orchestrator] [!] АВАРІЙНО ЗУПИНЕНО {holder2} (2-й crash, kill -9) "
            f"на iteration={iter_atomic.get()}, counter={counter_map.get('counter')}"
        )
        record(f"[{ts()}] [orchestrator] Очікування перехоплення FencedLock третім (останнім) сервісом...")

        # -------------------------------------------------------------
        # STAGE 3: Third holder takes over
        # -------------------------------------------------------------
        last_sid = [s for s in service_ids if s not in killed][0]
        while True:
            with lock:
                current = state["holder"]
            if current == last_sid:
                failover2_duration = time.time() - kill_time2
                saved_pos = iter_atomic.get()
                record(
                    f"[{ts()}] [orchestrator] [✓] Перехоплення 2 успішне! "
                    f"Затрачено {failover2_duration:.2f}с -> Останній сервіс [{last_sid}] захопив лок "
                    f"та продовжує зі збереженої позиції {saved_pos}!"
                )
                break
            time.sleep(0.25)

        pause(
            f""">>> [ПАУЗА ДЛЯ СКРІНШОТА 7]
>>> Аварійна зупинка першого ({holder1}), а потім другого сервісу ({holder2}).
>>> Блокування FencedLock було автоматично знято Hazelcast CP Subsystem.
>>> Третій сервіс [{last_sid}] успішно перехопив лок і продовжує роботу
>>> зі збереженого значення IAtomicLong('iteration'={saved_pos})!
>>>
>>> ДІЯ: Зробіть Скріншот 7 (вивід терміналу з моментами падіння та перехоплення).
>>> Після натискання [ENTER] третій сервіс продовжить інкремент до фінальних 100 000.""",
            interactive,
        )

        # Wait for the last service to complete up to TARGET
        exit_code = procs[last_sid].wait()
        for t in threads:
            t.join(timeout=2.0)

        final_counter = counter_map.get("counter")
        final_iter = iter_atomic.get()
        record(
            f"[{ts()}] [orchestrator] [✓] Експеримент завершено! {last_sid} вийшов з кодом {exit_code}. "
            f"Фінальний counter-map['counter']={final_counter}, AtomicLong('iteration')={final_iter}"
        )
        assert final_counter == TARGET, f"Очікувалось counter={TARGET}, отримано {final_counter}"
        assert final_iter == TARGET, f"Очікувалось iteration={TARGET}, отримано {final_iter}"

        # -------------------------------------------------------------
        # STAGE 4: Final result (100 000)
        # -------------------------------------------------------------
        pause(
            f""">>> [ПАУЗА ДЛЯ СКРІНШОТА 8]
>>> Завершення роботи!
>>> Фінальне значення каунтера в Hazelcast Map ('counter') = {final_counter}.
>>> Фінальне значення в IAtomicLong ('iteration') = {final_iter}.
>>> Лічильник коректно дійшов рівно до 100 000 без втрати даних та race condition!
>>>
>>> ДІЯ: Зробіть Скріншот 8 (фінальний результат 100 000 у терміналі або Management Center).
>>> Натисніть [ENTER] для завершення роботи скрипта.""",
            interactive,
        )

    finally:
        for proc in procs.values():
            if proc.poll() is None:
                proc.kill()
        monitor.shutdown()

    out_path = os.path.join("logs", "part2_results.txt")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(logs) + "\n")
    print(f"\n[✓] Повний протокол результатів Частини II збережено у {out_path}", flush=True)


if __name__ == "__main__":
    main()
