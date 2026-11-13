"""Part II: Distributed counter service using Hazelcast FencedLock, AtomicLong, and IMap."""

import argparse
import sys
import time
from datetime import datetime

import hazelcast

CLUSTER_NAME = "lab2-cluster"
CLUSTER_MEMBERS = ["127.0.0.1:5701", "127.0.0.1:5702", "127.0.0.1:5703"]
TARGET_DEFAULT = 100_000


def ts() -> str:
    """Return formatted local timestamp with milliseconds."""
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


def log(service_id: str, message: str) -> None:
    """Print a timestamped, line-buffered log message."""
    print(f"[{ts()}] [{service_id}] {message}", flush=True)


def run_service(
    service_id: str,
    target: int,
    sleep_sec: float,
    reset: bool,
    log_every: int,
    use_checkpoints: bool,
) -> None:
    """Connect to Hazelcast, acquire FencedLock, and increment counter up to target."""
    log(service_id, "Запуск сервісу та підключення до кластера Hazelcast...")
    client = hazelcast.HazelcastClient(
        cluster_name=CLUSTER_NAME,
        cluster_members=CLUSTER_MEMBERS,
        smart_routing=False,
    )

    try:
        counter_map = client.get_map("counter-map")
        lock = client.cp_subsystem.get_lock("lock").blocking()
        iteration = client.cp_subsystem.get_atomic_long("iteration")
        max_allowed_atomic = (
            client.cp_subsystem.get_atomic_long("max_allowed") if use_checkpoints else None
        )

        if reset:
            counter_map.set("counter", 0).result()
            iteration.set(0).result()
            if max_allowed_atomic:
                max_allowed_atomic.set(target).result()
            log(service_id, "Скинуто counter-map['counter']=0 та AtomicLong('iteration')=0")

        log(service_id, "Очікування захоплення FencedLock('lock')...")
        fence = lock.lock()
        try:
            start_iter = iteration.get().result()
            counter_val = counter_map.get("counter").result()
            if counter_val is None:
                counter_val = 0

            # Guard against a crash between the two concurrent updates in a prior owner
            if counter_val > start_iter:
                start_iter = counter_val
                iteration.set(start_iter).result()
            elif counter_val < start_iter:
                counter_val = start_iter
                counter_map.set("counter", counter_val).result()

            log(
                service_id,
                f"ЗАХОПЛЕНО FencedLock (fencing_token={fence}). "
                f"Продовження з iteration={start_iter}, counter={counter_val}, ціль={target}",
            )

            for i in range(start_iter, target):
                # Check staged checkpoint if enabled
                if max_allowed_atomic:
                    allowed = max_allowed_atomic.get().result()
                    if i >= allowed and allowed < target:
                        log(
                            service_id,
                            f"Досягнуто проміжну точку: iteration={i}. "
                            f"Утримання FencedLock для скріншота...",
                        )
                        while True:
                            time.sleep(0.3)
                            new_allowed = max_allowed_atomic.get().result()
                            if new_allowed > allowed or new_allowed >= target:
                                break

                counter_val += 1
                f_map = counter_map.set("counter", counter_val)
                f_iter = iteration.set(i + 1)
                f_map.result()
                f_iter.result()

                if (i + 1) % log_every == 0 or (i + 1) == target:
                    log(service_id, f"Прогрес: iteration={i + 1}, counter={counter_val}")

                if sleep_sec > 0:
                    time.sleep(sleep_sec)

            final_counter = counter_map.get("counter").result()
            final_iter = iteration.get().result()
            log(
                service_id,
                f"Успішно завершено! Фінальний counter={final_counter}, iteration={final_iter}",
            )
        finally:
            try:
                lock.unlock()
                log(service_id, "Звільнено FencedLock('lock').")
            except Exception as exc:  # noqa: BLE001
                log(service_id, f"Примітка unlock: {exc}")
    finally:
        client.shutdown()
        log(service_id, "З'єднання закрито.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Hazelcast Distributed Lock Counter Service")
    parser.add_argument("--service-id", required=True, help="Unique name for this service instance")
    parser.add_argument("--target", type=int, default=TARGET_DEFAULT, help="Target counter value")
    parser.add_argument("--sleep", type=float, default=0.0, help="Optional sleep per iteration (seconds)")
    parser.add_argument("--reset", action="store_true", help="Reset counter and iteration before running")
    parser.add_argument("--log-every", type=int, default=5000, help="Log progress every N iterations")
    parser.add_argument(
        "--use-checkpoints",
        action="store_true",
        help="Respect CP AtomicLong('max_allowed') for staged screenshot demonstrations",
    )
    args = parser.parse_args()

    try:
        run_service(
            service_id=args.service_id,
            target=args.target,
            sleep_sec=args.sleep,
            reset=args.reset,
            log_every=args.log_every,
            use_checkpoints=args.use_checkpoints,
        )
    except KeyboardInterrupt:
        log(args.service_id, "Перервано користувачем.")
        sys.exit(1)


if __name__ == "__main__":
    main()
