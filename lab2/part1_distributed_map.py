"""Part I: Distributed Map experiments on a 3-node Hazelcast 5.4.0 cluster."""

import argparse
import os
import subprocess
import time
from collections import Counter
from typing import Dict, List

import hazelcast
import requests

CLUSTER_NAME = "lab2-cluster"
CLUSTER_MEMBERS = ["127.0.0.1:5701", "127.0.0.1:5702", "127.0.0.1:5703"]
REST_ENDPOINTS = [
    "http://127.0.0.1:5701/hazelcast/health",
    "http://127.0.0.1:5702/hazelcast/health",
    "http://127.0.0.1:5703/hazelcast/health",
]
NUM_KEYS = 10_000


def create_client() -> hazelcast.HazelcastClient:
    """Create a Hazelcast Python client connected to the local cluster."""
    return hazelcast.HazelcastClient(
        cluster_name=CLUSTER_NAME,
        cluster_members=CLUSTER_MEMBERS,
        smart_routing=False,
    )


def run_cmd(cmd: List[str]) -> str:
    """Run a shell command and return stripped stdout."""
    res = subprocess.run(cmd, check=True, capture_output=True, text=True)
    return res.stdout.strip()


def wait_for_cluster_health(expected_size: int, timeout_s: float = 90.0) -> Dict:
    """Wait until an active node reports expected cluster size and zero pending migrations."""
    deadline = time.time() + timeout_s
    last_err = None
    while time.time() < deadline:
        for url in REST_ENDPOINTS:
            try:
                resp = requests.get(url, timeout=2.0)
                if resp.status_code == 200:
                    data = resp.json()
                    if (
                        data.get("clusterSize") == expected_size
                        and data.get("clusterSafe") is True
                        and data.get("migrationQueueSize", -1) == 0
                    ):
                        return data
            except Exception as exc:  # noqa: BLE001
                last_err = exc
        time.sleep(1.0)
    raise TimeoutError(
        f"Cluster did not reach size={expected_size}, clusterSafe=True within {timeout_s}s "
        f"(last_err={last_err})"
    )


def wait_for_partition_table(client: hazelcast.HazelcastClient, timeout_s: float = 30.0) -> None:
    """Wait until client partition table maps every partition to a live cluster member."""
    ps = client.partition_service
    p_count = ps.get_partition_count()
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        live_uuids = {m.uuid for m in client.cluster_service.get_members()}
        owners = [ps.get_partition_owner(pid) for pid in range(p_count)]
        if live_uuids and all(owner in live_uuids for owner in owners):
            return
        time.sleep(0.3)


def populate_map(client: hazelcast.HazelcastClient, map_name: str, count: int = NUM_KEYS) -> None:
    """Clear and populate a distributed map with keys 0..count-1."""
    hz_map = client.get_map(map_name).blocking()
    hz_map.clear()
    batch_size = 1000
    for start in range(0, count, batch_size):
        batch = {i: f"value-{i}" for i in range(start, min(start + batch_size, count))}
        hz_map.put_all(batch)


def collect_distribution(client: hazelcast.HazelcastClient, map_name: str) -> Dict:
    """Collect exact key and partition distribution per active cluster node."""
    hz_map = client.get_map(map_name).blocking()
    size = hz_map.size()
    wait_for_partition_table(client)

    ps = client.partition_service
    members = {m.uuid: str(m.address) for m in client.cluster_service.get_members()}

    partition_counts = Counter()
    for pid in range(ps.get_partition_count()):
        owner = ps.get_partition_owner(pid)
        partition_counts[members.get(owner, f"unknown-{owner}")] += 1

    keys = list(hz_map.key_set())
    key_counts = Counter()
    for k in keys:
        pid = ps.get_partition_id(k)
        owner = ps.get_partition_owner(pid)
        key_counts[members.get(owner, f"unknown-{owner}")] += 1

    lost = NUM_KEYS - len(keys)
    return {
        "map_name": map_name,
        "cluster_size": len(members),
        "members": sorted(members.values()),
        "map_size": size,
        "actual_keys": len(keys),
        "lost_keys": lost,
        "partition_counts": dict(sorted(partition_counts.items())),
        "key_counts": dict(sorted(key_counts.items())),
    }


def format_stats(title: str, stats: Dict) -> str:
    """Format distribution statistics as a readable protocol block in Ukrainian."""
    lines = [
        f"=== {title} ===",
        f"Мапа: {stats['map_name']} | Активні ноди: {stats['cluster_size']} | "
        f"Розмір мапи: {stats['map_size']} / {NUM_KEYS} | Втрачені ключі: {stats['lost_keys']}",
    ]
    for member in stats["members"]:
        host_port = member.replace("Address(host=", "").replace(", port=", ":").replace(")", "")
        p_cnt = stats["partition_counts"].get(member, 0)
        k_cnt = stats["key_counts"].get(member, 0)
        pct = (k_cnt / NUM_KEYS * 100.0) if NUM_KEYS else 0.0
        lines.append(f"  - Вузол {host_port}: {p_cnt} партицій, {k_cnt} ключів ({pct:.2f}%)")
    return "\n".join(lines)


def nuke_and_boot_cluster() -> None:
    """Nuke all containers/networks/volumes and boot a completely fresh cluster."""
    print("=" * 70, flush=True)
    print("[!] ОЧИЩЕННЯ ТА ПОВНИЙ ПЕРЕЗАПУСК ХАЗЕЛКАСТ (NUKING CLUSTER)...", flush=True)
    print("    Виконується docker compose down -v --remove-orphans ...", flush=True)
    run_cmd(["docker", "compose", "down", "-v", "--remove-orphans"])
    print("[+] Запуск чистого кластера Hazelcast (docker compose up -d)...", flush=True)
    run_cmd(["docker", "compose", "up", "-d"])
    print("[*] Очікування старту та синхронізації 3 нод кластера...", flush=True)
    wait_for_cluster_health(expected_size=3, timeout_s=120.0)
    time.sleep(3.0)
    print("[✓] Кластер успішно запущено з нуля у стані 'just-booted'!", flush=True)
    print("=" * 70 + "\n", flush=True)


def restart_full_cluster() -> None:
    """Recreate all Hazelcast nodes cleanly so CP Subsystem and cluster state are fresh."""
    run_cmd(["docker", "compose", "restart", "hazelcast-node-1", "hazelcast-node-2", "hazelcast-node-3"])
    wait_for_cluster_health(expected_size=3, timeout_s=120.0)
    time.sleep(3.0)


def pause_for_screenshot(stage_desc: str, enabled: bool = True) -> None:
    """Prompt user to press Enter so they have time to screenshot Management Center."""
    if not enabled:
        return
    print("\n" + "-" * 70, flush=True)
    print(f">>> [ПАУЗА ДЛЯ СКРІНШОТА] ЕТАП: {stage_desc}", flush=True)
    print(">>> Відкрийте Management Center у браузері: http://localhost:8080", flush=True)
    input(">>> Зробіть скріншот і натисніть [ENTER] для переходу до наступного кроку... ")
    print("-" * 70 + "\n", flush=True)


def run_experiments(interactive: bool = True) -> None:
    """Execute all Part I scenarios and save results to logs/part1_results.txt."""
    os.makedirs("logs", exist_ok=True)
    report_blocks: List[str] = []

    def log_block(text: str) -> None:
        print(text, flush=True)
        report_blocks.append(text)

    # 0. Nuke and restart cluster into fresh just-booted condition
    nuke_and_boot_cluster()
    pause_for_screenshot("0. Кластер щойно запущено з нуля (порожній стан перед записом даних)", interactive)

    # Scenario 1: 3 nodes active, backup-count=1
    client = create_client()
    try:
        print("[*] Запис 10 000 значень (ключі 0..9999) у distributed-map...", flush=True)
        populate_map(client, "distributed-map", NUM_KEYS)
        stats_3 = collect_distribution(client, "distributed-map")
        log_block(format_stats("1. Початковий стан (3 ноди, distributed-map, backup-count=1)", stats_3))
        pause_for_screenshot("1. Початковий стан (3 ноди активні, 10 000 ключів розподілено по нодах)", interactive)

        # Scenario 2: Stop 1 node gracefully
        print("\n[*] Зупинка однієї ноди (hazelcast-node-3) у штатному режимі (docker stop)...", flush=True)
        run_cmd(["docker", "stop", "hazelcast-node-3"])
        wait_for_cluster_health(expected_size=2, timeout_s=60.0)
        time.sleep(2.0)
        stats_2 = collect_distribution(client, "distributed-map")
        log_block(format_stats("2. Після штатного відключення 1 ноди (hazelcast-node-3)", stats_2))
        pause_for_screenshot("2. Штатно відключено 1 ноду (hazelcast-node-3), дані мігровано на 2 ноди без втрат", interactive)

        # Scenario 3: Sequentially stop 2nd node gracefully
        print("\n[*] Послідовна зупинка 2-ї ноди (hazelcast-node-2) у штатному режимі...", flush=True)
        run_cmd(["docker", "stop", "hazelcast-node-2"])
        wait_for_cluster_health(expected_size=1, timeout_s=60.0)
        time.sleep(2.0)
        stats_1 = collect_distribution(client, "distributed-map")
        log_block(format_stats("3. Після послідовного штатного відключення 2-ї ноди (hazelcast-node-2)", stats_1))
        pause_for_screenshot("3. Послідовно штатно відключено 2-гу ноду, всі 10 000 ключів збережено на 1 ноді", interactive)
    finally:
        client.shutdown()

    # Restore 3 nodes for simultaneous crash test (backup-count=1)
    print("\n[*] Відновлення зупинених нод перед тестом аварійного падіння...", flush=True)
    run_cmd(["docker", "start", "hazelcast-node-2", "hazelcast-node-3"])
    wait_for_cluster_health(expected_size=3, timeout_s=90.0)
    time.sleep(3.0)

    client = create_client()
    try:
        print("[*] Перезапис 10 000 ключів у distributed-map...", flush=True)
        populate_map(client, "distributed-map", NUM_KEYS)
        stats_before_kill = collect_distribution(client, "distributed-map")
        log_block(format_stats("4a. Стан перед одночасним аварійним відключенням (kill -9) (3 ноди, backup-count=1)", stats_before_kill))
        pause_for_screenshot("4a. 3 активні ноди з distributed-map перед одночасним аварійним падінням (kill -9)", interactive)

        print("\n[!] ОДНОЧАСНЕ АВАРІЙНЕ ПАДІННЯ 2 НОД (docker kill -s KILL hazelcast-node-2 hazelcast-node-3)...", flush=True)
        run_cmd(["docker", "kill", "-s", "KILL", "hazelcast-node-2", "hazelcast-node-3"])
        wait_for_cluster_health(expected_size=1, timeout_s=60.0)
        time.sleep(2.0)
        stats_after_kill = collect_distribution(client, "distributed-map")
        log_block(format_stats("4b. Після одночасного аварійного падіння 2 нод (kill -9) (backup-count=1)", stats_after_kill))
        pause_for_screenshot("4b. Після одночасного kill -9 двох нод (backup-count=1, втрачено ~33% ключів)", interactive)
    finally:
        client.shutdown()

    # Restore 3 nodes for zero-data-loss test with backup-count=2
    print("\n[*] Запуск 3 нод для тесту safe-map (backup-count=2)...", flush=True)
    run_cmd(["docker", "start", "hazelcast-node-2", "hazelcast-node-3"])
    wait_for_cluster_health(expected_size=3, timeout_s=90.0)
    time.sleep(3.0)

    client = create_client()
    try:
        print("[*] Запис 10 000 значень у safe-map (backup-count=2)...", flush=True)
        populate_map(client, "safe-map", NUM_KEYS)
        stats_safe_before = collect_distribution(client, "safe-map")
        log_block(format_stats("5a. Стан перед одночасним аварійним відключенням (kill -9) (3 ноди, safe-map, backup-count=2)", stats_safe_before))
        pause_for_screenshot("5a. 3 активні ноди з safe-map (backup-count=2) перед одночасним аварійним падінням", interactive)

        print("\n[!] ОДНОЧАСНЕ АВАРІЙНЕ ПАДІННЯ 2 НОД ДЛЯ SAFE-MAP (kill -9)...", flush=True)
        run_cmd(["docker", "kill", "-s", "KILL", "hazelcast-node-2", "hazelcast-node-3"])
        wait_for_cluster_health(expected_size=1, timeout_s=60.0)
        time.sleep(2.0)
        stats_safe_after = collect_distribution(client, "safe-map")
        log_block(format_stats("5b. Після одночасного аварійного падіння 2 нод (kill -9) (safe-map, backup-count=2)", stats_safe_after))
        pause_for_screenshot("5b. Після одночасного kill -9 двох нод з backup-count=2 (0% втрат, 10 000 ключів збережено)", interactive)
    finally:
        client.shutdown()

    # Clean restart of all 3 nodes so CP Subsystem is fresh for Part II
    print("\n[*] Відновлення та чистий перезапуск усіх 3 нод кластера...", flush=True)
    run_cmd(["docker", "start", "hazelcast-node-2", "hazelcast-node-3"])
    restart_full_cluster()
    pause_for_screenshot("Фінал. Всі 3 ноди відновлені та активні для виконання Частини II", interactive)

    out_path = os.path.join("logs", "part1_results.txt")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n\n".join(report_blocks) + "\n")
    print(f"\n[✓] Протокол результатів Частини I збережено у {out_path}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Hazelcast Part I: Distributed Map")
    parser.add_argument(
        "action",
        choices=["run-experiments", "populate", "stats", "nuke"],
        default="run-experiments",
        nargs="?",
        help="Action to perform",
    )
    parser.add_argument("--map", default="distributed-map", help="Target map name")
    parser.add_argument(
        "--no-pause",
        action="store_true",
        help="Run without pausing for Enter (useful for automated execution)",
    )
    args = parser.parse_args()

    if args.action == "run-experiments":
        run_experiments(interactive=not args.no_pause)
    elif args.action == "nuke":
        nuke_and_boot_cluster()
    elif args.action == "populate":
        client = create_client()
        try:
            populate_map(client, args.map, NUM_KEYS)
            print(format_stats(f"Записано дані у {args.map}", collect_distribution(client, args.map)))
        finally:
            client.shutdown()
    elif args.action == "stats":
        client = create_client()
        try:
            print(format_stats(f"Статистика для {args.map}", collect_distribution(client, args.map)))
        finally:
            client.shutdown()


if __name__ == "__main__":
    main()
