# Лабораторна робота 3 — Мікросервіси з використанням Hazelcast Distributed Map та PostgreSQL

Цей проєкт є розвитком архітектури банківських мікросервісів з першого завдання (Task 1).
У цій ітерації:
1. **logging-service** масштабовано до **3-х екземплярів** у контейнерах Docker для розподілу навантаження.
2. Як сховище транзакцій замість локальної пам'яті використано **In-Memory Distributed Map на базі кластера Hazelcast з 3-х нод**.
3. Кожен екземпляр `logging-service` підключається до своєї призначеної ноди кластеру Hazelcast (`hazelcast-1`, `hazelcast-2`, `hazelcast-3`).
4. **facade-service** випадковим чином обирає екземпляр `logging-service` для запису та читання повідомлень. Якщо обраний екземпляр недоступний, фасад автоматично перемикається на наступний (failover).
5. **counter-service** переведено на збереження балансів та транзакцій у дискову СУБД (**PostgreSQL 16**), що усуває втрату даних при перезапусках.

Виконав: Габунія А.П.
## Вимоги до репозиторію GitHub

Згідно із завданням, код розміщено в гілці `micro_hazelcast`:
```bash
git checkout -b micro_hazelcast
git add .
git commit -m "Implement Lab 3: Microservices with 3-node Hazelcast cluster, replicated logging-service, and PostgreSQL counter-service"
git push origin micro_hazelcast
```

