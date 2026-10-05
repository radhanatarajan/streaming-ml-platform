# Phase 1 — Kafka fundamentals: step-by-step log

A record of each step: what was run, why, and how it was checked. Steps are added once they have been run and verified. The overall plan is in [roadmap.md](roadmap.md); Phase 0 is in [phase-0-steps.md](phase-0-steps.md).

Learning programs live in `src/streamml/basics/` and run with `uv run python -m streamml.basics.<name>`.

| Step | Status |
|---|---|
| 1. Python producer | Done (2026-10-05) |
| 2. Python consumer, consumer groups, committed offsets | Done (2026-10-05) |
| 3. Partitions and keys | Done (2026-10-05) |
| 4. Two consumers in one group; rebalancing | Not started |
| 5. Topic creation script | Not started |
| 6. Avro producer with Schema Registry | Not started |
| 7. Avro consumer; schema evolution | Not started |
| 8. Tests, learning notes, Phase 1 PR | Not started |

---

## Step 1 — Python producer

**Concept:** `produce()` does not send. It puts the message in a buffer inside the program and returns at once. The client sends buffered messages in batches, and `flush()` waits until Kafka has confirmed every one. A **delivery callback** reports, per message, the partition and **offset** it landed at, or the error. An offset is a message's position in a partition, starting at 0; it never changes and is never reused.

**What to do**

```bash
uv add confluent-kafka
```

Create `src/streamml/basics/__init__.py` (empty; it makes the folder a package) and `src/streamml/basics/producer.py`:

```python
from confluent_kafka import Producer


def on_delivery(err, msg):
    if err is not None:
        print(f"failed: {err}")
    else:
        print(f"delivered to {msg.topic()} partition {msg.partition()} offset {msg.offset()}")


def main():
    producer = Producer({"bootstrap.servers": "localhost:9092"})

    for i in range(3):
        producer.produce("hello", value=f"message {i} from python", on_delivery=on_delivery)

    producer.flush()


if __name__ == "__main__":
    main()
```

```bash
uv run python -m streamml.basics.producer
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic hello --from-beginning
```

| Part | Meaning |
|---|---|
| `"bootstrap.servers": "localhost:9092"` | The program runs on the Mac, so it uses the `HOST` listener. |
| `producer.produce(...)` | Adds the message to the buffer and returns immediately. |
| `producer.flush()` | Sends everything buffered and waits for confirmation. Without it, a short program can exit before sending. |
| `on_delivery` | Called once per message when Kafka confirms or rejects it. |
| `if __name__ == "__main__":` | Runs `main()` only when the file is run as a program, not when it is imported. |

**Check**

- The producer prints `delivered to hello partition 0 offset 0`, then offsets 1 and 2.
- The console consumer prints the three messages.

**Observed:** both checks passed, with `confluent-kafka` 2.15.1.

An empty file runs without error in Python. The first attempt printed nothing at all because `producer.py` had not been saved: no output, not even an error, is the sign to check the file on disk.

## Step 2 — Python consumer, consumer groups and committed offsets

**Concept:** every consumer belongs to a **consumer group**, named by `group.id`. Kafka saves, per group and per partition, how far the group has read: the **committed offset**. A consumer that restarts in the same group continues from there instead of starting over. **Lag** is how far behind a group is: the newest offset in the partition minus the committed offset.

**What to do**

Create `src/streamml/basics/consumer.py`:

```python
from confluent_kafka import Consumer


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": "hello-readers",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe(["hello"])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"error: {msg.error()}")
                continue
            print(f"partition {msg.partition()} offset {msg.offset()}: {msg.value().decode()}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
```

| Part | Meaning |
|---|---|
| `"group.id"` | The group this consumer belongs to; Kafka saves progress under this name. |
| `"auto.offset.reset": "earliest"` | Where to start when the group has no committed offset yet. Ignored after the first commit. |
| `subscribe(["hello"])` | Which topics to read. Kafka decides which partitions this consumer gets. |
| `poll(1.0)` | Waits up to 1 second for a message; returns `None` if none arrived. |
| `close()` | Leaves the group cleanly and saves the final position. |

Offsets are committed automatically every few seconds and on `close()`. Phase 3 switches to manual commits, made only after the data has been written.

Run the consumer and leave it running. In a second terminal, run the producer. Then stop the consumer, start it again, stop it, and describe the group:

```bash
uv run python -m streamml.basics.consumer          # terminal 1
uv run python -m streamml.basics.producer          # terminal 2
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group hello-readers
```

**Check**

- The consumer prints offsets 0 to 2, then 3 to 5 live as the producer sends them.
- After a restart it prints nothing: it resumes at the committed offset.
- The group shows `CURRENT-OFFSET 6`, `LOG-END-OFFSET 6`, `LAG 0`.

**Observed:** all as expected. `kafka-consumer-groups.sh` reported "has no active members" because the consumer had been stopped; the committed offset stays saved in Kafka without any consumer running.

Two terminal details: `%` after `^C` is zsh marking output that did not end with a newline, and the `(streamml)` prefix in a new VS Code terminal is this project's own `.venv`, activated automatically. Neither matters.

## Step 3 — Partitions and keys

**Concept:** a **partition** is one slice of a topic: its own ordered log with its own offsets. Partitions let several consumers read one topic at the same time. A **key** decides the partition: Kafka hashes the key, so the same key always lands in the same partition. Order is guaranteed only within a partition, so the key chooses what stays in order. In `events.raw` the key is `product_id`, which keeps each product's events in order.

**What to do**

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic demo.events --partitions 6 --replication-factor 1
```

Kafka warns that topic names with a period or underscore can collide in metric names. Expected and harmless.

Create `src/streamml/basics/keyed_producer.py`:

```python
from confluent_kafka import Producer

PRODUCTS = ["p1", "p2", "p3", "p4", "p5", "p6", "p7", "p8"]


def on_delivery(err, msg):
    if err is not None:
        print(f"failed: {err}")
    else:
        print(f"key {msg.key().decode()} -> partition {msg.partition()} offset {msg.offset()}")


def main():
    producer = Producer({"bootstrap.servers": "localhost:9092"})

    for round_number in range(2):
        for product in PRODUCTS:
            producer.produce(
                "demo.events",
                key=product,
                value=f"event {round_number} for {product}",
                on_delivery=on_delivery,
            )

    producer.flush()


if __name__ == "__main__":
    main()
```

```bash
uv run python -m streamml.basics.keyed_producer
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic demo.events
```

**Check**

- Each key goes to the same partition in both rounds.
- The per-partition counts add up to 16.

**Observed:**

| Partition | Keys | Messages |
|---|---|---|
| 0 | p6 | 2 |
| 1 | p1, p3 | 4 |
| 2 | none | 0 |
| 3 | p8 | 2 |
| 4 | p4, p5, p7 | 6 |
| 5 | p2 | 2 |

- Every key stayed in one partition, and the total is 16.
- Within partition 1 the order is p1, p3, p1, p3: the order they were sent.
- Delivery reports arrived grouped by partition, not in send order, because the client sends one batch per partition.
- The spread is uneven. With 6 consumers, one would sit idle (partition 2) and one would do three times the work (partition 4). With thousands of products the spread evens out, but a single very popular key still loads one partition more than the rest. This is called a hot key.

The first run failed with "No module named 'streamml.basics.keyed_producer'" because the file had been saved as `keyes_producer.py`. A module name must match the file name exactly.
