# Phase 1 — Kafka fundamentals: step-by-step log

A record of each step: what was run, why, and how it was checked. Steps are added once they have been run and verified. The overall plan is in [roadmap.md](roadmap.md); Phase 0 is in [phase-0-steps.md](phase-0-steps.md).

Learning programs live in `src/streamml/basics/` and run with `uv run python -m streamml.basics.<name>`.

| Step | Status |
|---|---|
| 1. Python producer | Done (2026-10-05) |
| 2. Python consumer, consumer groups, committed offsets | Not started |
| 3. Partitions and keys | Not started |
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
