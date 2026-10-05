# Phase 1 — Kafka fundamentals: step-by-step log

A record of each step: what was run, why, and how it was checked. Steps are added once they have been run and verified. The overall plan is in [roadmap.md](roadmap.md); Phase 0 is in [phase-0-steps.md](phase-0-steps.md).

Learning programs live in `src/streamml/basics/` and run with `uv run python -m streamml.basics.<name>`.

| Step | Status |
|---|---|
| 1. Python producer | Done (2026-10-05) |
| 2. Python consumer, consumer groups, committed offsets | Done (2026-10-05) |
| 3. Partitions and keys | Done (2026-10-05) |
| 4. Two consumers in one group; rebalancing | Done (2026-10-05) — Phase 1 check passed |
| 5. Topic creation script | Done (2026-10-05) |
| 6. Avro producer with Schema Registry | Done (2026-10-05) |
| 7. Avro consumer; schema evolution | Done (2026-10-05) |
| 8a. Tests | Done (2026-10-05) |
| 8b. Learning notes, Phase 1 PR | Not started |

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

## Step 4 — Two consumers in one group; rebalancing

This is the roadmap's check for Phase 1.

**Concept:** within one consumer group, each partition is read by exactly one consumer at a time. Running more copies of the same consumer with the same `group.id` splits the partitions between them; that is how a consumer scales. A **rebalance** is Kafka redistributing partitions when a member joins or leaves: members give up their partitions (revoked), receive a new set (assigned), and continue from the group's committed offsets.

**What to do**

Create `src/streamml/basics/group_consumer.py`:

```python
from confluent_kafka import Consumer


def on_assign(consumer, partitions):
    print(f"assigned partitions: {sorted(p.partition for p in partitions)}")


def on_revoke(consumer, partitions):
    print(f"revoked partitions: {sorted(p.partition for p in partitions)}")


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": "demo-group",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe(["demo.events"], on_assign=on_assign, on_revoke=on_revoke)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"error: {msg.error()}")
                continue
            print(
                f"partition {msg.partition()} offset {msg.offset()} "
                f"key {msg.key().decode()}: {msg.value().decode()}"
            )
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
```

Run it in three terminals, a few seconds apart:

| # | Terminal | Command or action |
|---|---|---|
| a | 1 | `uv run python -m streamml.basics.group_consumer` |
| b | 2 | `uv run python -m streamml.basics.group_consumer` (a second copy of the same program) |
| c | 3 | `uv run python -m streamml.basics.keyed_producer` |
| d | 2 | Ctrl+C |
| e | 3 | `uv run python -m streamml.basics.keyed_producer` |

| Part | Meaning |
|---|---|
| `on_assign` | Called when this consumer receives partitions. |
| `on_revoke` | Called when partitions are taken away. In Phase 3 this is where a consumer finishes its write and commits. |

Different `group.id`s would not split anything: each group reads every partition independently. That is how the sink and feature consumers will both read all of `events.raw` in Phase 3.

**Observed:**

| Moment | Terminal 1 | Terminal 2 |
|---|---|---|
| a | Assigned [0–5]; read all 16 messages | — |
| b | Revoked [0–5]; assigned [3, 4, 5] | Assigned [0, 1, 2]; no messages (the group had already read them) |
| c | 10 new messages, all from partitions 3, 4, 5 | 6 new messages, all from partitions 0, 1 |
| d | Revoked [3, 4, 5]; assigned [0–5] | Ctrl+C: revoked [0, 1, 2], left the group |
| e | All 16 new messages | — |

No message was read twice by the group and none was skipped. The Phase 1 check passed.

In plain terms: think of terminals 1 and 2 as Reader A and Reader B, two copies of the same program on the same team (`demo-group`), and terminal 3 as the Sender. Kafka's rule is that each partition goes to exactly one member of a team, so it shares the 6 partitions between A and B, and gives them all back to A when B leaves. In the project, A and B will be copies of the sink consumer and the Sender will be the replayer. Kafka UI → Consumers → `demo-group` shows each member and the partitions it owns.

Facts worth keeping:

- **More consumers than partitions does not help.** A 7th consumer on a 6-partition topic gets nothing. The partition count caps how far a group can scale.
- **Ctrl+C is a clean exit.** `close()` tells Kafka the consumer is leaving, so the rebalance starts at once. A crashed consumer is noticed only after the session timeout (45 seconds by default).

## Step 5 — Topic creation script

**Concept:** topic settings belong in code, so `make topics` creates the same topics with the same settings on any machine. The script skips topics that already exist, so it is safe to run again: it is **idempotent**. **Compaction** (`cleanup.policy=compact`) keeps the latest message for each key instead of deleting messages by age, which turns `catalog.products` into a table of the current details of every product.

**What to do**

Create `src/streamml/topics.py`:

```python
from confluent_kafka.admin import AdminClient, NewTopic

TOPICS = [
    NewTopic("events.raw", num_partitions=6, replication_factor=1),
    NewTopic("events.dlq", num_partitions=1, replication_factor=1),
    NewTopic(
        "catalog.products",
        num_partitions=1,
        replication_factor=1,
        config={"cleanup.policy": "compact"},
    ),
    NewTopic("catalog.enriched", num_partitions=1, replication_factor=1),
    NewTopic("predictions", num_partitions=1, replication_factor=1),
]


def main():
    admin = AdminClient({"bootstrap.servers": "localhost:9092"})
    existing = admin.list_topics(timeout=10).topics

    for topic in TOPICS:
        if topic.topic in existing:
            print(f"exists:  {topic.topic}")
            continue
        admin.create_topics([topic])[topic.topic].result()
        print(f"created: {topic.topic}")


if __name__ == "__main__":
    main()
```

Add to the `Makefile`:

```make
topics:
	uv run python -m streamml.topics
```

| Part | Meaning |
|---|---|
| `AdminClient` | The client for managing Kafka (topics, settings), not for sending or reading messages. |
| `list_topics(...).topics` | The topics that exist now. |
| `create_topics([...])[name].result()` | Creates the topic and waits for the answer; raises an error if creation fails. |
| `config={"cleanup.policy": "compact"}` | Turns on compaction for that topic. |

Partition counts: only `events.raw` carries high volume, so only it has 6 partitions. The other topics carry one message per product or per failure, so 1 is enough.

```bash
make topics
make topics
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic catalog.products
```

**Check**

- The first run prints `created:` for all five topics; the second prints `exists:` for all five.
- `catalog.products` shows `PartitionCount: 1` and `cleanup.policy=compact`.

**Observed:** all checks passed. `events.raw` has 6 partitions.

## Step 6 — Avro producer with Schema Registry

**Concept:** the producer checks each message against an Avro schema and encodes it as compact binary. The first time, it registers the schema with Schema Registry, which returns a schema ID. Every message starts with that ID, so a consumer can fetch the schema and decode the bytes.

The demo sends to a separate topic, `demo.avro`, not `events.raw`: test events in `events.raw` would distort the Phase 3 row counts.

**What to do**

```bash
uv add "confluent-kafka[avro]"
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic demo.avro --partitions 1 --replication-factor 1
```

Create `schemas/event.avsc` in the project root (next to `src/`, not inside it):

```json
{
  "type": "record",
  "name": "Event",
  "namespace": "streamml",
  "fields": [
    {"name": "event_time", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "event_type", "type": {"type": "enum", "name": "EventType", "symbols": ["view", "cart", "remove_from_cart", "purchase"]}},
    {"name": "product_id", "type": "long"},
    {"name": "category_id", "type": "long"},
    {"name": "category_code", "type": ["null", "string"], "default": null},
    {"name": "brand", "type": ["null", "string"], "default": null},
    {"name": "price", "type": "double"},
    {"name": "user_id", "type": "long"},
    {"name": "user_session", "type": ["null", "string"], "default": null}
  ]
}
```

Create `src/streamml/basics/avro_producer.py`:

```python
import json
from datetime import UTC, datetime
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

TOPIC = "demo.avro"


def on_delivery(err, msg):
    if err is not None:
        print(f"failed: {err}")
    else:
        print(f"delivered to {msg.topic()} offset {msg.offset()}: {len(msg.value())} bytes as Avro")


def main():
    registry = SchemaRegistryClient({"url": "http://localhost:8081"})
    serialize = AvroSerializer(registry, Path("schemas/event.avsc").read_text())
    producer = Producer({"bootstrap.servers": "localhost:9092"})
    context = SerializationContext(TOPIC, MessageField.VALUE)

    event = {
        "event_time": datetime.now(UTC),
        "event_type": "view",
        "product_id": 1001,
        "category_id": 2001,
        "category_code": "electronics.smartphone",
        "brand": "acme",
        "price": 199.99,
        "user_id": 5001,
        "user_session": "demo-session-1",
    }
    producer.produce(
        TOPIC,
        key=str(event["product_id"]),
        value=serialize(event, context),
        on_delivery=on_delivery,
    )
    producer.flush()
    print(f"the same event as JSON would be {len(json.dumps(event, default=str))} bytes")

    bad_event = {**event, "price": "twelve"}
    try:
        serialize(bad_event, context)
    except (TypeError, ValueError) as error:
        print(f"rejected before sending: {error}")


if __name__ == "__main__":
    main()
```

| Part | Meaning |
|---|---|
| `SchemaRegistryClient` | Connects to Schema Registry at `localhost:8081`. |
| `AvroSerializer(registry, schema)` | Checks a dict against the schema and returns Avro bytes; registers the schema on first use. |
| `SerializationContext(TOPIC, MessageField.VALUE)` | Topic plus message part; together they give the subject name `demo.avro-value`. |

```bash
uv run python -m streamml.basics.avro_producer
curl -s http://localhost:8081/subjects
curl -s http://localhost:8081/subjects/demo.avro-value/versions/1
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic demo.avro --from-beginning
```

**Observed**

- The event was 72 bytes as Avro; the same event as JSON is 241 bytes, so about 30%.
- `price="twelve"` was rejected before sending: "could not convert string to float: 'twelve'". The message does not name the field.
- Schema Registry holds subject `demo.avro-value`, version 1, schema ID 1.
- The console consumer printed binary with a few readable strings (`electronics.smartphone`, `acme`, `demo-session-1`): Avro stores text as-is and numbers in binary.
- The raw message starts `00 00 00 00 01`: one marker byte (0), then the 4-byte schema ID (1). That is how a consumer knows which schema to fetch.
- Kafka UI decodes the message into fields when the value decoder is "SchemaRegistry". Optional fields show as `{"string": "acme"}`, the way Avro writes a value that may be empty.
- An `AuthlibDeprecationWarning` printed on startup comes from a dependency. Harmless.
- The code above is the corrected version. The first version used `timezone.utc` and `except Exception`, which `ruff` 0.16 rejects (UP017, BLE001). The serializer raises `ValueError` for a bad number and `TypeError` for text in an integer field.

**What the serializer does and does not catch** (tested directly):

| Value | Result |
|---|---|
| `price="twelve"` | Rejected |
| `price="12.50"` (number as text) | Accepted, converted to 12.5 |
| `price=12` | Accepted as 12.0 |
| `product_id="42"` | Rejected: "an integer is required on field product_id" |
| `event_type="click"` | Rejected: not one of the allowed values |
| `brand` left out | Accepted as empty (null), as the schema allows |

The serializer tries to convert text to a decimal number before rejecting it. The topic still receives correct types, but a program sending text by mistake is not warned. The Phase 2 replayer reads a CSV, which is all text, so it must convert each column to its proper type itself.

## Step 7 — Avro consumer and schema changes

**Concept:** `AvroDeserializer` reads the schema ID at the front of each message, fetches that schema from the registry once, and returns a Python dict. The consumer is never given a schema file. Schema Registry checks every new version of a schema against a **compatibility rule**; the default, **BACKWARD**, means a consumer using the new schema must still be able to read messages written with the old one.

**What to do**

Create `src/streamml/basics/avro_consumer.py`:

```python
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext


def main():
    registry = SchemaRegistryClient({"url": "http://localhost:8081"})
    deserialize = AvroDeserializer(registry)
    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": "avro-readers",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe(["demo.avro"])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"error: {msg.error()}")
                continue
            event = deserialize(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            print(f"offset {msg.offset()}: {event}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
```

Create `src/streamml/basics/schema_check.py`:

```python
import json
from pathlib import Path

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient

SUBJECT = "demo.avro-value"


def changed_schema(change):
    schema = json.loads(Path("schemas/event.avsc").read_text())
    change(schema["fields"])
    return Schema(json.dumps(schema), "AVRO")


def add_discount_with_default(fields):
    fields.append({"name": "discount", "type": "double", "default": 0.0})


def add_discount_without_default(fields):
    fields.append({"name": "discount", "type": "double"})


def rename_product_id(fields):
    for field in fields:
        if field["name"] == "product_id":
            field["name"] = "item_id"


def main():
    registry = SchemaRegistryClient({"url": "http://localhost:8081"})
    print(f"compatibility rule: {registry.get_compatibility()}")

    for change in (add_discount_with_default, add_discount_without_default, rename_product_id):
        allowed = registry.test_compatibility(SUBJECT, changed_schema(change))
        print(f"{change.__name__:30} allowed: {allowed}")


if __name__ == "__main__":
    main()
```

`test_compatibility` asks whether a new version would be accepted, without registering it.

```bash
uv run python -m streamml.basics.avro_consumer
uv run python -m streamml.basics.schema_check
```

**Observed**

- The consumer printed the event as a dict with all nine fields. `event_time` came back as a `datetime` in UTC with millisecond precision (`952000` microseconds): `timestamp-millis` stores milliseconds, so anything finer is dropped.
- Compatibility rule: BACKWARD.

| Change | Allowed | Why |
|---|---|---|
| Add `discount` with a default | Yes | Old messages lack it; the default fills in. |
| Add `discount` without a default | No | Old messages lack it and there is nothing to fill in. |
| Rename `product_id` to `item_id` | No | Seen as removing `product_id` and adding `item_id` with no default. |

The subject still has only version 1.

## Step 8a — Tests

**Concept:** CI has no Kafka, so the tests check what can be checked without it: the topic settings in `topics.py`, and that the event schema accepts and rejects the right things. `fastavro` (the Avro library inside `confluent-kafka`) encodes and decodes without Schema Registry.

**What to do**

`tests/test_topics.py`:

```python
from streamml.topics import TOPICS


def topic(name):
    return next(t for t in TOPICS if t.topic == name)


def test_events_raw_has_six_partitions():
    assert topic("events.raw").num_partitions == 6


def test_catalog_products_is_compacted():
    assert topic("catalog.products").config["cleanup.policy"] == "compact"
```

`tests/test_event_schema.py`:

```python
import io
import json
from datetime import UTC, datetime
from pathlib import Path

import fastavro
import pytest

SCHEMA_PATH = Path(__file__).parent.parent / "schemas" / "event.avsc"
SCHEMA = fastavro.parse_schema(json.loads(SCHEMA_PATH.read_text()))

EVENT = {
    "event_time": datetime(2026, 10, 1, 12, 0, tzinfo=UTC),
    "event_type": "purchase",
    "product_id": 1001,
    "category_id": 2001,
    "category_code": "electronics.smartphone",
    "brand": "acme",
    "price": 199.99,
    "user_id": 5001,
    "user_session": "session-1",
}


def round_trip(event):
    buffer = io.BytesIO()
    fastavro.schemaless_writer(buffer, SCHEMA, event)
    buffer.seek(0)
    return fastavro.schemaless_reader(buffer, SCHEMA)


def test_event_round_trips():
    assert round_trip(EVENT) == EVENT


def test_missing_brand_is_allowed():
    assert round_trip({**EVENT, "brand": None})["brand"] is None


def test_unknown_event_type_is_rejected():
    with pytest.raises(ValueError):
        round_trip({**EVENT, "event_type": "click"})
```

| Test | What it protects |
|---|---|
| `test_events_raw_has_six_partitions` | The partition count, which caps how many consumers can share `events.raw` |
| `test_catalog_products_is_compacted` | Compaction on the catalog topic |
| `test_event_round_trips` | Encoding then decoding gives back the same event |
| `test_missing_brand_is_allowed` | Products without a brand are accepted |
| `test_unknown_event_type_is_rejected` | Only the four event types are allowed |

```bash
make lint
make test
```

**Check:** `All checks passed!` and `6 passed`.

**Observed:** both passed. The first commit of this step went in without the test files and without the lint fix to `avro_producer.py`, because those edits had not been saved; `make test` reporting "collected 1 item" was the sign. Fixed in a second commit.
