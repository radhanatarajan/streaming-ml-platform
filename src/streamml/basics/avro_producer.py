import json
from datetime import datetime, timezone
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
        "event_time": datetime.now(timezone.utc),
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
    except Exception as error:
        print(f"rejected before sending: {error}")


if __name__ == "__main__":
    main()
