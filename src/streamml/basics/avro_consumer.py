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
