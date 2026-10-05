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
