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
