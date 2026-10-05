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
