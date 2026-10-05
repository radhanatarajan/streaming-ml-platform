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
