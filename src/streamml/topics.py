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
