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
