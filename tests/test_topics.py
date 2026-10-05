from streamml.topics import TOPICS


def topic(name):
    return next(t for t in TOPICS if t.topic == name)


def test_events_raw_has_six_partitions():
    assert topic("events.raw").num_partitions == 6


def test_catalog_products_is_compacted():
    assert topic("catalog.products").config["cleanup.policy"] == "compact"
