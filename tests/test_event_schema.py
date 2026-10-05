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
