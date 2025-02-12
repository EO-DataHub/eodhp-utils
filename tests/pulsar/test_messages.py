import json

import jsonschema
import pytest

from eodhp_utils.pulsar.messages import (
    BillingEvent,
    generate_billingevent_schema,
    generate_harvest_schema,
    generate_schema,
    get_message_data,
)


class MockMessage:
    def __init__(self):
        self.message = {
            "id": "1234abcd",
            "bucket_name": "my_bucket",
            "added_keys": ["added"],
            "updated_keys": ["updated"],
            "deleted_keys": ["deleted"],
            "source": "source",
            "target": "target",
        }

    def data(self):
        return json.dumps(self.message).encode("utf-8")


@pytest.fixture
def mock_message():
    return MockMessage()


def test_generate_harvest_schema():
    schema = generate_harvest_schema()

    assert len(schema.keys()) == 3
    assert len(schema["properties"]) == 7
    assert len(schema["required"]) == 3


def test_generate_schema__empty():
    schema = generate_schema()

    assert set(schema.keys()) == {"type", "properties", "required"}


def test_generate_schema__required():
    required = ["a", "b", "c"]
    schema = generate_schema(required=required)

    assert set(schema.keys()) == {"type", "properties", "required"}
    assert schema["required"] == required


def test_generate_schema__properties():
    properties = {"a": 1, "b": 2, "c": 3}
    schema = generate_schema(properties=properties)

    assert set(schema.keys()) == {"type", "properties", "required"}
    assert schema["properties"] == properties


def test_get_data(mock_message):
    data = get_message_data(mock_message)

    assert data == MockMessage().message


def test_get_message_data__schema(mock_message):
    schema = generate_schema()
    data = get_message_data(mock_message, schema)

    assert data == MockMessage().message


def test_get_message_data__schema_fail(mock_message):
    schema = generate_harvest_schema()
    mock_message.message = {"different_key": "different value"}
    with pytest.raises(jsonschema.exceptions.ValidationError) as e:
        get_message_data(mock_message, schema)

    assert "is a required property" in e.value.args[0]


def test_generate_billingevent_schema_encode_decode_restores_object():
    schema = generate_billingevent_schema()

    be = BillingEvent.get_fake()

    enced = schema.encode(be)
    decd = schema.decode(enced)

    assert decd == be
