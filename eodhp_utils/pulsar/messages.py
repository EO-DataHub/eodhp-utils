import json
import logging

import jsonschema
import jsonschema.exceptions
from faker import Faker
from pulsar.schema import Double, JsonSchema, Record, Schema, String


class BillingEvent(Record):
    correlation_id = String()
    uuid = String()
    event_start = String()  # ISO datetime in UTC
    event_end = String()  # ISO datetime in UTC
    sku = String()
    user = String()  # UUID for the user
    workspace = String()  # workspace name
    quantity = Double()

    @staticmethod
    def get_fake():
        """
        This returns a fake BillingEvent - useful for tests.

        This doesn't strictly belong here, but including it here (vs the tests directory),
        means that services which use eodhp_utils can also use it.
        """
        fake = Faker()

        be = BillingEvent()
        be.correlation_id = fake.uuid4()
        be.uuid = fake.uuid4()

        start = fake.past_datetime("-30d")
        be.event_start = start.isoformat()
        be.event_end = (start + fake.time_delta("+10m")).isoformat()
        be.sku = fake.pystr(4, 10)
        be.user = fake.uuid4()
        be.workspace = fake.user_name()
        be.quantity = fake.pyfloat()

        return be


def generate_billingevent_schema() -> Schema:
    return JsonSchema(BillingEvent)


def generate_harvest_schema():
    """Generates a populated JSON schema for the harvester"""
    properties = {
        "id": {"type": "string"},
        "bucket_name": {"type": "string"},
        "added_keys": {"type": "array"},
        "updated_keys": {"type": "array"},
        "deleted_keys": {"type": "array"},
        "source": {"type": "string"},
        "target": {"type": "string"},
    }
    required = ["bucket_name", "source", "target"]
    return generate_schema(properties=properties, required=required)


def generate_schema(properties: dict = None, required: list = None) -> dict:
    """Generates a JSON schema with 'type", 'required' and 'properties' fields"""

    if properties is None:
        properties = {}
    if required is None:
        required = []
    return {
        "type": "object",
        "properties": properties,
        "required": required,
    }


def get_message_data(msg, schema=None):
    """Collects and formats message data. Checks against a schema if one is provided"""
    data = msg.data().decode("utf-8")
    data_dict = json.loads(data)
    if schema:
        try:
            jsonschema.validate(data_dict, schema)
        except jsonschema.exceptions.ValidationError as e:
            logging.error(f"Validation failed: {e}")
            raise
    return data_dict
