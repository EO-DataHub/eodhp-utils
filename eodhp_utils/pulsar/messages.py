import json
import logging
import uuid

import jsonschema
import jsonschema.exceptions
from faker import Faker
from pulsar.schema import Array, Double, JsonSchema, Record, Schema, String


class BillingEvent(Record):
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


class WorkspaceObjectStoreSettings(Record):
    store_id = String()  # UUID
    name = String()
    bucket = String()
    prefix = String()
    host = String()
    env_var = String()
    access_point_arn = String()
    access_url = String()

    @staticmethod
    def get_fake():
        fake = Faker()

        obj = WorkspaceObjectStoreSettings()
        obj.store_id = str(uuid.uuid4())
        obj.name = fake.user_name()
        obj.bucket = "eodh-workspaces"
        obj.prefix = "/workspaces/" + obj.name
        obj.host = fake.hostname()
        obj.env_var = "fake-var"
        obj.access_point_arn = "fake-arn"
        obj.access_url = fake.url()

        return obj


class WorkspaceBlockStoreSettings(Record):
    store_id = String()  # UUID
    name = String()
    access_point_id = String()
    mount_point = String()

    @staticmethod
    def get_fake():
        fake = Faker()

        obj = WorkspaceBlockStoreSettings()
        obj.store_id = str(uuid.uuid4())
        obj.name = fake.user_name()
        obj.access_point_id = str(fake.random_number())
        obj.mount_point = "/workspaces/my-workspace/" + obj.name

        return obj


class WorkspaceStoresSettings(Record):
    object = Array(WorkspaceObjectStoreSettings())
    block = Array(WorkspaceBlockStoreSettings())

    @staticmethod
    def get_fake():
        obj = WorkspaceStoresSettings()
        obj.object = [WorkspaceObjectStoreSettings.get_fake()]
        obj.block = [WorkspaceBlockStoreSettings.get_fake()]

        return obj


class WorkspaceSettings(Record):
    id = String()  # UUID
    name = String()
    account = String()  # UUID
    member_group = String()
    status = String()  # 'creating', 'deleting', 'updating', 'updated' or 'created'
    stores = Array(WorkspaceStoresSettings())
    last_update = String()  # ISO date

    @staticmethod
    def get_fake():
        fake = Faker()

        obj = WorkspaceSettings()
        obj.id = str(uuid.uuid4())
        obj.name = fake.user_name()
        obj.account = str(uuid.uuid4())
        obj.member_group = obj.name + "-group"
        obj.status = "created"
        obj.stores = [WorkspaceStoresSettings.get_fake()]
        obj.last_update = fake.past_datetime("-30d").isoformat()

        return obj


def generate_workspacesettings_schema() -> Schema:
    return JsonSchema(WorkspaceSettings)


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
