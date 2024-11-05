import json
import logging

import jsonschema
import jsonschema.exceptions


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
