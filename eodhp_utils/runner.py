import logging
import os
from importlib.metadata import PackageNotFoundError, version

import boto3
from pulsar import Client, ConsumerDeadLetterPolicy, ConsumerType

from eodhp_utils.messagers import CatalogueChangeMessager

pulsar_client = None
aws_client = None


def get_pulsar_client():
    global pulsar_client
    if pulsar_client is None:
        pulsar_url = os.environ.get("PULSAR_URL")
        pulsar_client = Client(pulsar_url)
    return pulsar_client


def get_boto3_session():
    global aws_client
    if not aws_client:
        aws_client = boto3.session.Session(
            # AWS_ACCESS_KEY_ID is the standard one AWS tools use. AWS_ACCESS_KEY has been widely
            # used in EODH.
            #
            # If these are not set then Boto will look in locations like ~/.aws/credentials.
            aws_access_key_id=(
                os.environ.get("AWS_ACCESS_KEY") or os.environ.get("AWS_ACCESS_KEY_ID")
            ),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        )
    return aws_client


def run(messagers: dict[str, CatalogueChangeMessager], subscription_name: str):
    """Run loop to monitor arrival of pulsar messages on a given topic.

    Example usage:
    annotations_messager = AnnotationsMessager(s3_client=s3_client, output_bucket=destination_bucket)
    run(
        {
            "transformed-annotations": annotations_messager
        },
        "annotations-ingester",
    )
    """

    try:
        __version__ = version("eodhp_utils")
        logging.info(f"eodhp-utils runner starting, version {__version__}")
    except PackageNotFoundError:
        # Not installed as a package, eg running directly from Git clone.
        logging.info("eodhp_utils runner starting from dev environment")

    topics = list(messagers.keys())

    max_redelivery_count = 3
    delay_ms = 30000

    client = get_pulsar_client()

    consumer = client.subscribe(
        topic=topics,
        subscription_name=subscription_name,
        consumer_type=ConsumerType.Shared,
        dead_letter_policy=ConsumerDeadLetterPolicy(
            max_redeliver_count=max_redelivery_count,
            dead_letter_topic=f"dead-letter-{subscription_name}",  # noqa:F541
        ),
        negative_ack_redelivery_delay_ms=delay_ms,
    )

    while True:
        pulsar_message = consumer.receive()

        topic_name = pulsar_message.topic_name().split("/")[-1]

        messager = messagers[topic_name]

        failures = messager.consume(pulsar_message)

        if failures.any_temporary():
            consumer.negative_acknowledge(pulsar_message)
        else:
            consumer.acknowledge(pulsar_message)
