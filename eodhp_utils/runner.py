import json
import logging
import os
import time
from importlib.metadata import PackageNotFoundError, version

import boto3.session
from pulsar import Client, ConsumerDeadLetterPolicy, ConsumerType, Timeout

from eodhp_utils.messagers import CatalogueChangeMessager

pulsar_client = None
aws_client = None
DEBUG_TOPIC = "eodhp-utils-debugging"
SUSPEND_TIME = 5


def get_pulsar_client(pulsar_url=None):
    global pulsar_client
    if pulsar_client is None:
        pulsar_url = pulsar_url or os.environ.get("PULSAR_URL")
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


LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"


def setup_logging(verbosity=0):
    """
    This should be called based on command line arguments. eg:

    @click.option('-v', '--verbose', count=True)
    def my_cli(verbose):
        setup_logging(verbosity=verbose)
    """
    if verbosity == 0:
        logging.getLogger("botocore").setLevel(logging.CRITICAL)
        logging.getLogger("boto3").setLevel(logging.CRITICAL)
        logging.getLogger("urllib3").setLevel(logging.CRITICAL)

        logging.basicConfig(level=logging.WARNING, format=LOG_FORMAT)
    elif verbosity == 1:
        logging.getLogger("botocore").setLevel(logging.ERROR)
        logging.getLogger("boto3").setLevel(logging.ERROR)
        logging.getLogger("urllib3").setLevel(logging.ERROR)

        logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
    elif verbosity == 2:
        logging.getLogger("botocore").setLevel(logging.WARNING)
        logging.getLogger("boto3").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)

        logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
    elif verbosity > 2:
        logging.getLogger("botocore").setLevel(logging.DEBUG)
        logging.getLogger("boto3").setLevel(logging.DEBUG)
        logging.getLogger("urllib3").setLevel(logging.DEBUG)

        logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)


def log_component_version(component_name):
    """Logs a version number for a Python component using setuptools-git-versioning."""
    try:
        __version__ = version(component_name)
        logging.info(f"{component_name} starting, version {__version__}")
    except PackageNotFoundError:
        # Not installed as a package, eg running directly from Git clone.
        logging.info("{component_name} starting from dev environment")


def run(
    messagers: dict[str, CatalogueChangeMessager],
    subscription_name: str,
    takeover_mode=False,
    msg_limit=None,
    pulsar_url=None,
):
    """Run loop to monitor arrival of pulsar messages on a given topic.

    Example usage:
    annotations_messager = AnnotationsMessager(s3_client=s3_client, output_bucket=destination_bucket)
    run(
        {
            "transformed-annotations": annotations_messager
        },
        "annotations-ingester",
    )

    If 'takeover_mode' is True then messages will be sent to prevent other instances of this runner
    from processing any messages for this subscription. This is useful for debugging:
      - Use port-forwarding to get access to Pulsar
      - Run your development code in takeover mode
      - Inject messages
      - Be guaranteed that your development component will receive them
    """
    log_component_version("eodhp_utils")

    topics = list(messagers.keys())

    max_redelivery_count = 3
    delay_ms = 30000

    client = get_pulsar_client(pulsar_url=pulsar_url)

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

    # This relates to 'takeover mode' and suspension, which are used for debugging. A developer
    # can run a local copy of the service in takeover mode, resulting in this test copy receiving
    # messages instead of the copy in the cluster.
    #
    # If we're not in takeover mode then:
    #  - We listen to an additional topic, the debug topic.
    #  - If we receive a takeover message on that topic with our subscription name listed then
    #    we stop receiving messages for SUSPEND_TIME milliseconds.
    #
    # If we /are/ in takeover mode then:
    #  - We send a takeover message every SUSPEND_TIME/2 milliseconds
    #  - We ignore takeover messages.
    #
    suspended_until = 0
    if takeover_mode:
        takeover_producer = client.create_producer(
            topic=DEBUG_TOPIC,
            producer_name=f"{subscription_name}-takeover",
        )

        takeover_msg = json.dumps({"suspend_subscription": subscription_name})
    else:
        takeover_consumer = client.subscribe(
            topic=DEBUG_TOPIC,
            subscription_name=subscription_name + "-takeover",
            consumer_type=ConsumerType.Shared,
        )

    while msg_limit is None or msg_limit > 0:
        if msg_limit is not None:
            msg_limit -= 1

        if takeover_mode:
            now = time.time()
            if suspended_until < now:
                # Confirm our takeover
                logging.debug("Sending takeover message")
                takeover_producer.send(bytes(takeover_msg, "utf-8"))
                suspension_remaining = SUSPEND_TIME / 2
                suspended_until = now + suspension_remaining
        else:
            # Check for takeover messages and wait for takeover to finish
            while True:
                now = time.time()
                try:
                    while True:
                        pulsar_message = takeover_consumer.receive(0)
                        takeover_consumer.acknowledge(pulsar_message)

                        data_dict = json.loads(pulsar_message.data().decode("utf-8"))
                        if data_dict.get("suspend_subscription") == subscription_name:
                            suspended_until = max(
                                suspended_until,
                                pulsar_message.publish_timestamp() / 1000.0 + SUSPEND_TIME,
                            )
                except Timeout:
                    pass

                # Wait for takeover expiry
                suspension_remaining = suspended_until - now

                if suspension_remaining > 0:
                    logging.warning(
                        f"Takeover active, pausing message reception for {suspension_remaining}"
                    )
                    time.sleep(suspension_remaining)
                else:
                    suspension_remaining = 0
                    break

        try:
            pulsar_message = consumer.receive(
                int(suspension_remaining * 1000)
                if suspension_remaining > 0
                else int(SUSPEND_TIME * 1000)
            )
        except Timeout:
            continue

        topic_name = pulsar_message.topic_name().split("/")[-1]

        messager = messagers[topic_name]

        failures = messager.consume(pulsar_message)

        if failures.any_temporary():
            consumer.negative_acknowledge(pulsar_message)
        else:
            consumer.acknowledge(pulsar_message)
