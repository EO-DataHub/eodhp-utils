import json
import logging
import os
import time
from importlib.metadata import PackageNotFoundError, version
from typing import Optional

import boto3.session
from pulsar import Client, Consumer, ConsumerDeadLetterPolicy, ConsumerType

from eodhp_utils.messagers import CatalogueChangeMessager

pulsar_client = None
aws_client = None
DEBUG_TOPIC = "eodhp-utils-debugging"
SUSPEND_TIME = 5


def get_pulsar_client(pulsar_url=None, message_listener_threads=1):
    global pulsar_client
    if pulsar_client is None:
        pulsar_url = pulsar_url or os.environ.get("PULSAR_URL")
        pulsar_client = Client(pulsar_url, message_listener_threads=message_listener_threads)
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


class Runner:
    messagers: dict[str, CatalogueChangeMessager]
    subscription_name: str
    takeover_mode: bool
    msg_limit: Optional[int]
    pulsar_url: Optional[str]
    threads: int

    _pulsar_client: Client
    _suspended_until: int
    _messager_consumers: list[Consumer]

    def __init__(
        self,
        messagers: dict[str, CatalogueChangeMessager],
        subscription_name: str,
        takeover_mode=False,
        msg_limit=None,
        pulsar_url=None,
        threads=1,
    ):
        self.messagers = messagers
        self.subscription_name = subscription_name
        self.takeover_mode = takeover_mode
        self.msg_limit = msg_limit
        self.pulsar_url = pulsar_url
        self.threads = threads

        self._pulsar_client = get_pulsar_client(
            pulsar_url=pulsar_url, message_listener_threads=threads
        )
        self._suspended_until = 0
        self._messager_consumers = []

        self._create_subscriptions()

    def _listener(self, consumer, msg):
        """
        This is called asynchronous (there may be multiple threads) when a message is received.
        The message may be for any of our messagers or may be a 'takeover' message.
        """
        topic_name = msg.topic_name().split("/")[-1]

        if not self.takeover_mode and topic_name == DEBUG_TOPIC:
            # We have received a takeover message and must stop processing normal messages.
            # This allows a dev runner on a developers machine to process all the messages
            # instead.
            self._process_takeover(consumer, msg)
        else:
            self._process_messager_msg(topic_name, consumer, msg)

    def _process_messager_msg(self, topic_name, consumer, msg):
        messager = self.messagers[topic_name]

        failures = messager.consume(msg)

        if failures.any_temporary():
            consumer.negative_acknowledge(msg)
        else:
            consumer.acknowledge(msg)

    def _process_takeover(self, consumer, msg):
        """
        Allow another runner to take over for SUSPEND_TIME seconds. ie, stop processing any
        normal messages and process only further takeover messages (which will extend this time).

        The takeover is only allowed if it's taking over /our/ subscription.
        """
        consumer.acknowledge(msg)

        now = time.time()

        data_dict = json.loads(msg.data().decode("utf-8"))
        if data_dict.get("suspend_subscription") == self.subscription_name:
            self._suspended_until = max(
                self._suspended_until,
                msg.publish_timestamp() / 1000.0 + SUSPEND_TIME,
            )

            suspension_remaining = self._suspended_until - now
            logging.warning(
                f"Takeover active, pausing message reception for {suspension_remaining}"
            )

            for consumer in self._messager_consumers:
                consumer.pause_message_listener()

    def _end_takeover(self):
        for consumer in self._messager_consumers:
            consumer.resume_message_listener()

        self._suspended_until = 0

        logging.warning("Takeover ended")

    def _create_subscriptions(self):
        max_redelivery_count = 3
        delay_ms = 30000

        for topic, messager in self.messagers.items():
            consumer = self._pulsar_client.subscribe(
                topic=topic,
                subscription_name=self.subscription_name,
                consumer_type=ConsumerType.Shared,
                dead_letter_policy=ConsumerDeadLetterPolicy(
                    max_redeliver_count=max_redelivery_count,
                    dead_letter_topic=f"dead-letter-{self.subscription_name}",  # noqa:F541
                ),
                negative_ack_redelivery_delay_ms=delay_ms,
                schema=messager.get_schema(),
                message_listener=lambda cons, msg: self._listener(cons, msg),
            )

            self._messager_consumers.append(consumer)

    def run(self, max_loops=None):
        """
        Runner main loop.
        """
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
        if self.takeover_mode:
            takeover_producer = self._pulsar_client.create_producer(
                topic=DEBUG_TOPIC,
                producer_name=f"{self.subscription_name}-takeover",
            )

            takeover_msg = json.dumps({"suspend_subscription": self.subscription_name})
        else:
            self._pulsar_client.subscribe(
                topic=DEBUG_TOPIC,
                subscription_name=self.subscription_name + "-takeover",
                consumer_type=ConsumerType.Shared,
            )

        # Messages are processed in other threads. This loop is here to handle takeover mode and
        # msg_limit.
        while True:
            if self._suspended_until != 0:
                if self._suspended_until < time.time():
                    self._end_takeover()

            if self.takeover_mode:
                logging.debug("Sending takeover message")
                takeover_producer.send(bytes(takeover_msg, "utf-8"))

            if max_loops:
                max_loops -= 1
                if max_loops <= 0:
                    return

            time.sleep(SUSPEND_TIME / 2)


def run(
    messagers: dict[str, CatalogueChangeMessager],
    subscription_name: str,
    takeover_mode=False,
    msg_limit=None,
    pulsar_url=None,
    threads=1,
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

    runner = Runner(
        messagers=messagers,
        subscription_name=subscription_name,
        takeover_mode=takeover_mode,
        msg_limit=msg_limit,
        pulsar_url=pulsar_url,
        threads=threads,
    )

    runner.run()
