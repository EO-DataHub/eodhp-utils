# Tests adapted from Apache Pulsar tests:
# https://github.com/apache/pulsar-client-python/blob/main/tests/pulsar_test.py
import subprocess
from unittest import main

import pytest
from _pulsar import Timeout

from eodhp_utils.pulsar.client import Consumer, Producer

timeout_ms = 2000  #
service_url = "pulsar://localhost:6650"


@pytest.fixture(autouse=True)
def environment_variables(monkeypatch):
    monkeypatch.setenv("PULSAR_URL", service_url)


@pytest.fixture(scope="session")
def run_pulsar_service():
    subprocess.run(["build-support/pulsar-test-service-start.sh"])
    yield
    subprocess.run(["build-support/pulsar-test-service-stop.sh"])


@pytest.mark.parametrize(
    "message",
    [b"Hello", "Hello", {"hello": "hello"}],
)
def test_producer(message, run_pulsar_service):
    topic_name = "test_topic"
    producer_name = "test_producer"

    producer = Producer(topic=topic_name, producer=producer_name)
    producer.send(message)

    assert producer
    producer.close()


def test_producer_send_consumer_receive(run_pulsar_service):
    topic_name = "test_topic"
    producer_name = "test_producer"

    message = "Test"

    producer = Producer(topic=topic_name, producer=producer_name)

    consumer = Consumer(topic=topic_name, subscription=producer_name)

    try:  # Clear any messages in the queue. Timeout added in case there are none
        while old_message := consumer.receive(timeout_ms):
            consumer.acknowledge(old_message)
    except Timeout:
        pass

    message_id = producer.send(message)
    received_message = consumer.receive()
    consumer.acknowledge(received_message)

    assert received_message.data().decode("utf-8") == message

    assert message_id == received_message.message_id()
    consumer.close()
    producer.close()


if __name__ == "__main__":
    main()
