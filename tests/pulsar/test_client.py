from unittest import mock

import pytest

from eodhp_utils.pulsar import client
from eodhp_utils.pulsar.client import Consumer, Producer


@pytest.fixture(autouse=True)
def environment_variables(monkeypatch):
    monkeypatch.setenv("PULSAR_URL", "test-pulsar.com")


@pytest.fixture
def mock_topic():
    return "topic_name"


@pytest.fixture
def mock_subscription():
    return "subscription_name"


# @pytest.fixture
# def mock_pulsar_consumer():
#     mock_message = mock.Mock()
#     mock_message.data.return_value = b"Test"
#     mock_message.message_id.return_value = "message_1234"
#
#     consumer = mock.MagicMock()
#     consumer.topic = mock_topic
#     consumer.subscription = mock_subscription
#
#     consumer.receive.return_value = mock_message
#     consumer.acknowledge = mock.Mock()
#     consumer.close = mock.Mock()
#
#     yield consumer


# @pytest.fixture
# def mock_pulsar_producer(mock_topic, mock_subscription):
#     producer = mock.MagicMock()
#     producer.topic = mock_topic
#     producer.producer_name = mock_subscription
#
#     producer.send.return_value = "message_1234"
#     producer.close = mock.Mock()
#
#     yield producer

# @pytest.fixture
# def mock_pulsar_client():
#     client = mock.MagicMock()
#     client.return_value = 3
#     # client.__init__.return_value = None
#     client.create_producer.return_value = Producer
#     client.subscribe.return_value = Consumer


# @pytest.fixture
# def mock_pulsar():
#     client = mock.MagicMock()
#     client.return_value = mock_pulsar_client


@pytest.fixture
def mock_message():

    message = mock.MagicMock()
    message.data.return_value = b"Test"
    message.message_id.return_value = "message_1234"

    return message


@pytest.fixture
def mock_pulsar_producer():
    producer = mock.MagicMock()
    producer.send.return_value = "message_1234"

    return producer


@pytest.fixture
def mock_pulsar_consumer(mock_message):
    consumer = mock.MagicMock()
    consumer.receive.return_value = mock_message

    return consumer


@pytest.fixture
def mock_pulsar_client(mock_pulsar_producer, mock_pulsar_consumer):
    client = mock.MagicMock()
    client.create_producer.return_value = mock_pulsar_producer
    client.subscribe.return_value = mock_pulsar_consumer

    return client


@pytest.fixture
def mock_pulsar(mock_pulsar_client):
    package = mock.MagicMock()
    package.return_value = mock_pulsar_client

    return package


@pytest.mark.parametrize(
    "message",
    [b"Hello", "Hello", {"hello": "hello"}],
)
def test_producer(
    message, mock_pulsar, mock_pulsar_producer, mock_pulsar_client, mock_topic, mock_subscription
):
    with mock.patch("pulsar.Client", new=mock_pulsar):
        from importlib import reload

        reload(client)

        producer = Producer(topic=mock_topic, producer_name=mock_subscription)
        producer.send(message)

        assert producer
        producer.close()


def test_producer_send_consumer_receive(
    mock_pulsar,
    mock_pulsar_client,
    mock_pulsar_consumer,
    mock_pulsar_producer,
    mock_topic,
    mock_subscription,
):
    message = "Test"

    with mock.patch("pulsar.Client", new=mock_pulsar):
        from importlib import reload

        reload(client)

        producer = Producer(topic=mock_topic, producer_name=mock_subscription)
        consumer = Consumer(topic=mock_topic, subscription=mock_subscription)

        message_id = producer.send(message)
        received_message = consumer.receive()
        consumer.acknowledge(received_message)

        assert received_message.data().decode("utf-8") == message

        assert message_id == received_message.message_id()
        consumer.close()
        producer.close()
