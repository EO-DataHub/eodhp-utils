import os
from argparse import Action
from typing import Sequence
from unittest import mock

import pulsar

import eodhp_utils
import eodhp_utils.runner
from eodhp_utils import runner
from eodhp_utils.messagers import Messager


class MessagerTester(Messager[str]):
    messages_received = []

    def process_msg(self, msg: str) -> Sequence[Action]:
        self.messages_received.append(msg)

        if msg == "EXIT":
            raise KeyboardInterrupt()

        return []

    def gen_empty_catalogue_message(self, msg: str) -> dict:
        return {}


def test_s3_session_uses_supplied_key():
    runner.aws_client = None

    with mock.patch.dict(
        os.environ, {"AWS_ACCESS_KEY": "ACCESSKEY", "AWS_SECRET_ACCESS_KEY": "SECKEY"}
    ):
        sess = runner.get_boto3_session()

        assert sess.get_credentials().secret_key == "SECKEY"
        assert sess.get_credentials().access_key == "ACCESSKEY"


def test_messagers_given_messages():
    with mock.patch("eodhp_utils.runner.get_pulsar_client") as mock_getclient:
        mock_consumer = mock.MagicMock(name="consumer")
        mock_getclient().subscribe.return_value = mock_consumer

        # Mock a messager that returns no error.
        mock_messager = mock.MagicMock(name="messager")
        mock_messager.consume.return_value.any_temporary.return_value = False

        # Mock reception of a message from a test-topic.
        mock_message = mock.MagicMock(name="message")

        def receive_mock(timeout):
            if timeout == eodhp_utils.runner.SUSPEND_TIME * 1000:
                return mock_message

            # Takeover reception
            raise pulsar.Timeout()

        mock_consumer.receive.side_effect = receive_mock
        mock_message.topic_name.return_value = "x/test-topic"

        runner.run({"test-topic": mock_messager}, "test-subscription", msg_limit=1)

        mock_messager.consume.assert_called_once_with(mock_message)
        mock_consumer.acknowledge.assert_called_once_with(mock_message)


def test_setup_logging_doesnt_error():
    eodhp_utils.runner.setup_logging(0)
    eodhp_utils.runner.setup_logging(1)
    eodhp_utils.runner.setup_logging(2)
    eodhp_utils.runner.setup_logging(3)


def test_pulsar_client_uses_arg_over_env_when_set():
    with (
        mock.patch("eodhp_utils.runner.Client") as pulsar_client,
        mock.patch.dict(os.environ, {"PULSAR_URL": "pulsar://example.com/2"}),
    ):
        eodhp_utils.runner.get_pulsar_client("pulsar://example.com/1")

        eodhp_utils.runner.pulsar_client = None
        eodhp_utils.runner.get_pulsar_client()

        pulsar_client.assert_has_calls(
            (mock.call("pulsar://example.com/1"), mock.call("pulsar://example.com/2"))
        )


def test_takeover_sends_takeover_messages():
    # Tests that, in takover mode, we send a takeover message every 2.5S.
    with (
        mock.patch("eodhp_utils.runner.get_pulsar_client") as mock_getclient,
        mock.patch("eodhp_utils.runner.time.time") as mock_time,
    ):
        mock_consumer = mock.MagicMock(name="consumer")
        mock_getclient().subscribe.return_value = mock_consumer

        # Mock reception of a message from a test-topic.
        mock_messager = mock.MagicMock(name="messager")
        mock_message = mock.MagicMock(name="message")
        mock_message.topic_name.return_value = "x/test-topic"

        # We mock the following five steps:
        #  - Time 50: takeover message sent, no message received
        #  - Time 2600: takeover message sent, no message received
        #  - Time 5100: takeover message sent, no message received
        #  - Time 6000: no takeover message sent, message received
        #  - Time 7600: takeover message sent, no message received
        mock_time.side_effect = [50, 2600, 5150, 6000, 7700]
        mock_consumer.receive.side_effect = [
            pulsar.Timeout(),
            pulsar.Timeout(),
            pulsar.Timeout(),
            mock_message,
            pulsar.Timeout(),
        ]

        runner.run(
            {"test-topic": mock_messager}, "test-subscription", takeover_mode=True, msg_limit=5
        )

        mock_getclient().create_producer(
            topic=eodhp_utils.runner.DEBUG_TOPIC, producer_name=any
        ).send.assert_has_calls(
            [
                mock.call(b'{"suspend_subscription": "test-subscription"}'),
                mock.call(b'{"suspend_subscription": "test-subscription"}'),
                mock.call(b'{"suspend_subscription": "test-subscription"}'),
                mock.call(b'{"suspend_subscription": "test-subscription"}'),
            ]
        )

        mock_messager.consume.assert_called_once_with(mock_message)

        mock_consumer.pause_message_listener.assert_not_called()
        mock_consumer.resume_message_listener.assert_not_called()


def test_takeover_results_in_pause():
    # Tests that, when a takeover happens, other consumers pause message reception.
    with (
        mock.patch("eodhp_utils.runner.get_pulsar_client") as mock_getclient,
        mock.patch("eodhp_utils.runner.time") as mock_time,
        mock.patch("eodhp_utils.runner.logging"),  # logging calls time.time
    ):
        mock_consumer = mock.MagicMock(name="consumer")
        mock_getclient().subscribe.return_value = mock_consumer

        # Mock reception of a message from a test-topic.
        mock_messager = mock.MagicMock(name="messager")

        mock_message = mock.MagicMock(name="message")
        mock_message.topic_name.return_value = "x/test-topic"

        mock_takeover_message = mock.MagicMock(name="takeover-message")
        mock_takeover_message.topic_name.return_value = f"x/{eodhp_utils.runner.DEBUG_TOPIC}"
        mock_takeover_message.data.return_value = b'{"suspend_subscription": "test-subscription"}'
        mock_takeover_message.publish_timestamp.return_value = 1000

        # We mock the following five steps:
        #  - Time 50: two takeover messages received, sleep until 6001
        #  - Time 6010: no takeover messages, real message
        #  - Time 7700: takeover message received, no sleep because message too old, real message received
        #
        # Note: logger calls time.time()
        mock_time.time.side_effect = [0.050, 6.01, 7.7]
        mock_consumer.receive.side_effect = [
            mock_takeover_message,
            mock_takeover_message,
            pulsar.Timeout(),
            pulsar.Timeout(),
            mock_message,
            mock_takeover_message,
            pulsar.Timeout(),
            mock_message,
        ]

        runner.run({"test-topic": mock_messager}, "test-subscription", msg_limit=2)

        mock_time.sleep.assert_called_once_with(5.95)

        # assert_has_calls includes extra calls which were not made - no idea why
        assert mock_messager.consume.call_args_list == [
            mock.call(mock_message),
            mock.call(mock_message),
        ]
