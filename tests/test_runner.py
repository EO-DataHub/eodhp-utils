import os
from argparse import Action
from typing import Sequence
from unittest import mock

import eodhp_utils
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
        mock_message = mock_consumer.receive(None)
        mock_message.topic_name.return_value = "x/test-topic"

        runner.run({"test-topic": mock_messager}, "test-subscription", msg_limit=1)

        mock_messager.consume.assert_called_once_with(mock_message)
        mock_consumer.acknowledge.assert_called_once_with(mock_message)


def test_setup_logging_doesnt_error():
    eodhp_utils.runner.setup_logging(0)
    eodhp_utils.runner.setup_logging(1)
    eodhp_utils.runner.setup_logging(2)
    eodhp_utils.runner.setup_logging(3)


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
            TimeoutError(),
            TimeoutError(),
            TimeoutError(),
            mock_message,
            TimeoutError(),
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
        mock.patch("eodhp_utils.runner.time.time") as mock_time,
    ):
        mock_consumer = mock.MagicMock(name="consumer")
        mock_getclient().subscribe.return_value = mock_consumer

        # Mock reception of a message from a test-topic.
        mock_messager = mock.MagicMock(name="messager")

        mock_message = mock.MagicMock(name="message")
        mock_message.topic_name.return_value = "x/test-topic"

        mock_takeover_message = mock.MagicMock(name="message")
        mock_takeover_message.topic_name.return_value = f"x/{eodhp_utils.runner.DEBUG_TOPIC}"
        mock_takeover_message.data.return_value = b'{"suspend_subscription": "test-subscription"}'
        mock_takeover_message.publish_timestamp = -5000

        # We mock the following five steps:
        #  - Time 50: takeover message received
        #  - Time 2600: takeover message received
        #  - Time 5100: takeover message received
        #  - Time 6000: ordinary message received
        #  - Time 7600: takeover message received
        mock_time.side_effect = [50, 2600, 5150, 6000, 7700]
        mock_consumer.receive.side_effect = [
            mock_takeover_message,
            mock_takeover_message,
            mock_takeover_message,
            mock_message,
            mock_takeover_message,
        ]

        runner.run({"test-topic": mock_messager}, "test-subscription", msg_limit=5)

        mock_messager.consume.assert_called_once_with(mock_message)
        mock_consumer.pause_message_listener.assert_has_calls([mock.call()] * 4)
        mock_consumer.resume_message_listener.assert_has_calls([mock.call()] * 4)
