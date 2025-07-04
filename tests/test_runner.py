import os
import threading
import time
from math import ceil
from typing import Iterator, Sequence
from unittest import mock

import pytest
from opentelemetry import trace
from opentelemetry.baggage import get_baggage, set_baggage
from opentelemetry.context import attach
from opentelemetry.propagate import inject

import eodhp_utils
import eodhp_utils.runner
from eodhp_utils import runner
from eodhp_utils.messagers import Messager


class MessagerTester(Messager[str, bytes]):
    messages_received: list[str] = []

    def __init__(self, *args, **kwargs):
        self.messages_received = []
        super().__init__(*args, **kwargs)

    def process_msg(self, msg: str) -> Sequence[Messager.Action]:
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
    with mock.patch("eodhp_utils.runner.get_pulsar_client"):
        ########### Setup
        # Mock a messager that returns no error.
        mock_messager = mock.MagicMock(name="messager")
        mock_messager.consume.return_value.any_temporary.return_value = False

        mock_message = mock.MagicMock(name="message")
        mock_message.topic_name.return_value = "x/test-topic"

        mock_consumer = mock.MagicMock(name="consumer")

        ############ Deliver message
        runner = eodhp_utils.runner.Runner({"test-topic": mock_messager}, "test-subscription")
        runner._listener(mock_consumer, mock_message)

        ############ Check behaviour
        mock_messager.consume.assert_called_once_with(mock_message)
        mock_consumer.acknowledge.assert_called_once_with(mock_message)


def test_setup_logging_doesnt_error():
    eodhp_utils.runner.setup_logging(0)
    eodhp_utils.runner.setup_logging(1)
    eodhp_utils.runner.setup_logging(2)
    eodhp_utils.runner.setup_logging(3)
    eodhp_utils.runner.setup_logging(4)


def test_pulsar_client_uses_arg_over_env_when_set():
    with (
        mock.patch("eodhp_utils.runner.Client") as pulsar_client,
        mock.patch.dict(os.environ, {"PULSAR_URL": "pulsar://example.com/2"}),
    ):
        eodhp_utils.runner.get_pulsar_client("pulsar://example.com/1")

        eodhp_utils.runner.pulsar_client = None
        eodhp_utils.runner.get_pulsar_client()

        pulsar_client.assert_has_calls(
            (
                mock.call("pulsar://example.com/1", message_listener_threads=1, io_threads=1),
                mock.call("pulsar://example.com/2", message_listener_threads=1, io_threads=1),
            )
        )


def test_takeover_sends_takeover_messages():
    # Tests that, in takover mode, we send a takeover message every 2.5S.
    with (
        mock.patch("eodhp_utils.runner.get_pulsar_client") as mock_getclient,
        mock.patch("eodhp_utils.runner.time.sleep"),
    ):
        ####### Setup
        mock_consumer = mock.MagicMock(name="consumer")
        mock_getclient().subscribe.return_value = mock_consumer

        ####### Run runner
        runner = eodhp_utils.runner.Runner(
            {"tst": mock.MagicMock()}, "test-subscription", takeover_mode=True
        )
        runner.run(max_loops=4)

        ####### Check behaviour
        # Four takeover messages should have been sent.
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

        mock_consumer.pause_message_listener.assert_not_called()
        mock_consumer.resume_message_listener.assert_not_called()


def test_takeover_results_in_pause():
    # Tests that, when a takeover happens, other consumers pause message reception.
    with (
        mock.patch("eodhp_utils.runner.get_pulsar_client") as mock_getclient,
        mock.patch("eodhp_utils.runner.time.sleep"),
    ):
        ####### Setup
        mock_consumer = mock.MagicMock(name="consumer")
        mock_getclient().subscribe.return_value = mock_consumer

        mock_takeover_message = mock.MagicMock(name="takeover-message")
        mock_takeover_message.topic_name.return_value = f"x/{eodhp_utils.runner.DEBUG_TOPIC}"
        mock_takeover_message.data.return_value = b'{"suspend_subscription": "test-subscription"}'
        mock_takeover_message.publish_timestamp.return_value = time.time() * 1000 + 1000

        ####### Create runner and give it a takeover message
        runner = eodhp_utils.runner.Runner({"tst": mock_consumer}, "test-subscription")
        runner._listener(mock.MagicMock(name="takeover consumer"), mock_takeover_message)

        mock_consumer.pause_message_listener.assert_called_once()
        mock_consumer.resume_message_listener.assert_not_called()
        assert runner._suspended_until > time.time()

        ######## Simulate time passing
        runner._suspended_until = time.time() - 10
        runner.run(max_loops=1)

        mock_consumer.resume_message_listener.assert_called_once()


def test_baggage_propagated_across_call():
    ######### Setup - get a simulated set of properties which would be attached to a Pulsar msg.
    tracer = trace.get_tracer(__name__)
    with tracer.start_as_current_span("harvester_span"):
        attach(set_baggage("test", "testval"))
        props = {}
        inject(props)

    mock_message = mock.MagicMock(name="takeover-message")
    mock_message.properties.return_value = props
    mock_message.topic_name.return_value = "x/test-topic"

    class MessagerBaggageTester(Messager[str, bytes]):
        def process_msg(self, msg: str) -> Sequence[Messager.Action]:
            self.baggage_value = get_baggage("test")
            return []

        def gen_empty_catalogue_message(self, msg: str) -> dict:
            return {}

    msgr = MessagerBaggageTester()

    ######### Test
    runner = eodhp_utils.runner.Runner({"test-topic": msgr}, "test-subscription")
    runner._listener(mock.MagicMock(), mock_message)

    ###### Check result
    assert msgr.baggage_value == "testval"


class IterMessagerTester(Messager[Iterator[str], bytes]):
    messages_received: list[list[str]] = []
    thread_ids: set[int]

    def __init__(self, *args, **kwargs):
        self.messages_received = []
        self.thread_ids = set()
        super().__init__(*args, **kwargs)

    def process_msg(self, msg: Iterator[str]) -> Sequence[Messager.Action]:
        msg_list = list(msg)
        self.messages_received.append(msg_list)
        self.thread_ids.add(threading.get_ident())

        # This ensures we need the number of threads the test asks for.
        time.sleep(0.01)

        if 666 in msg_list:
            raise ValueError()

        return []

    def gen_empty_catalogue_message(self, msg: str) -> dict:
        return {}


@pytest.mark.parametrize(
    "length, batch_size, expected",
    [
        pytest.param(5, 1, [[0], [1], [2], [3], [4]]),
        pytest.param(6, 2, [[0, 1], [2, 3], [4, 5]]),
        pytest.param(7, 2, [[0, 1], [2, 3], [4, 5], [6]]),
        pytest.param(2, 8, [[0, 1]]),
        pytest.param(0, 1, []),
        pytest.param(0, 10, []),
    ],
)
def test_generatorrunner_runs_messager_with_single_thread(length, batch_size, expected):
    messager = IterMessagerTester()

    gr = eodhp_utils.runner.GeneratorRunner[int, int](messager, batch_size=batch_size)

    failures = gr.consume(iter(range(length)))

    assert messager.messages_received == expected
    assert not failures.any_permanent()
    assert not failures.any_temporary()
    assert (not messager.thread_ids and not expected) or messager.thread_ids == {
        threading.get_ident(),
    }


@pytest.mark.parametrize(
    "length, batch_size, expected",
    [
        pytest.param(5, 1, [[0], [1], [2], [3], [4]]),
        pytest.param(6, 2, [[0, 1], [2, 3], [4, 5]]),
        pytest.param(7, 2, [[0, 1], [2, 3], [4, 5], [6]]),
        pytest.param(2, 8, [[0, 1]]),
        pytest.param(0, 1, []),
        pytest.param(0, 10, []),
    ],
)
def test_generatorrunner_runs_messager_with_multiple_threads(length, batch_size, expected):
    for threads in range(1, 10):
        messager = IterMessagerTester()

        gr = eodhp_utils.runner.GeneratorRunner[int, int](
            messager, batch_size=batch_size, threads=threads
        )

        failures = gr.consume(iter(range(length)))

        assert messager.messages_received == expected
        assert not failures.any_permanent()
        assert not failures.any_temporary()

        expected_threads = min(threads, ceil(length / batch_size))
        assert len(messager.thread_ids) == expected_threads


def test_generatorrunner_handles_errors():
    for threads in range(1, 10):
        messager = IterMessagerTester()

        gr = eodhp_utils.runner.GeneratorRunner[int, int](messager, batch_size=4, threads=threads)

        failures = gr.consume([1] * 5 + [666] + [2] * 5)

        assert messager.messages_received == [[1, 1, 1, 1], [1, 666, 2, 2], [2, 2, 2]]
        assert failures.any_permanent()
        assert not failures.any_temporary()
