import os
import threading
import time
from collections.abc import Iterator, Sequence
from math import ceil
from pathlib import Path
from typing import cast
from unittest import mock

import pulsar
import pytest
from botocore.client import BaseClient
from opentelemetry import trace
from opentelemetry.baggage import get_baggage, set_baggage
from opentelemetry.context import attach
from opentelemetry.propagate import inject

import eodhp_utils
import eodhp_utils.runner
from eodhp_utils import runner
from eodhp_utils.messagers import CatalogueChangeMessager, Messager

PULSAR_ENV_VARS = (
    "PULSAR_TOKEN_FILE",
    "PULSAR_TOKEN",
    "PULSAR_DEBUG_TOPIC",
    "PULSAR_TAKEOVER_ENABLED",
    "PULSAR_DEAD_LETTER_TOPIC",
)


@pytest.fixture(autouse=True)
def clear_pulsar_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in PULSAR_ENV_VARS:
        monkeypatch.delenv(name, raising=False)


class MessagerTester(Messager[str, bytes]):
    def __init__(
        self,
        s3_client: BaseClient | None = None,
        output_bucket: str | None = None,
        cat_output_prefix: str = "",
        producer: pulsar.Producer | None = None,
    ) -> None:
        self.messages_received: list[str] = []
        super().__init__(s3_client, output_bucket, cat_output_prefix, producer)

    def process_msg(self, msg: str) -> Sequence[Messager.Action]:
        self.messages_received.append(msg)

        if msg == "EXIT":
            raise KeyboardInterrupt()

        return []

    def gen_empty_catalogue_message(self, msg: str) -> dict:
        return {}


def test_s3_session_uses_supplied_key() -> None:
    runner.aws_client = None

    with mock.patch.dict(os.environ, {"AWS_ACCESS_KEY": "ACCESSKEY", "AWS_SECRET_ACCESS_KEY": "SECKEY"}):
        sess = runner.get_boto3_session()

        assert sess.get_credentials().secret_key == "SECKEY"
        assert sess.get_credentials().access_key == "ACCESSKEY"


def test_messagers_given_messages() -> None:
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


def test_setup_logging_doesnt_error() -> None:
    eodhp_utils.runner.setup_logging(0)
    eodhp_utils.runner.setup_logging(1)
    eodhp_utils.runner.setup_logging(2)
    eodhp_utils.runner.setup_logging(3)
    eodhp_utils.runner.setup_logging(4)


def test_pulsar_client_uses_arg_over_env_when_set() -> None:
    with (
        mock.patch("eodhp_utils.runner.Client") as pulsar_client,
        mock.patch.dict(os.environ, {"PULSAR_URL": "pulsar://example.com/2"}),
    ):
        eodhp_utils.runner.get_pulsar_client("pulsar://example.com/1")

        eodhp_utils.runner.pulsar_client = None
        eodhp_utils.runner.get_pulsar_client()

        pulsar_client.assert_has_calls(
            (
                mock.call("pulsar://example.com/1", message_listener_threads=1, io_threads=1, authentication=None),
                mock.call("pulsar://example.com/2", message_listener_threads=1, io_threads=1, authentication=None),
            )
        )


def test_takeover_sends_takeover_messages() -> None:
    # Tests that, in takover mode, we send a takeover message every 2.5S.
    with (
        mock.patch("eodhp_utils.runner.get_pulsar_client") as mock_getclient,
        mock.patch("eodhp_utils.runner.time.sleep"),
    ):
        ####### Setup
        mock_consumer = mock.MagicMock(name="consumer")
        mock_getclient().subscribe.return_value = mock_consumer

        ####### Run runner
        runner = eodhp_utils.runner.Runner({"tst": mock.MagicMock()}, "test-subscription", takeover_mode=True)
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


def test_takeover_results_in_pause() -> None:
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


def test_pulsar_authentication_is_none_without_token() -> None:
    assert eodhp_utils.runner.pulsar_authentication() is None


def test_pulsar_authentication_uses_token_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PULSAR_TOKEN", " token-from-env\n")

    with mock.patch("eodhp_utils.runner.AuthenticationToken") as auth_token:
        auth = eodhp_utils.runner.pulsar_authentication()

    auth_token.assert_called_once_with("token-from-env")
    assert auth is auth_token.return_value


def test_pulsar_authentication_rereads_token_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    token_file = tmp_path / "token"
    token_file.write_text("first-token\n")
    monkeypatch.setenv("PULSAR_TOKEN_FILE", str(token_file))
    monkeypatch.setenv("PULSAR_TOKEN", "ignored-when-file-set")

    with mock.patch("eodhp_utils.runner.AuthenticationToken") as auth_token:
        eodhp_utils.runner.pulsar_authentication()

    supplier = auth_token.call_args.args[0]
    assert callable(supplier)
    assert supplier() == "first-token"

    token_file.write_text("rotated-token\n")
    assert supplier() == "rotated-token"


def test_token_file_supplier_returns_empty_token_when_file_unreadable(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    token_file = tmp_path / "token"
    token_file.write_text("a-token")
    monkeypatch.setenv("PULSAR_TOKEN_FILE", str(token_file))

    with mock.patch("eodhp_utils.runner.AuthenticationToken") as auth_token:
        eodhp_utils.runner.pulsar_authentication()

    supplier = auth_token.call_args.args[0]
    token_file.unlink()

    assert supplier() == ""
    assert "Could not read Pulsar token file" in caplog.text


def test_pulsar_authentication_fails_at_startup_if_token_file_missing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("PULSAR_TOKEN_FILE", str(tmp_path / "missing"))

    with pytest.raises(FileNotFoundError):
        eodhp_utils.runner.pulsar_authentication()


def test_pulsar_authentication_fails_at_startup_if_token_file_empty(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    token_file = tmp_path / "token"
    token_file.write_text("\n")
    monkeypatch.setenv("PULSAR_TOKEN_FILE", str(token_file))

    with pytest.raises(ValueError, match="empty"):
        eodhp_utils.runner.pulsar_authentication()


def test_pulsar_client_library_accepts_token_supplier(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    token_file = tmp_path / "token"
    token_file.write_text("a-token")
    monkeypatch.setenv("PULSAR_TOKEN_FILE", str(token_file))

    assert isinstance(eodhp_utils.runner.pulsar_authentication(), pulsar.AuthenticationToken)


def test_pulsar_client_gets_authentication(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PULSAR_TOKEN", "a-token")
    monkeypatch.setattr(eodhp_utils.runner, "pulsar_client", None)

    with mock.patch("eodhp_utils.runner.Client") as pulsar_client:
        eodhp_utils.runner.get_pulsar_client("pulsar://example.com/1")

    assert isinstance(pulsar_client.call_args.kwargs["authentication"], pulsar.AuthenticationToken)


def _message_on(topic: str) -> mock.MagicMock:
    msg = mock.MagicMock(name=f"message on {topic}")
    msg.topic_name.return_value = topic
    return msg


def _subscribed_topics(mock_getclient: mock.MagicMock) -> list[str]:
    return [c.kwargs["topic"] for c in mock_getclient().subscribe.call_args_list]


def test_messager_with_fully_qualified_topic_gets_messages() -> None:
    with mock.patch("eodhp_utils.runner.get_pulsar_client"):
        messager = mock.MagicMock(name="messager")
        messager.consume.return_value.any_temporary.return_value = False

        runner = eodhp_utils.runner.Runner({"persistent://public/billing/billing-events": messager}, "sub")
        msg = _message_on("persistent://public/billing/billing-events")
        runner._listener(mock.MagicMock(), msg)

        messager.consume.assert_called_once_with(msg)


def test_same_messager_under_two_topic_names_reads_both() -> None:
    with mock.patch("eodhp_utils.runner.get_pulsar_client") as mock_getclient:
        messager = mock.MagicMock(name="messager")
        messager.consume.return_value.any_temporary.return_value = False

        runner = eodhp_utils.runner.Runner(
            {"billing-events": messager, "persistent://public/billing/billing-events": messager}, "sub"
        )

        assert _subscribed_topics(mock_getclient) == [
            "billing-events",
            "persistent://public/billing/billing-events",
        ]

        old_msg = _message_on("persistent://public/default/billing-events")
        new_msg = _message_on("persistent://public/billing/billing-events")
        runner._listener(mock.MagicMock(), old_msg)
        runner._listener(mock.MagicMock(), new_msg)

        assert messager.consume.call_args_list == [mock.call(old_msg), mock.call(new_msg)]


def test_topics_with_same_short_name_route_by_namespace() -> None:
    with mock.patch("eodhp_utils.runner.get_pulsar_client"):
        default_messager = mock.MagicMock(name="default messager")
        billing_messager = mock.MagicMock(name="billing messager")

        runner = eodhp_utils.runner.Runner(
            {"billing-events": default_messager, "public/billing/billing-events": billing_messager}, "sub"
        )
        runner._listener(mock.MagicMock(), _message_on("persistent://public/default/billing-events"))
        runner._listener(mock.MagicMock(), _message_on("persistent://public/billing/billing-events"))

        default_messager.consume.assert_called_once()
        billing_messager.consume.assert_called_once()


def test_partitioned_topic_messages_are_routed() -> None:
    with mock.patch("eodhp_utils.runner.get_pulsar_client"):
        messager = mock.MagicMock(name="messager")

        runner = eodhp_utils.runner.Runner({"test-topic": messager}, "sub")
        runner._listener(mock.MagicMock(), _message_on("persistent://public/default/test-topic-partition-3"))

        messager.consume.assert_called_once()


def test_message_on_unknown_topic_raises() -> None:
    with mock.patch("eodhp_utils.runner.get_pulsar_client"):
        runner = eodhp_utils.runner.Runner({"test-topic": mock.MagicMock()}, "sub")

        with pytest.raises(KeyError):
            runner._listener(mock.MagicMock(), _message_on("persistent://public/default/other-topic"))


def test_debug_topic_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    debug_topic = "persistent://public/billing/eodhp-utils-debugging"
    monkeypatch.setenv("PULSAR_DEBUG_TOPIC", debug_topic)

    with (
        mock.patch("eodhp_utils.runner.get_pulsar_client") as mock_getclient,
        mock.patch("eodhp_utils.runner.time.sleep"),
    ):
        mock_consumer = mock.MagicMock(name="consumer")
        mock_getclient().subscribe.return_value = mock_consumer

        runner = eodhp_utils.runner.Runner({"tst": mock.MagicMock()}, "test-subscription")
        runner.run(max_loops=1)
        assert _subscribed_topics(mock_getclient) == ["tst", debug_topic]

        # The debug topic in the default namespace is no longer special.
        default_ns_msg = _message_on(f"persistent://public/default/{eodhp_utils.runner.DEBUG_TOPIC}")
        with pytest.raises(KeyError):
            runner._listener(mock.MagicMock(), default_ns_msg)
        mock_consumer.pause_message_listener.assert_not_called()

        takeover_msg = _message_on(debug_topic)
        takeover_msg.data.return_value = b'{"suspend_subscription": "test-subscription"}'
        takeover_msg.publish_timestamp.return_value = time.time() * 1000 + 1000
        runner._listener(mock.MagicMock(), takeover_msg)
        mock_consumer.pause_message_listener.assert_called_once()


def test_takeover_mode_sends_to_debug_topic_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PULSAR_DEBUG_TOPIC", "persistent://public/billing/eodhp-utils-debugging")

    with (
        mock.patch("eodhp_utils.runner.get_pulsar_client") as mock_getclient,
        mock.patch("eodhp_utils.runner.time.sleep"),
    ):
        runner = eodhp_utils.runner.Runner({"tst": mock.MagicMock()}, "test-subscription", takeover_mode=True)
        runner.run(max_loops=1)

        assert (
            mock_getclient().create_producer.call_args.kwargs["topic"]
            == "persistent://public/billing/eodhp-utils-debugging"
        )


@pytest.mark.parametrize("value", ["false", "False", "0", "no", "off"])
def test_takeover_subscription_can_be_disabled(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    monkeypatch.setenv("PULSAR_TAKEOVER_ENABLED", value)

    with (
        mock.patch("eodhp_utils.runner.get_pulsar_client") as mock_getclient,
        mock.patch("eodhp_utils.runner.time.sleep"),
    ):
        mock_consumer = mock.MagicMock(name="consumer")
        mock_getclient().subscribe.return_value = mock_consumer

        runner = eodhp_utils.runner.Runner({"tst": mock.MagicMock()}, "test-subscription")
        runner.run(max_loops=1)
        assert _subscribed_topics(mock_getclient) == ["tst"]

        takeover_msg = _message_on(f"persistent://public/default/{eodhp_utils.runner.DEBUG_TOPIC}")
        with pytest.raises(KeyError):
            runner._listener(mock.MagicMock(), takeover_msg)
        mock_consumer.pause_message_listener.assert_not_called()


@pytest.mark.parametrize(
    ("env_value", "expected"),
    [
        (None, "dead-letter-test-subscription"),
        ("persistent://public/billing/dead-letter-billing", "persistent://public/billing/dead-letter-billing"),
    ],
)
def test_dead_letter_topic(monkeypatch: pytest.MonkeyPatch, env_value: str | None, expected: str) -> None:
    if env_value is not None:
        monkeypatch.setenv("PULSAR_DEAD_LETTER_TOPIC", env_value)

    with mock.patch("eodhp_utils.runner.get_pulsar_client") as mock_getclient:
        eodhp_utils.runner.Runner({"tst": mock.MagicMock()}, "test-subscription", threads=2)

        policies = [c.kwargs["dead_letter_policy"] for c in mock_getclient().subscribe.call_args_list]
        assert len(policies) == 2
        assert all(p.dead_letter_topic == expected for p in policies)


def test_baggage_propagated_across_call() -> None:
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
    runner = eodhp_utils.runner.Runner({"test-topic": cast(CatalogueChangeMessager, msgr)}, "test-subscription")
    runner._listener(mock.MagicMock(), mock_message)

    ###### Check result
    assert msgr.baggage_value == "testval"


class IterMessagerTester(Messager[Iterator[int], bytes]):
    thread_ids: set[int]

    def __init__(
        self,
        s3_client: BaseClient | None = None,
        output_bucket: str | None = None,
        cat_output_prefix: str = "",
        producer: pulsar.Producer | None = None,
    ) -> None:
        self.messages_received: list[list[int]] = []
        self.thread_ids = set()
        super().__init__(s3_client, output_bucket, cat_output_prefix, producer)

    def process_msg(self, msg: Iterator[int]) -> Sequence[Messager.Action]:
        msg_list = list(msg)
        self.messages_received.append(msg_list)
        self.thread_ids.add(threading.get_ident())

        # This ensures we need the number of threads the test asks for.
        time.sleep(0.01)

        if 666 in msg_list:
            raise ValueError()

        return []

    def gen_empty_catalogue_message(self, msg: Iterator[int]) -> dict:
        return {}


@pytest.mark.parametrize(
    ("length", "batch_size", "expected"),
    [
        pytest.param(5, 1, [[0], [1], [2], [3], [4]]),
        pytest.param(6, 2, [[0, 1], [2, 3], [4, 5]]),
        pytest.param(7, 2, [[0, 1], [2, 3], [4, 5], [6]]),
        pytest.param(2, 8, [[0, 1]]),
        pytest.param(0, 1, []),
        pytest.param(0, 10, []),
    ],
)
def test_generatorrunner_runs_messager_with_single_thread(
    length: int, batch_size: int, expected: list[list[int]]
) -> None:
    messager = IterMessagerTester()

    gr = eodhp_utils.runner.GeneratorRunner[int, bytes](messager, batch_size=batch_size)

    failures = gr.consume(iter(range(length)))

    assert messager.messages_received == expected
    assert not failures.any_permanent()
    assert not failures.any_temporary()
    assert (not messager.thread_ids and not expected) or messager.thread_ids == {
        threading.get_ident(),
    }


@pytest.mark.parametrize(
    ("length", "batch_size", "expected"),
    [
        pytest.param(5, 1, [[0], [1], [2], [3], [4]]),
        pytest.param(6, 2, [[0, 1], [2, 3], [4, 5]]),
        pytest.param(7, 2, [[0, 1], [2, 3], [4, 5], [6]]),
        pytest.param(2, 8, [[0, 1]]),
        pytest.param(0, 1, []),
        pytest.param(0, 10, []),
    ],
)
def test_generatorrunner_runs_messager_with_multiple_threads(
    length: int, batch_size: int, expected: list[list[int]]
) -> None:
    for threads in range(1, 10):
        messager = IterMessagerTester()

        gr = eodhp_utils.runner.GeneratorRunner[int, bytes](messager, batch_size=batch_size, threads=threads)

        failures = gr.consume(iter(range(length)))

        assert messager.messages_received == expected
        assert not failures.any_permanent()
        assert not failures.any_temporary()

        expected_threads = min(threads, ceil(length / batch_size))
        assert len(messager.thread_ids) == expected_threads


def test_generatorrunner_handles_errors() -> None:
    for threads in range(1, 10):
        messager = IterMessagerTester()

        gr = eodhp_utils.runner.GeneratorRunner[int, bytes](messager, batch_size=4, threads=threads)

        failures = gr.consume(iter([1] * 5 + [666] + [2] * 5))

        assert messager.messages_received == [[1, 1, 1, 1], [1, 666, 2, 2], [2, 2, 2]]
        assert failures.any_permanent()
        assert not failures.any_temporary()
