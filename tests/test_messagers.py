from argparse import Action
from typing import Sequence

from eodhp_utils.messagers import Messager, TemporaryFailure

SOURCE_PATH = "https://example.link.for.test/"
TARGET = "/target_directory/"
OUTPUT_ROOT = "https://output.root.test"


def test_messages_delivered_to_messager_subclass():
    messages_received = []

    class TestMessager(Messager[str]):
        def process_msg(self, msg: str) -> Sequence[Action]:
            messages_received.append(msg)
            return []

        def gen_catalogue_message(self, msg: str, added_keys, updated_keys, deleted_keys) -> dict:
            return {}

    testmessager = TestMessager(None, "testbucket", "testprefix")

    testmessager.consume("string-1")
    testmessager.consume("string-2")

    assert messages_received == ["string-1", "string-2"]


def test_temporary_or_permanent_failure_from_subclass_recorded_in_result():
    class TestMessager(Messager[str]):
        def process_msg(self, msg: str) -> Sequence[Action]:
            if msg == "temp":
                raise TemporaryFailure("process_msg")
            elif msg == "perm":
                raise Exception("process_msg")
            else:
                return []

        def gen_catalogue_message(self, msg: str, added_keys, updated_keys, deleted_keys) -> dict:
            return {}

    testmessager = TestMessager(None, "testbucket", "testprefix")

    assert testmessager.consume("temp") == Messager.Failures(permanent=False, temporary=True)
    assert testmessager.consume("perm") == Messager.Failures(permanent=True, temporary=False)
    assert testmessager.consume("") == Messager.Failures(permanent=False, temporary=False)
