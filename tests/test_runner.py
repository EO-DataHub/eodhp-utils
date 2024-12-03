import os
from argparse import Action
from typing import Sequence
from unittest import mock

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
