import json
import os

from pulsar import Client as PulsarClient


class Client:
    def __init__(self, url=None):
        if url is None:
            url = os.environ.get("PULSAR_URL")
        self.client = PulsarClient(url)


class Producer(Client):
    def __init__(self, topic, producer):
        super().__init__()

        self.producer = self.client.create_producer(topic=topic, producer_name=producer)

    def send(self, message: bytes):
        """Send a message. preferred format is bytes but also converts str and dict"""
        if isinstance(message, dict):
            message = (json.dumps(message)).encode("utf-8")
        elif isinstance(message, str):
            message = message.encode("utf-8")

        return self.producer.send(message)

    def close(self):
        """Close client and producer"""
        self.producer.close()
        self.client.close()


class Consumer(Client):
    def __init__(self, topic, subscription):
        super().__init__()

        self.consumer = self.client.subscribe(topic=topic, subscription_name=subscription)

    def receive(self, timeout: int = None):
        """Receive next message"""
        return self.consumer.receive(timeout)

    def acknowledge(self, message):
        """Acknowledge a given message"""
        return self.consumer.acknowledge(message)

    def close(self):
        """Close client and consumer"""
        self.consumer.close()
        self.client.close()
