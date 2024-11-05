import os

from pulsar import Client


def run(messagers_dict: dict, subscription_name: str):

    pulsar_url = os.environ.get("PULSAR_URL")
    client = Client(pulsar_url)

    topics = list(messagers_dict.keys())

    consumer = client.subscribe(topic=topics, subscription_name=subscription_name)

    while True:
        pulsar_message = consumer.receive()

        topic_name = pulsar_message.topic_name().split("/")[-1]

        messager = messagers_dict[topic_name]

        failures = messager.consume(pulsar_message)

        if failures.any_temporary():
            pulsar_message.negative_acknowledge()
        elif failures.any_permanent():
            pass
        else:
            pulsar_message.acknowledge()
