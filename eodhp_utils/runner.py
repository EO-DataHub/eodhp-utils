import os

from pulsar import Client, ConsumerDeadLetterPolicy, ConsumerType


def run(messagers_dict: dict, subscription_name: str):

    pulsar_url = os.environ.get("PULSAR_URL")
    client = Client(pulsar_url)

    topics = list(messagers_dict.keys())

    max_redelivery_count = 3
    delay_ms = 30000

    consumer = client.subscribe(
        topic=topics,
        subscription_name=subscription_name,
        consumer_type=ConsumerType.Shared,
        dead_letter_policy=ConsumerDeadLetterPolicy(
            max_redeliver_count=max_redelivery_count,
            dead_letter_topic=f"dead-letter-annotations-ingester",  # noqa:F541
        ),
        negative_ack_redelivery_delay_ms=delay_ms,
    )

    while True:
        pulsar_message = consumer.receive()

        topic_name = pulsar_message.topic_name().split("/")[-1]

        messager = messagers_dict[topic_name]

        failures = messager.process_update(pulsar_message)

        if failures.any_temporary():
            pulsar_message.negative_acknowledge()
        elif failures.any_permanent():
            pass
        else:
            pulsar_message.acknowledge()
