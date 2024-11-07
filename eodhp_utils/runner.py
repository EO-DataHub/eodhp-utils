import os

from pulsar import Client, ConsumerDeadLetterPolicy, ConsumerType


def run(messagers_dict: dict, subscription_name: str):
    """Run loop to monitor arrival of pulsar messages on a given topic.

    Example usage:
    annotations_messager = AnnotationsMessager(s3_client=s3_client, output_bucket=destination_bucket)
    run(
        {
            "transformed-annotations": annotations_messager
        },
        "annotations-ingester",
    )
    """

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

        failures = messager.consume(pulsar_message)

        if failures.any_temporary():
            consumer.negative_acknowledge(pulsar_message)
        elif failures.any_permanent():
            pass
        else:
            consumer.acknowledge(pulsar_message)
