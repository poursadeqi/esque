import itertools
from typing import List, Optional, Type
from unittest import mock
from unittest.mock import Mock

from confluent_kafka import Consumer, KafkaError, Producer
from confluent_kafka.admin import ClusterMetadata, TopicMetadata
from pytest_cases import fixture

from esque.io.handlers.kafka import KafkaHandler, KafkaHandlerConfig
from esque.io.messages import Message
from esque.io.stream_events import StreamEvent, TemporaryEndOfPartition


@fixture(autouse=True)
def consumer_cls_mock(topic_id: str, messages: List[Message]):
    with mock.patch("esque.io.handlers.kafka.Consumer", autospec=True) as mocked_cls:
        mocked_instance = mocked_cls({})
        cluster_meta = generate_value_for_list_topics(messages, topic_id)
        mocked_instance.list_topics.return_value = cluster_meta
        yield mocked_cls


def generate_value_for_list_topics(messages: List[Message], topic_id: str) -> ClusterMetadata:
    cluster_meta = ClusterMetadata()
    topic_meta = TopicMetadata()
    topic_meta.partitions = {msg.partition: None for msg in messages}
    cluster_meta.topics = {topic_id: topic_meta}
    return cluster_meta


@fixture(autouse=True)
def producer_cls_mock():
    with mock.patch("esque.io.handlers.kafka.Producer", autospec=True) as mocked_cls:
        yield mocked_cls


@fixture(params=[True, False], ids=["SEND_TIMESTAMP", "NO_SEND_TIMESTAMP"])
def kafka_handler(unittest_config, topic_id: str, request):
    return KafkaHandler(
        KafkaHandlerConfig(
            context="docker",
            topic=topic_id,
            consumer_group_id="test_consumer",
            send_timestamp=request.param,
        )
    )


def test_write_single_message(
    producer_cls_mock: Type[Producer],
    event_stream_messages: List[StreamEvent],
    kafka_handler: KafkaHandler,
    topic_id: str,
):
    event = event_stream_messages[0]
    message = event.message
    kafka_handler.write_message(event)

    producer_mock: Producer = producer_cls_mock(config={})
    producer_mock.produce.assert_called_once_with(
        key=message.key,
        value=message.value,
        topic=topic_id,
        partition=message.partition,
        timestamp=int(message.timestamp.timestamp() * 1000) if kafka_handler.config.send_timestamp else 0,
        headers=(
            [(h.key, h.value.encode("utf-8") if h.value is not None else None) for h in message.headers]
            if message.headers
            else None
        ),
        on_delivery=kafka_handler._delivery_callback,
    )
    producer_mock.flush.assert_called_once()


def test_write_many_messages(
    producer_cls_mock: Type[Producer],
    event_stream_messages: List[StreamEvent],
    kafka_handler: KafkaHandler,
    topic_id: str,
):
    kafka_handler.write_many_messages(event_stream_messages)

    producer_mock: Producer = producer_cls_mock(config={})
    for event in event_stream_messages:
        message = event.message
        producer_mock.produce.assert_any_call(
            key=message.key,
            value=message.value,
            topic=topic_id,
            partition=message.partition,
            timestamp=int(message.timestamp.timestamp() * 1000) if kafka_handler.config.send_timestamp else 0,
            headers=(
                [(h.key, h.value.encode("utf-8") if h.value is not None else None) for h in message.headers]
                if message.headers
                else None
            ),
            on_delivery=kafka_handler._delivery_callback,
        )
    producer_mock.flush.assert_called_once()


def test_read_message(
    event_stream_messages: List[StreamEvent],
    consumer_cls_mock: Type[Consumer],
    topic_id: str,
    kafka_handler: KafkaHandler,
):
    message = event_stream_messages[0].message
    confluent_message = message_to_confluent_message(message, topic_id)
    consumer_mock = consumer_cls_mock({})
    consumer_mock.poll.return_value = confluent_message

    assert kafka_handler.read_stream_event().message == message


def test_read_many_messages(
    event_stream_messages: List[StreamEvent],
    consumer_cls_mock: Type[Consumer],
    topic_id: str,
    kafka_handler: KafkaHandler,
):
    confluent_messages = [message_to_confluent_message(event.message, topic_id) for event in event_stream_messages]
    consumer_mock = consumer_cls_mock({})
    consumer_mock.poll.side_effect = confluent_messages

    # make sure message_stream doesn't yield less than len(binary_message) items
    message_stream = itertools.chain(kafka_handler.message_stream(), itertools.repeat(None))
    for expected_message, actual_message in zip(event_stream_messages, message_stream):
        assert expected_message.message == actual_message.message


def test_temporary_end_of_stream_events_non_streaming(
    event_stream_messages: List[StreamEvent],
    consumer_cls_mock: Type[Consumer],
    topic_id: str,
    kafka_handler: KafkaHandler,
):
    partitions = set(event.message.partition for event in event_stream_messages)
    consumer_mock = consumer_cls_mock({})

    poll_return_values: List[Optional[Mock]] = [
        confluent_eof_message(topic_id, partition, offset=0) for partition in partitions
    ]
    poll_return_values.append(None)
    consumer_mock.poll.side_effect = poll_return_values

    for partition_id in partitions:
        stream_event = kafka_handler.read_stream_event()
        assert isinstance(stream_event, TemporaryEndOfPartition)
        assert stream_event.partition == partition_id

    stream_event = kafka_handler.read_stream_event()
    assert isinstance(stream_event, TemporaryEndOfPartition)
    assert stream_event.partition == TemporaryEndOfPartition.ALL_PARTITIONS


def test_temporary_end_of_stream_events_streaming(
    event_stream_messages: List[StreamEvent],
    consumer_cls_mock: Type[Consumer],
    topic_id: str,
    kafka_handler: KafkaHandler,
):
    partitions = set(event.message.partition for event in event_stream_messages)
    consumer_mock = consumer_cls_mock({})

    poll_return_values: List[Optional[Mock]] = [
        confluent_eof_message(topic_id, partition, offset=0) for partition in partitions
    ]
    poll_return_values.append(None)
    consumer_mock.poll.side_effect = poll_return_values

    stream_iterator = iter(kafka_handler.message_stream())

    for partition_id in partitions:
        stream_event = next(stream_iterator)
        assert isinstance(stream_event, TemporaryEndOfPartition)
        assert stream_event.partition == partition_id

    stream_event = next(stream_iterator)
    assert isinstance(stream_event, TemporaryEndOfPartition)
    assert stream_event.partition == TemporaryEndOfPartition.ALL_PARTITIONS


def confluent_eof_message(topic_id: str, partition: int, offset: int):
    confluent_message = Mock()
    confluent_message.key.return_value = None
    confluent_message.value.return_value = None
    confluent_message.topic.return_value = topic_id
    confluent_message.partition.return_value = partition
    confluent_message.offset.return_value = offset
    confluent_message.headers.return_value = None
    confluent_message.timestamp.return_value = None

    error_mock = Mock()
    error_mock.code.return_value = KafkaError._PARTITION_EOF
    confluent_message.error.return_value = error_mock
    return confluent_message


def message_to_confluent_message(message: Message, topic_id: str):
    confluent_message = Mock()
    confluent_message.key.return_value = message.key
    confluent_message.value.return_value = message.value
    confluent_message.topic.return_value = topic_id
    confluent_message.partition.return_value = message.partition
    confluent_message.offset.return_value = message.offset
    confluent_message.error.return_value = None

    if not message.headers:
        confluent_message.headers.return_value = None
    else:
        confluent_message.headers.return_value = [
            (h.key, h.value.encode("utf-8") if h.value is not None else None) for h in message.headers
        ]

    confluent_message.timestamp.return_value = (0, int(message.timestamp.timestamp() * 1000))
    return confluent_message
