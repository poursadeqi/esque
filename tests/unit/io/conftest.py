import dataclasses
import datetime
import random
from string import ascii_letters
from typing import Any, Dict, List, Optional, Tuple, Union

from pytest_cases import fixture

from esque.io.handlers.base import BaseHandler
from esque.io.messages import WritableMessage, MessageHeader, PrintableMessagePayload, PrintableMessage
from esque.io.pipeline import HandlerSerializerMessageReader, HandlerSerializerMessageWriter, PipelineBuilder
from esque.io.serializers.base import MessageSerializer
from esque.io.serializers.string import StringSerializer, StringSerializerConfig
from esque.io.stream_events import PermanentEndOfStream, StreamEvent, TemporaryEndOfPartition


@dataclasses.dataclass()
class DummyHandlerConfig:
    pass


class DummyHandler(BaseHandler):
    def __init__(self, config: DummyHandlerConfig):
        super.__init__()
        self.config = config
        self._messages: List[Optional[WritableMessage]] = []
        self._serializer_configs: Tuple[Dict[str, Any], Dict[str, Any]] = ({}, {})
        self._peof_counter = 0
        self._left_bound = 0

    def get_serializer_configs(self) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        return self._serializer_configs

    def put_serializer_configs(self, configs: Tuple[Dict[str, Any], Dict[str, Any]]) -> None:
        self._serializer_configs = configs

    def write_message(self, stream_event: Union[WritableMessage, StreamEvent]) -> None:
        if isinstance(stream_event, StreamEvent):
            return
        self._messages.append(stream_event)

    def read_message(self) -> Union[WritableMessage, StreamEvent]:
        while True:
            msg = self._next_message()
            if isinstance(msg, StreamEvent) or msg.offset >= self._left_bound:
                return msg

    def _next_message(self) -> Union[StreamEvent, WritableMessage]:
        if self._messages:
            elem = self._messages.pop(0)
            if elem is None:
                return TemporaryEndOfPartition("Temporary end of stream")
            return elem
        else:
            if self._peof_counter == 10:
                raise RuntimeError(
                    "This is the tenth time a permanent end of stream is returned. Do you have an endless loop?"
                )
            self._peof_counter += 1
            return PermanentEndOfStream("No messages left in memory")

    def get_messages(self) -> List[WritableMessage]:
        return self._messages.copy()

    def set_messages(self, messages: List[WritableMessage]):
        self._messages = messages.copy()

    def insert_temporary_end_of_stream(self, position: int):
        self._messages.insert(position, None)

    @classmethod
    def create_default(cls) -> "DummyHandler":
        return cls(config=DummyHandlerConfig(host="", path="", scheme="dummy"))

    def seek(self, position: int):
        self._left_bound = position

    def close(self) -> None:
        pass  # nothing to do


@fixture
def topic_id() -> str:
    return "".join(random.choices(ascii_letters, k=5))


@fixture
def dummy_handler() -> DummyHandler:
    return DummyHandler.create_default()


@fixture()
def binary_messages() -> List[WritableMessage]:
    return [
        WritableMessage(
            key=b"foo1",
            value=b"bar1",
            partition=0,
            offset=0,
            timestamp=datetime.datetime(year=2021, month=1, day=1, hour=0, minute=0, tzinfo=datetime.timezone.utc),
            headers=[MessageHeader("a", "b")],
        ),
        WritableMessage(
            key=b"foo2",
            value=b"bar2",
            partition=0,
            offset=1,
            timestamp=datetime.datetime(year=2021, month=1, day=1, hour=0, minute=1, tzinfo=datetime.timezone.utc),
            headers=[MessageHeader("c", None)],
        ),
        WritableMessage(
            key=b"foo3",
            value=b"bar3",
            partition=1,
            offset=0,
            timestamp=datetime.datetime(year=2021, month=1, day=1, hour=0, minute=2, tzinfo=datetime.timezone.utc),
            headers=[],
        ),
        WritableMessage(
            key=b"foo4",
            value=b"bar4",
            partition=1,
            offset=1,
            timestamp=datetime.datetime(year=2021, month=1, day=1, hour=0, minute=3, tzinfo=datetime.timezone.utc),
            headers=[],
        ),
        WritableMessage(
            key=b"foo5",
            value=b"bar5",
            partition=1,
            offset=2,
            timestamp=datetime.datetime(year=2021, month=1, day=1, hour=0, minute=4, tzinfo=datetime.timezone.utc),
            headers=[],
        ),
        WritableMessage(
            key=b"foo6",
            value=b"bar6",
            partition=1,
            offset=3,
            timestamp=datetime.datetime(year=2021, month=1, day=1, hour=0, minute=5, tzinfo=datetime.timezone.utc),
            headers=[],
        ),
    ]


@fixture()
def printable_messages() -> List[PrintableMessage]:
    return [
        PrintableMessage(
            key=PrintableMessagePayload(payload="foo1"),
            value=PrintableMessagePayload(payload="bar1"),
            partition=0,
            offset=0,
            timestamp=datetime.datetime(year=2021, month=1, day=1, hour=0, minute=0, tzinfo=datetime.timezone.utc),
            headers=[MessageHeader("a", "b")],
        ),
        PrintableMessage(
            key=PrintableMessagePayload(payload="foo2"),
            value=PrintableMessagePayload(payload="bar2"),
            partition=0,
            offset=1,
            timestamp=datetime.datetime(year=2021, month=1, day=1, hour=0, minute=1, tzinfo=datetime.timezone.utc),
            headers=[MessageHeader("c", None)],
        ),
        PrintableMessage(
            key=PrintableMessagePayload(payload="foo3"),
            value=PrintableMessagePayload(payload="bar3"),
            partition=1,
            offset=0,
            timestamp=datetime.datetime(year=2021, month=1, day=1, hour=0, minute=2, tzinfo=datetime.timezone.utc),
            headers=[],
        ),
        PrintableMessage(
            key=PrintableMessagePayload(payload="foo4"),
            value=PrintableMessagePayload(payload="bar4"),
            partition=1,
            offset=1,
            timestamp=datetime.datetime(year=2021, month=1, day=1, hour=0, minute=3, tzinfo=datetime.timezone.utc),
            headers=[],
        ),
        PrintableMessage(
            key=PrintableMessagePayload(payload="foo5"),
            value=PrintableMessagePayload(payload="bar5"),
            partition=1,
            offset=2,
            timestamp=datetime.datetime(year=2021, month=1, day=1, hour=0, minute=4, tzinfo=datetime.timezone.utc),
            headers=[],
        ),
        PrintableMessage(
            key=PrintableMessagePayload(payload="foo6"),
            value=PrintableMessagePayload(payload="bar6"),
            partition=1,
            offset=3,
            timestamp=datetime.datetime(year=2021, month=1, day=1, hour=0, minute=5, tzinfo=datetime.timezone.utc),
            headers=[],
        ),
    ]


@fixture(scope="session")
def no_data() -> PrintableMessagePayload:
    return PrintableMessagePayload()


@fixture()
def partition_count(binary_messages) -> int:
    # partitions are 0-based, so add 1 to get the actual amount of partitions
    return max(m.partition for m in binary_messages) + 1


@fixture()
def string_messages(
    binary_messages: List[WritableMessage], string_message_serializer: MessageSerializer
) -> List[PrintableMessage]:
    return list(string_message_serializer.deserialize_many(binary_messages))


@fixture()
def string_serializer() -> StringSerializer:
    return StringSerializer(StringSerializerConfig(scheme="str"))


@fixture()
def string_message_serializer(string_serializer: StringSerializer) -> MessageSerializer:
    return MessageSerializer(string_serializer)


class DummyMessageReader(HandlerSerializerMessageReader):
    _handler: DummyHandler

    def __init__(self):
        super().__init__(
            handler=DummyHandler(config=DummyHandlerConfig(host="", path="", scheme="")),
            message_serializer=MessageSerializer(StringSerializer(StringSerializerConfig(scheme="str"))),
        )

    def set_messages(self, messages: List[WritableMessage]) -> None:
        self._handler.set_messages(messages)


@fixture
def dummy_message_reader() -> DummyMessageReader:
    return DummyMessageReader()


class DummyMessageWriter(HandlerSerializerMessageWriter):
    _handler: DummyHandler

    def __init__(self):
        super().__init__(
            handler=DummyHandler(config=DummyHandlerConfig(host="", path="", scheme="")),
            message_serializer=MessageSerializer(StringSerializer(StringSerializerConfig(scheme="str"))),
        )

    def get_written_messages(self) -> List[WritableMessage]:
        return self._handler.get_messages()


@fixture
def dummy_message_writer() -> DummyMessageWriter:
    return DummyMessageWriter()


@fixture
def prepared_builder(
    dummy_message_reader: DummyMessageReader,
    dummy_message_writer: DummyMessageWriter,
    binary_messages: List[WritableMessage],
) -> PipelineBuilder:
    builder = PipelineBuilder()
    builder.with_message_reader(dummy_message_reader)
    builder.with_message_writer(dummy_message_writer)
    dummy_message_reader.set_messages(binary_messages)

    return builder
