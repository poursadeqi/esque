import dataclasses
import datetime
import random
from string import ascii_letters
from typing import Any, Dict, List, Optional, Tuple

from pytest_cases import fixture

from esque.io.handlers.base import BaseHandler
from esque.io.messages import Message, MessageHeader
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
        self._messages: List[Optional[Message]] = []
        self._serializer_configs: Tuple[Dict[str, Any], Dict[str, Any]] = ({}, {})
        self._peof_counter = 0
        self._left_bound = 0

    def get_serializer_configs(self) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        return self._serializer_configs

    def put_serializer_configs(self, configs: Tuple[Dict[str, Any], Dict[str, Any]]) -> None:
        self._serializer_configs = configs

    def write_message(self, stream_event: StreamEvent) -> None:
        if isinstance(stream_event, StreamEvent):
            return
        self._messages.append(stream_event)

    def read_message(self) -> StreamEvent:
        while True:
            event = self._next_message()
            if event.message is None or event.message.offset >= self._left_bound:
                return event

    def _next_message(self) -> StreamEvent:
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

    def get_messages(self) -> List[Message]:
        return self._messages.copy()

    def set_messages(self, messages: List[Message]):
        self._messages = messages.copy()

    def insert_temporary_end_of_stream(self, position: int):
        self._messages.insert(position, None)

    @classmethod
    def create_default(cls) -> "DummyHandler":
        return cls(config=DummyHandlerConfig())

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
def messages() -> List[Message]:
    return [
        Message(
            key="foo1",
            value="bar1",
            partition=0,
            offset=0,
            timestamp=datetime.datetime(year=2021, month=1, day=1, hour=0, minute=0, tzinfo=datetime.timezone.utc),
            headers=[MessageHeader("a", "b")],
        ),
        Message(
            key="foo2",
            value="bar2",
            partition=0,
            offset=1,
            timestamp=datetime.datetime(year=2021, month=1, day=1, hour=0, minute=1, tzinfo=datetime.timezone.utc),
            headers=[MessageHeader("c", None)],
        ),
        Message(
            key="foo3",
            value="bar3",
            partition=1,
            offset=0,
            timestamp=datetime.datetime(year=2021, month=1, day=1, hour=0, minute=2, tzinfo=datetime.timezone.utc),
            headers=[],
        ),
        Message(
            key="foo4",
            value="bar4",
            partition=1,
            offset=1,
            timestamp=datetime.datetime(year=2021, month=1, day=1, hour=0, minute=3, tzinfo=datetime.timezone.utc),
            headers=[],
        ),
        Message(
            key="foo5",
            value="bar5",
            partition=1,
            offset=2,
            timestamp=datetime.datetime(year=2021, month=1, day=1, hour=0, minute=4, tzinfo=datetime.timezone.utc),
            headers=[],
        ),
        Message(
            key="foo6",
            value="bar6",
            partition=1,
            offset=3,
            timestamp=datetime.datetime(year=2021, month=1, day=1, hour=0, minute=5, tzinfo=datetime.timezone.utc),
            headers=[],
        ),
    ]


@fixture(scope="session")
def no_data() -> None:
    return None


@fixture()
def partition_count(binary_messages) -> int:
    # partitions are 0-based, so add 1 to get the actual amount of partitions
    return max(m.partition for m in binary_messages) + 1


@fixture()
def string_messages(messages: List[Message], string_message_serializer: MessageSerializer) -> List[Message]:
    return list(string_message_serializer.deserialize(msg) for msg in messages)


@fixture()
def string_serializer() -> StringSerializer:
    return StringSerializer(StringSerializerConfig())


@fixture()
def string_message_serializer(string_serializer: StringSerializer) -> MessageSerializer:
    return MessageSerializer(key=string_serializer, value=string_serializer)


class DummyMessageReader(HandlerSerializerMessageReader):
    _handler: DummyHandler

    def __init__(self):
        super().__init__(
            handler=DummyHandler(config=DummyHandlerConfig()),
        )

    def set_messages(self, messages: List[Message]) -> None:
        self._handler.set_messages(messages)


@fixture
def dummy_message_reader() -> DummyMessageReader:
    return DummyMessageReader()


class DummyMessageWriter(HandlerSerializerMessageWriter):
    _handler: DummyHandler

    def __init__(self):
        super().__init__(
            handler=DummyHandler(config=DummyHandlerConfig()),
        )

    def get_written_messages(self) -> List[Message]:
        return self._handler.get_messages()


@fixture
def dummy_message_writer() -> DummyMessageWriter:
    return DummyMessageWriter()


@fixture
def prepared_builder(
        dummy_message_reader: DummyMessageReader,
        dummy_message_writer: DummyMessageWriter,
        binary_messages: List[Message],
) -> PipelineBuilder:
    builder = PipelineBuilder()
    builder.with_message_reader(dummy_message_reader)
    builder.with_message_writer(dummy_message_writer)
    dummy_message_reader.set_messages(binary_messages)

    return builder
