import functools
from abc import ABC, abstractmethod
from contextlib import closing
from dataclasses import dataclass
from typing import Callable, Iterable, List, Optional

from esque.io.exceptions import EsqueIOInvalidPipelineBuilderState
from esque.io.handlers.base import BaseHandler
from esque.io.messages import Message
from esque.io.serializers.base import MessageSerializer
from esque.io.stream_decorators import stop_after_nth_message
from esque.io.stream_events import StreamEvent


class MessageReader(ABC):
    @abstractmethod
    def stream(self) -> Iterable[Message]:
        raise NotImplementedError

    @abstractmethod
    def seek(self, position: int) -> None:
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError


class MessageWriter(ABC):
    @abstractmethod
    def write_many_messages(self, message_stream: Iterable[StreamEvent]):
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError


class HandlerSerializerMessageReader(MessageReader):
    _handler: BaseHandler
    _message_serializer: MessageSerializer

    def __init__(self, handler: BaseHandler):
        self._handler = handler

    def stream(self) -> Iterable[StreamEvent]:
        return self._handler.stream()

    def seek(self, position: int):
        self._handler.seek(position)

    def close(self):
        self._handler.close()


class HandlerSerializerMessageWriter(MessageWriter):
    _handler: BaseHandler
    _message_serializer: MessageSerializer

    def __init__(self, handler: BaseHandler):
        self._handler = handler

    def write_message(self, stream_event: StreamEvent):
        self._handler.write_message(stream_event=stream_event)

    def write_many_messages(self, message_stream: Iterable[StreamEvent]):
        self._handler.write_many_messages(message_stream=message_stream)

    def close(self):
        self._handler.close()


class Pipeline:
    _input_element: MessageReader
    _output_element: MessageWriter
    _stream_decorators: List[Callable[[Iterable], Iterable]]

    def __init__(
            self,
            input_element: MessageReader,
            output_element: MessageWriter,
            stream_decorators: List[Callable[[Iterable], Iterable]],
    ):
        self._input_element = input_element
        self._output_element = output_element
        self._stream_decorators = stream_decorators

    def message_stream(self) -> Iterable:
        return self._input_element.stream()

    def decorated_message_stream(self) -> Iterable:
        stream = self.message_stream()
        for decorator in self._stream_decorators:
            stream = decorator(stream)
        return stream

    def run_pipeline(self):
        with closing(self):
            self._output_element.write_many_messages(self.decorated_message_stream())

    def write_many_messages(self, message_stream: Iterable[StreamEvent]):
        self._output_element.write_many_messages(message_stream=message_stream)

    def close(self) -> None:
        self._input_element.close()
        self._output_element.close()

class PipelineBuilder:
    _input_handler: Optional[BaseHandler] = None
    _input_serializer: Optional[MessageSerializer] = None
    _message_reader: Optional[MessageReader] = None

    _output_handler: Optional[BaseHandler] = None
    _output_serializer: Optional[MessageSerializer] = None
    _message_writer: Optional[MessageWriter] = None

    _stream_decorators: List[Callable[[Iterable], Iterable]]
    _start: Optional[int] = None
    _errors: List[str]

    def __init__(self):
        """
        Creates a new PipelineBuilder. In case no methods are called other than :meth:`PipelineBuilder.build()`, the created pipeline
        will have a pair of console handlers (stdin and stdout) and UTF-8 string message serializers.
        """
        self._stream_decorators = []
        self._errors = []
        self._start: Optional[int] = None
        self._limit: Optional[int] = None

    def with_input_handler(self, handler: BaseHandler) -> "PipelineBuilder":
        if handler is not None:
            self._input_handler = handler
        return self

    def with_output_handler(self, handler: BaseHandler) -> "PipelineBuilder":
        if handler is not None:
            self._output_handler = handler
        return self

    def with_stream_decorator(self, decorator: Callable[[Iterable], Iterable]) -> "PipelineBuilder":
        if decorator is not None:
            self._stream_decorators.append(decorator)
        return self

    @functools.cached_property
    def _pipeline(self) -> Pipeline:
        message_reader = self._build_message_reader()
        message_writer = self._build_message_writer()
        self._add_limit_decorator()
        if self._errors:
            raise EsqueIOInvalidPipelineBuilderState(
                "Errors while building pipeline object:\n" + "\n".join(self._errors)
            )

        return Pipeline(message_reader, message_writer, self._stream_decorators)

    def _build_message_reader(self) -> Optional[MessageReader]:
        message_reader = HandlerSerializerMessageReader(self._input_handler)
        if self._start is not None:
            message_reader.seek(self._start)
        return message_reader

    def _build_message_writer(self) -> Optional[MessageWriter]:
        return HandlerSerializerMessageWriter(self._output_handler)

    def with_range(self, start: Optional[int] = None, limit: Optional[int] = None):
        self._start = start
        self._limit = limit

    def _add_limit_decorator(self):
        if self._limit is not None:
            self.with_stream_decorator(stop_after_nth_message(self._limit))

    def build(self) -> Pipeline:
        return self._pipeline
