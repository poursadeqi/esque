from abc import ABC, abstractmethod
from typing import Iterable, TypeVar, Union

from esque.io.messages import PrintableMessage
from esque.io.stream_events import PermanentEndOfStream, StreamEvent

H = TypeVar("H", bound="BaseHandler")


class BaseHandler(ABC):
    def __init__(self):
        """
        Base class for all Esque IO handlers. A handler is responsible for writing and reading messages
        to and from a source. The handler is unaware of the underlying message's format and treats all
        sources as binary. It may support persisting the serializer config for easier data retrieval.
        """

    @abstractmethod
    def seek(self, position: int):
        """
        Seek the handler's reading position to the given offset for all partitions. The next message that is read will
        be at this offset if it is still available and after this offset if and only if it is not available anymore.

        :param position: The offset to seek to
        """
        raise NotImplementedError

    @abstractmethod
    def write_message(self, printable_message: StreamEvent) -> None:
        """
        Write the message from `binary_message` to this handler's source.
        The handler may choose which action to take upon receiving any :class:`StreamEvent`
        instances but mostly the appropriate action is to just ignore them.

        :param printable_message: The message that is supposed to be written.
        """
        raise NotImplementedError

    def write_many_messages(self, message_stream: Iterable[Union[PrintableMessage, StreamEvent]]) -> None:
        """
        Write all messages from the iterable `message_stream` to this handler's source.
        The handler may choose which action to take upon receiving any :class:`StreamEvent`
        instances but mostly the appropriate action is to just ignore them.

        :param message_stream: The messages that are supposed to be written.
        """
        for message in message_stream:
            self.write_message(message)

    @abstractmethod
    def read_message(self) -> Union[PrintableMessage, StreamEvent]:
        """
        Read the next :class:`BinaryMessage` from this handler's source.
        Returns an object of :class:`StreamEvent` to indicate certain events that may happen while reading from the
        source.
        For example if the handler has reached a permanent end, like the end of a file or a closed stream, then
        it will return a :class:`PermanentEndOfStream` object.
        If the handler has reached a temporary end (e.g. the end of a topic was reached but new messages might come in
        at some point) then it will return an object of :class:`TemporaryEndOfStream`.
        Both of these classes are subclasses of :class:`EndOfStream`.

        :return: The next message from this handler's source, or a stream event.
        :raises EsqueIOHandlerReadException: When there was a failure accessing the source. Like a broken pipe.
        """
        raise NotImplementedError

    def stream(self) -> Iterable[Union[PrintableMessage, StreamEvent]]:
        while True:
            msg = self.read_message()
            yield msg
            if isinstance(msg, PermanentEndOfStream):
                break

    @abstractmethod
    def close(self) -> None:
        """
        Close all resources that have been opened by this handler.
        """
        raise NotImplementedError
