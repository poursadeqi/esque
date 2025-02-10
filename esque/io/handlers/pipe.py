import base64
import datetime
import json
from dataclasses import dataclass
from enum import Enum
from typing import IO, Any, Dict, Optional, Union

from rich.console import Console

from esque.io.exceptions import EsqueIOHandlerReadException
from esque.io.handlers.base import BaseHandler
from esque.io.handlers.base_config import BaseHandlerConfig
from esque.io.messages import Message, MessageHeader
from esque.io.stream_events import PermanentEndOfStream, StreamEvent, StoppableEvent


class ByteEncoding(Enum):
    BASE64 = "base64"
    UTF_8 = "utf-8"
    HEX = "hex"


@dataclass()
class PipeHandlerConfig(BaseHandlerConfig):
    key_encoding: Union[str, ByteEncoding] = ByteEncoding.UTF_8.value
    value_encoding: Union[str, ByteEncoding] = ByteEncoding.UTF_8.value
    file: Optional[IO[str]] = None
    pretty_print: bool = False

    def _validate_fields(self) -> List[str]:
        problems = super()._validate_fields()
        try:
            ByteEncoding(self.key_encoding)
        except ValueError:
            problems.append(
                f"Invalid value for key_encoding: {self.key_encoding!r}. Valid values are: {', '.join(ByteEncoding)}"
            )

        try:
            ByteEncoding(self.value_encoding)
        except ValueError:
            problems.append(
                f"Invalid value for value_encoding: {self.value_encoding!r}. Valid values are: {', '.join(ByteEncoding)}"
            )

        return problems


class PipeHandler(BaseHandler):
    def __init__(self, config: PipeHandlerConfig):
        super().__init__()
        self.config = config
        self._console = Console(file=config.file)
        self._left_bound = -1

    def write_message(self, event: StreamEvent) -> None:
        if not event.message:
            return
        self._console.print_json(
            json.dumps(
                {
                    "key": self.config.write_serializer.key.serialize(
                        event.message.key, event.message.key_version
                    ),
                    "value": self.config.write_serializer.value.serialize(
                        event.message.value, event.message.value_version
                    ),
                    "partition": event.message.partition,
                    "offset": event.message.offset,
                    "timestamp": event.message.timestamp.timestamp(),
                    "timestamp_iso": event.message.timestamp.isoformat(),
                    "headers": [{"key": h.key, "value": h.value} for h in event.message.headers],
                    "key_version": event.message.key_version,
                    "value_version": event.message.value_version,
                    "keyenc": str(self.config.key_encoding),
                    "valueenc": str(self.config.value_encoding),
                }
            ),
            indent=2 if self.config.pretty_print else None,
        )

    def read_stream_event(self) -> StreamEvent:
        while True:
            event = self._next_message()
            if isinstance(event, StoppableEvent) or event.message.offset >= self._left_bound:
                return event

    def _next_message(self) -> StreamEvent:
        line = ""
        while not line.strip():
            line = self.config.file.readline()
            if line == "":
                return PermanentEndOfStream("End of pipe reached")

        try:
            deserialized_object: Dict[str, Any] = json.loads(line)
        except ValueError as e:
            raise EsqueIOHandlerReadException(
                "Error parsing JSON object from input. "
                f"Make sure json objects are single-line and not pretty printed. Original Error: {e}"
            )
        key_encoding = deserialized_object.get("keyenc", self.config.key_encoding)
        value_encoding = deserialized_object.get("valueenc", self.config.value_encoding)
        return StreamEvent(
            Message(
                key=extract(deserialized_object.get("key"), key_encoding),
                value=extract(deserialized_object.get("value"), value_encoding),
                offset=deserialized_object.get("offset", -1),
                partition=deserialized_object.get("partition", -1),
                timestamp=datetime.datetime.fromtimestamp(
                    deserialized_object.get("timestamp", 0), tz=datetime.timezone.utc
                ),
                headers=[MessageHeader(h["key"], h.get("value")) for h in deserialized_object.get("headers", [])],
                key_version=deserialized_object.get("key_version"),
                value_version=deserialized_object.get("value_version"),
            )
        )

    def seek(self, position: int):
        self._left_bound = position

    def close(self) -> None:
        pass  # stdin or stdout don't have to be closed


def embed(input_value: Optional[bytes], encoding: Union[str, ByteEncoding]) -> Any:
    encoding = ByteEncoding(encoding)

    if input_value is None:
        return None
    if encoding == ByteEncoding.UTF_8:
        return input_value.decode(encoding="UTF-8")
    elif encoding == ByteEncoding.BASE64:
        return base64.b64encode(input_value).decode(encoding="UTF-8")
    elif encoding == ByteEncoding.HEX:
        return input_value.hex()


def extract(input_value: Optional[str], encoding: Union[str, ByteEncoding]) -> Optional[bytes]:
    encoding = ByteEncoding(encoding)

    if input_value is None:
        return None
    if encoding == ByteEncoding.UTF_8:
        return input_value.encode(encoding="UTF-8")
    elif encoding == ByteEncoding.BASE64:
        return base64.b64decode(input_value.encode(encoding="UTF-8"))
    elif encoding == ByteEncoding.HEX:
        return bytes.fromhex(input_value)
