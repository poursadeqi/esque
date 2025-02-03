import base64
import datetime
import json
from dataclasses import dataclass
from enum import Enum
from typing import IO, Any, Dict, List, NoReturn, Optional, Union

from rich.console import Console

from esque.io.exceptions import EsqueIOHandlerReadException, EsqueIOSerializerConfigNotSupported
from esque.io.handlers.base import BaseHandler
from esque.io.messages import BinaryMessage, MessageHeader, PrintableMessage
from esque.io.stream_events import PermanentEndOfStream, StreamEvent


class ByteEncoding(Enum):
    BASE64 = "base64"
    UTF_8 = "utf-8"


@dataclass()
class PipeHandlerConfig:
    file: Optional[IO[str]]
    key_encoding: Union[str, ByteEncoding] = ByteEncoding.UTF_8.value
    value_encoding: Union[str, ByteEncoding] = ByteEncoding.UTF_8.value
    pretty_print: bool = False

    def _validate_fields(self) -> List[str]:
        problems = []
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

    def get_serializer_configs(self) -> NoReturn:
        raise EsqueIOSerializerConfigNotSupported

    def put_serializer_configs(self, config: Dict[str, Any]) -> NoReturn:
        raise EsqueIOSerializerConfigNotSupported

    def write_message(self, message: Union[PrintableMessage, StreamEvent]) -> None:
        if isinstance(message, StreamEvent):
            return
        self._console.print_json(
            json.dumps(
                {
                    "key": message.key.payload,
                    "value": message.value.payload,
                    "partition": message.partition,
                    "offset": message.offset,
                    "timestamp": message.timestamp.isoformat(),
                    "headers": [{"key": h.key, "value": h.value} for h in message.headers],
                }
            ),
            indent=2 if self.config.pretty_print else None,
        )

    def read_message(self) -> Union[StreamEvent, BinaryMessage]:
        while True:
            msg = self._next_message()
            if isinstance(msg, StreamEvent) or msg.offset >= self._left_bound:
                return msg

    def _next_message(self) -> Union[StreamEvent, BinaryMessage]:
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
        return BinaryMessage(
            key=extract(deserialized_object.get("key"), key_encoding),
            value=extract(deserialized_object.get("value"), value_encoding),
            offset=deserialized_object.get("offset", -1),
            partition=deserialized_object.get("partition", -1),
            timestamp=datetime.datetime.fromtimestamp(
                deserialized_object.get("timestamp", 0), tz=datetime.timezone.utc
            ),
            headers=[MessageHeader(h["key"], h.get("value")) for h in deserialized_object.get("headers", [])],
        )

    def seek(self, position: int):
        self._left_bound = position

    def close(self) -> None:
        pass  # stdin or stdout don't have to be closed


def extract(input_value: Optional[str], encoding: Union[str, ByteEncoding]) -> Optional[bytes]:
    encoding = ByteEncoding(encoding)

    if input_value is None:
        return None
    if encoding == ByteEncoding.UTF_8:
        return input_value.encode(encoding="UTF-8")
    elif encoding == ByteEncoding.BASE64:
        return base64.b64decode(input_value.encode(encoding="UTF-8"))
