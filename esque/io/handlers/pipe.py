import datetime
import json
from dataclasses import dataclass
from typing import IO, Any, Dict, Optional, Union

from rich.console import Console

from esque.io.exceptions import EsqueIOHandlerReadException
from esque.io.handlers.base import BaseHandler
from esque.io.handlers.base_config import BaseHandlerConfig
from esque.io.messages import MessageHeader, PrintableMessage
from esque.io.stream_events import PermanentEndOfStream, StreamEvent


@dataclass()
class PipeHandlerConfig(BaseHandlerConfig):
    file: Optional[IO[str]] = None
    pretty_print: bool = False


class PipeHandler(BaseHandler):
    def __init__(self, config: PipeHandlerConfig):
        super().__init__()
        self.config = config
        self._console = Console(file=config.file)
        self._left_bound = -1

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

    def read_message(self) -> Union[StreamEvent, PrintableMessage]:
        while True:
            msg = self._next_message()
            if isinstance(msg, StreamEvent) or msg.offset >= self._left_bound:
                return msg

    def _next_message(self) -> Union[StreamEvent, PrintableMessage]:
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

        return PrintableMessage(
            key=self.config.read_serializer.key.deserialize(deserialized_object.get("key")),
            value=self.config.read_serializer.value.deserialize(deserialized_object.get("value")),
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
