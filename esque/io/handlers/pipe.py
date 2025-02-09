import datetime
import json
from dataclasses import dataclass
from typing import IO, Any, Dict, Optional

from rich.console import Console

from esque.io.exceptions import EsqueIOHandlerReadException
from esque.io.handlers.base import BaseHandler
from esque.io.handlers.base_config import BaseHandlerConfig
from esque.io.messages import Message, MessageHeader
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

    def write_message(self, event: StreamEvent) -> None:
        if not event.message:
            return
        self._console.print_json(
            json.dumps(
                {
                    "key": self.config.write_serializer.key.serialize(
                        event.message.key,
                        event.message.key_version),
                    "value": self.config.write_serializer.value.serialize(event.message.value,
                                                                          event.message.value_version),
                    "partition": event.message.partition,
                    "offset": event.message.offset,
                    "timestamp": event.message.timestamp.timestamp(),
                    "timestamp_iso": event.message.timestamp.isoformat(),
                    "headers": [{"key": h.key, "value": h.value} for h in event.message.headers],
                    "key_version": event.message.key_version,
                    "value_version": event.message.value_version,
                }
            ),
            indent=2 if self.config.pretty_print else None,
        )

    def read_message(self) -> StreamEvent:
        while True:
            event = self._next_message()
            if isinstance(event, StreamEvent) or event.offset >= self._left_bound:
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

        return StreamEvent(
            Message(
                key=self.config.read_serializer.key.deserialize(deserialized_object.get("key")),
                value=self.config.read_serializer.value.deserialize(deserialized_object.get("value")),
                offset=deserialized_object.get("offset", -1),
                partition=deserialized_object.get("partition", -1),
                timestamp=datetime.datetime.fromtimestamp(
                    deserialized_object.get("timestamp", 0), tz=datetime.timezone.utc
                ),
                headers=[MessageHeader(h["key"], h.get("value")) for h in deserialized_object.get("headers", [])],
                key_version=deserialized_object.get("key_version"),
                value_version=deserialized_object.get("value_version")
            )
        )

    def seek(self, position: int):
        self._left_bound = position

    def close(self) -> None:
        pass  # stdin or stdout don't have to be closed
