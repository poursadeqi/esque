import base64
import dataclasses
import datetime as dt
import json
from typing import Any, Optional

from esque.io.messages import MessagePayload
from esque.io.serializers import SerializerConfig
from esque.io.serializers.base import DataSerializer


@dataclasses.dataclass()
class JsonSerializerConfig(SerializerConfig):
    indent: Optional[str] = None
    encoding: str = "UTF-8"


# TODO: implement solution to handle data types when they are known
class JsonSerializer(DataSerializer[JsonSerializerConfig]):
    config_cls = JsonSerializerConfig

    def serialize(self, data: MessagePayload) -> Optional[bytes]:
        indent = None
        if self.config.indent is not None:
            indent = int(self.config.indent)
        return json.dumps(data.payload, indent=indent, default=self.field_serializer).encode(
            encoding=self.config.encoding
        )

    def deserialize(self, raw_data: Optional[bytes]) -> MessagePayload:
        if raw_data is None:
            return MessagePayload()
        return MessagePayload(payload=json.loads(raw_data.decode(self.config.encoding)))

    def field_serializer(self, data: Any) -> str:
        if isinstance(data, (dt.datetime, dt.date, dt.time)):
            return data.isoformat()
        if isinstance(data, bytes):
            return base64.b64encode(data).decode(self.config.encoding)
        raise TypeError(f"Object of type {type(data).__name__} is not JSON serializable")
