import base64
import dataclasses
import datetime as dt
import json
from typing import Any, Optional, Union

from esque.io.messages import PrimaryTypes
from esque.io.serializers.base import DataSerializer


@dataclasses.dataclass()
class JsonSerializerConfig:
    indent: Optional[str] = None
    encoding: str = "UTF-8"


# TODO: implement solution to handle data types when they are known
class JsonSerializer(DataSerializer):
    def __init__(self, config: JsonSerializerConfig):
        super().__init__()
        self.config = config

    def serialize(self, data: PrimaryTypes, **kwargs) -> Optional[str]:
        if data is None:
            return None
        indent = None
        if self.config.indent is not None:
            indent = int(self.config.indent)
        return json.dumps(data, indent=indent, default=self.field_serializer)

    def deserialize(self, raw_data: Optional[bytes]) -> Union[dict, None]:
        if raw_data is None:
            return None
        return json.loads(raw_data.decode(self.config.encoding))

    def field_serializer(self, data: Any) -> str:
        if isinstance(data, (dt.datetime, dt.date, dt.time)):
            return data.isoformat()
        if isinstance(data, bytes):
            return base64.b64encode(data).decode(self.config.encoding)
        raise TypeError(f"Object of type {type(data).__name__} is not JSON serializable")
