import dataclasses
import json
import importlib
from types import ModuleType
from typing import Any, Optional

from esque.io.data_types import NoData, UnknownDataType
from esque.io.messages import Data
from esque.io.serializers import SerializerConfig
from esque.io.serializers.base import DataSerializer
from google.protobuf.json_format import MessageToDict
import sys


@dataclasses.dataclass
class ProtoSerializerConfig(SerializerConfig):
    _message_class: object = dataclasses.field(init=False)
    protoc_py_path: str
    module_name: str
    class_name: str
    indent: Optional[str] = None
    encoding: str = "UTF-8"

    def __post_init__(self):
        sys.path.append(self.protoc_py_path)
        module = importlib.import_module(self.module_name)
        self._message_class = getattr(module, self.class_name)

    def get_message_class(self) -> object:
        return self._message_class


# TODO: implement solution to handle data types when they are known
class ProtoSerializer(DataSerializer[ProtoSerializerConfig]):
    config_cls = ProtoSerializerConfig
    unknown_data_type: UnknownDataType = UnknownDataType()

    def serialize(self, data: Data) -> Optional[bytes]:
        if data.payload is None:
            return data.NO_DATA
        return data.payload

    def deserialize(self, raw_data: Optional[bytes]) -> Data:
        if raw_data is None:
            return Data.NO_DATA

        message = self.config.get_message_class()()
        message.ParseFromString(raw_data)

        data = MessageToDict(message, preserving_proto_field_name=True)
        return Data(payload=(json.dumps(data)).encode(self.config.encoding), data_type=self.unknown_data_type)
