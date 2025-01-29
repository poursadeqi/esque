import dataclasses
import importlib
from typing import Optional, Type

from esque.io.data_types import UnknownDataType
from esque.io.messages import Data
from esque.io.serializers import SerializerConfig
from esque.io.serializers.base import DataSerializer
from google.protobuf.json_format import MessageToDict
from google.protobuf.message import Message
import sys


@dataclasses.dataclass
class ProtoSerializerConfig(SerializerConfig):
    protoc_py_path: str
    module_name: str
    class_name: str
    indent: Optional[str] = None
    encoding: str = "UTF-8"

    def __post_init__(self):
        self._message_class = self._load_message_class()

    def _load_message_class(self) -> Type[Message]:
        """Dynamically loads the protobuf message class."""
        sys.path.append(self.protoc_py_path)
        module = importlib.import_module(self.module_name)
        return getattr(module, self.class_name)

    def get_message_class(self) -> Type[Message]:
        return self._message_class


class ProtoSerializer(DataSerializer[ProtoSerializerConfig]):
    config_cls = ProtoSerializerConfig
    unknown_data_type: UnknownDataType = UnknownDataType()

    def serialize(self, data: Data) -> Optional[bytes]:
        """Serializes the Data object into bytes."""
        return data.payload if data.payload is not None else data.NO_DATA

    def deserialize(self, raw_data: Optional[bytes]) -> Data:
        """Deserializes raw bytes into a Data object."""
        if raw_data is None:
            return Data.NO_DATA

        message = self.config.get_message_class()()
        message.ParseFromString(raw_data)

        payload = MessageToDict(message, preserving_proto_field_name=True)
        return Data(payload=payload, data_type=self.unknown_data_type)
