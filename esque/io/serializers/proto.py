import dataclasses
import importlib
import sys
from typing import Optional, Type

from google.protobuf.json_format import MessageToDict
from google.protobuf.message import Message

from esque.io.data_types import UnknownDataType
from esque.io.messages import MessagePayload
from esque.io.serializers import SerializerConfig
from esque.io.serializers.base import DataSerializer


@dataclasses.dataclass
class ProtoSerializerConfig(SerializerConfig):
    protoc_py_path: str
    module_name: str
    class_name: str

    def __post_init__(self):
        self._message_class = self._load_message_class()

    def _load_message_class(self) -> Type[Message]:
        sys.path.append(self.protoc_py_path)
        module = importlib.import_module(self.module_name)
        return getattr(module, self.class_name)

    def get_message_class(self) -> Type[Message]:
        return self._message_class


class ProtoSerializer(DataSerializer[ProtoSerializerConfig]):
    config_cls = ProtoSerializerConfig
    unknown_data_type: UnknownDataType = UnknownDataType()

    def serialize(self, data: MessagePayload) -> Optional[bytes]:
        raise NotImplementedError

    def deserialize(self, raw_data: Optional[bytes]) -> MessagePayload:
        if raw_data is None:
            return MessagePayload()

        message = self.config.get_message_class()()
        message.ParseFromString(raw_data)

        payload = MessageToDict(message, preserving_proto_field_name=True)
        return MessagePayload(payload=payload)
