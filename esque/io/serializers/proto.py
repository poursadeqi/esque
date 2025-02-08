import dataclasses
import importlib
import sys
from typing import Optional, Type, Any, Union

from google.protobuf.json_format import MessageToDict
from google.protobuf.message import Message

from esque.io.messages import PrimaryTypes
from esque.io.serializers.base import DataSerializer


@dataclasses.dataclass
class ProtoSerializerConfig:
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


class ProtoSerializer(DataSerializer):
    def __init__(self, config: ProtoSerializerConfig):
        self.config = config

    def serialize(self, data: PrimaryTypes) -> Optional[bytes]:
        raise NotImplementedError

    def deserialize(self, raw_data: Optional[bytes]) -> Union[dict, None]:
        if raw_data is None:
            return None

        message = self.config.get_message_class()()
        message.ParseFromString(raw_data)
        return MessageToDict(message, preserving_proto_field_name=True, always_print_fields_with_no_presence=True)
