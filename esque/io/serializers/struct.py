import dataclasses
from struct import unpack
from typing import Optional

from esque.io.data_types import UnknownDataType
from esque.io.messages import MessagePayload
from esque.io.serializers.base import DataSerializer, SerializerConfig


@dataclasses.dataclass()
class StructSerializerConfig(SerializerConfig):
    struct_format: str


class StructSerializer(DataSerializer):
    config_cls = StructSerializerConfig
    unknown_data_type: UnknownDataType = UnknownDataType()

    def deserialize(self, raw_data: Optional[bytes]) -> MessagePayload:
        if raw_data is None:
            return MessagePayload()
        output = unpack(self.config.struct_format, raw_data)[0]
        return MessagePayload(payload=output)

    def serialize(self, data: MessagePayload) -> str:
        if data.is_empty():
            return ""
        # if not isinstance(data.payload, bytes):
        #     raise TypeError(f"Data payload has to be bytes, not {type(data.payload).__name__}!")
        return data.payload
