import dataclasses
from struct import unpack, pack
from typing import Optional

from esque.io.messages import MessagePayload
from esque.io.serializers.base import DataSerializer


@dataclasses.dataclass()
class StructSerializerConfig:
    deserializer_struct_format: str = dataclasses.field(init=False)
    serializer_struct_format: str = dataclasses.field(init=False)


class StructSerializer(DataSerializer):
    def __init__(self, config: StructSerializerConfig):
        self.config = config

    def deserialize(self, raw_data: Optional[bytes]) -> MessagePayload:
        if raw_data is None:
            return MessagePayload()
        output = unpack(self.config.deserializer_struct_format, raw_data)[0]
        return MessagePayload(payload=output)

    def serialize(self, data: MessagePayload) -> MessagePayload:
        if data.is_empty():
            return MessagePayload(b"")
        if not isinstance(data.payload, bytes):
            raise TypeError(f"Data payload must be bytes or bytearray, not {type(data.payload).__name__}!")
        return MessagePayload(pack(self.config.serializer_struct_format, data.payload))
