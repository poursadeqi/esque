import dataclasses
from struct import pack, unpack
from typing import Optional, Union

from esque.io.messages import PrimaryTypes
from esque.io.serializers.base import DataSerializer


@dataclasses.dataclass()
class StructSerializerConfig:
    deserializer_struct_format: str = None
    serializer_struct_format: str = None


class StructSerializer(DataSerializer):
    def __init__(self, config: StructSerializerConfig):
        self.config = config

    def deserialize(self, raw_data: Optional[bytes]) -> PrimaryTypes:
        if raw_data is None:
            return None
        return unpack(self.config.deserializer_struct_format, raw_data)[0]

    def serialize(self, data: PrimaryTypes) -> Union[bytes, None]:
        if data is None:
            return None
        if not isinstance(data, bytes):
            raise TypeError(f"Data payload must be bytes or bytearray, not {type(data).__name__}!")
        return pack(self.config.serializer_struct_format, data)
