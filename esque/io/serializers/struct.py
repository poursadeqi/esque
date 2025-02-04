import dataclasses
from struct import unpack
from typing import Optional

from esque.io.messages import PrintableMessagePayload
from esque.io.serializers.base import DataSerializer


@dataclasses.dataclass()
class StructSerializerConfig:
    struct_format: str


class StructSerializer(DataSerializer):
    def __init__(self, config: StructSerializerConfig):
        self.config = config

    def deserialize(self, raw_data: Optional[bytes]) -> PrintableMessagePayload:
        if raw_data is None:
            return PrintableMessagePayload()
        output = unpack(self.config.struct_format, raw_data)[0]
        return PrintableMessagePayload(payload=output)

    def serialize(self, data: PrintableMessagePayload) -> str:
        if data.is_empty():
            return ""
        # if not isinstance(data.payload, bytes):
        #     raise TypeError(f"Data payload has to be bytes, not {type(data.payload).__name__}!")
        return data.payload
