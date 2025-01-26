import dataclasses
from typing import Optional

from esque.io.data_types import NoData, UnknownDataType
from esque.io.messages import Data
from esque.io.serializers.base import DataSerializer, SerializerConfig
from struct import unpack


@dataclasses.dataclass()
class StructSerializerConfig(SerializerConfig):
    struct_format: str


class StructSerializer(DataSerializer):
    config_cls = StructSerializerConfig
    unknown_data_type: UnknownDataType = UnknownDataType()

    def deserialize(self, raw_data: Optional[bytes]) -> Data:
        if raw_data is None:
            return Data.NO_DATA
        output = unpack(self.config.struct_format, raw_data)[0]
        return Data(payload=output, data_type=self.unknown_data_type)

    def serialize(self, data: Data) -> str:
        if isinstance(data.data_type, NoData):
            return ""
        # if not isinstance(data.payload, bytes):
        #     raise TypeError(f"Data payload has to be bytes, not {type(data.payload).__name__}!")
        return data.payload
