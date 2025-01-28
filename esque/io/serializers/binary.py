import dataclasses
from typing import Optional

from esque.io.data_types import NoData, UnknownDataType
from esque.io.messages import Data
from esque.io.serializers.base import DataSerializer, SerializerConfig
from base64 import b64encode


@dataclasses.dataclass()
class BinarySerializerConfig(SerializerConfig):
    pass


class BinarySerializer(DataSerializer):
    config_cls = BinarySerializerConfig
    unknown_data_type: UnknownDataType = UnknownDataType()

    def deserialize(self, raw_data: Optional[bytes]) -> Data:
        if raw_data is None:
            return Data.NO_DATA
        return Data(payload=raw_data, data_type=self.unknown_data_type)

    def serialize(self, data: Data) -> str:
        if isinstance(data.data_type, NoData):
            return ""
        if not isinstance(data.payload, bytes):
            raise TypeError(f"Data payload has to be bytes, not {type(data.payload).__name__}!")
        return b64encode(data.payload).decode("UTF-8")
