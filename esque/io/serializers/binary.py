import dataclasses
from typing import Optional

from esque.io.data_types import NoData, UnknownDataType
from esque.io.messages import MessagePayload
from esque.io.serializers.base import DataSerializer, SerializerConfig
from base64 import b64encode


@dataclasses.dataclass()
class BinarySerializerConfig(SerializerConfig):
    pass


class BinarySerializer(DataSerializer):
    config_cls = BinarySerializerConfig
    unknown_data_type: UnknownDataType = UnknownDataType()

    def deserialize(self, raw_data: Optional[bytes]) -> MessagePayload:
        if raw_data is None:
            return MessagePayload.NO_DATA
        return MessagePayload(payload=raw_data, data_type=self.unknown_data_type)

    def serialize(self, data: MessagePayload) -> str:
        if isinstance(data.data_type, NoData):
            return ""
        if not isinstance(data.payload, bytes):
            raise TypeError(f"Data payload has to be bytes, not {type(data.payload).__name__}!")
        return b64encode(data.payload).decode("UTF-8")
