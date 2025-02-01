import dataclasses
from typing import Optional

from esque.io.data_types import NoData, String
from esque.io.messages import MessagePayload
from esque.io.serializers.base import DataSerializer, SerializerConfig


@dataclasses.dataclass()
class StringSerializerConfig(SerializerConfig):
    encoding: str = "UTF-8"


class StringSerializer(DataSerializer[StringSerializerConfig]):
    data_type: String = String()

    def serialize(self, data: MessagePayload) -> Optional[bytes]:
        if isinstance(data.data_type, NoData):
            return None
        return data.payload

    def deserialize(self, raw_data: Optional[bytes]) -> MessagePayload:
        if raw_data is None:
            return MessagePayload()
        return MessagePayload(payload=raw_data.decode(encoding=self.config.encoding, errors="replace"))
