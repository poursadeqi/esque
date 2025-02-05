import dataclasses
from typing import Optional

from esque.io.messages import MessagePayload
from esque.io.serializers.base import DataSerializer


@dataclasses.dataclass()
class StringSerializerConfig:
    encoding: str = "UTF-8"


class StringSerializer(DataSerializer):
    def __init__(self, config: StringSerializerConfig):
        self.config = config

    def serialize(self, data: MessagePayload) -> Optional[str]:
        return data.payload

    def deserialize(self, raw_data: Optional[bytes]) -> MessagePayload:
        if raw_data is None:
            return MessagePayload()
        return MessagePayload(payload=raw_data.decode(encoding=self.config.encoding, errors="replace"))
