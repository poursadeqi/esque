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

    def deserialize(self, raw_data: MessagePayload) -> MessagePayload:
        payload = None
        if isinstance(raw_data, str):
            payload = raw_data
        if isinstance(raw_data, bytes):
            payload = raw_data.decode(encoding=self.config.encoding, errors="replace")
        return MessagePayload(payload=payload)
