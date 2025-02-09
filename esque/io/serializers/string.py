import dataclasses
from typing import Optional

from esque.io.messages import PrimaryTypes
from esque.io.serializers.base import DataSerializer


@dataclasses.dataclass()
class StringSerializerConfig:
    encoding: str = "UTF-8"


class StringSerializer(DataSerializer):
    def __init__(self, config: StringSerializerConfig):
        self.config = config

    def serialize(self, data: PrimaryTypes, version=None) -> Optional[bytes]:
        if data is None:
            return None
        return data.encode(encoding=self.config.encoding)

    def deserialize(self, raw_data: PrimaryTypes) -> str:
        payload = None
        if isinstance(raw_data, str):
            payload = raw_data
        if isinstance(raw_data, bytes):
            payload = raw_data.decode(encoding=self.config.encoding, errors="replace")
        return payload
