import dataclasses
from typing import Optional

from esque.io.messages import MessagePayload
from esque.io.serializers.base import DataSerializer, SerializerConfig
from base64 import b64encode


@dataclasses.dataclass()
class BinarySerializerConfig(SerializerConfig):
    pass


class BinarySerializer(DataSerializer):
    config_cls = BinarySerializerConfig

    def deserialize(self, raw_data: Optional[bytes]) -> MessagePayload:
        if raw_data is None:
            return MessagePayload()
        return MessagePayload(payload=b64encode(raw_data).decode("UTF-8"))

    def serialize(self, data: MessagePayload) -> Optional[bytes]:
        raise NotImplementedError
