from base64 import b64encode
from typing import Optional

from esque.io.messages import MessagePayload
from esque.io.serializers.base import DataSerializer


class BinarySerializer(DataSerializer):
    def serialize(self, data: MessagePayload) -> Optional[bytes]:
        raise NotImplementedError

    def deserialize(self, raw_data: Optional[bytes]) -> MessagePayload:
        if raw_data is None:
            return MessagePayload()
        return MessagePayload(payload=b64encode(raw_data).decode("UTF-8"))
