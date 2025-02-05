from base64 import b64encode, b64decode
from typing import Optional

from esque.io.messages import MessagePayload
from esque.io.serializers.base import DataSerializer


class BinarySerializer(DataSerializer):
    def serialize(self, data: MessagePayload) -> MessagePayload:
        return MessagePayload(payload=b64decode(data.payload).decode("UTF-8"))

    def deserialize(self, raw_data: Optional[bytes]) -> MessagePayload:
        if raw_data is None:
            return MessagePayload()
        return MessagePayload(payload=b64encode(raw_data).decode("UTF-8"))
