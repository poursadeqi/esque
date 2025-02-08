from base64 import b64decode, b64encode
from typing import Optional

from confluent_kafka.serialization import SerializationError

from esque.io.messages import MessagePayload
from esque.io.serializers.base import DataSerializer


class BinarySerializer(DataSerializer):
    def serialize(self, data: MessagePayload) -> MessagePayload:
        return MessagePayload(payload=b64decode(data.payload).decode("UTF-8"))

    def deserialize(self, raw_data: Optional[bytes]) -> MessagePayload:
        if not isinstance(raw_data, bytes):
            raise SerializationError("binary deserializer expect to get bytes")
        if raw_data is None:
            return MessagePayload()
        return MessagePayload(payload=b64encode(raw_data).decode("UTF-8"))
