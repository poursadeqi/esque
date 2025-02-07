from esque.io.messages import MessagePayload, PrimaryTypes
from esque.io.serializers.base import DataSerializer


class NoneSerializer(DataSerializer):
    def serialize(self, data: MessagePayload) -> PrimaryTypes:
        return data.payload

    def deserialize(self, raw_data: PrimaryTypes) -> MessagePayload:
        return MessagePayload(payload=raw_data)
