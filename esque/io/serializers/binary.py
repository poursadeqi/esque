from base64 import b64encode
from typing import Optional

from esque.io.messages import PrintableMessagePayload
from esque.io.serializers.base import DataSerializer


class BinarySerializer(DataSerializer):
    def serialize(self, data: PrintableMessagePayload) -> Optional[bytes]:
        raise NotImplementedError

    def deserialize(self, raw_data: Optional[bytes]) -> PrintableMessagePayload:
        if raw_data is None:
            return PrintableMessagePayload()
        return PrintableMessagePayload(payload=b64encode(raw_data).decode("UTF-8"))
