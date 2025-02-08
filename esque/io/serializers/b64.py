from base64 import b64decode, b64encode
from typing import Optional, Union
import re

from confluent_kafka.serialization import SerializationError

from esque.io.messages import PrimaryTypes
from esque.io.serializers.base import DataSerializer


class Base64Serializer(DataSerializer):
    def serialize(self, data: PrimaryTypes) -> str:
        return b64encode(data).decode("UTF-8")

    def deserialize(self, raw_data: Optional[str]) -> Union[bytes, None]:
        if not is_base64(raw_data):
            raise SerializationError("b64 deserializer expect to get a base64 encoded string")
        if raw_data is None:
            return None
        return b64decode(raw_data, validate=True)


def is_base64(s: str) -> bool:
    if not isinstance(s, str):
        return False
    # Check if length is a multiple of 4
    if len(s) % 4 != 0:
        return False

    # Check if it only contains valid Base64 characters
    if not re.fullmatch(r'^[A-Za-z0-9+/]*={0,2}$', s):
        return False

    return True
