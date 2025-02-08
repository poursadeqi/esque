import dataclasses
from typing import Optional

from esque.io.serializers.base import MessageSerializer
from esque.io.serializers.raw import RawSerializer


@dataclasses.dataclass
class BaseHandlerConfig:
    read_serializer: Optional[MessageSerializer] = MessageSerializer(key=RawSerializer(), value=RawSerializer())
    write_serializer: Optional[MessageSerializer] = MessageSerializer(key=RawSerializer(), value=RawSerializer())
