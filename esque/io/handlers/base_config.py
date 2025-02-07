import dataclasses
from typing import Optional

from esque.io.serializers.base import MessageSerializer
from esque.io.serializers.none import NoneSerializer


@dataclasses.dataclass
class BaseHandlerConfig:
    read_serializer: Optional[MessageSerializer] = MessageSerializer(key=NoneSerializer(), value=NoneSerializer())
    write_serializer: Optional[MessageSerializer] = MessageSerializer(key=NoneSerializer(), value=NoneSerializer())
