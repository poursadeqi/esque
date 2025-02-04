import dataclasses
from typing import Optional

from esque.io.serializers.base import MessageSerializer


@dataclasses.dataclass
class BaseHandlerConfig:
    read_serializer: Optional[MessageSerializer] = None
    write_serializer: Optional[MessageSerializer] = None
