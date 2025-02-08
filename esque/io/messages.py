import dataclasses
import datetime
from typing import List, NamedTuple, Optional, Union

from esque.io.serializers.registry_avro import AvroType


class MessageHeader(NamedTuple):
    key: str
    value: Optional[str]


PrimaryTypes = Union[dict, str, bytes, AvroType, None]


def now_utc() -> datetime.datetime:
    return datetime.datetime.now(tz=datetime.timezone.utc)


@dataclasses.dataclass
class Message:
    key: PrimaryTypes
    value: PrimaryTypes
    partition: int = -1
    offset: int = -1
    timestamp: datetime.datetime = dataclasses.field(default_factory=now_utc)
    headers: List[MessageHeader] = dataclasses.field(default_factory=list)
