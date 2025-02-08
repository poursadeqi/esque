import dataclasses
import datetime
from typing import List, NamedTuple, Optional, Union


class MessageHeader(NamedTuple):
    key: str
    value: Optional[str]


PrimaryTypes = Union[dict, list, tuple, str, int, float, bool, bytes, None]


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
