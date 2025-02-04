import dataclasses
import datetime
from typing import List, NamedTuple, Optional, Union


class MessageHeader(NamedTuple):
    key: str
    value: Optional[str]


@dataclasses.dataclass
class PrintableMessagePayload:
    PrimaryTypes = Union[dict, list, tuple, str, int, float, bool, None]
    payload: PrimaryTypes = None

    def is_empty(self):
        return self.payload is None

    def __repr__(self):
        return self.payload


def now_utc() -> datetime.datetime:
    return datetime.datetime.now(tz=datetime.timezone.utc)


@dataclasses.dataclass
class PrintableMessage:
    key: PrintableMessagePayload
    value: PrintableMessagePayload
    partition: int = -1
    offset: int = -1
    timestamp: datetime.datetime = dataclasses.field(default_factory=now_utc)
    headers: List[MessageHeader] = dataclasses.field(default_factory=list)
