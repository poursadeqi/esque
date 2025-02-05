import base64
import dataclasses
import datetime
from typing import List, NamedTuple, Optional, Union


class MessageHeader(NamedTuple):
    key: str
    value: Optional[str]


PrimaryTypes = Union[dict, list, tuple, str, int, float, bool, bytes, None]

@dataclasses.dataclass
class MessagePayload:
    payload: PrimaryTypes = None

    def is_empty(self) -> bool:
        return self.payload is None

    def is_printable(self) -> bool:
        return not isinstance(self.payload, bytes)

    def __repr__(self) -> str:
        if self.is_printable():
            return repr(self.payload)
        return base64.b64encode(self.payload).decode("utf-8")

def now_utc() -> datetime.datetime:
    return datetime.datetime.now(tz=datetime.timezone.utc)


@dataclasses.dataclass
class Message:
    key: MessagePayload
    value: MessagePayload
    partition: int = -1
    offset: int = -1
    timestamp: datetime.datetime = dataclasses.field(default_factory=now_utc)
    headers: List[MessageHeader] = dataclasses.field(default_factory=list)
