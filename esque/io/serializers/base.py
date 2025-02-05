import dataclasses
from abc import ABC, abstractmethod
from typing import Union

from esque.io.messages import MessagePayload


class DataSerializer(ABC):
    @abstractmethod
    def serialize(self, data: MessagePayload) -> Union[bytes, str, None]:
        raise NotImplementedError

    @abstractmethod
    def deserialize(self, raw: Union[bytes, str, None]) -> MessagePayload:
        raise NotImplementedError


@dataclasses.dataclass()
class MessageSerializer:
    key: DataSerializer
    value: DataSerializer
