import dataclasses
from abc import ABC, abstractmethod
from typing import Union, Optional

from esque.io import Message
from esque.io.messages import PrimaryTypes


class DataSerializer(ABC):
    @abstractmethod
    def serialize(self, data: PrimaryTypes, version: str = "") -> Union[bytes, str, None]:
        raise NotImplementedError

    @abstractmethod
    def deserialize(self, raw: Union[bytes, str, None]) -> PrimaryTypes:
        raise NotImplementedError

    def get_version(self, raw_data: Optional[bytes]) -> dict:
        return {}

@dataclasses.dataclass()
class MessageSerializer:
    key: DataSerializer = None
    value: DataSerializer = None

    def deserialize(self, message: Message):
        message.key = self.key.deserialize(message.key) if self.key else message.key
        message.val = self.value.deserialize(message.value) if self.value else message.val
        return message

    def serialize(self, message: Message):
        message.key = self.key.serialize(message.key) if self.key else message.key
        message.val = self.value.serialize(message.value) if self.value else message.val
        return message
