import dataclasses
from abc import ABC, abstractmethod
from typing import Union

from esque.io.messages import PrintableMessagePayload


class DataSerializer(ABC):
    @abstractmethod
    def serialize(self, data: PrintableMessagePayload) -> Union[bytes, str, None]:
        raise NotImplementedError

    @abstractmethod
    def deserialize(self, raw: Union[bytes, str, None]) -> PrintableMessagePayload:
        raise NotImplementedError


@dataclasses.dataclass()
class MessageSerializer:
    key: DataSerializer
    value: DataSerializer
