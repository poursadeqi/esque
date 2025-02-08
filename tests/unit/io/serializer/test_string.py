from typing import List

from pytest_cases import fixture

from esque.io.messages import PrimaryTypes
from esque.io.serializers.string import StringSerializer, StringSerializerConfig


@fixture(params=["latin1", "utf-8"])
def serializer_config(request) -> StringSerializerConfig:
    return StringSerializerConfig(encoding=request.param)


def test_string_serializer(serializer_config):
    string_serializer: StringSerializer = StringSerializer(config=serializer_config)
    raw_message: bytes = "Übung".encode(encoding=serializer_config.encoding)
    expected_deserialized_message = "Übung"
    deserialized_message = string_serializer.deserialize(raw_message)
    assert deserialized_message == expected_deserialized_message
    serializer_message: str = string_serializer.serialize(deserialized_message)
    assert serializer_message == raw_message


def test_string_serializer_many(serializer_config):
    string_serializer: StringSerializer = StringSerializer(config=serializer_config)
    raw_messages: List[bytes] = [
        "Änderung".encode(encoding=serializer_config.encoding),
        "Übung".encode(encoding=serializer_config.encoding),
    ]
    expected_deserialized_messages: List[PrimaryTypes] = ["Änderung", "Übung"]
    deserialized_messages: List[PrimaryTypes] = list(string_serializer.deserialize(raw_messages))
    assert deserialized_messages == expected_deserialized_messages
    serializer_messages: List[str] = list(string_serializer.serialize(deserialized_messages))
    assert serializer_messages == raw_messages
