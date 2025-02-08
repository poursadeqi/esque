from typing import List

import pytest
from pytest_cases import fixture

from esque.io.messages import MessagePayload
from esque.io.serializers.binary import Base64Serializer


@fixture
def serializer() -> Base64Serializer:
    return Base64Serializer()


@fixture
def many_expected_bytes() -> List[bytes]:
    return [b"lorem", b"ipsum", b"dolor", b"sit", b"amet"]


@fixture
def many_expected_data(many_expected_bytes: List[bytes]) -> List[MessagePayload]:
    return [MessagePayload(payload=expected_bytes) for expected_bytes in many_expected_bytes]


def test_raw_serialize(serializer: Base64Serializer, many_expected_bytes, many_expected_data):
    actual_bytes = serializer.serialize(many_expected_data[0])
    assert actual_bytes == many_expected_bytes[0]


def test_raw_deserialize(serializer: Base64Serializer, many_expected_bytes, many_expected_data):
    actual_data = serializer.deserialize(many_expected_bytes[0])
    assert actual_data.payload == many_expected_data[0].payload


def test_raw_serialize_raises_on_non_bytes(serializer):
    with pytest.raises(TypeError):
        serializer.serialize(MessagePayload(payload=1))


def test_raw_serialize_many(serializer: Base64Serializer, many_expected_bytes, many_expected_data):
    many_actual_bytes = list(serializer.serialize_many(many_expected_data))
    assert many_actual_bytes == many_expected_bytes


def test_raw_deserialize_many(serializer: Base64Serializer, many_expected_bytes, many_expected_data):
    many_actual_data = serializer.deserialize_many(many_expected_bytes)

    for actual_data, expected_data in zip(many_actual_data, many_expected_data):
        assert actual_data.payload == expected_data.payload
