from typing import List

from pytest_cases import parametrize_with_cases

from esque.io.messages import PrimaryTypes
from esque.io.serializers import DataSerializer


@parametrize_with_cases("serializer")
def test_serialize_many_no_data(serializer: DataSerializer, no_data: PrimaryTypes):
    actual_serialized_data = serializer.serialize(no_data)
    assert actual_serialized_data is None

    actual_deserialized_data: PrimaryTypes = serializer.deserialize(None)
    assert actual_deserialized_data == no_data


@parametrize_with_cases("serializer")
def test_serialize_no_data(serializer: DataSerializer, no_data: PrimaryTypes):
    actual_serialized_data: None = serializer.serialize(no_data)
    assert actual_serialized_data is None

    actual_deserialized_data: PrimaryTypes = serializer.deserialize(None)
    assert actual_deserialized_data == no_data
