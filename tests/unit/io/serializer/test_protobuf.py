import base64
import datetime

import pytest
import pytest_cases.case_parametrizer_new
from pytest_cases import fixture

from esque.io.serializers.proto import ProtoSerializer, ProtoSerializerConfig

CET = datetime.timezone(datetime.timedelta(seconds=3600), "CET")


@fixture
def serializer() -> ProtoSerializer:
    return ProtoSerializer(
        ProtoSerializerConfig(protoc_py_path="./pb", module_name="hi_pb2", class_name="HelloWorldResponse")
    )


@pytest_cases.case
def proto_cases_only_name_is_set():
    return ("CgdlYnJhaGlt", {
        "type_string": "ebrahim",
        "type_enum": "ENUM_TYPE_UNSPECIFIED",
        "type_int32": 0,
        "type_int64": "0",
    })


@pytest_cases.parametrize_with_cases(argnames=("b64", "expected"), prefix="proto_cases")
def test_proto_deserializer(serializer, b64, expected: dict):
    actual_result = serializer.deserialize(base64.b64decode(b64))
    assert actual_result == expected
