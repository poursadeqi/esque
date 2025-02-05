from esque.io.serializers import (
    BinarySerializer,
    DataSerializer,
    JsonSerializer,
    RegistryAvroSerializer,
    StringSerializer,
)
from esque.io.serializers.json import JsonSerializerConfig
from esque.io.serializers.registry_avro import RegistryAvroSerializerConfig
from esque.io.serializers.string import StringSerializerConfig


def case_json_serializer() -> DataSerializer:
    return JsonSerializer(JsonSerializerConfig())


def case_raw_serializer() -> DataSerializer:
    return BinarySerializer()


def case_registry_avro_serializer() -> DataSerializer:
    return RegistryAvroSerializer(
        RegistryAvroSerializerConfig(schema_registry_uri="memory://foobar")
    )


def case_string_serializer() -> DataSerializer:
    return StringSerializer(StringSerializerConfig())
