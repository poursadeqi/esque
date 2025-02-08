# flake8: noqa
import dataclasses
import io
import urllib.parse
from abc import ABC, abstractmethod
from typing import Dict, Optional, Type
from urllib.parse import ParseResult

import fastavro

from esque.io.messages import PrimaryTypes
from esque.io.serializers.avro.type import AvroType
from esque.io.serializers.base import DataSerializer

SCHEMA_REGISTRY_CLIENT_SCHEME_MAP: Dict[str, Type["SchemaRegistryClient"]] = {}
MAGIC_BYTE = b"\x00"


def create_schema_id_prefix(schema_id: int) -> bytes:
    return MAGIC_BYTE + schema_id.to_bytes(length=4, byteorder="big")


def get_schema_id_from_prefix(prefix: bytes) -> int:
    assert prefix[:1] == MAGIC_BYTE
    return int.from_bytes(prefix[1:5], byteorder="big")


class SchemaRegistryClient(ABC):
    @abstractmethod
    def get_avro_type_by_id(self, schema_id: int) -> "AvroType":
        raise NotImplementedError

    @abstractmethod
    def get_or_create_id_for_avro_type(self, avro_type: "AvroType") -> int:
        raise NotImplementedError

    @classmethod
    def from_config(cls, config: "RegistryAvroSerializerConfig") -> "SchemaRegistryClient":
        scheme: str = config.parsed_uri().scheme
        schema_registry_client_cls = SCHEMA_REGISTRY_CLIENT_SCHEME_MAP[scheme]
        assert cls == SchemaRegistryClient, f"Make sure you implement from_config on {cls.__name__}"
        return schema_registry_client_cls.from_config(config)


# hash to schema id
IndexData = Dict[str, int]


@dataclasses.dataclass()
class RegistryAvroSerializerConfig:
    schema_registry_uri: str
    schema_subject: str = ""

    def parsed_uri(self) -> ParseResult:
        return urllib.parse.urlparse(url=self.schema_registry_uri)

    def _validate_fields(self):
        problems = []
        if not self.schema_registry_uri:
            problems.append("uri cannot be None")
        try:
            parsed_uri_result: ParseResult = self.parsed_uri()
        except Exception as e:  # noqa
            problems.append(f"exception of type {type(e).__name__} occurred during uri parsing: {e.args}")
        else:
            if parsed_uri_result.scheme not in SCHEMA_REGISTRY_CLIENT_SCHEME_MAP:
                problems.append(
                    f"unknown scheme for schema registry client: {parsed_uri_result.scheme}. "
                    f"Supported client schemes: {','.join(SCHEMA_REGISTRY_CLIENT_SCHEME_MAP.keys())}"
                )

        return problems

    def with_key_subject_for_topic(self, topic: str) -> "RegistryAvroSerializerConfig":
        return dataclasses.replace(self, schema_subject=f"{topic}-key")

    def with_value_subject_for_topic(self, topic: str) -> "RegistryAvroSerializerConfig":
        return dataclasses.replace(self, schema_subject=f"{topic}-value")


class RegistryAvroSerializer(DataSerializer):
    config = RegistryAvroSerializerConfig

    def __init__(self, config: RegistryAvroSerializerConfig):
        super().__init__()
        self.config = config
        self._registry_client = SchemaRegistryClient.from_config(config)

    def serialize(self, data: AvroType) -> Optional[bytes]:
        schema_id = self._registry_client.get_or_create_id_for_avro_type(data)
        buffer = io.BytesIO()
        fastavro.schemaless_writer(buffer, data.fastavro_schema, data)
        return create_schema_id_prefix(schema_id) + buffer.getvalue()

    def deserialize(self, raw_data: Optional[bytes]) -> PrimaryTypes:
        if raw_data is None:
            return None

        with io.BytesIO(raw_data) as fake_stream:
            schema_id = get_schema_id_from_prefix(fake_stream.read(5))
            avro_type = self._registry_client.get_avro_type_by_id(schema_id)
            return fastavro.schemaless_reader(fake_stream, avro_type.fastavro_schema)
