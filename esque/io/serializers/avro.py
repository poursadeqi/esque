# flake8: noqa
import io
from abc import ABC, abstractmethod
from typing import Optional

import fastavro

from esque.io.messages import PrimaryTypes
from esque.io.serializers.base import DataSerializer
from esque.io.serializers.schema_registry import SchemaRegistryClient
from esque.io.serializers.schema_registry.config import RegistryAvroSerializerConfig

MAGIC_BYTE = b"\x00"


def create_schema_id_prefix(schema_id: int) -> bytes:
    return MAGIC_BYTE + schema_id.to_bytes(length=4, byteorder="big")


def get_schema_id_from_prefix(prefix: bytes) -> int:
    assert prefix[:1] == MAGIC_BYTE
    return int.from_bytes(prefix[1:5], byteorder="big")


class RegistryAvroSerializer(DataSerializer):
    config = RegistryAvroSerializerConfig

    def __init__(self, config: RegistryAvroSerializerConfig):
        super().__init__()
        self.config = config
        self._registry_client = SchemaRegistryClient.from_config(config)

    def serialize(self, data: dict, version: str = "") -> Optional[bytes]:
        schema = self._registry_client.get_or_create_id_with_version(version)
        buffer = io.BytesIO()
        fastavro.schemaless_writer(buffer, schema.fastavro_schema, data)
        return create_schema_id_prefix(schema.id) + buffer.getvalue()

    def deserialize(self, raw_data: Optional[bytes]) -> PrimaryTypes:
        if raw_data is None:
            return None

        with io.BytesIO(raw_data) as fake_stream:
            schema_id = get_schema_id_from_prefix(fake_stream.read(5))
            avro_type = self._registry_client.get_avro_type_by_id(schema_id)
            return fastavro.schemaless_reader(fake_stream, avro_type.fastavro_schema)
