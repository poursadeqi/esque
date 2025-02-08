import itertools
from typing import Dict, Iterator, ClassVar

from esque.io.exceptions import EsqueIONoSuchSchemaException
from esque.io.serializers.avro.type import AvroType
from esque.io.serializers.registry_avro import SchemaRegistryClient, SCHEMA_REGISTRY_CLIENT_SCHEME_MAP, \
    RegistryAvroSerializerConfig


class InMemorySchemaRegistryClient(SchemaRegistryClient):
    _IN_MEMORY_REGISTRIES: ClassVar[Dict[str, "SchemaRegistryClient"]] = {}

    def __init__(self):
        self._avro_types_by_id: Dict[int, AvroType] = {}
        self._ids_by_avro_type: Dict[AvroType, int] = {}
        self._id_counter: Iterator[int] = itertools.count()

    def get_avro_type_by_id(self, schema_id: int) -> "AvroType":
        if schema_id not in self._avro_types_by_id:
            raise EsqueIONoSuchSchemaException(f"Unknown schema ID {schema_id}")

        return self._avro_types_by_id[schema_id]

    def get_or_create_id_for_avro_type(self, avro_type: "AvroType") -> int:
        if avro_type in self._ids_by_avro_type:
            return self._ids_by_avro_type[avro_type]
        else:
            schema_id = next(self._id_counter)
            self._ids_by_avro_type[avro_type] = schema_id
            self._avro_types_by_id[schema_id] = avro_type
            return schema_id

    @classmethod
    def from_config(cls, config: "RegistryAvroSerializerConfig") -> SchemaRegistryClient:
        hostname = config.parsed_uri().hostname
        if hostname not in cls._IN_MEMORY_REGISTRIES:
            cls._IN_MEMORY_REGISTRIES[hostname] = cls()
        return cls._IN_MEMORY_REGISTRIES[hostname]


SCHEMA_REGISTRY_CLIENT_SCHEME_MAP["memory"] = InMemorySchemaRegistryClient
