from abc import ABC, abstractmethod

# flake8:noqa
from . import type
from .config import RegistryAvroSerializerConfig
from .type import AvroType


class SchemaRegistryClient(ABC):
    @abstractmethod
    def get_avro_type_by_id(self, schema_id: int) -> "AvroType":
        raise NotImplementedError

    @abstractmethod
    def get_or_create_id_with_version(self, version: str) -> AvroType:
        raise NotImplementedError

    @classmethod
    def from_config(cls, config: "RegistryAvroSerializerConfig") -> "SchemaRegistryClient":
        scheme: str = config.parsed_uri().scheme
        from .memory import InMemorySchemaRegistryClient
        from .rest import RestSchemaRegistryClient

        schema_mapping = {
            "http": RestSchemaRegistryClient,
            "https": RestSchemaRegistryClient,
            "memory": InMemorySchemaRegistryClient,
        }
        schema_registry_client_cls = schema_mapping[scheme]
        assert cls == SchemaRegistryClient, f"Make sure you implement from_config on {cls.__name__}"
        return schema_registry_client_cls.from_config(config)
