from abc import ABC, abstractmethod

# flake8:noqa
from . import type
from .config import RegistryAvroSerializerConfig
from .schema_factory import get_supported_schema
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
        schema_registry_client_cls = get_supported_schema()[scheme]
        assert cls == SchemaRegistryClient, f"Make sure you implement from_config on {cls.__name__}"
        return schema_registry_client_cls.from_config(config)
