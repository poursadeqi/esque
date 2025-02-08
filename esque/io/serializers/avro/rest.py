import functools
import json
from typing import Optional, Dict

import requests

from esque.io.exceptions import EsqueIOSerializerConfigException
from esque.io.serializers.avro.type import AvroType
from esque.io.serializers.registry_avro import SCHEMA_REGISTRY_CLIENT_SCHEME_MAP, RegistryAvroSerializerConfig, \
    SchemaRegistryClient


class RestSchemaRegistryClient(SchemaRegistryClient):
    def __init__(self, registry_url: str, subject: str):
        self._base_url = registry_url
        self._subject = subject
        self._request_session = requests.Session()

    @functools.lru_cache(maxsize=512)
    def get_avro_type_by_id(self, schema_id: int) -> "AvroType":
        url = f"{self._base_url}/schemas/ids/{schema_id}"
        response = self._request_session.get(url)
        response.raise_for_status()
        schema: Dict = json.loads(response.json()["schema"])
        return AvroType(avro_schema=schema)

    @functools.lru_cache(maxsize=512)
    def get_or_create_id_for_avro_type(self, avro_type: "AvroType") -> int:
        self._assert_subject_valid()
        schema_id = self._try_get_existing_schema_id_from_subject(avro_type)

        if schema_id is None:
            schema_id = self._register_new_version_on_subject_and_get_schema_id(avro_type)

        return schema_id

    def _assert_subject_valid(self):
        if not self._subject:
            raise EsqueIOSerializerConfigException(
                "Need to provide a key or value schema subject! I.e. topic suffixed with '-key' or '-value'."
            )
        elif not (self._subject.endswith("key") or self._subject.endswith("value")):
            raise EsqueIOSerializerConfigException("Need to provide a specific key or value subject.")

    def _try_get_existing_schema_id_from_subject(self, avro_type: "AvroType") -> Optional[int]:
        url = f"{self._base_url}/subjects/{self._subject}"
        response = self._request_session.post(url, json={"schema": json.dumps(avro_type.avro_schema)})
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()["id"]

    def _register_new_version_on_subject_and_get_schema_id(self, avro_type: "AvroType") -> int:
        url = f"{self._base_url}/subjects/{self._subject}/versions"
        response = self._request_session.post(url, json={"schema": json.dumps(avro_type.avro_schema)})
        response.raise_for_status()
        return response.json()["id"]

    @classmethod
    def from_config(cls, config: "RegistryAvroSerializerConfig") -> "RestSchemaRegistryClient":
        return cls(registry_url=config.schema_registry_uri, subject=config.schema_subject)
SCHEMA_REGISTRY_CLIENT_SCHEME_MAP["http"] = RestSchemaRegistryClient
SCHEMA_REGISTRY_CLIENT_SCHEME_MAP["https"] = RestSchemaRegistryClient