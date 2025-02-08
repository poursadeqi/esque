import functools
import json
import pathlib
import urllib

from esque.io.exceptions import EsqueIONoSuchSchemaException
from esque.io.serializers.schema_registry import IndexData, RegistryAvroSerializerConfig, SchemaRegistryClient
from esque.io.serializers.schema_registry.type import AvroType


class PathSchemaRegistryClient(SchemaRegistryClient):
    def __init__(self, schema_registry_uri: str):
        # skip the leading slash to allow for relative paths
        path = urllib.parse.urlparse(schema_registry_uri).path[1:]
        self._base_path = pathlib.Path(path)

    @functools.lru_cache
    def get_avro_type_by_id(self, schema_id: int) -> "AvroType":
        path = self._path_for_id(schema_id)
        if not path.exists():
            raise EsqueIONoSuchSchemaException(f"Unknown schema ID {schema_id}")
        with path.open("r") as o:
            data = json.load(o)
        return AvroType(data)

    def _path_for_id(self, schema_id: int) -> pathlib.Path:
        return self._base_path / f"schema_{schema_id:03}.avsc"

    def get_or_create_id_with_version(self, avro_type: "AvroType") -> int:
        # JSON doesn't support int keys, so we need to make them strings here
        type_hash = str(hash(avro_type))
        if type_hash in self._index_data:
            return self._index_data[type_hash]

        schema_id = max(self._index_data.values(), default=-1) + 1
        self._write_schema_file(avro_type, schema_id)

        self._index_data[type_hash] = schema_id
        self._update_index_file()
        return schema_id

    def _write_schema_file(self, avro_type: "AvroType", schema_id: int):
        schema_file = self._path_for_id(schema_id)
        schema_file.parent.mkdir(exist_ok=True)
        with schema_file.open("w") as o:
            json.dump(avro_type.avro_schema, o)

    def _get_index_path(self) -> pathlib.Path:
        return self._base_path / "schema_index.json"

    @functools.cached_property
    def _index_data(self) -> IndexData:
        index_path = self._get_index_path()
        if not index_path.exists():
            return {}
        with index_path.open("r") as o:
            return json.load(o)

    def _update_index_file(self):
        with self._get_index_path().open("w") as o:
            json.dump(self._index_data, o)

    @classmethod
    def from_config(cls, config: "RegistryAvroSerializerConfig") -> "PathSchemaRegistryClient":
        return cls(config.schema_registry_uri)
