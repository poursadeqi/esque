import dataclasses
import urllib
from urllib.parse import ParseResult

from esque.io.serializers.schema_registry.schema_factory import get_supported_schema


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
            if parsed_uri_result.scheme not in get_supported_schema().keys():
                problems.append(
                    f"unknown scheme for schema registry client: {parsed_uri_result.scheme}. "
                    f"Supported client schemes: {','.join(get_supported_schema().keys())}"
                )

        return problems

    def with_key_subject_for_topic(self, topic: str) -> "RegistryAvroSerializerConfig":
        return dataclasses.replace(self, schema_subject=f"{topic}-key")

    def with_value_subject_for_topic(self, topic: str) -> "RegistryAvroSerializerConfig":
        return dataclasses.replace(self, schema_subject=f"{topic}-value")
