import dataclasses
import functools
import hashlib
import json
from typing import Any, Dict

import fastavro


@dataclasses.dataclass
class AvroType:
    avro_schema: Dict

    def __hash__(self) -> int:
        data_bytes: bytes = json.dumps(self.avro_schema, sort_keys=True).encode(encoding="utf-8")
        digest: bytes = hashlib.md5(data_bytes).digest()
        return int.from_bytes(digest, byteorder="big")

    @functools.cached_property
    def fastavro_schema(self) -> Any:
        return fastavro.parse_schema(schema=self.avro_schema)
