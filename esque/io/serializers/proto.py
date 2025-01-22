import dataclasses
import json
import importlib
from typing import Any, Optional

from esque.io.data_types import NoData, UnknownDataType
from esque.io.messages import Data
from esque.io.serializers import SerializerConfig
from esque.io.serializers.base import DataSerializer
from google.protobuf import descriptor_pool, message_factory
from google.protobuf.json_format import MessageToJson


@dataclasses.dataclass(frozen=True)
class ProtoSerializerConfig(SerializerConfig):
    indent: Optional[str] = None
    encoding: str = "UTF-8"


# TODO: implement solution to handle data types when they are known
class ProtoSerializer(DataSerializer[ProtoSerializerConfig]):
    config_cls = ProtoSerializerConfig
    unknown_data_type: UnknownDataType = UnknownDataType()

    def serialize(self, data: Data) -> Optional[bytes]:
        module_name = "api_stubs_py.spads_backend.ads_updates_pb2"
        class_name = "ProductAdvertisementEventEnvelope"
        module = importlib.import_module(module_name)
        message_class = getattr(module, class_name)

        message = message_class()
        message.ParseFromString(data)

        json_data = MessageToJson(message)
        return json_data

    def deserialize(self, raw_data: Optional[bytes]) -> Data:
        module_name = "api_stubs_py.spads_backend.ads_updates_pb2"
        class_name = "ProductAdvertisementEventEnvelope"
        module = importlib.import_module(module_name)
        message_class = getattr(module, class_name)

        message = message_class()
        message.ParseFromString(raw_data)

        json_data = MessageToJson(message)
        if raw_data is None:
            return Data.NO_DATA
        return Data(payload=json.loads(raw_data.decode(self.config.encoding)), data_type=self.unknown_data_type)
