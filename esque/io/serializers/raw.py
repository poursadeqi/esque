from esque.io.messages import PrimaryTypes
from esque.io.serializers.base import DataSerializer


class RawSerializer(DataSerializer):
    def serialize(self, data: PrimaryTypes, version=None) -> PrimaryTypes:
        return data

    def deserialize(self, raw_data: PrimaryTypes) -> PrimaryTypes:
        return raw_data
