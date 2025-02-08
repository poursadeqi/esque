from esque.io.messages import PrimaryTypes
from esque.io.serializers.base import DataSerializer


class RawSerializer(DataSerializer):
    def serialize(self, data: PrimaryTypes) -> PrimaryTypes:
        return data

    def deserialize(self, raw_data: PrimaryTypes) -> PrimaryTypes:
        return raw_data
