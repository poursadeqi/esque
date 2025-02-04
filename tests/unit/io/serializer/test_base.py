from typing import List

from esque.io.messages import WritableMessage, PrintableMessage
from esque.io.serializers.base import MessageSerializer
from esque.io.serializers.string import StringSerializer


def test_message_serializer(
    binary_messages: List[WritableMessage], string_messages: List[PrintableMessage], string_serializer: StringSerializer
):
    serializer: MessageSerializer = MessageSerializer(key_serializer=string_serializer)
    deserialized_message: PrintableMessage = serializer.deserialize(binary_messages[0])
    assert deserialized_message == string_messages[0]
    serialized_message: WritableMessage = serializer.serialize(string_messages[0])
    assert serialized_message == binary_messages[0]


def test_message_serializer_many(
    binary_messages: List[WritableMessage], string_messages: List[PrintableMessage], string_serializer: StringSerializer
):
    serializer: MessageSerializer = MessageSerializer(key_serializer=string_serializer)
    deserialized_messages: List[PrintableMessage] = list(serializer.deserialize_many(binary_messages))
    assert deserialized_messages == string_messages
    serialized_messages: List[WritableMessage] = list(serializer.serialize_many(string_messages))
    assert serialized_messages == binary_messages
