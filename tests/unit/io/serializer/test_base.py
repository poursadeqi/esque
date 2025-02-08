from typing import List

from esque.io.messages import Message
from esque.io.serializers.base import MessageSerializer
from esque.io.serializers.string import StringSerializer


def test_message_serializer(
    messages: List[Message], string_messages: List[Message], string_serializer: StringSerializer
):
    serializer: MessageSerializer = MessageSerializer(key=string_serializer)
    deserialized_message: Message = serializer.deserialize(messages[0])
    assert deserialized_message == string_messages[0]
    serialized_message: Message = serializer.serialize(string_messages[0])
    assert serialized_message == messages[0]
