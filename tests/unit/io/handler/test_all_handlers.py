from operator import attrgetter
from typing import List

from pytest_cases import parametrize_with_cases

from esque.io.handlers.base import BaseHandler
from esque.io.messages import Message
from esque.io.stream_decorators import skip_stoppable_events, stop_at_temporary_end_of_all_stream_partitions
from esque.io.stream_events import StreamEvent, TemporaryEndOfPartition, StoppableEvent


@parametrize_with_cases("input_handler, output_handler")
def test_write_read_message(
        event_stream_messages: List[StreamEvent], input_handler: BaseHandler, output_handler: BaseHandler
):
    for msg in event_stream_messages[:2]:
        output_handler.write_message(msg)
    output_handler.close()

    events_retrieved: List[StreamEvent] = []
    for _ in range(2):
        while True:
            actual_event = input_handler.read_stream_event()
            if actual_event.message is not None:
                break
        events_retrieved.append(actual_event)

    events_retrieved.sort(key=lambda event: event.message.timestamp)
    assert events_retrieved == event_stream_messages[:2]


@parametrize_with_cases("input_handler, output_handler")
def test_write_read_many_messages(event_stream_messages, input_handler: BaseHandler, output_handler: BaseHandler):
    output_handler.write_many_messages(event_stream_messages)
    output_handler.close()
    actual_messages = list(
        skip_stoppable_events(stop_at_temporary_end_of_all_stream_partitions(input_handler.stream())))
    actual_messages.sort(key=attrgetter("timestamp"))
    input_handler.close()
    assert event_stream_messages == actual_messages


@parametrize_with_cases("_, output_handler")
def test_write_single_stream_event(output_handler: BaseHandler, _):
    output_handler.write_message(TemporaryEndOfPartition("test", StoppableEvent.ALL_PARTITIONS))


@parametrize_with_cases("_, output_handler")
def test_write_many_stream_events(output_handler: BaseHandler, _):
    output_handler.write_many_messages([TemporaryEndOfPartition("test", StoppableEvent.ALL_PARTITIONS)])


@parametrize_with_cases("input_handler, output_handler")
def test_seek(event_stream_messages, input_handler: BaseHandler, output_handler: BaseHandler):
    seek_offset = 2
    output_handler.write_many_messages(event_stream_messages)
    output_handler.close()

    input_handler.seek(seek_offset)
    actual_messages = list(
        skip_stoppable_events(stop_at_temporary_end_of_all_stream_partitions(input_handler.stream())))
    input_handler.close()

    actual_messages.sort(key=lambda event: event.message.timestamp)
    expected = [event for event in event_stream_messages if event.message.offset >= seek_offset]
    assert actual_messages == expected
