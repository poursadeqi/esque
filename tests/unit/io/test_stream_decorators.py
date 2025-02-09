from typing import Iterable, List

from pytest_cases import parametrize_with_cases

from esque.io.messages import Message
from esque.io.stream_decorators import (
    skip_messages_with_offset_below,
    skip_stoppable_events,
    stop_after_nth_message,
    stop_at_temporary_end_of_stream,
    yield_messages_sorted_by_timestamp,
)
from esque.io.stream_events import StreamEvent
from tests.unit.io.conftest import DummyHandler


def test_stop_at_temporary_end_of_stream_with_temporary_end(
        event_stream_messages: List[StreamEvent], dummy_handler: DummyHandler
):
    dummy_handler.set_events(events=event_stream_messages)
    temporarily_ended_stream = stop_at_temporary_end_of_stream(dummy_handler.stream())
    dummy_handler.insert_temporary_end_of_stream(2)
    assert list(skip_stoppable_events(temporarily_ended_stream)) == event_stream_messages[:2]


def test_stop_at_temporary_end_of_stream_with_permanent_end(
        event_stream_messages: List[StreamEvent], dummy_handler: DummyHandler
):
    dummy_handler.set_events(events=event_stream_messages)
    temporarily_ended_stream = stop_at_temporary_end_of_stream(dummy_handler.stream())
    assert list(skip_stoppable_events(temporarily_ended_stream)) == event_stream_messages


def test_reading_until_count_reached(event_stream_messages: List[StreamEvent], dummy_handler: DummyHandler):
    dummy_handler.set_events(events=event_stream_messages)
    dummy_handler.insert_temporary_end_of_stream(1)
    limit_ended_stream = stop_after_nth_message(2)(dummy_handler.stream())
    assert list(skip_stoppable_events(limit_ended_stream)) == event_stream_messages[:2]


def test_skip_messages_with_offset_below(event_stream_messages: List[StreamEvent], dummy_handler: DummyHandler):
    dummy_handler.set_events(events=event_stream_messages)
    stream_with_skipped_messages = skip_messages_with_offset_below(2)(dummy_handler.stream())
    assert list(skip_stoppable_events(stream_with_skipped_messages)) == [
        msg for msg in event_stream_messages if msg.message.offset >= 2
    ]


@parametrize_with_cases("partition_count, input_stream, expected_output", cases=".message_sort_cases")
def test_yield_messages_sorted_by_timestamp(
        partition_count: int, input_stream: Iterable[StreamEvent], expected_output: Iterable[StreamEvent]
):
    actual_output = yield_messages_sorted_by_timestamp(partition_count)(input_stream)

    assert list(actual_output) == list(expected_output)
