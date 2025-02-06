from typing import List

import pytest

from esque.io.messages import Message
from esque.io.pipeline import Pipeline, PipelineBuilder
from tests.unit.io.conftest import DummyMessageWriter


def test_pipeline_without_special_decorators_runs_successfully(
    dummy_message_writer: DummyMessageWriter, binary_messages: List[Message], prepared_builder: PipelineBuilder
):
    pipeline = prepared_builder.build()

    assert isinstance(pipeline, Pipeline)
    pipeline.run_pipeline()
    assert dummy_message_writer.get_written_messages() == binary_messages


def test_limited_read_with_absolute_offset(
    dummy_message_writer: DummyMessageWriter, binary_messages: List[Message], prepared_builder: PipelineBuilder
):
    prepared_builder.with_range(start=1, limit=1)
    pipeline = prepared_builder.build()

    assert isinstance(pipeline, Pipeline)
    pipeline.run_pipeline()
    assert len(dummy_message_writer.get_written_messages()) == 1
    assert dummy_message_writer.get_written_messages()[0] in [msg for msg in binary_messages if msg.offset >= 1]


@pytest.mark.xfail(reason="Not yet implemented")
def test_limited_read_with_relative_offset_from_end(
    dummy_message_writer: DummyMessageWriter, binary_messages: List[Message], prepared_builder: PipelineBuilder
):
    prepared_builder.with_range(start=-2, limit=1)
    pipeline = prepared_builder.build()

    assert isinstance(pipeline, Pipeline)
    pipeline.run_pipeline()
    assert dummy_message_writer.get_written_messages() == binary_messages[-2:-1]
