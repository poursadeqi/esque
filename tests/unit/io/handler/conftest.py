import functools
from io import StringIO
from typing import Callable

from pytest_cases import fixture

from esque.io.handlers.pipe import PipeHandlerConfig, PipeHandler


@fixture
def pipe_handler_stream() -> StringIO:
    return StringIO()


@fixture
def pipe_handler_factory(pipe_handler_stream: StringIO) -> Callable[[], PipeHandler]:
    def _pipe_handler_factory() -> PipeHandler:
        handler = PipeHandler(PipeHandlerConfig(file=pipe_handler_stream))
        handler.close = functools.partial(pipe_handler_stream.seek, 0)
        return handler

    return _pipe_handler_factory
