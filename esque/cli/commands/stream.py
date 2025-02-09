import click

from esque.cli.autocomplete import list_consumergroups, list_contexts, list_topics
from esque.cli.options import State, default_options
from esque.cluster import Cluster
from esque.io.handlers.kafka import KafkaHandler
from esque.io.pipeline import PipelineBuilder
from esque.io.stream_decorators import event_counter, yield_messages_sorted_by_timestamp, yield_only_matching_messages
from esque.io.stream_pipeline_builder import (
    StreamOptions,
    create_input_handler,
    create_key_value_serializer,
    create_output_handler,
)


@click.command("stream", context_settings={"help_option_names": ["-h", "--help"]})
@click.option(
    "--input-ctx",
    "input_ctx",
    metavar="<input_ctx>",
    help="Source context. If not provided, the current context will be used.",
    shell_complete=list_contexts,
    type=click.STRING,
    required=False,
)
@click.option(
    "--output-ctx",
    metavar="<output_ctx>",
    help="Source context. If not provided, the current context will be used.",
    shell_complete=list_contexts,
    type=click.STRING,
    required=False,
)
@click.option(
    "--input-topic",
    "input_topic",
    metavar="<input_topic>",
    help="Source context. If not provided, the current context will be used.",
    shell_complete=list_topics,
    type=click.STRING,
    required=False,
)
@click.option(
    "--output-topic",
    "output_topic",
    help="Source context. If not provided, the current context will be used.",
    shell_complete=list_topics,
    type=click.STRING,
    required=False,
)
@click.option(
    "--input-source",
    type=click.Choice(["kafka", "stdin"], case_sensitive=False),
    help="input source",
    default="kafka",
)
@click.option(
    "--output-dest",
    type=click.Choice(["kafka", "stdout", "stderr"], case_sensitive=False),
    help="output destination",
    default="stdout",
)
@click.option(
    "-n", "--number", metavar="<n>", help="Number of messages.", type=click.INT, default=None, required=False
)
@click.option(
    "-m",
    "--match",
    metavar="<filter_expression>",
    help="Message filtering expression.",
    type=click.STRING,
    required=False,
)
@click.option(
    "--last/--first",
    help="Start consuming from the earliest or latest offset in the topic."
    "Latest means at the end of the topic _not including_ the last message(s),"
    "so if no new data is coming in nothing will be consumed. this is only applicable if input source if kafka",
    default=False,
)
@click.option("--input-key-struct-format", help="Set this flag to set encoding for key", type=str)
@click.option("--input-value-struct-format", help="Set this flag to set output encoding for value.", type=str)
@click.option("--output-key-struct-format", help="Set this flag to set encoding for key", type=str)
@click.option("--output-value-struct-format", help="Set this flag to set output encoding for value.", type=str)
@click.option(
    "--input-key-deserializer",
    type=click.Choice(["str", "b64", "raw", "avro", "proto", "struct"], case_sensitive=False),
    help="Specify deserialization for keys",
    default="str",
)
@click.option(
    "--input-value-deserializer",
    type=click.Choice(["str", "b64", "raw", "avro", "proto", "struct"], case_sensitive=False),
    help="Specify deserialization for keys",
    default="str",
)
@click.option(
    "--output-key-serializer",
    type=click.Choice(["str", "b64", "raw", "avro", "proto", "struct"], case_sensitive=False),
    help="Specify deserialization for keys",
    default="raw",
)
@click.option(
    "--output-value-serializer",
    type=click.Choice(["str", "b64", "raw", "avro", "proto", "struct"], case_sensitive=False),
    help="Specify deserialization for keys",
    default="raw",
)
@click.option(
    "-c",
    "--consumer-group",
    "consumer_group",
    metavar="<consumer_group>",
    help="Consumer group to store the offset in.",
    type=click.STRING,
    shell_complete=list_consumergroups,
    default=None,
    required=False,
)
@click.option(
    "--preserve-order",
    help="Preserve the order of messages, regardless of their partition. "
    "Order is determined by timestamp and this feature assumes message timestamps are monotonically increasing "
    "within each partition. Will cause the consumer to stop at temporary ends which means it will ignore new messages.",
    default=False,
    is_flag=True,
)
@click.option(
    "-p",
    "--pretty-print",
    help="Use multiple lines to represent each kafka message instead of putting every JSON object into a single "
    "line. Only has an effect when consuming to stdout.",
    default=False,
    is_flag=True,
)
@default_options
def stream(state: State, **kwargs):
    """Consume messages from a topic.

    Read messages from a given topic in a given context. These messages will be written into STDOUT.

    If writing to STDOUT, then data will be represented as a JSON object with the message key and the message value
    always being a string.
    With the --avro option, those strings are JSON serialized objects.
    With the --binary option those strings contain the base64 encoded binary data.
    Without any of the two options, the data in the messages is treated utf-8 encoded strings and will be used as-is.

    \b
    EXAMPLES:
    # Consume the first 10 messages from TOPIC in the current context and print them to STDOUT in order.
    esque consume --first -n 10 --preserve-order --pretty-print --stdout TOPIC

    \b
    # Consume <n> messages, starting from the 10th, from TOPIC in the <source_ctx> context and write them to files.
    esque consume --match "message.offset > 9" -n <n> TOPIC -f <source_ctx>

    \b
    # Extract json objects from keys
    esque consume --stdout --avro TOPIC | jq '.key | fromjson'

    \b
    # Extract binary data from keys (depending on the data this could mess up your console)
    esque consume --stdout --binary TOPIC | jq '.key | @base64d'
    """
    stream_options = StreamOptions(**kwargs)

    if not stream_options.input_ctx:
        stream_options.input_ctx = state.config.current_context
    state.config.context_switch(stream_options.input_ctx)

    input_deserializer = create_key_value_serializer(
        state,
        stream_options.input_key_deserializer,
        stream_options.input_key_struct_format,
        stream_options.input_value_deserializer,
        stream_options.input_value_struct_format,
        stream_options,
    )

    output_serializer = create_key_value_serializer(
        state,
        stream_options.output_key_serializer,
        stream_options.output_key_struct_format,
        stream_options.output_value_serializer,
        stream_options.output_value_struct_format,
        stream_options,
    )

    builder = PipelineBuilder()
    builder.with_input_handler(create_input_handler(input_deserializer, stream_options))
    builder.with_output_handler(create_output_handler(state, output_serializer, stream_options))

    if stream_options.last:
        start = KafkaHandler.OFFSET_AFTER_LAST_MESSAGE
    else:
        start = KafkaHandler.OFFSET_AT_FIRST_MESSAGE

    builder.with_range(start=start, limit=stream_options.number)

    if stream_options.preserve_order:
        topic_data = Cluster().topic_controller.get_cluster_topic(
            stream_options.input_topic, retrieve_partition_watermarks=False
        )
        builder.with_stream_decorator(yield_messages_sorted_by_timestamp(len(topic_data.partitions)))

    if stream_options.match:
        builder.with_stream_decorator(yield_only_matching_messages(stream_options.match))

    counter, counter_decorator = event_counter()

    builder.with_stream_decorator(counter_decorator)

    builder.build().run_pipeline()
