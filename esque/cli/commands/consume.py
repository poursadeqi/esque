from typing import Optional

import click

from esque.cli.autocomplete import list_consumergroups, list_contexts, list_topics
from esque.cli.options import State, default_options
from esque.cluster import Cluster
from esque.config import ESQUE_GROUP_ID
from esque.io.handlers import KafkaHandler
from esque.io.handlers.kafka import KafkaHandlerConfig
from esque.io.handlers.pipe import PipeHandler, PipeHandlerConfig
from esque.io.pipeline import PipelineBuilder
from esque.io.serializers import JsonSerializer, BinarySerializer, RegistryAvroSerializer, StringSerializer
from esque.io.serializers.base import MessageSerializer
from esque.io.serializers.json import JsonSerializerConfig
from esque.io.serializers.binary import BinarySerializerConfig
from esque.io.serializers.registry_avro import RegistryAvroSerializerConfig
from esque.io.serializers.string import StringSerializerConfig
from esque.io.serializers.proto import ProtoSerializer, ProtoSerializerConfig
from esque.io.serializers.struct import StructSerializer, StructSerializerConfig
from esque.io.stream_decorators import event_counter, yield_messages_sorted_by_timestamp, yield_only_matching_messages
from dataclasses import dataclass


@dataclass
class ConsumeOptions:
    state: State
    topic: str
    from_context: str
    number: Optional[int]
    match: str
    last: bool
    consumer_group: str
    preserve_order: bool
    pretty_print: bool
    key_serializer: str
    value_serializer: str
    key_struct_format: str
    value_struct_format: str


@click.command("consume", context_settings={"help_option_names": ["-h", "--help"]})
@click.argument("topic", shell_complete=list_topics)
@click.option(
    "-f",
    "--from",
    "from_context",
    metavar="<source_ctx>",
    help="Source context. If not provided, the current context will be used.",
    shell_complete=list_contexts,
    type=click.STRING,
    required=False,
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
    "so if no new data is coming in nothing will be consumed.",
    default=False,
)
@click.option("--key-struct-format", help="Set this flag to set encoding for key", type=str)
@click.option("--value-struct-format", help="Set this flag to set output encoding for value.", type=str)
@click.option(
    "-k",
    "--key-serializer",
    type=click.Choice(["str", "binary", "avro", "proto", "struct"], case_sensitive=False),
    help="Specify deserialization for keys",
    default="binary",
)
@click.option(
    "-v",
    "--value-serializer",
    type=click.Choice(["str", "binary", "avro", "proto", "struct"], case_sensitive=False),
    help="Specify deserialization for keys",
    default="binary",
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
def consume(*args, **kwargs):
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
    kwargs["state"] = args[0]
    consumer_options = ConsumeOptions(**kwargs)

    if not consumer_options.from_context:
        consumer_options.from_context = consumer_options.state.config.current_context
    consumer_options.state.config.context_switch(consumer_options.from_context)

    builder = PipelineBuilder()
    builder.with_input_message_serializer(create_messages_serializer(consumer_options))

    input_handler = create_input_handler(consumer_options)
    builder.with_input_handler(input_handler)

    output_handler = create_output_handler(consumer_options)
    builder.with_output_handler(output_handler)

    output_message_serializer = create_messages_serializer(consumer_options)
    builder.with_output_message_serializer(output_message_serializer)

    if consumer_options.last:
        start = KafkaHandler.OFFSET_AFTER_LAST_MESSAGE
    else:
        start = KafkaHandler.OFFSET_AT_FIRST_MESSAGE

    builder.with_range(start=start, limit=consumer_options.number)

    if consumer_options.preserve_order:
        topic_data = Cluster().topic_controller.get_cluster_topic(
            consumer_options.topic, retrieve_partition_watermarks=False
        )
        builder.with_stream_decorator(yield_messages_sorted_by_timestamp(len(topic_data.partitions)))

    if consumer_options.match:
        builder.with_stream_decorator(yield_only_matching_messages(consumer_options.match))

    counter, counter_decorator = event_counter()

    builder.with_stream_decorator(counter_decorator)

    builder.build().run_pipeline()


def create_input_handler(consumer_options: ConsumeOptions):
    consumer_group = consumer_options.consumer_group
    if not consumer_group:
        consumer_group = ESQUE_GROUP_ID
    input_handler = KafkaHandler(
        KafkaHandlerConfig(
            scheme="kafka",
            host=consumer_options.from_context,
            path=consumer_options.topic,
            consumer_group_id=consumer_group,
        )
    )
    return input_handler


def create_messages_serializer(consumer_options: ConsumeOptions) -> MessageSerializer:
    key_serializer = create_serializer(
        consumer_options.key_serializer, consumer_options.key_struct_format, consumer_options
    )

    val_serializer = create_serializer(
        consumer_options.value_serializer, consumer_options.value_struct_format, consumer_options
    )

    return MessageSerializer(key_serializer=key_serializer, value_serializer=val_serializer)


def create_output_handler(consumer_options: ConsumeOptions):
    return PipeHandler(
        PipeHandlerConfig(
            scheme="pipe",
            host="stdout",
            path="",
            key_encoding="utf-8",
            value_encoding="utf-8",
            pretty_print="1" if consumer_options.pretty_print else "",
        )
    )


def create_serializer(serializer: str, struct_format: str, consumer_options: ConsumeOptions):
    config = consumer_options.state.config
    if serializer == "json":
        return JsonSerializer(JsonSerializerConfig(scheme="json"))
    elif serializer == "avro":
        return RegistryAvroSerializer(
            RegistryAvroSerializerConfig(scheme="reg-avro", schema_registry_uri=config.schema_registry)
        )
    elif serializer == "str":
        serializer = StringSerializer(StringSerializerConfig(scheme="str"))
    elif serializer == "proto" and consumer_options.topic not in config.proto:
        raise RuntimeError(
            "topic name was not found in proto configs. please add it to the configuration or use raw serializer"
        )
    elif serializer == "proto" and consumer_options.topic in config.proto:
        proto_cfg = config.proto[consumer_options.topic]
        serializer = ProtoSerializer(
            ProtoSerializerConfig(
                scheme="proto",
                protoc_py_path=proto_cfg.get("protoc_py_path"),
                module_name=proto_cfg.get("module_name"),
                class_name=proto_cfg.get("class_name"),
            )
        )
    elif serializer == "struct":
        serializer = StructSerializer(StructSerializerConfig(scheme="struct", struct_format=struct_format))
    else:
        serializer = BinarySerializer(BinarySerializerConfig(scheme="raw"))
    return serializer
