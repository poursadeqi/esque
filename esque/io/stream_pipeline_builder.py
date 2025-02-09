import sys
from dataclasses import dataclass
from typing import Optional

import click

from esque.cli.helpers import ensure_approval
from esque.cli.options import State
from esque.cluster import Cluster
from esque.config import ESQUE_GROUP_ID
from esque.io.handlers.kafka import KafkaHandler, KafkaHandlerConfig
from esque.io.handlers.pipe import PipeHandler, PipeHandlerConfig
from esque.io.serializers import Base64Serializer, RegistryAvroSerializer, StringSerializer
from esque.io.serializers.base import MessageSerializer
from esque.io.serializers.json import JsonSerializer, JsonSerializerConfig
from esque.io.serializers.proto import ProtoSerializer, ProtoSerializerConfig
from esque.io.serializers.raw import RawSerializer
from esque.io.serializers.schema_registry import RegistryAvroSerializerConfig
from esque.io.serializers.string import StringSerializerConfig
from esque.io.serializers.struct import StructSerializer, StructSerializerConfig
from esque.resources.topic import Topic


@dataclass
class StreamOptions:
    input_ctx: str
    input_source: str
    input_key_deserializer: str
    input_value_deserializer: str
    input_key_struct_format: str
    input_value_struct_format: str
    input_topic: str
    consumer_group: str
    preserve_order: bool

    output_ctx: str
    output_dest: str
    output_topic: str
    output_key_serializer: str
    output_value_serializer: str
    output_key_struct_format: str
    output_value_struct_format: str

    number: Optional[int]
    match: str
    last: bool
    pretty_print: bool


def create_key_value_serializer(
    state: State,
    key_deserializer: str,
    key_struct_format: str,
    val_deserializer: str,
    val_struct_format: str,
    consumer_options: StreamOptions,
) -> MessageSerializer:
    key_serializer = create_serializer(state, key_deserializer, key_struct_format, consumer_options)
    val_serializer = create_serializer(state, val_deserializer, val_struct_format, consumer_options)

    return MessageSerializer(key=key_serializer, value=val_serializer)


def create_serializer(state: State, serializer: str, struct_format: str, consumer_options: StreamOptions):
    if serializer == "json":
        return JsonSerializer(JsonSerializerConfig())
    elif serializer == "avro":
        return RegistryAvroSerializer(RegistryAvroSerializerConfig(schema_registry_uri=state.config.schema_registry))
    elif serializer == "str":
        serializer = StringSerializer(StringSerializerConfig())
    elif serializer == "raw":
        serializer = RawSerializer()
    elif serializer == "proto" and consumer_options.input_topic not in state.config.proto:
        raise RuntimeError(
            "topic name was not found in proto configs. please add it to the configuration or use raw serializer"
        )
    elif serializer == "proto" and consumer_options.input_topic in state.config.proto:
        proto_cfg = state.config.proto[consumer_options.input_topic]
        serializer = ProtoSerializer(
            ProtoSerializerConfig(
                protoc_py_path=proto_cfg.get("protoc_py_path"),
                module_name=proto_cfg.get("module_name"),
                class_name=proto_cfg.get("class_name"),
            )
        )
    elif serializer == "struct":
        serializer = StructSerializer(StructSerializerConfig(deserializer_struct_format=struct_format))
    else:
        serializer = Base64Serializer()
    return serializer


def create_output_handler(state: State, serializer: MessageSerializer, stream_options: StreamOptions):
    if stream_options.output_dest == "kafka":
        topic_controller = Cluster().topic_controller
        topic = stream_options.output_topic
        if not topic_controller.topic_exists(stream_options.output_topic):
            if ensure_approval(
                f"Topic {topic!r} does not exist, do you want to create it?", no_verify=state.no_verify
            ):
                topic_controller.create_topics([Topic(topic)])
            else:
                click.echo(click.style("Aborted!", bg="red"))
                return
        return KafkaHandler(
            KafkaHandlerConfig(
                write_serializer=serializer,
                context=stream_options.output_ctx,
                topic=stream_options.output_topic,
            )
        )
    return PipeHandler(
        PipeHandlerConfig(write_serializer=serializer, file=sys.stdout, pretty_print=stream_options.pretty_print)
    )


def create_input_handler(read_serializer: MessageSerializer, stream_options: StreamOptions):
    if stream_options.input_source == "stdin":
        return PipeHandler(
            PipeHandlerConfig(
                read_serializer=read_serializer, pretty_print=stream_options.pretty_print, file=sys.stdin
            )
        )
    consumer_group = stream_options.consumer_group
    if not consumer_group:
        consumer_group = ESQUE_GROUP_ID
    return KafkaHandler(
        KafkaHandlerConfig(
            read_serializer=read_serializer,
            context=stream_options.input_ctx,
            topic=stream_options.input_topic,
            consumer_group_id=consumer_group,
        )
    )
