def get_supported_schema() -> dict:
    from esque.io.serializers.schema_registry.memory import InMemorySchemaRegistryClient
    from esque.io.serializers.schema_registry.rest import RestSchemaRegistryClient

    return {
        "http": RestSchemaRegistryClient,
        "https": RestSchemaRegistryClient,
        "memory": InMemorySchemaRegistryClient,
    }
