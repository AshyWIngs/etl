package kz.qazmarka.h2k.payload.serializer.avro;

import java.util.List;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

/**
 * Реализация {@link SchemaRegistryClientFactory}, создающая {@link CachedSchemaRegistryClient}
 * с заданным кешем (identity map). Используется для переиспользования соединений SR.
 */
final class CachedSchemaRegistryClientFactory implements SchemaRegistryClientFactory {

    /** Единственный экземпляр (shared singleton). */
    static final SchemaRegistryClientFactory INSTANCE = new CachedSchemaRegistryClientFactory();

    private CachedSchemaRegistryClientFactory() { /* singleton */ }

    @Override
    public SchemaRegistryClient create(List<String> urls,
                                       Map<String, Object> clientConfig,
                                       int identityMapCapacity) {
        return new CachedSchemaRegistryClient(urls, identityMapCapacity, clientConfig);
    }
}
