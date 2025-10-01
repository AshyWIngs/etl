package kz.qazmarka.h2k.payload.serializer.avro;

import java.util.List;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

/**
 * Минимальный интерфейс фабрики {@link SchemaRegistryClient}, позволяющий подменять реализацию в тестах.
 */
@FunctionalInterface
public interface SchemaRegistryClientFactory {

    /**
     * Создаёт экземпляр {@link SchemaRegistryClient} с заданными параметрами кеша и конфигурации.
     *
     * @param urls                  список URL Schema Registry (не пустой)
     * @param clientConfig          конфигурация клиента (basic auth и прочие опции)
     * @param identityMapCapacity   размер кеша схем на subject
     * @return готовый {@link SchemaRegistryClient}
     */
    SchemaRegistryClient create(List<String> urls,
                                Map<String, Object> clientConfig,
                                int identityMapCapacity);

    /**
     * @return фабрика по умолчанию, создающая {@link io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient}.
     */
    static SchemaRegistryClientFactory cached() {
        return CachedSchemaRegistryClientFactory.INSTANCE;
    }
}
