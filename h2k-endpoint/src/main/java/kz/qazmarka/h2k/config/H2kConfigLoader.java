package kz.qazmarka.h2k.config;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import kz.qazmarka.h2k.schema.registry.PhoenixTableMetadataProvider;
import kz.qazmarka.h2k.util.Parsers;

/**
 * Загружает {@link H2kConfig} из HBase {@link Configuration}, инкапсулируя логику парсинга.
 */
final class H2kConfigLoader {

    /** Загружает конфигурацию h2k без табличного провайдера (используется NOOP). */
    H2kConfig load(Configuration cfg, String bootstrap) {
        return load(cfg, bootstrap, PhoenixTableMetadataProvider.NOOP);
    }

    /**
     * Формирует основной {@link H2kConfig}, объединяя секции {@code h2k.*} из конфигурации.
     *
     * @param cfg              исходная конфигурация HBase/endpoint
     * @param bootstrap        обязательный список брокеров (host:port)
     * @param metadataProvider поставщик табличных метаданных Avro (позволяет брать соль/ёмкость из .avsc)
     * @return иммутабельная конфигурация, готовая к передаче в рабочие компоненты
     * @throws IllegalArgumentException если bootstrap не задан
     */
    H2kConfig load(Configuration cfg, String bootstrap, PhoenixTableMetadataProvider metadataProvider) {
        if (bootstrap == null || bootstrap.trim().isEmpty()) {
            throw new IllegalArgumentException("Отсутствует обязательный параметр bootstrap.servers: h2k.kafka.bootstrap.servers пустой или не задан");
        }
        bootstrap = bootstrap.trim();

        TopicSection topic = TopicSection.from(cfg);
        PayloadSection payload = PayloadSection.from(cfg);
        AvroSection avro = AvroSection.from(cfg);
        EnsureSection ensure = EnsureSection.from(cfg);
        ProducerBatchSection batch = ProducerBatchSection.from(cfg);
        TableMapSection tables = TableMapSection.from(cfg);
        Map<String, String> topicConfigs = Parsers.readWithPrefix(cfg, H2kConfig.Keys.TOPIC_CONFIG_PREFIX);

        H2kConfig.Builder builder = new H2kConfig.Builder(bootstrap);

        builder.topic()
                .pattern(topic.topicPattern)
                .maxLength(topic.topicMaxLength)
                .cfNames(topic.cfNames)
                .filterExplicit(topic.cfFilterExplicit)
                .configs(topicConfigs)
                .done();

        builder.tables()
                .saltBytes(tables.saltMap)
                .capacityHints(tables.capacityHints)
                .metadataProvider(metadataProvider)
                .done();

        builder.payload()
                .includeRowKey(payload.includeRowKey)
                .rowkeyEncoding(payload.rowkeyEncoding)
                .rowkeyBase64(payload.rowkeyBase64)
                .includeMeta(payload.includeMeta)
                .includeMetaWal(payload.includeMetaWal)
                .jsonSerializeNulls(payload.jsonSerializeNulls)
                .format(payload.payloadFormat)
                .serializerFactory(payload.serializerFactoryClass)
                .avroMode(avro.mode)
                .avroSchemaDir(avro.schemaDir)
                .schemaRegistryUrls(avro.schemaRegistryUrls)
                .schemaRegistryAuth(avro.auth)
                .avroProps(avro.props)
                .done();

        builder.ensure()
                .enabled(ensure.ensureTopics)
                .allowIncreasePartitions(ensure.ensureIncreasePartitions)
                .allowDiffConfigs(ensure.ensureDiffConfigs)
                .partitions(ensure.topicPartitions)
                .replication(ensure.topicReplication)
                .adminTimeoutMs(ensure.adminTimeoutMs)
                .adminClientId(ensure.adminClientId)
                .unknownBackoffMs(ensure.unknownBackoffMs)
                .done();

        builder.producer()
                .awaitEvery(batch.awaitEvery)
                .awaitTimeoutMs(batch.awaitTimeoutMs)
                .batchCountersEnabled(batch.countersEnabled)
                .batchDebugOnFailure(batch.debugOnFailure)
                .done();

        return builder.build();
    }
}
