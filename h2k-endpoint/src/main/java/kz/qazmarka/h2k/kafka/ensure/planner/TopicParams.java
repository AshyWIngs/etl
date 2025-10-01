package kz.qazmarka.h2k.kafka.ensure.planner;

import java.util.Map;

import org.apache.kafka.clients.admin.NewTopic;

import kz.qazmarka.h2k.kafka.ensure.config.TopicEnsureConfig;

/**
 * Параметры создаваемого Kafka-топика: partitions/replication/configs.
 * Выделены в отдельный класс, чтобы исключить дублирование при одиночном и batch-создании.
 */
public final class TopicParams {
    private final int partitions;
    private final short replication;
    private final Map<String, String> configs;

    private TopicParams(int partitions, short replication, Map<String, String> configs) {
        this.partitions = partitions;
        this.replication = replication;
        this.configs = configs;
    }

    public static TopicParams from(TopicEnsureConfig config) {
        return new TopicParams(config.topicPartitions(), config.topicReplication(), config.topicConfigs());
    }

    /** Строит {@link NewTopic} с заданными partitions/replication/configs. */
    public NewTopic newTopic(String name) {
        NewTopic nt = new NewTopic(name, partitions, replication);
        if (!configs.isEmpty()) {
            nt.configs(configs);
        }
        return nt;
    }

    public int partitions() {
        return partitions;
    }

    public short replication() {
        return replication;
    }

    public Map<String, String> configs() {
        return configs;
    }
}
