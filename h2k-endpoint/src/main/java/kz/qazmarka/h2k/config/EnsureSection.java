package kz.qazmarka.h2k.config;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kz.qazmarka.h2k.util.Parsers;

/**
 * Конфигурация подсистемы ensureTopics: целевые партиции/репликация, таймауты, client.id и политика backoff.
 * Значения валидируются и приводятся к минимально допустимым, чтобы избежать некорректных настроек в рантайме.
 */
public final class EnsureSection {
    private static final Logger LOG = LoggerFactory.getLogger(EnsureSection.class);
    private static final String ERR_MIN_FMT = "Некорректное значение {}={}, устанавливаю минимум: {}";

    private static final class Params {
        boolean ensureTopics;
        boolean ensureIncreasePartitions;
        boolean ensureDiffConfigs;
        int topicPartitions;
        short topicReplication;
        long adminTimeoutMs;
        String adminClientId;
        long unknownBackoffMs;
    }

    final boolean ensureTopics;
    final boolean ensureIncreasePartitions;
    final boolean ensureDiffConfigs;
    final int topicPartitions;
    final short topicReplication;
    final long adminTimeoutMs;
    final String adminClientId;
    final long unknownBackoffMs;

    private EnsureSection(Params p) {
        this.ensureTopics = p.ensureTopics;
        this.ensureIncreasePartitions = p.ensureIncreasePartitions;
        this.ensureDiffConfigs = p.ensureDiffConfigs;
        this.topicPartitions = p.topicPartitions;
        this.topicReplication = p.topicReplication;
        this.adminTimeoutMs = p.adminTimeoutMs;
        this.adminClientId = p.adminClientId;
        this.unknownBackoffMs = p.unknownBackoffMs;
    }

    /**
     * Считывает ключи {@code h2k.ensure.*} и возврашает иммутабельный объект с безопасными значениями.
     *
     * @param cfg конфигурация, содержащая параметры ensure
     * @return нормализованный набор ensure-настроек
     */
    static EnsureSection from(Configuration cfg) {
        boolean ensureTopics = cfg.getBoolean(H2kConfig.K_ENSURE_TOPICS, H2kConfig.DEFAULT_ENSURE_TOPICS);
        boolean ensureIncreasePartitions = cfg.getBoolean(H2kConfig.K_ENSURE_INCREASE_PARTITIONS, H2kConfig.DEFAULT_ENSURE_INCREASE_PARTITIONS);
        boolean ensureDiffConfigs = cfg.getBoolean(H2kConfig.K_ENSURE_DIFF_CONFIGS, H2kConfig.DEFAULT_ENSURE_DIFF_CONFIGS);

        int topicPartitions = cfg.getInt(H2kConfig.K_TOPIC_PARTITIONS, H2kConfig.DEFAULT_TOPIC_PARTITIONS);
        if (topicPartitions < 1) {
            LOG.warn(ERR_MIN_FMT, H2kConfig.K_TOPIC_PARTITIONS, topicPartitions, 1);
            topicPartitions = 1;
        }

        short topicReplication = (short) cfg.getInt(H2kConfig.K_TOPIC_REPLICATION_FACTOR, H2kConfig.DEFAULT_TOPIC_REPLICATION);
        if (topicReplication < 1) {
            LOG.warn(ERR_MIN_FMT, H2kConfig.K_TOPIC_REPLICATION_FACTOR, topicReplication, 1);
            topicReplication = 1;
        }

        long adminTimeoutMs = Parsers.readLong(cfg, H2kConfig.K_ADMIN_TIMEOUT_MS, H2kConfig.DEFAULT_ADMIN_TIMEOUT_MS);
        String adminClientId = Parsers.buildAdminClientId(cfg, H2kConfig.K_ADMIN_CLIENT_ID, H2kConfig.DEFAULT_ADMIN_CLIENT_ID);

        long unknownBackoffMs = cfg.getLong(H2kConfig.K_ENSURE_UNKNOWN_BACKOFF_MS, H2kConfig.DEFAULT_UNKNOWN_BACKOFF_MS);
        if (unknownBackoffMs < 1L) {
            LOG.warn(ERR_MIN_FMT, H2kConfig.K_ENSURE_UNKNOWN_BACKOFF_MS, unknownBackoffMs, 1L);
            unknownBackoffMs = 1L;
        }

        Params p = new Params();
        p.ensureTopics = ensureTopics;
        p.ensureIncreasePartitions = ensureIncreasePartitions;
        p.ensureDiffConfigs = ensureDiffConfigs;
        p.topicPartitions = topicPartitions;
        p.topicReplication = topicReplication;
        p.adminTimeoutMs = adminTimeoutMs;
        p.adminClientId = adminClientId;
        p.unknownBackoffMs = unknownBackoffMs;

        return new EnsureSection(p);
    }
}
