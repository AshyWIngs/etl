package kz.qazmarka.h2k.config;

import kz.qazmarka.h2k.util.Parsers;

import org.apache.hadoop.conf.Configuration;

/**
 * Настройки батч-отправки продьюсера (awaitEvery/timeout и диагностические флаги).
 */
public final class ProducerBatchSection {
    final int awaitEvery;
    final int awaitTimeoutMs;
    final boolean countersEnabled;
    final boolean debugOnFailure;

    private ProducerBatchSection(int awaitEvery,
                                 int awaitTimeoutMs,
                                 boolean countersEnabled,
                                 boolean debugOnFailure) {
        this.awaitEvery = awaitEvery;
        this.awaitTimeoutMs = awaitTimeoutMs;
        this.countersEnabled = countersEnabled;
        this.debugOnFailure = debugOnFailure;
    }

    /**
     * Формирует секцию по ключам {@code h2k.producer.*}, гарантируя минимальные значения.
     *
     * @param cfg конфигурация Hadoop с параметрами продьюсера
     * @return секция настроек BatchSender
     */
    static ProducerBatchSection from(Configuration cfg) {
        int awaitEvery = Parsers.readIntMin(cfg, H2kConfig.K_PRODUCER_AWAIT_EVERY, H2kConfig.DEFAULT_AWAIT_EVERY, 1);
        int awaitTimeoutMs = Parsers.readIntMin(cfg, H2kConfig.K_PRODUCER_AWAIT_TIMEOUT_MS, H2kConfig.DEFAULT_AWAIT_TIMEOUT_MS, 1);
        boolean countersEnabled = cfg.getBoolean(H2kConfig.Keys.PRODUCER_BATCH_COUNTERS_ENABLED, H2kConfig.DEFAULT_PRODUCER_BATCH_COUNTERS_ENABLED);
        boolean debugOnFailure = cfg.getBoolean(H2kConfig.Keys.PRODUCER_BATCH_DEBUG_ON_FAILURE, H2kConfig.DEFAULT_PRODUCER_BATCH_DEBUG_ON_FAILURE);
        return new ProducerBatchSection(awaitEvery, awaitTimeoutMs, countersEnabled, debugOnFailure);
    }
}
