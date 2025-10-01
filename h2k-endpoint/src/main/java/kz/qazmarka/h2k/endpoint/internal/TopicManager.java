package kz.qazmarka.h2k.endpoint.internal;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.LongSupplier;

import org.apache.hadoop.hbase.TableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.kafka.ensure.TopicEnsurer;

/**
 * Отвечает за разрешение имён Kafka-топиков и ленивые ensure-вызовы.
 * Использует {@link H2kConfig#topicFor(org.apache.hadoop.hbase.TableName)} для кеширования имён,
 * а также {@link TopicEnsurer} в режиме NOOP/active — вызывающий код не проверяет конфигурацию.
 */
public final class TopicManager {

    private static final Logger LOG = LoggerFactory.getLogger(TopicManager.class);

    private final H2kConfig cfg;
    private final TopicEnsurer topicEnsurer;
    /**
     * Кеш разрешённых имён топиков. Потокобезопасный {@link ConcurrentMap}, чтобы несколько потоков
     * ReplicationEndpoint не гонялись за одним и тем же TableName.
     */
    private final ConcurrentMap<TableName, String> topicCache = new ConcurrentHashMap<>(8);
    private final ConcurrentMap<String, LongSupplier> extraMetrics = new ConcurrentHashMap<>(8);
    public TopicManager(H2kConfig cfg, TopicEnsurer topicEnsurer) {
        this.cfg = Objects.requireNonNull(cfg, "конфигурация h2k");
        this.topicEnsurer = topicEnsurer == null ? TopicEnsurer.disabled() : topicEnsurer;
    }

    /**
     * Возвращает имя Kafka-топика для таблицы, используя кеш и шаблон из {@link H2kConfig}.
     */
    public String resolveTopic(TableName table) {
        return topicCache.computeIfAbsent(table, cfg::topicFor);
    }

    /**
     * Ленивая ensure-проверка: при необходимости вызывает {@link TopicEnsurer#ensureTopic(String)}
     * после валидации имени. В режиме NOOP вызов безопасно игнорируется.
     */
    public void ensureTopicIfNeeded(String topic) {
        if (topic == null || topic.isEmpty()) {
            return;
        }
        if (!topicEnsurer.isEnabled()) {
            return;
        }
        try {
            topicEnsurer.ensureTopic(topic);
        } catch (Exception e) {
            LOG.warn("Проверка/создание топика '{}' не выполнена: {}. Репликацию не прерываю", topic, e.toString());
            if (LOG.isDebugEnabled()) {
                LOG.debug("Трассировка ошибки ensureTopic()", e);
            }
        }
    }

    /**
     * @return {@code true}, если ensure действительно активен (не NOOP).
     */
    public boolean ensureEnabled() {
        return topicEnsurer.isEnabled();
    }

    /**
     * Регистрирует дополнительную числовую метрику, которую необходимо включить в снимок {@link #getMetrics()}.
     * Повторная регистрация с тем же именем перезаписывает предыдущий supplier.
     *
     * @param name     уникальное имя метрики (не пустое)
     * @param supplier поставщик значения; вызывается при формировании снимка
     */
    public void registerMetric(String name, LongSupplier supplier) {
        Objects.requireNonNull(name, "metric name");
        Objects.requireNonNull(supplier, "metric supplier");
        String trimmed = name.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException("metric name must not be blank");
        }
        extraMetrics.put(trimmed, supplier);
    }

    /**
     * Возвращает снимок метрик ensure и зарегистрированных дополнительных счётчиков.
     * Ключи ensure.* сохраняются как есть, дополнительные метрики добавляются без предиктов.
     * Возвращаемая карта неизменяема и безопасна для экспорта в JMX.
     */
    public Map<String, Long> getMetrics() {
        Map<String, Long> ensureMetrics = topicEnsurer.getMetrics();
        if (extraMetrics.isEmpty()) {
            return ensureMetrics;
        }
        Map<String, Long> snapshot = new LinkedHashMap<>(ensureMetrics.size() + extraMetrics.size());
        snapshot.putAll(ensureMetrics);
        for (Map.Entry<String, LongSupplier> e : extraMetrics.entrySet()) {
            try {
                snapshot.put(e.getKey(), e.getValue().getAsLong());
            } catch (RuntimeException ex) {
                LOG.warn("Метрика '{}' недоступна: {}", e.getKey(), ex.toString());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Трассировка ошибки метрики '{}'", e.getKey(), ex);
                }
            }
        }
        return Collections.unmodifiableMap(snapshot);
    }

    /**
     * Закрывает обёрнутый {@link TopicEnsurer}, проглатывая исключения на случай остановки.
     */
    public void closeQuietly() {
        try {
            topicEnsurer.close();
        } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Ошибка при закрытии TopicEnsurer (игнорируется при остановке)", e);
            }
        }
    }
}
