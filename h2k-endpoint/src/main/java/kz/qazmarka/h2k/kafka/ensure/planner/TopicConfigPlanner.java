package kz.qazmarka.h2k.kafka.ensure.planner;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kz.qazmarka.h2k.kafka.ensure.admin.KafkaTopicAdmin;
import kz.qazmarka.h2k.kafka.ensure.metrics.TopicEnsureState;
import kz.qazmarka.h2k.kafka.ensure.state.TopicBackoffManager;

/**
 * Формирует параметры создаваемых топиков и выполняет batch create с учётом timeouts/backoff.
 */
public final class TopicConfigPlanner {

    private static final Logger LOG = LoggerFactory.getLogger(TopicConfigPlanner.class);
    private static final String CFG_RETENTION_MS = "retention.ms";
    private static final String CFG_CLEANUP_POLICY = "cleanup.policy";
    private static final String CFG_COMPRESSION_TYPE = "compression.type";
    private static final String CFG_MIN_INSYNC_REPLICAS = "min.insync.replicas";

    private final KafkaTopicAdmin admin;
    private final TopicParams params;
    private final TopicEnsureState state;
    private final TopicBackoffManager backoffManager;

    public TopicConfigPlanner(KafkaTopicAdmin admin,
                              TopicParams params,
                              TopicEnsureState state,
                              TopicBackoffManager backoffManager) {
        this.admin = admin;
        this.params = params;
        this.state = state;
        this.backoffManager = backoffManager;
    }

    /** Собирает {@link NewTopic} для каждого имени, используя параметры из конфигурации. */
    public List<NewTopic> planTopics(List<String> names) {
        List<NewTopic> newTopics = new ArrayList<>(names.size());
        for (String t : names) {
            newTopics.add(params.newTopic(t));
        }
        return newTopics;
    }

    /** Отмечает успешное создание темы, обновляя метрики и кеш. */
    public void recordSuccess(String topic) {
        state.createOk.increment();
        state.ensured.add(topic);
        backoffManager.markSuccess(topic);
    }

    /** Отмечает гонку (TopicExists) как успешный исход и снимает backoff. */
    public void recordRace(String topic) {
        state.createRace.increment();
        state.ensured.add(topic);
        backoffManager.markSuccess(topic);
    }

    /** Регистрирует неуспех и планирует повторную попытку. */
    public void recordFailure(String topic, Throwable cause) {
        state.createFail.increment();
        backoffManager.scheduleRetry(topic);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Не удалось создать Kafka-топик '{}': {}", topic, cause.toString());
        }
    }

    public KafkaTopicAdmin admin() {
        return admin;
    }

    /** Создаёт тему через AdminClient и фиксирует метрики/логи. */
    public void createTopic(String topic, long timeoutMs)
            throws InterruptedException, java.util.concurrent.ExecutionException, java.util.concurrent.TimeoutException {
        NewTopic nt = params.newTopic(topic);
        if (LOG.isDebugEnabled() && !params.configs().isEmpty()) {
            LOG.debug("Применяю конфиги для Kafka-топика '{}': {}", topic, summarizeConfigs());
        }
        admin.createTopic(nt, timeoutMs);
        recordSuccess(topic);
        logTopicCreated(topic);
    }

    /** Пишет INFO о созданной теме и DEBUG о конфигурации при наличии. */
    public void logTopicCreated(String topic) {
        LOG.info("Создал Kafka-топик '{}': partitions={}, replication={}",
                topic, params.partitions(), params.replication());
        if (LOG.isDebugEnabled() && !params.configs().isEmpty()) {
            LOG.debug("Конфиги Kafka-топика '{}': {}", topic, summarizeConfigs());
        }
    }

    /** Возвращает краткое текстовое представление ключевых конфигов темы. */
    public String summarizeConfigs() {
        if (params.configs().isEmpty()) {
            return "без явных конфигов";
        }
        String retention = params.configs().get(CFG_RETENTION_MS);
        String cleanup   = params.configs().get(CFG_CLEANUP_POLICY);
        String comp      = params.configs().get(CFG_COMPRESSION_TYPE);
        String minIsr    = params.configs().get(CFG_MIN_INSYNC_REPLICAS);
        StringBuilder sb = new StringBuilder(128);
        boolean first = true;
        first = append(sb, CFG_RETENTION_MS, retention, first);
        first = append(sb, CFG_CLEANUP_POLICY, cleanup, first);
        first = append(sb, CFG_COMPRESSION_TYPE, comp, first);
        first = append(sb, CFG_MIN_INSYNC_REPLICAS, minIsr, first);
        int others = params.configs().size() - countNonNull(retention, cleanup, comp, minIsr);
        if (others > 0) {
            if (!first) sb.append(", ");
            sb.append("+").append(others).append(" др.");
        }
        return sb.toString();
    }

    private static boolean append(StringBuilder sb, String key, String value, boolean first) {
        if (value == null) return first;
        if (!first) sb.append(", ");
        sb.append(key).append("=").append(value);
        return false;
    }

    private static int countNonNull(Object... vals) {
        int n = 0;
        if (vals != null) {
            for (Object v : vals) if (v != null) n++;
        }
        return n;
    }
}
