package kz.qazmarka.h2k.kafka.ensure;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.kafka.ensure.admin.KafkaTopicAdmin;
import kz.qazmarka.h2k.kafka.ensure.admin.KafkaTopicAdminClient;
import kz.qazmarka.h2k.kafka.ensure.config.TopicEnsureConfig;
import kz.qazmarka.h2k.kafka.ensure.metrics.TopicEnsureState;

/**
 * Высокоуровневый фасад ensure-логики Kafka-топиков.
 * Инкапсулирует {@link TopicEnsureService}, поставляя безопасный NOOP-экземпляр для сценариев,
 * когда ensure отключён конфигурацией или отсутствуют настройки bootstrap. Благодаря этому
 * вызывающий код не обязан проверять {@code null} и может работать с единым API.
 */
public final class TopicEnsurer implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(TopicEnsurer.class);

    private static final TopicEnsurer DISABLED = new TopicEnsurer(null, true);

    private final TopicEnsureService service;
    private final boolean disabledMode;

    private TopicEnsurer(TopicEnsureService service) {
        this(service, false);
    }

    private TopicEnsurer(TopicEnsureService service, boolean disabledMode) {
        this.service = service;
        this.disabledMode = disabledMode;
    }

    /**
     * Возвращает активный энсюрер при включённом {@code h2k.ensure.topics} и корректном bootstrap,
     * либо NOOP-экземпляр в остальных случаях. В логах фиксируется причина отклонения.
     */
    public static TopicEnsurer createIfEnabled(H2kConfig cfg) {
        if (!cfg.isEnsureTopics()) return disabled();
        final String bootstrap = cfg.getBootstrap();
        if (bootstrap == null || bootstrap.trim().isEmpty()) {
            LOG.warn("TopicEnsurer: не задан bootstrap Kafka — ensureTopics будет отключён");
            return disabled();
        }
        Properties props = cfg.kafkaAdminProps();
        String baseId = props.getProperty(AdminClientConfig.CLIENT_ID_CONFIG, "h2k-admin");
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, uniqueClientId(baseId));
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) cfg.getAdminTimeoutMs());
        AdminClient adminClient = AdminClient.create(props);
        KafkaTopicAdmin admin = new KafkaTopicAdminClient(adminClient);
        TopicEnsureConfig config = TopicEnsureConfig.from(cfg);
        TopicEnsureState state = new TopicEnsureState();
        TopicEnsureService service = new TopicEnsureService(admin, config, state);
        return new TopicEnsurer(service);
    }

    /**
     * Возвращает заранее созданный NOOP-экземпляр, который безопасно игнорирует все вызовы.
     */
    public static TopicEnsurer disabled() { return DISABLED; }

    /**
     * Инициирует ensure для одиночной темы; вызов безопасен, даже если ensure отключён (NOOP).
     */
    public void ensureTopic(String topic) {
        if (disabledMode) {
            return;
        }
        service.ensureTopic(topic);
    }

    /**
     * Проверяет наличие темы и при необходимости инициирует ensure; возвращает true только после подтверждения.
     */
    public boolean ensureTopicOk(String topic) {
        if (disabledMode) {
            return false;
        }
        return service.ensureTopicOk(topic);
    }

    /**
     * Пакетный ensure для набора тем; NOOP в отключённом режиме.
     */
    public void ensureTopics(Collection<String> topics) {
        if (disabledMode) {
            return;
        }
        service.ensureTopics(topics);
    }

    /**
     * Снимок внутренних метрик ensure-процесса; для NOOP-реализации возвращает пустую карту.
     */
    public Map<String, Long> getMetrics() {
        if (disabledMode) {
            return java.util.Collections.emptyMap();
        }
        return service.getMetrics();
    }

    public boolean isEnabled() { return !disabledMode; }

    @Override
    /**
     * Закрывает обёрнутый {@link TopicEnsureService}; в режиме NOOP ничего не делает.
     */
    public void close() {
        if (disabledMode) {
            return;
        }
        service.close();
    }

    @Override
    public String toString() {
        if (disabledMode) {
            return "TopicEnsurer[disabled]";
        }
        return service.toString();
    }

    /**
     * Вспомогательный метод генерации уникального client.id для AdminClient.
     * Используется для избежания конфликтов MBean AppInfo.
     */
    private static String uniqueClientId(String base) {
        String host = "host";
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException ignore) { /* no-op */ }
        String pid = ManagementFactory.getRuntimeMXBean().getName();
        int at = pid.indexOf('@');
        if (at > 0) pid = pid.substring(0, at);
        long ts = System.currentTimeMillis();
        String prefix = (base == null || base.trim().isEmpty()) ? "h2k-admin" : base.trim();
        return prefix + "-" + host + "-" + pid + "-" + ts;
    }
}
