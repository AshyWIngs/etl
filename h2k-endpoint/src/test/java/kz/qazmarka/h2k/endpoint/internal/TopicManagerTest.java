package kz.qazmarka.h2k.endpoint.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.kafka.ensure.TopicEnsurer;

class TopicManagerTest {

    @Test
    @DisplayName("getMetrics() включает зарегистрированные дополнительные счётчики")
    void metricsIncludeRegisteredValues() {
        TopicManager manager = topicManager();
        manager.registerMetric("custom.metric", () -> 42L);
        Map<String, Long> metrics = manager.getMetrics();
        assertEquals(42L, metrics.get("custom.metric"));
        UnsupportedOperationException immutableSnapshot =
                assertThrows(UnsupportedOperationException.class, () -> metrics.put("x", 1L));
        assertEquals(UnsupportedOperationException.class, immutableSnapshot.getClass());
    }

    @Test
    @DisplayName("registerMetric() отклоняет пустое имя")
    void registerMetricRejectsBlankName() {
        TopicManager manager = topicManager();
        IllegalArgumentException invalidName =
                assertThrows(IllegalArgumentException.class, () -> manager.registerMetric("  ", () -> 1L));
        assertEquals(IllegalArgumentException.class, invalidName.getClass());
    }

    private static TopicManager topicManager() {
        Configuration cfg = new Configuration(false);
        cfg.set("h2k.kafka.bootstrap.servers", "mock:9092");
        H2kConfig h2k = H2kConfig.from(cfg, "mock:9092");
        return new TopicManager(h2k, TopicEnsurer.disabled());
    }
}
