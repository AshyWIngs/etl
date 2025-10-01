package kz.qazmarka.h2k.kafka.ensure;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import kz.qazmarka.h2k.kafka.ensure.config.TopicEnsureConfig;
import kz.qazmarka.h2k.kafka.ensure.metrics.TopicEnsureState;

/**
 * Проверяет контракт {@link TopicEnsureService#getBackoffSnapshot()} на корректные значения и защиту от модификации.
 */
class TopicEnsureServiceBackoffSnapshotTest {

    @Test
    @DisplayName("Пустой backoff → неизменяемая пустая карта")
    void emptySnapshot() {
        TopicEnsureState state = new TopicEnsureState();
        try (TopicEnsureService service = service(state)) {
            Map<String, Long> snapshot = service.getBackoffSnapshot();
            assertTrue(snapshot.isEmpty(), "ожидается пустой снимок");
            assertEquals(Collections.emptyMap(), snapshot, "коллекция должна совпадать с emptyMap");
            UnsupportedOperationException putError = assertThrows(UnsupportedOperationException.class, () -> snapshot.put("x", 1L));
            assertNotNull(putError, "ожидалось исключение UnsupportedOperationException при snapshot.put");
        }
    }

    @Test
    @DisplayName("Будущие дедлайны → положительные значения, прошедшие → 0, карта неизменяема")
    void snapshotValuesAreNonNegative() {
        TopicEnsureState state = new TopicEnsureState();
        long now = System.nanoTime();
        state.scheduleUnknown("topic.future", now + TimeUnit.MILLISECONDS.toNanos(120));
        state.scheduleUnknown("topic.past", now - TimeUnit.MILLISECONDS.toNanos(40));

        try (TopicEnsureService service = service(state)) {
            Map<String, Long> snapshot = service.getBackoffSnapshot();

            assertEquals(2, snapshot.size(), "должны присутствовать две темы");
            snapshot.values().forEach(v -> assertTrue(v >= 0L, "значения не могут быть отрицательными"));
            assertEquals(0L, snapshot.get("topic.past"), "прошедший дедлайн должен обнуляться");
            assertNotSame(Collections.emptyMap(), snapshot, "для непустого состояния возвращается собственная копия");
            UnsupportedOperationException clearError = assertThrows(UnsupportedOperationException.class, snapshot::clear, "снимок не должен модифицироваться");
            assertNotNull(clearError, "ожидалось UnsupportedOperationException при snapshot.clear");
        }
    }

    private static TopicEnsureService service(TopicEnsureState state) {
        TopicEnsureConfig config = TopicEnsureConfig.builder()
                .topicNameMaxLen(249)
                .topicPartitions(1)
                .topicReplication((short) 1)
                .adminTimeoutMs(1_000L)
                .unknownBackoffMs(200L)
                .build();
        return new TopicEnsureService(null, config, state);
    }
}
