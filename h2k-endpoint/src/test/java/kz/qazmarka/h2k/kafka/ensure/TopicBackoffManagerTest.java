package kz.qazmarka.h2k.kafka.ensure;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import kz.qazmarka.h2k.kafka.ensure.metrics.TopicEnsureState;
import kz.qazmarka.h2k.kafka.ensure.state.TopicBackoffManager;

class TopicBackoffManagerTest {

    @Test
    @DisplayName("scheduleRetry → shouldSkip до истечения дедлайна")
    void scheduleRetrySkipsUntilDeadline() {
        TopicEnsureState state = new TopicEnsureState();
        TopicBackoffManager manager = new TopicBackoffManager(state, 50L); // 50 мс базовый backoff

        manager.scheduleRetry("topic");
        assertTrue(manager.shouldSkip("topic"), "должен пропускать до истечения backoff");

        // имитируем истечение окна ожидания
        state.scheduleUnknown("topic", System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(1));
        assertFalse(manager.shouldSkip("topic"), "после истечения backoff повтор разрешён");
    }

    @Test
    @DisplayName("markSuccess снимает топик с backoff")
    void markSuccessClearsBackoff() {
        TopicEnsureState state = new TopicEnsureState();
        TopicBackoffManager manager = new TopicBackoffManager(state, 10L);

        manager.scheduleRetry("topic");
        manager.markSuccess("topic");

        assertFalse(manager.shouldSkip("topic"), "успешный ensure очищает backoff");
    }

    @Test
    @DisplayName("computeDelayMillis укладывается в диапазон base±20%")
    void computeDelayWithinBand() {
        TopicEnsureState state = new TopicEnsureState();
        TopicBackoffManager manager = new TopicBackoffManager(state, 100L);

        for (int i = 0; i < 10; i++) {
            long delay = manager.computeDelayMillis();
            assertTrue(delay >= 80L && delay <= 120L,
                    "ожидался джиттер в пределах 20%, получено " + delay);
        }
    }
}
