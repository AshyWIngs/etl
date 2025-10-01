package kz.qazmarka.h2k.kafka.producer;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Тесты для {@link BackoffPolicy}: проверяем рамки джиттера и поведение минимального порога.
 */
final class BackoffPolicyTest {

    @Test
    @DisplayName("BackoffPolicy удерживает задержку внутри допустимого коридора")
    void jitterWithinBand() {
        BackoffPolicy policy = new BackoffPolicy(TimeUnit.MILLISECONDS.toNanos(5), 20);
        long base = TimeUnit.MILLISECONDS.toNanos(100);
        long jitter = Math.max(1L, (base * 20) / 100L);

        for (int i = 0; i < 256; i++) {
            long value = policy.nextDelayNanos(base);
            assertTrue(value >= base - jitter, "нижняя граница нарушена");
            assertTrue(value <= base + jitter, "верхняя граница нарушена");
        }
    }

    @Test
    @DisplayName("BackoffPolicy применяет минимальный порог при малых значениях")
    void respectsMinimumFloor() {
        BackoffPolicy policy = new BackoffPolicy(1_000L, 50);
        long value = policy.nextDelayNanos(10L);
        assertTrue(value >= 1_000L, "мин. порог игнорирован");
    }
}
