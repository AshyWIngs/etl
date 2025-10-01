package kz.qazmarka.h2k.kafka.producer;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Простая политика backoff с псевдослучайным джиттером (±N%) и гарантией
 * минимального шага. Используется как в продьюсерских утилитах, так и в ensure-пакете.
 */
public final class BackoffPolicy {
    private final long minNanos;
    private final int jitterPercent;

    /**
     * @param minNanos минимальное значение, к которому прижимаются отрицательные выборки
     * @param jitterPercent допустимое отклонение в процентах относительно базового шага
     */
    public BackoffPolicy(long minNanos, int jitterPercent) {
        this.minNanos = minNanos;
        this.jitterPercent = jitterPercent;
    }

    /**
     * Рассчитывает задержку вокруг {@code baseNanos} с учётом джиттера и граничных значений типа.
     */
    public long nextDelayNanos(long baseNanos) {
        long jitter = Math.max(1L, (baseNanos * jitterPercent) / 100L);
        long lower = safeSubtract(baseNanos, jitter);
        long upper = safeAdd(baseNanos, jitter);

        long upperExclusive = (upper == Long.MAX_VALUE) ? Long.MAX_VALUE : upper + 1L;
        if (upperExclusive <= lower) {
            upperExclusive = lower + 1L; // минимальный рабочий диапазон
        }

        long candidate = ThreadLocalRandom.current().nextLong(lower, upperExclusive);
        return (candidate < minNanos) ? minNanos : candidate;
    }

    private static long safeAdd(long base, long delta) {
        long r = base + delta;
        if (delta > 0 && r < base) {
            return Long.MAX_VALUE;
        }
        return (delta < 0 && r > base) ? Long.MIN_VALUE : r;
    }

    private static long safeSubtract(long base, long delta) {
        long r = base - delta;
        if (delta > 0 && r > base) {
            return Long.MIN_VALUE;
        }
        return (delta < 0 && r < base) ? Long.MAX_VALUE : r;
    }
}
