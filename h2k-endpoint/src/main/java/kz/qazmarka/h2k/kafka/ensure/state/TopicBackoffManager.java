package kz.qazmarka.h2k.kafka.ensure.state;

import java.util.concurrent.TimeUnit;

import kz.qazmarka.h2k.kafka.ensure.metrics.TopicEnsureState;
import kz.qazmarka.h2k.kafka.producer.BackoffPolicy;

/**
 * Управляет backoff-циклами при повторных ensure-операциях: хранит дедлайны по темам,
 * рассчитывает задержки через {@link BackoffPolicy} и снимает блокировки после успеха.
 * Предназначен исключительно для внутреннего использования ensure-сервиса.
 */
public final class TopicBackoffManager {

    private final TopicEnsureState state;
    private final BackoffPolicy policy;
    private final long baseDelayNs;

    public TopicBackoffManager(TopicEnsureState state, long baseDelayMs) {
        this(state, baseDelayMs,
                new BackoffPolicy(TimeUnit.MILLISECONDS.toNanos(1), 20));
    }

    public TopicBackoffManager(TopicEnsureState state, long baseDelayMs, BackoffPolicy policy) {
        this.state = state;
        this.policy = policy;
        this.baseDelayNs = TimeUnit.MILLISECONDS.toNanos(Math.max(0L, baseDelayMs));
    }

    /**
     * @return {@code true}, если повтор стоит отложить из-за активного backoff.
     */
    public boolean shouldSkip(String topic) {
        Long deadline = state.getUnknownDeadline(topic);
        if (deadline == null) {
            return false;
        }
        long now = System.nanoTime();
        if (now < deadline) {
            return true;
        }
        state.resetUnknownUntil(topic);
        return false;
    }

    /** Планирует повторную попытку ensure по теме. */
    public void scheduleRetry(String topic) {
        long delayNs = nextDelayNanos();
        state.scheduleUnknown(topic, System.nanoTime() + delayNs);
    }

    /** Снимает backoff для темы после успешного ensure. */
    public void markSuccess(String topic) {
        state.resetUnknownUntil(topic);
    }

    public long baseDelayMs() {
        return TimeUnit.NANOSECONDS.toMillis(baseDelayNs);
    }

    /** Вычисляет очередную задержку с джиттером (миллисекунды). */
    public long computeDelayMillis() {
        return TimeUnit.NANOSECONDS.toMillis(nextDelayNanos());
    }

    private long nextDelayNanos() {
        return policy.nextDelayNanos(baseDelayNs);
    }
}
