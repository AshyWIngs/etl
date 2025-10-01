package kz.qazmarka.h2k.kafka.ensure.metrics;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Потокобезопасное состояние ensure-процесса: кеш подтверждённых тем, дедлайны повторных
 * попыток и счётчики диагностики. Используется только внутри пакета ensure.
 */
public final class TopicEnsureState {
    public final Set<String> ensured = ConcurrentHashMap.newKeySet();
    public final ConcurrentMap<String, Long> unknownUntil = new ConcurrentHashMap<>();

    public final LongAdder ensureInvocations = new LongAdder();
    public final LongAdder ensureHitCache   = new LongAdder();
    public final LongAdder existsTrue       = new LongAdder();
    public final LongAdder existsFalse      = new LongAdder();
    public final LongAdder existsUnknown    = new LongAdder();
    public final LongAdder createOk         = new LongAdder();
    public final LongAdder createRace       = new LongAdder();
    public final LongAdder createFail       = new LongAdder();

    /** Удаляет дедлайн повторной проверки для темы. */
    public void resetUnknownUntil(String topic) {
        unknownUntil.remove(topic);
    }

    /** Сохраняет дедлайн повторной проверки (System.nanoTime-based). */
    public void scheduleUnknown(String topic, long deadlineNs) {
        unknownUntil.put(topic, deadlineNs);
    }

    /** @return дедлайн backoff или {@code null}, если не запланирован. */
    public Long getUnknownDeadline(String topic) {
        return unknownUntil.get(topic);
    }

    /** Немодифицируемая копия карты дедлайнов (для диагностики). */
    public Map<String, Long> snapshotUnknown() {
        return java.util.Collections.unmodifiableMap(new java.util.HashMap<>(unknownUntil));
    }

    /**
     * Возвращает текущий размер backoff-очереди «неизвестных» топиков.
     * Константная сложность {@code O(1)}. Метод потокобезопасен, так как
     * {@link #unknownUntil} — это {@code ConcurrentMap}, размер которого
     * запрашивается без внешней синхронизации.
     *
     * @return количество элементов в очереди backoff
     */
    public int unknownSize() {
        return unknownUntil.size();
    }
}
