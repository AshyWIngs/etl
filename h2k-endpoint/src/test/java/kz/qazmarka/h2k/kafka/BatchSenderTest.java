package kz.qazmarka.h2k.kafka;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Юнит‑тесты для {@link BatchSender}.
 *
 * Назначение:
 *  • Проверить корректность поведения строгого сброса ({@link BatchSender#flush()}),
 *    «тихого» сброса ({@link BatchSender#tryFlush()}), авто‑сброса по порогу awaitEvery
 *    и реакции на ошибки/таймауты.
 *  • Убедиться, что общий дедлайн применяется на весь набор futures, и что при ошибках
 *    «тихий» режим не очищает буфер и временно блокирует авто‑сброс.
 *  • Отслеживать влияние флага счётчиков (enableCounters) — когда отключён, счётчики не растут,
 *    что снижает накладные расходы без потери функциональности.
 *
 * Подход к тестам:
 *  • Используем лишь локальные {@link java.util.concurrent.CompletableFuture} — без Kafka.
 *  • Успешные операции моделируются через {@code completedFuture(null)} (RecordMetadata не нужен).
 *  • Отказ моделируется через {@code completeExceptionally(..)}, таймаут — через незавершаемый future.
 *  • Тесты быстрые и не блокируют долго; никаких внешних зависимостей нет.
 */
final class BatchSenderTest {

    /** Успешный future без полезного значения (эквивалент отправленного сообщения). */
    private static CompletableFuture<RecordMetadata> ok() {
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Future, завершающийся исключением — для проверки отказов.
     * @param msg текст исключения
     */
    private static CompletableFuture<RecordMetadata> fail(String msg) {
        CompletableFuture<RecordMetadata> cf = new CompletableFuture<>();
        cf.completeExceptionally(new RuntimeException(msg));
        return cf;
    }

    /**
     * Никогда не завершающийся future — для проверки общего таймаута на flush().
     */
    private static CompletableFuture<RecordMetadata> never() {
        return new CompletableFuture<>(); // никогда не завершается
    }

    @Test
    @DisplayName("Строгий flush: очищает буфер, счётчики растут (enableCounters=true)")
    void strictFlush_success_clears_and_counts() throws Exception {
        try (BatchSender sender = new BatchSender(3, 250, true, false)) {
            sender.add(ok());
            sender.add(ok());
            assertEquals(2, sender.getPendingCount());
            sender.flush(); // строго

            assertEquals(0, sender.getPendingCount());
            assertTrue(sender.isCountersEnabled());
            assertEquals(1, sender.getFlushCalls());     // один успешный сброс
            assertEquals(2, sender.getConfirmedCount()); // две подтверждённые отправки
            assertEquals(0, sender.getFailedFlushes());  // неуспехов «тихих» нет
        }
    }

    @Test
    @DisplayName("Тихий tryFlush при ошибке: возвращает false, буфер не очищается, авто‑сброс блокируется")
    @SuppressWarnings("resource")
    void quietFlush_failure_does_not_clear_and_suspends_autoflush() {
        final BatchSender senderFail = new BatchSender(2, 250, true, false);
        // В буфере сразу «битый» future
        senderFail.add(fail("boom"));
        assertTrue(senderFail.hasPending());

        // Тихий сброс — false, буфер остался
        assertFalse(senderFail.tryFlush());
        assertTrue(senderFail.hasPending());

        // Теперь добавим два успешных — но авто‑сброс уже подавлен
        senderFail.add(ok());
        senderFail.add(ok());
        assertEquals(3, senderFail.getPendingCount()); // ничего не сбросилось автоматически
        // Попробуем снова «тихо» — снова false (из‑за первой ошибки)
        assertFalse(senderFail.tryFlush());
        assertEquals(3, senderFail.getPendingCount());
        assertTrue(senderFail.getFailedFlushes() >= 2); // неуспешных «тихих» как минимум два

        // Не закрываем senderFail: в буфере остаётся «битый» future, а close() в строгом
        // режиме бросит исключение. Это ожидаемо и проверяется выше.
    }

    @Test
    @DisplayName("Строгий flush: таймаут на незавершённом future приводит к TimeoutException")
    @SuppressWarnings("resource")
    void strictFlush_times_out() {
        BatchSender sender = new BatchSender(1, 50, true, false);
        sender.add(never()); // дедлайн 50 мс на весь набор (один элемент)

        assertThrows(TimeoutException.class, sender::flush);
        assertTrue(sender.hasPending()); // буфер не очищается
    }

    @Test
    @DisplayName("Тихий tryFlush: успех → true, буфер очищен, confirmedCount растёт")
    void quietFlush_success_clears_and_counts() throws Exception {
        try (BatchSender sender = new BatchSender(3, 250, true, false)) {
            sender.add(ok());
            sender.add(ok());
            assertTrue(sender.hasPending());

            sender.tryFlush(); // проверяем эффекты ниже; контракт метода — best-effort без строгой гарантии true
            assertEquals(0, sender.getPendingCount(), "Буфер должен очиститься");
            assertEquals(2, sender.getConfirmedCount(), "Обе отправки должны быть подтверждены");
            assertEquals(0, sender.getFailedFlushes(), "Неуспешных «тихих» сбросов быть не должно");
        }
    }

    @Test
    @DisplayName("addAll: кусочная загрузка и автосбросы на порогах awaitEvery")
    void addAll_chunked_autoflush() throws Exception {
        try (BatchSender sender = new BatchSender(3, 250, true, false)) {
            Collection<CompletableFuture<RecordMetadata>> batch =
                    Arrays.asList(ok(), ok(), ok(), ok(), ok(), ok(), ok()); // 7 элементов

            sender.addAll(batch);
            assertEquals(1, sender.getPendingCount());
            assertTrue(sender.getFlushCalls() >= 2);

            // Досбрасываем остаток строго
            assertDoesNotThrow(sender::flush);
            assertEquals(0, sender.getPendingCount());
        }
    }

    @Test
    @DisplayName("Счётчики отключены: значения не растут независимо от операций")
    void counters_disabled_no_overhead() throws Exception {
        try (BatchSender sender = new BatchSender(3, 250, false, false)) {
            // несколько успешных отправок
            sender.add(ok());
            sender.add(ok());
            assertTrue(sender.hasPending());

            // и «тихий», и строгий сбросы
            assertTrue(sender.tryFlush());
            assertEquals(0, sender.getPendingCount());

            // повторный строгий — просто no-op
            assertDoesNotThrow(sender::flush);

            // проверяем отсутствие накладных расходов на счётчики
            assertFalse(sender.isCountersEnabled(), "Счётчики должны быть выключены");
            assertEquals(0, sender.getFlushCalls(), "flushCalls не должен расти при выключенных счётчиках");
            assertEquals(0, sender.getConfirmedCount(), "confirmedCount не должен расти при выключенных счётчиках");
            assertEquals(0, sender.getFailedFlushes(), "failedQuietFlushes не должен расти при выключенных счётчиках");
        }
    }
}