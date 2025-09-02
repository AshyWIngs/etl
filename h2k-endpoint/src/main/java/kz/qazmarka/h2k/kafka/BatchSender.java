package kz.qazmarka.h2k.kafka;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Простая утилита для дозированной отправки в Kafka:
 * накапливает futures и периодически ждёт подтверждений, не допуская переполнения in‑flight.
 *
 * Потокобезопасность: экземпляр не потокобезопасен. Предназначен для использования из одного потока.
 *
 * Использование (пример):
 *   BatchSender sender = new BatchSender(500, 180_000);
 *   sender.add(producer.send(record1));
 *   ...
 *   sender.flush(); // дождаться «хвоста»
 *   // или пакетно:
 *   sender.addAll(futures);
 *   // удобно использовать try-with-resources
 *   try (BatchSender s = new BatchSender(500, 180_000)) {
 *       s.add(producer.send(record1));
 *   } // close() дождётся «хвоста»
 *
 * Режимы ожидания:
 * - Строгий — метод {@link #flush()} (также вызывается в {@link #close()}): блокирующе ждёт все накопленные futures и при любой ошибке выбрасывает исключение. Буфер не очищается при неуспехе.
 * - Тихий — {@link #tryFlush()} и автосброс из {@link #add(Future)} при достижении порога: ждёт подтверждения, но не бросает checked‑исключений — возвращает true/false. При неуспехе буфер не очищается.
 * По желанию можно включить лёгкие счётчики (конструктор с флагом enableCounters). Дополнительно можно включить точечный DEBUG‑лог ошибок (четвёртый параметр конструктора).
 *
 * Подсказки по выбору awaitEvery:
 * - fast (макс. скорость, умеренная надёжность): 200–1000. Хорошо грузит сеть и брокеры.
 * - balanced (компромисс скорость/надёжность): 500–2000. Оптимально для большинства нагрузок.
 * - reliable (acks=all, idempotence=true, max.in.flight=1): 100–500. Бóльшие значения не вредят, но выигрыша немного, т.к. in‑flight ограничен самим продьюсером.
 * Чем выше awaitEvery, тем реже ожидания и меньше нагрузка на CPU, но тем дольше «хвост» при остановке/сбое.
 * Для коротких спайков зачастую достаточно 200–500; для равномерных потоков — 1000–2000.
 */
public final class BatchSender implements AutoCloseable {
    /** Логгер класса. Все сообщения — на русском языке. */
    private static final Logger LOG = LoggerFactory.getLogger(BatchSender.class);
    private static final String TIMEOUT_MSG = "Таймаут ожидания подтверждений от Kafka";

    /** Сколько отправок накапливать перед ожиданием подтверждений. */
    private final int awaitEvery;
    /** Общий таймаут ожидания подтверждений (на один цикл flush), миллисекунды. */
    private final int awaitTimeoutMs;
    /** Буфер накопленных futures на подтверждение от Kafka. */
    private final ArrayList<Future<RecordMetadata>> sent;

    /** Включать ли диагностические счётчики (влияние на горячий путь минимальное). */
    private final boolean enableCounters;
    /** Писать ли подробный DEBUG при неуспехе «тихого» сброса. */
    private final boolean debugOnFailure;
    /** Количество подтверждённых отправок, суммарно по успешным flush/tryFlush. */
    private long confirmedCount;
    /** Сколько раз успешно вызывали flush/tryFlush. */
    private long flushCalls;
    /** Сколько раз фиксировался неуспех flush/tryFlush. */
    private long failedFlushes;

    /**
     * Предохранитель: после неуспешного «тихого» сброса авто‑сбросы из add()/addAll()
     * временно подавляются до первого успешного flush/tryFlush.
     */
    private boolean autoFlushSuspended;

    /**
     * Упрощённый конструктор: счётчики и DEBUG отключены.
     * @param awaitEvery порог ожидания подтверждений (>0)
     * @param awaitTimeoutMs общий таймаут ожидания, мс (>0)
     */
    public BatchSender(int awaitEvery, int awaitTimeoutMs) {
        this(awaitEvery, awaitTimeoutMs, false, false);
    }

    /**
     * Конструктор с включаемыми счётчиками (DEBUG по-прежнему отключён).
     * @param awaitEvery порог ожидания подтверждений (>0)
     * @param awaitTimeoutMs общий таймаут ожидания, мс (>0)
     * @param enableCounters включить ли диагностические счётчики
     */
    public BatchSender(int awaitEvery, int awaitTimeoutMs, boolean enableCounters) {
        this(awaitEvery, awaitTimeoutMs, enableCounters, false);
    }

    /**
     * @param awaitEvery     сколько отправок копить перед ожиданием подтверждений (&gt; 0)
     * @param awaitTimeoutMs общий таймаут ожидания подтверждений на один цикл flush, миллисекунды (&gt; 0)
     * @param enableCounters включить лёгкие диагностические счётчики (по умолчанию false)
     * @param debugOnFailure логировать причины неуспеха в DEBUG (по умолчанию false)
     * @throws IllegalArgumentException если параметры некорректны
     */
    public BatchSender(int awaitEvery, int awaitTimeoutMs, boolean enableCounters, boolean debugOnFailure) {
        if (awaitEvery <= 0) {
            throw new IllegalArgumentException("awaitEvery должен быть > 0");
        }
        if (awaitTimeoutMs <= 0) {
            throw new IllegalArgumentException("awaitTimeoutMs должен быть > 0");
        }
        this.awaitEvery = awaitEvery;
        this.awaitTimeoutMs = awaitTimeoutMs;
        this.enableCounters = enableCounters;
        this.debugOnFailure = debugOnFailure;
        this.sent = new ArrayList<>(awaitEvery);
        this.confirmedCount = 0L;
        this.flushCalls = 0L;
        this.failedFlushes = 0L;
        this.autoFlushSuspended = false;
    }

    /**
     * Добавить future в буфер. При достижении порога сразу «тихо» ждёт подтверждений
     * (не бросает исключения). Для строгой семантики вызывайте затем {@link #flush()}.
     */
    public void add(Future<RecordMetadata> f) {
        if (f == null) {
            return;
        }
        sent.add(f);
        if (sent.size() >= awaitEvery && !autoFlushSuspended) {
            // "Тихий" сброс: не нарушает горячий путь checked-исключениями.
            // Ошибку можно получить позже через явный flush().
            boolean cleared = quietFlush("add");
            if (!cleared) {
                autoFlushSuspended = true; // больше не пытаемся авто‑сбрасывать до успешного flush
            }
        }
    }

    /**
     * Добавить коллекцию futures «кусками», минимизируя число проверок/сбросов.
     * Тихий {@code flushInternal(false)} вызывается только при реальном достижении/превышении
     * порога {@code awaitEvery} за серию вставок. Остаток (&lt; {@code awaitEvery}) остаётся
     * в буфере до следующего {@link #add(Future)} или явного {@link #flush()} / {@link #tryFlush()}.
     */
    /**
     * Попытаться выполнить «тихий» авто‑сброс. Возвращает новое значение счётчика remainingToThreshold:
     * awaitEvery — если буфер очищён; Integer.MAX_VALUE — если авто‑сброс подавлён из‑за неуспеха.
     * Также поднимает флаг autoFlushSuspended при неуспехе.
     */
    private int tryAutoQuietFlush(String where) {
        boolean cleared = quietFlush(where);
        if (!cleared) {
            autoFlushSuspended = true;
            return Integer.MAX_VALUE;
        }
        return awaitEvery;
    }

    /**
     * Рассчитать начальное количество оставшихся до порога отправок для addAll() с учётом текущего состояния.
     */
    private int initialRemainingForAddAll() {
        if (autoFlushSuspended) return Integer.MAX_VALUE;
        int remainingToThreshold = awaitEvery - sent.size();
        if (remainingToThreshold > 0) return remainingToThreshold;
        return tryAutoQuietFlush("addAll/iter-pre");
    }

    public void addAll(Collection<? extends Future<RecordMetadata>> futures) {
        if (futures == null || futures.isEmpty()) {
            return; // быстрый путь
        }
        // Предварительно зарезервируем место под вставки (микро-оптимизация под ArrayList)
        sent.ensureCapacity(sent.size() + futures.size());

        // Сколько элементов осталось добавить до ближайшего порога awaitEvery
        int remainingToThreshold = initialRemainingForAddAll();

        for (Future<RecordMetadata> f : futures) {
            if (f == null) {
                continue;
            }
            sent.add(f);
            if (--remainingToThreshold == 0) {
                remainingToThreshold = autoFlushSuspended ? Integer.MAX_VALUE : tryAutoQuietFlush("addAll/iter");
            }
        }
        // Остаток < awaitEvery оставляем в буфере — поведение идентично множественным add()
    }

    /**
     * Дождаться подтверждения для всех futures с одним общим таймаутом.
     * Ветвится на оптимизированный путь для RandomAccess и обычный итераторный.
     */
    private static void waitAll(List<Future<RecordMetadata>> futures, int timeoutMs)
            throws InterruptedException, java.util.concurrent.ExecutionException, java.util.concurrent.TimeoutException {
        if (futures == null || futures.isEmpty()) {
            return;
        }
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        if (futures instanceof java.util.RandomAccess) {
            waitAllRA(futures, deadline);
        } else {
            waitAllIter(futures, deadline);
        }
    }

    /** Быстрый путь: доступ к списку по индексу (RandomAccess). */
    private static void waitAllRA(List<Future<RecordMetadata>> futures, long deadlineNs)
            throws InterruptedException, java.util.concurrent.ExecutionException, java.util.concurrent.TimeoutException {
        for (int i = 0, n = futures.size(); i < n; i++) {
            awaitOne(futures.get(i), deadlineNs);
        }
    }

    /** Универсальный путь: обход коллекции через итератор. */
    private static void waitAllIter(List<Future<RecordMetadata>> futures, long deadlineNs)
            throws InterruptedException, java.util.concurrent.ExecutionException, java.util.concurrent.TimeoutException {
        for (Future<RecordMetadata> f : futures) {
            awaitOne(f, deadlineNs);
        }
    }

    /**
     * Ожидание подтверждения одного future с учётом общего дедлайна.
     * Если {@code f == null}, метод ничего не делает.
     */
    private static void awaitOne(Future<RecordMetadata> f, long deadlineNs)
            throws InterruptedException, java.util.concurrent.ExecutionException, java.util.concurrent.TimeoutException {
        if (f == null) {
            return;
        }
        long leftNs = deadlineNs - System.nanoTime();
        if (leftNs <= 0L) {
            throw new java.util.concurrent.TimeoutException(TIMEOUT_MSG);
        }
        f.get(leftNs, TimeUnit.NANOSECONDS);
    }

    /**
     * Внутренний общий метод ожидания подтверждений.
     * @param strict если true — при неуспехе выбрасывает исключение; если false — возвращает false
     * @return true при успешном ожидании, false при неуспехе в «тихом» режиме
     */
    private boolean flushInternal(boolean strict)
            throws InterruptedException, java.util.concurrent.ExecutionException, java.util.concurrent.TimeoutException {
        if (strict) {
            flushStrictInternal();
            return true;
        }
        return flushQuietInternal();
    }

    /** Строгий сброс: при неуспехе выбрасывает исключение. */
    private void flushStrictInternal()
            throws InterruptedException, java.util.concurrent.ExecutionException, java.util.concurrent.TimeoutException {
        if (sent.isEmpty()) {
            return;
        }
        final int n = sent.size();
        waitAll(sent, awaitTimeoutMs);
        sent.clear();
        autoFlushSuspended = false; // успешный строгий сброс снимает блокировку
        if (enableCounters) {
            flushCalls++;
            confirmedCount += n;
        }
    }

    /** Тихий сброс: при неуспехе возвращает false и при необходимости пишет DEBUG. */
    private boolean flushQuietInternal() {
        return flushQuietInternal("flushInternal");
    }

    /**
     * Тихий сброс с указанием контекста для лога (например, "add", "addAll/iter", "tryFlush").
     * При неуспехе возвращает false; обновляет счётчики и пишет DEBUG‑сообщение, если включено.
     */
    private boolean flushQuietInternal(String where) {
        if (sent.isEmpty()) {
            return true;
        }
        final boolean dbg = debugOnFailure && LOG.isDebugEnabled();
        final int n = sent.size();
        try {
            waitAll(sent, awaitTimeoutMs);
            sent.clear();
            autoFlushSuspended = false; // успешный тихий сброс снимает блокировку
            if (enableCounters) {
                flushCalls++;
                confirmedCount += n;
            }
            return true;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            if (enableCounters) {
                failedFlushes++;
            }
            if (dbg) {
                LOG.debug("{}: тихий сброс прерван: size={}, pendingBeforeClear={}", where, n, sent.size(), ie);
            }
            return false;
        } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException e) {
            if (enableCounters) {
                failedFlushes++;
            }
            if (dbg) {
                LOG.debug("{}: тихий сброс неуспешен: size={}, pendingBeforeClear={}", where, n, sent.size(), e);
            }
            return false;
        }
    }

    /**
     * Последовательно ждёт подтверждений каждого future до первого сбоя.
     * Возвращает количество успешно подтверждённых элементов.
     * Важно: буфер не очищается и счётчики не изменяются — метод
     * предназначен для диагностики (например, чтобы понять, на каком элементе
     * произошёл первый сбой). Для обычной работы используйте {@link #flush()}.
     */
    public int flushUpToFirstFailure()
            throws InterruptedException, java.util.concurrent.ExecutionException, java.util.concurrent.TimeoutException {
        if (sent.isEmpty()) {
            return 0;
        }
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(awaitTimeoutMs);
        final boolean dbg = debugOnFailure && LOG.isDebugEnabled();
        if (sent instanceof java.util.RandomAccess) {
            return flushUpToFirstFailureRA(deadline, dbg);
        }
        return flushUpToFirstFailureIter(deadline, dbg);
    }

    private int flushUpToFirstFailureRA(long deadlineNs, boolean dbg)
            throws InterruptedException, java.util.concurrent.ExecutionException, java.util.concurrent.TimeoutException {
        int ok = 0;
        for (int idx = 0, n = sent.size(); idx < n; idx++) {
            Future<RecordMetadata> f = sent.get(idx);
            if (f == null) {
                continue;
            }
            long leftNs = deadlineNs - System.nanoTime();
            if (leftNs <= 0L) {
                throw new java.util.concurrent.TimeoutException(TIMEOUT_MSG);
            }
            try {
                f.get(leftNs, TimeUnit.NANOSECONDS);
                ok++;
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                if (dbg) {
                    LOG.debug("flushUpToFirstFailure() прерван на индексе {}", ok, ie);
                }
                throw ie;
            } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException e) {
                if (dbg) {
                    LOG.debug("flushUpToFirstFailure() первый сбой на индексе {}", ok, e);
                }
                throw e;
            }
        }
        return ok;
    }

    private int flushUpToFirstFailureIter(long deadlineNs, boolean dbg)
            throws InterruptedException, java.util.concurrent.ExecutionException, java.util.concurrent.TimeoutException {
        int ok = 0;
        for (Future<RecordMetadata> f : sent) {
            if (f == null) {
                continue;
            }
            long leftNs = deadlineNs - System.nanoTime();
            if (leftNs <= 0L) {
                throw new java.util.concurrent.TimeoutException(TIMEOUT_MSG);
            }
            try {
                f.get(leftNs, TimeUnit.NANOSECONDS);
                ok++;
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                if (dbg) {
                    LOG.debug("flushUpToFirstFailure() прерван на индексе {}", ok, ie);
                }
                throw ie;
            } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException e) {
                if (dbg) {
                    LOG.debug("flushUpToFirstFailure() первый сбой на индексе {}", ok, e);
                }
                throw e;
            }
        }
        return ok;
    }

    /**
     * Немедленно ожидает подтверждений для накопленных отправок.
     * Объединённый таймаут применяется на весь набор futures.
     *
     * Семантика ошибок: при первой ошибке отдельного future будет выброшено
     * соответствующее исключение (обычно {@link java.util.concurrent.ExecutionException}
     * с исходной причиной внутри). Повторный вызов {@code flush()} дождёт оставшиеся
     * futures (если буфер не был очищён), то есть можно обрабатывать ошибку снаружи
     * и затем попытаться завершить ожидание ещё раз.
     */
    public void flush()
            throws InterruptedException, java.util.concurrent.ExecutionException, java.util.concurrent.TimeoutException {
        flushInternal(true);
    }

    /**
     * Пытается немедленно дождаться подтверждений для накопленных отправок,
     * не выбрасывая исключения.
     *
     * @return {@code true} — всё успешно подтверждено (буфер очищен);
     *         {@code false} — произошла ошибка отправки/таймаут или поток был прерван
     *         (флаг прерывания сохранён).
     *
     * При неуспехе буфер НЕ очищается — чтобы можно было вызвать обычный
     * {@link #flush()} и получить исходное исключение там, где это уместно.
     */
    public boolean tryFlush() {
        return quietFlush("tryFlush");
    }

    /**
     * Текущее число отправок, накопленных и ещё не подтверждённых внутри BatchSender.
     * Важно: это объём локального буфера между вызовами {@link #flush()},
     * а не количество in‑flight на стороне брокера Kafka.
     */
    public int getPendingCount() {
        return sent.size();
    }

    /** Есть ли сейчас неподтверждённые отправки. */
    public boolean hasPending() {
        return !sent.isEmpty();
    }

    /**
     * Текущее целевое количество отправок в пачке перед ожиданием.
     */
    public int getAwaitEvery() {
        return awaitEvery;
    }

    /**
     * Общий таймаут ожидания подтверждений (на один цикл flush), мс.
     */
    public int getAwaitTimeoutMs() {
        return awaitTimeoutMs;
    }

    /**
     * Быстрая проверка, пуст ли буфер накопленных отправок.
     */
    public boolean isEmpty() {
        return sent.isEmpty();
    }

    /** Включены ли счётчики. */
    public boolean isCountersEnabled() {
        return enableCounters;
    }

    /** Включено ли логирование причин неуспеха в DEBUG. */
    public boolean isDebugOnFailureEnabled() {
        return debugOnFailure;
    }

    /** Сколько подтверждений получено успешно (суммарно по успешным flush/tryFlush). */
    public long getConfirmedCount() {
        return confirmedCount;
    }

    /** Сколько успешных вызовов flush/tryFlush. */
    public long getFlushCalls() {
        return flushCalls;
    }

    /** Сколько неуспешных попыток flush/tryFlush. */
    public long getFailedFlushes() {
        return failedFlushes;
    }

    /** Сбросить диагностические счётчики в ноль. */
    public void resetCounters() {
        confirmedCount = 0L;
        flushCalls = 0L;
        failedFlushes = 0L;
    }

    /** Закрывает отправитель: выполняет строгий {@link #flush()} и пробрасывает исключения наружу. */
    @Override
    public void close() throws Exception {
        flushInternal(true);
    }

    /** Краткое текстовое описание состояния отправителя (может использоваться в логах). */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("BatchSender{");
        sb.append("порог=").append(awaitEvery)
          .append(", таймаут_мс=").append(awaitTimeoutMs)
          .append(", в_буфере=").append(sent.size());
        if (enableCounters) {
            sb.append(", подтверждений=").append(confirmedCount)
              .append(", успешных_сбросов=").append(flushCalls)
              .append(", неуспешных_сбросов=").append(failedFlushes);
        }
        sb.append('}');
        return sb.toString();
    }
    /**
     * Выполнить «тихий» сброс без выбрасывания checked-исключений.
     * Возвращает true при успехе, false при неуспехе. Логирует причины в DEBUG при включённом флаге.
     * @param where короткая метка для контекста лога (например, "add", "addAll/iter")
     */
    private boolean quietFlush(String where) {
        return flushQuietInternal(where);
    }
}