package kz.qazmarka.h2k.kafka;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.RandomAccess;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Простая утилита для дозированной отправки в Kafka:
 * накапливает ожидания подтверждений (объекты Future<RecordMetadata>) и периодически ждёт подтверждений, не допуская переполнения in‑flight.
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
    /**
     * Сообщение об истечении общего дедлайна ожидания подтверждений
     * (общий таймаут на набор ожиданий — {@code Future<RecordMetadata>}).
     */
    private static final String TIMEOUT_MSG = "Таймаут ожидания подтверждений от Kafka (истёк общий дедлайн на набор ожиданий — Future<RecordMetadata>)";

    /**
     * Сентинел для обозначения подавленного авто‑сброса.
     * Возвращается вспомогательными методами, когда авто‑сброс временно отключён.
     */
    private static final int AUTO_FLUSH_SUSPENDED = Integer.MAX_VALUE;
    /**
     * Коэффициент, при превышении которого после очень больших партий
     * выполняется усадка внутреннего {@link ArrayList#trimToSize()}.
     */
    private static final int TRIM_FACTOR = 8;

    /** Антишум: минимальный интервал между DEBUG‑сообщениями о «тихих» неуспехах, наносекунды. */
    private static final long QUIET_FAIL_LOG_THROTTLE_NS = TimeUnit.SECONDS.toNanos(5);


    /** Сколько отправок накапливать перед ожиданием подтверждений. */
    private final int awaitEvery;
    /** Общий таймаут ожидания подтверждений (на один цикл flush), миллисекунды. */
    private final int awaitTimeoutMs;
    /** Буфер накопленных ожиданий подтверждения (Future<RecordMetadata>) от Kafka. */
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

    /** Чтобы не «шуметь» в логах — предупреждаем об отключении авто‑сброса один раз. */
    private boolean warnedAutoFlush;

    /** Время последнего DEBUG‑лога о «тихом» неуспехе (nanoTime), для троттлинга. */
    private long lastQuietFailLogNs;
    /** Длина текущей полосы «тихих» неуспехов (для логики «первый из серии»). */
    private int quietFailStreak;

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
     * @param awaitEvery     сколько отправок копить перед ожиданием подтверждений ({@code > 0})
     * @param awaitTimeoutMs общий таймаут ожидания подтверждений на один цикл flush, миллисекунды ({@code > 0})
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
        this.warnedAutoFlush = false;
        this.lastQuietFailLogNs = 0L;
        this.quietFailStreak = 0;
    }

    /**
     * Добавить ожидание (Future<RecordMetadata>) в буфер. При достижении порога сразу «тихо» ждёт подтверждений
     * (не бросает исключения). Для строгой семантики вызывайте затем {@link #flush()}.
     *
     * @param f ожидание подтверждения от Kafka; {@code null} игнорируется
     */
    public void add(Future<RecordMetadata> f) {
        if (f == null) {
            return;
        }
        sent.add(f);
        if (sent.size() >= awaitEvery && !autoFlushSuspended) {
            // Унифицированный авто‑сброс: даёт однократный WARN и сам поднимает блокировку при неуспехе
            // (поведение совпадает с addAll()). Возврат значения нам не нужен.
            tryAutoQuietFlush("add");
        }
    }

    /**
     * Пытается выполнить «тихий» авто‑сброс накопленного буфера.
     *
     * При неуспехе поднимает флаг {@code autoFlushSuspended} — дальнейшие авто‑сбросы
     * подавляются до первого успешного явного/тихого {@link #flush()} / {@link #tryFlush()}.
     *
     * @param where короткая метка для лога (контекст вызова)
     * @return {@code awaitEvery}, если буфер очищён; {@code AUTO_FLUSH_SUSPENDED}, если авто‑сброс подавлён/неуспешен
     */
    private int tryAutoQuietFlush(String where) {
        boolean cleared = flushQuietInternal(where);
        if (!cleared) {
            autoFlushSuspended = true;
            if (!warnedAutoFlush) {
                warnedAutoFlush = true;
                LOG.warn("Авто-сброс временно отключён после неуспешного {} — выполните явный flush() для возобновления авто‑сбросов", where);
            }
            return AUTO_FLUSH_SUSPENDED;
        }
        return awaitEvery;
    }

    /**
     * Рассчитывает начальное количество элементов до порога {@code awaitEvery} для {@link #addAll(Collection)},
     * учитывая текущее состояние буфера и возможную блокировку авто‑сброса.
     *
     * @return положительное число, если до порога ещё есть место;
     *         {@code awaitEvery} — если произошёл успешный авто‑сброс в рамках подготовки;
     *         {@code AUTO_FLUSH_SUSPENDED} — если авто‑сброс временно подавлён
     */
    private int initialRemainingForAddAll() {
        if (autoFlushSuspended) return AUTO_FLUSH_SUSPENDED;
        int remainingToThreshold = awaitEvery - sent.size();
        if (remainingToThreshold > 0) return remainingToThreshold;
        return tryAutoQuietFlush("addAll/iter-pre");
    }

    /**
     * Добавить коллекцию ожиданий (Future<RecordMetadata>) «кусками», минимизируя число проверок и «тихих» сбросов.
     *
     * Алгоритм:
     *  - сначала вычисляется, сколько элементов осталось до ближайшего порога {@code awaitEvery}
     *    с учётом уже накопленного буфера;
     *  - затем элементы добавляются по одному; когда счётчик достигает нуля —
     *    выполняется «тихий» авто‑сброс (если он не подавлен после предыдущей ошибки);
     *  - остаток (меньше {@code awaitEvery}) остаётся в буфере до следующего {@link #add(Future)}
     *    или явного {@link #flush()} / {@link #tryFlush()}.
     *
     * Исключения в «тихом» режиме не пробрасываются (см. {@link #tryFlush()});
     * для строгой семантики используйте {@link #flush()}.
     *
     * В случае неуспеха «тихого» авто‑сброса буфер не очищается; повторные авто‑сбросы временно подавляются до успешного {@link #flush()} или {@link #tryFlush()}.
     *
     * @param futures коллекция ожиданий (Future<RecordMetadata>) на подтверждение; null‑элементы пропускаются, пустая коллекция игнорируется
     */
    public void addAll(Collection<? extends Future<RecordMetadata>> futures) {
        if (futures == null || futures.isEmpty()) {
            return; // быстрый путь
        }
        // Предварительно зарезервируем место под вставки (микро-оптимизация под ArrayList)
        sent.ensureCapacity(clampCapacity(sent.size(), futures.size()));

        // Сколько элементов осталось добавить до ближайшего порога awaitEvery
        int remainingToThreshold = initialRemainingForAddAll();

        for (Future<RecordMetadata> f : futures) {
            if (f != null) {
                sent.add(f);
                if (remainingToThreshold != AUTO_FLUSH_SUSPENDED && --remainingToThreshold == 0) {
                    remainingToThreshold = autoFlushSuspended ? AUTO_FLUSH_SUSPENDED : tryAutoQuietFlush("addAll/iter");
                }
            }
        }
        // Остаток < awaitEvery оставляем в буфере — поведение идентично множественным add()
    }
    /**
     * Защита от переполнения при предварительном резервировании ёмкости {@link ArrayList}.
     *
     * @param base текущее количество элементов в буфере
     * @param extra предполагаемое число добавляемых элементов; отрицательные значения трактуются как 0
     * @return безопасная ёмкость, не превышающая практический предел для {@code ArrayList}
     */
    private static int clampCapacity(int base, int extra) {
        final int max = Integer.MAX_VALUE - 8; // практический лимит для ArrayList
        if (extra < 0) extra = 0;
        long target = (long) base + (long) extra;
        return (target > max) ? max : (int) target;
    }

    /**
     * Дождаться подтверждений для всех ожиданий (Future<RecordMetadata>) в пределах общего таймаута.
     * Унифицированный обход: для RandomAccess и обычных списков.
     *
     * @param futures список ожиданий (Future<RecordMetadata>) (null или пустой — быстрый выход)
     * @param timeoutMs общий таймаут ожидания в миллисекундах на весь набор
     * @throws InterruptedException если поток прерван (флаг прерывания сохраняется вызывающим кодом)
     * @throws ExecutionException при ошибке выполнения
     * @throws TimeoutException если дедлайн истёк
     */
    private static void waitAll(List<Future<RecordMetadata>> futures, int timeoutMs)
            throws InterruptedException, ExecutionException, TimeoutException {
        if (futures == null || futures.isEmpty()) {
            return;
        }
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        forEachFuture(futures, (f, idx) -> awaitOne(f, deadline));
    }
    /** Визитор по Future-элементам с передачей индекса. */
    @FunctionalInterface
    private interface FutureVisitor {
        void accept(Future<RecordMetadata> f, int index)
                throws InterruptedException, ExecutionException, TimeoutException;
    }

    /**
     * Унифицированный обход списка ожиданий (Future): выбирает быстрый путь для {@link java.util.RandomAccess}
     * и передаёт (future, index) в указанный visitor.
     *
     * @param futures список ожиданий (может быть {@code null})
     * @param visitor обработчик элемента и его индекса
     * @throws InterruptedException если ожидание прервано
     * @throws ExecutionException при ошибке выполнения
     * @throws TimeoutException если истёк общий дедлайн
     */
    private static void forEachFuture(List<Future<RecordMetadata>> futures, FutureVisitor visitor)
            throws InterruptedException, ExecutionException, TimeoutException {
        if (futures == null) {
            return;
        }
        if (futures instanceof RandomAccess) {
            for (int i = 0, n = futures.size(); i < n; i++) {
                visitor.accept(futures.get(i), i);
            }
        } else {
            int i = 0;
            for (Future<RecordMetadata> f : futures)
                visitor.accept(f, i++);
        }
    }

    /**
     * Ожидание подтверждения одного ожидания (Future<RecordMetadata>) с учётом общего дедлайна.
     *
     * @param f ожидание (Future<RecordMetadata>); если {@code null}, метод ничего не делает
     * @param deadlineNs абсолютный дедлайн в наносекундах (System.nanoTime())
     * @throws InterruptedException при прерывании ожидания (флаг прерывания сохраняется)
     * @throws ExecutionException при ошибке выполнения
     * @throws TimeoutException если дедлайн истёк
     */
    private static void awaitOne(Future<RecordMetadata> f, long deadlineNs)
            throws InterruptedException, ExecutionException, TimeoutException {
        if (f == null) {
            return;
        }
        long leftNs = deadlineNs - System.nanoTime();
        if (leftNs <= 0L) {
            throw new TimeoutException(TIMEOUT_MSG);
        }
        f.get(leftNs, TimeUnit.NANOSECONDS);
    }

    // rethrowExecutionCause больше не нужен — сохраняем исходные типы исключений.

    /**
     * Общая реализация сброса: строгий (с исключениями) или «тихий» (true/false).
     *
     * В строгом режиме ошибки отправки приводят к {@link java.util.concurrent.ExecutionException},
     * таймаут — к {@link java.util.concurrent.TimeoutException}, прерывание — {@link InterruptedException}.
     *
     * @param strict {@code true} — строгая семантика; {@code false} — «тихий» режим
     * @return {@code true} при успехе (в «тихом» режиме)
     * @throws InterruptedException см. {@link #waitAll(List, int)}
     * @throws ExecutionException при ошибке выполнения
     * @throws TimeoutException если дедлайн истёк
     */
    private boolean flushInternal(boolean strict)
            throws InterruptedException, ExecutionException, TimeoutException {
        if (strict) {
            flushStrictInternal();
            return true;
        }
        return flushQuietInternal();
    }

    /**
     * Строгий сброс: ожидает все futures и очищает буфер либо выбрасывает {@link java.util.concurrent.ExecutionException} или {@link java.util.concurrent.TimeoutException} при неуспехе.
     * @throws InterruptedException при прерывании ожидания
     * @throws ExecutionException при ошибке выполнения
     * @throws TimeoutException если дедлайн истёк
     */
    private void flushStrictInternal()
            throws InterruptedException, ExecutionException, TimeoutException {
        if (sent.isEmpty()) {
            return;
        }
        final int n = sent.size();
        waitAll(sent, awaitTimeoutMs);
        sent.clear();
        if (n >= (awaitEvery * TRIM_FACTOR)) { // усушка буфера после очень больших партий
            sent.trimToSize();
        }
        autoFlushSuspended = false; // успешный строгий сброс снимает блокировку
        quietFailStreak = 0;
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
     * «Тихий» сброс: пытается дождаться подтверждений без выбрасывания checked‑исключений.
     *
     * @param where короткая метка для контекста лога (например, "add", "addAll/iter", "tryFlush")
     * @return {@code true} — буфер очищен; {@code false} — произошла ошибка/таймаут/прерывание
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
            if (n >= (awaitEvery * TRIM_FACTOR)) { // усушка буфера после очень больших партий
                sent.trimToSize();
            }
            autoFlushSuspended = false; // успешный тихий сброс снимает блокировку
            quietFailStreak = 0; // успешный тихий сброс сбрасывает полосу неуспехов
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
            if (dbg) maybeLogQuietFailure(where, n, ie, "прерван");
            return false;
        } catch (ExecutionException | TimeoutException e) {
            if (enableCounters) {
                failedFlushes++;
            }
            if (dbg) maybeLogQuietFailure(where, n, e, "неуспешен");
            return false;
        }
    }

    /**
     * Последовательно ждёт подтверждений каждого ожидания (Future<RecordMetadata>) до первого сбоя.
     * Возвращает количество успешно подтверждённых элементов.
     * Важно: буфер не очищается и счётчики не изменяются — метод
     * предназначен для диагностики (например, чтобы понять, на каком элементе
     * произошёл первый сбой). Для обычной работы используйте {@link #flush()}.
     */
    public int flushUpToFirstFailure()
            throws InterruptedException, ExecutionException, TimeoutException {
        if (sent.isEmpty()) {
            return 0;
        }
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(awaitTimeoutMs);
        final boolean dbg = debugOnFailure && LOG.isDebugEnabled();
        final int[] ok = {0};
        forEachFuture(sent, (f, idx) -> ok[0] += awaitWithDebug(f, deadline, dbg, ok[0]));
        return ok[0];
    }

    /**
     * Ожидает одно ожидание (Future) с логированием причин сбоя в DEBUG (если {@code dbg} == true).
     *
     * @param f ожидание; {@code null} возвращает 0 без ожидания
     * @param deadlineNs абсолютный дедлайн (наносекунды, {@link System#nanoTime()})
     * @param dbg включать ли подробный DEBUG при неуспехе
     * @param okSoFar количество успешно подтверждённых элементов на момент вызова (для контекста лога)
     * @return 1 при успешном подтверждении; 0 если {@code f} == null
     * @throws InterruptedException при прерывании ожидания
     * @throws ExecutionException при ошибке выполнения
     * @throws TimeoutException при истечении дедлайна
     */
    private int awaitWithDebug(Future<RecordMetadata> f, long deadlineNs, boolean dbg, int okSoFar)
            throws InterruptedException, ExecutionException, TimeoutException {
        if (f == null) {
            return 0;
        }
        try {
            awaitOne(f, deadlineNs);
            return 1;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            if (dbg) LOG.debug("flushUpToFirstFailure() прерван на индексе {}", okSoFar, ie);
            throw ie;
        } catch (ExecutionException | TimeoutException e) {
            if (dbg) LOG.debug("flushUpToFirstFailure() первый сбой на индексе {}", okSoFar, e);
            throw e;
        }
    }

    /**
     * Немедленно ожидает подтверждений для накопленных отправок.
     * Объединённый таймаут применяется на весь набор ожиданий.
     *
     * Семантика ошибок:
     *  - при первой ошибке — {@link ExecutionException};
     *  - при таймауте — {@link TimeoutException};
     *  - при прерывании — InterruptedException (флаг прерывания сохраняется).
     *
     * @throws InterruptedException при прерывании ожидания
     * @throws ExecutionException при ошибке выполнения
     * @throws TimeoutException при истечении общего дедлайна
     */
    public void flush()
            throws InterruptedException, ExecutionException, TimeoutException {
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
     *
     * @since 1.0
     */
    public boolean tryFlush() {
        return flushQuietInternal("tryFlush");
    }

    /**
     * Текущее число отправок, накопленных и ещё не подтверждённых внутри BatchSender.
     * Важно: это объём локального буфера между вызовами {@link #flush()},
     * а не количество in‑flight на стороне брокера Kafka.
     * @return текущий размер буфера неподтверждённых отправок
     */
    public int getPendingCount() {
        return sent.size();
    }

    /**
     * Есть ли сейчас неподтверждённые отправки.
     * @return есть ли сейчас неподтверждённые отправки
     */
    public boolean hasPending() {
        return !sent.isEmpty();
    }

    /**
     * Текущее целевое количество отправок в пачке перед ожиданием.
     * @return целевое количество отправок в пачке перед ожиданием
     */
    public int getAwaitEvery() {
        return awaitEvery;
    }

    /**
     * Общий таймаут ожидания подтверждений (на один цикл flush), мс.
     * @return общий таймаут ожидания подтверждений, мс
     */
    public int getAwaitTimeoutMs() {
        return awaitTimeoutMs;
    }

    /**
     * Быстрая проверка, пуст ли буфер накопленных отправок.
     * @return true, если буфер пуст
     */
    public boolean isEmpty() {
        return sent.isEmpty();
    }

    /**
     * Включены ли счётчики.
     * @return включены ли диагностические счётчики
     */
    public boolean isCountersEnabled() {
        return enableCounters;
    }

    /**
     * Включено ли логирование причин неуспеха в DEBUG.
     * @return включено ли DEBUG‑логирование неуспехов тихого сброса
     */
    public boolean isDebugOnFailureEnabled() {
        return debugOnFailure;
    }

    /**
     * Сколько подтверждений получено успешно (суммарно по успешным flush/tryFlush).
     * @return количество подтверждённых отправок (накопительно)
     */
    public long getConfirmedCount() {
        return confirmedCount;
    }

    /**
     * Сколько успешных вызовов flush/tryFlush.
     * @return число успешных вызовов flush/tryFlush
     */
    public long getFlushCalls() {
        return flushCalls;
    }

    /**
     * Сколько неуспешных попыток «тихого» сброса.
     * @return число неуспешных «тихих» сбросов (tryFlush/авто-сброс); строгий flush() счётчик не изменяет
     */
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
    public void close() throws InterruptedException, ExecutionException, TimeoutException {
        final int pending = sent.size();
        try {
            flushInternal(true);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Закрытие BatchSender: pending={}, итог=успех", pending);
            }
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Закрытие BatchSender: pending={}, итог=ошибка ({})", pending, e.getClass().getSimpleName());
            }
            throw e;
        }
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
     * Снять блокировку авто‑сброса после неуспешного tryFlush()/авто‑сброса.
     * Сбрасывает флаги {@code autoFlushSuspended} и {@code warnedAutoFlush}.
     * Полезно для операционных сценариев восстановления.
     */
    public void resumeAutoFlush() {
        autoFlushSuspended = false;
        warnedAutoFlush = false;
    }

    /**
     * Антишум для DEBUG‑лога «тихих» неуспехов: логируем первый сбой в серии и затем не чаще,
     * чем раз в {@link #QUIET_FAIL_LOG_THROTTLE_NS}. Сброс полосы происходит при успешном flush/tryFlush.
     */
    private void maybeLogQuietFailure(String where, int sizeAtStart, Exception e, String tag) {
        long now = System.nanoTime();
        boolean firstOfStreak = (quietFailStreak == 0);
        boolean throttleOk = (now - lastQuietFailLogNs) >= QUIET_FAIL_LOG_THROTTLE_NS;
        if (firstOfStreak || throttleOk) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("{}: тихий сброс {}: size={}, pendingBeforeClear={}", where, tag, sizeAtStart, sent.size(), e);
            }
            lastQuietFailLogNs = now;
            quietFailStreak++;
        }
    }
}