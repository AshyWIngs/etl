package kz.qazmarka.h2k.kafka;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Набор юнит‑тестов для {@link TopicEnsurer}.
 *
 * Назначение:
 *  • Проверить корректность и идемпотентность логики обеспечения наличия Kafka‑топиков:
 *    существующая тема → пропускаем; отсутствующая → создаём; гонка на создании (TopicExists) → считаем успехом.
 *  • Зафиксировать поведение при неопределённом результате describe (таймаут) — тема считается UNKNOWN,
 *    включается короткий backoff на повторную проверку.
 *  • Проверить batch‑ветку (describe нескольких тем + выборочное создание только отсутствующих).
 *  • Провалидировать простые внутренние метрики (счётчики событий) и отсутствие лишних исключений в «штатных» сценариях.
 *
 * Подход:
 *  • Без Mockito и внешних зависимостей — используется JDK Proxy поверх приватного интерфейса
 *    {@code TopicEnsurer$TopicAdmin}. Это делает тест изолированным и предсказуемым.
 *  • Для Future'ов — {@link KafkaFutureImpl}: управляем завершением (успех/исключение) или намеренно
 *    оставляем незавершённым (для эмуляции таймаута).
 *  • Экземпляр {@link TopicEnsurer} создаётся через reflection (приватный конструктор), куда
 *    инжектится прокси‑админ.
 *
 * Производительность тестов:
 *  • Таймауты в тестах малы (10–25 мс), внешних сетевых вызовов нет — запуск быстрый и «дешёвый» для CI.
 *
 * Примечание по стилю:
 *  • В Javadoc тестов намеренно нет HTML‑тегов — только обычный текст и списки, совместимый с нашими
 *    корпоративными правилами документирования.
 */
class TopicEnsurerTest {

    /* ====================== ВСПОМОГАТЕЛЬНАЯ ИНФРА ====================== */

    /**
     * Создаёт минимальный {@link TopicDescription} для используемого в тестах топика.
     *
     * Важно: нам достаточно факта «describe отработал успешно», поэтому структура описания предельно простая:
     * один раздел, фиктивные брокеры и реплики.
     *
     * @param topic имя топика
     * @return валидный объект описания топика
     */
    private static TopicDescription td(String topic) {
        // Один фиктивный раздел
        TopicPartitionInfo p0 = new TopicPartitionInfo(
                0, new Node(1, "broker", 9092), Collections.emptyList(), Collections.emptyList());
        return new TopicDescription(topic, false, Collections.singletonList(p0));
    }

    /**
     * Успешное завершение describe конкретного топика.
     *
     * @param topic имя топика
     * @return завершённый {@code KafkaFuture} c {@link TopicDescription}
     */
    private static KafkaFuture<TopicDescription> okDesc(String topic) {
        return KafkaFuture.completedFuture(td(topic));
    }

    /**
     * Эмулирует ситуацию «топик не найден» — {@link UnknownTopicOrPartitionException} внутри future.
     *
     * @return завершённый с исключением {@code KafkaFuture}
     */
    private static KafkaFuture<TopicDescription> notFound() {
        KafkaFutureImpl<TopicDescription> f = new KafkaFutureImpl<>();
        f.completeExceptionally(new UnknownTopicOrPartitionException("not found"));
        return f;
    }

    /**
     * Эмулирует «подвисший» describe: future никогда не завершается.
     * Вызов {@code get(timeout)} в тесте приведёт к {@link java.util.concurrent.TimeoutException}.
     *
     * @return незавершимый {@code KafkaFuture}
     */
    private static KafkaFuture<TopicDescription> neverCompletes() {
        return new KafkaFutureImpl<>();
    }

    /**
     * Успешное завершение операции создания топика.
     *
     * @return завершённый успешно {@code KafkaFuture<Void>}
     */
    private static KafkaFuture<Void> okCreate() {
        KafkaFutureImpl<Void> f = new KafkaFutureImpl<>();
        f.complete(null);
        return f;
    }

    /**
     * Тестовая реализация «админа» поверх JDK Proxy.
     *
     * Поддерживает приватный интерфейс {@code TopicEnsurer$TopicAdmin} с методами:
     *  • {@code describeTopics(Set<String>)} → {@code Map<String, KafkaFuture<TopicDescription>>}
     *  • {@code createTopics(List<NewTopic>)} → {@code Map<String, KafkaFuture<Void>>}
     *  • {@code createTopic(NewTopic, long timeoutMs)} → синхронный путь (OK/исключение)
     *  • {@code close(Duration)} → no‑op
     *
     * В поведение методов можно «вписать» сценарии через fluent‑настройки {@code will*}.
     */
    private static final class FakeAdmin implements InvocationHandler {
        final Map<String, KafkaFuture<TopicDescription>> describeMap = new HashMap<>();
        final Map<String, KafkaFuture<Void>> createMap = new HashMap<>();
        final Map<String, Action> createSingle = new HashMap<>();
        // Захватываем последние вызовы create для проверок в тестах
        final List<NewTopic> createdBatch = new ArrayList<>();
        NewTopic createdSingle = null;

        /**
         * Сценарии для одиночного {@code createTopic}.
         * OK — успех; EXISTS — гонка (оборачивается в {@link TopicExistsException});
         * TIMEOUT — бросается {@link java.util.concurrent.TimeoutException};
         * INTERRUPT — имитация {@link InterruptedException};
         * FAIL — общая ошибка выполнения.
         */
        enum Action { OK, EXISTS, TIMEOUT, INTERRUPT, FAIL }

        /**
         * Задать результат describe для указанного топика как «успешно».
         * @param topic имя топика
         * @return this (для чейнинга)
         */
        FakeAdmin willDescribeOk(String topic) { describeMap.put(topic, okDesc(topic)); return this; }

        /**
         * Задать результат describe для указанного топика как «не найден» (UnknownTopicOrPartitionException).
         * @param topic имя топика
         * @return this (для чейнинга)
         */
        FakeAdmin willDescribeNotFound(String topic) { describeMap.put(topic, notFound()); return this; }

        /**
         * Задать результат describe для указанного топика как «таймаут» (never completes).
         * @param topic имя топика
         * @return this (для чейнинга)
         */
        FakeAdmin willDescribeTimeout(String topic) { describeMap.put(topic, neverCompletes()); return this; }

        /**
         * Задать результат createTopics для указанного топика как успешный.
         * @param topic имя топика
         * @return this (для чейнинга)
         */
        FakeAdmin willCreateOk(String topic) { createMap.put(topic, okCreate()); return this; }

        /**
         * Задать сценарий одиночного createTopic для указанного топика.
         * @param topic имя топика
         * @param a сценарий (OK, EXISTS, TIMEOUT, INTERRUPT, FAIL)
         * @return this (для чейнинга)
         */
        FakeAdmin willCreateTopicSingle(String topic, Action a) { createSingle.put(topic, a); return this; }

        /**
         * Центральная точка прокси: маршрутизирует вызовы приватного интерфейса TopicAdmin
         * к подготовленным сценариям. Используется только в пределах теста.
         */
        @Override public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String name = method.getName();
            switch (name) {
                case "describeTopics": {
                    @SuppressWarnings("unchecked")
                    Set<String> names = (Set<String>) args[0];
                    Map<String, KafkaFuture<TopicDescription>> out = new LinkedHashMap<>();
                    for (String t : names) {
                        out.put(t, describeMap.getOrDefault(t, okDesc(t)));
                    }
                    return out;
                }
                case "createTopics": {
                    @SuppressWarnings("unchecked")
                    List<NewTopic> nts = (List<NewTopic>) args[0];
                    // зафиксируем параметры созданных топиков для последующих проверок
                    createdBatch.clear();
                    createdBatch.addAll(nts);
                    Map<String, KafkaFuture<Void>> out = new LinkedHashMap<>();
                    for (NewTopic nt : nts) {
                        String t = nt.name();
                        out.put(t, createMap.getOrDefault(t, okCreate()));
                    }
                    return out;
                }
                case "createTopic": {
                    NewTopic nt = (NewTopic) args[0];
                    // зафиксируем параметры созданного топика для последующих проверок
                    createdSingle = nt;
                    String t = nt.name();
                    Action a = createSingle.getOrDefault(t, Action.OK);
                    switch (a) {
                        case OK: return null;
                        case EXISTS: throw new java.util.concurrent.ExecutionException(new TopicExistsException("exists"));
                        case TIMEOUT: throw new java.util.concurrent.TimeoutException("timeout");
                        case INTERRUPT: throw new InterruptedException("interrupt");
                        case FAIL: throw new java.util.concurrent.ExecutionException(new RuntimeException("create-fail"));
                        default: return null;
                    }
                }
                case "close": return null;
                default: throw new UnsupportedOperationException("Unexpected method: " + name);
            }
        }
    }

    /**
     * Создаёт {@link TopicEnsurer} через приватный конструктор и инжектит тестовый админ (JDK Proxy).
     *
     * @param fa               подготовленный обработчик прокси
     * @param adminTimeoutMs   таймаут админ‑вызовов (мс)
     * @param partitions       число разделов по умолчанию при создании
     * @param replication      фактор репликации по умолчанию при создании
     * @param topicConfigs     конфиги топика для create (может быть пустым)
     * @param unknownBackoffMs backoff для тем со статусом UNKNOWN (мс)
     * @return новый экземпляр {@link TopicEnsurer}
     * @throws Exception если reflection не удался
     */
    private static TopicEnsurer newEnsurer(FakeAdmin fa,
                                           long adminTimeoutMs,
                                           int partitions,
                                           short replication,
                                           Map<String, String> topicConfigs,
                                           long unknownBackoffMs) throws Exception {
        Class<?> ta = Class.forName("kz.qazmarka.h2k.kafka.TopicEnsurer$TopicAdmin");
        Object proxyAdmin = Proxy.newProxyInstance(
                ta.getClassLoader(), new Class<?>[]{ta}, fa);
        Constructor<?> ctor = TopicEnsurer.class.getDeclaredConstructor(
                ta, long.class, int.class, short.class, Map.class, long.class);
        ctor.setAccessible(true);
        return (TopicEnsurer) ctor.newInstance(proxyAdmin, adminTimeoutMs, partitions, replication, topicConfigs, unknownBackoffMs);
    }

    /**
     * Утилита чтения числовой метрики из {@link TopicEnsurer#getMetrics()}.
     * Если ключ отсутствует — возвращает −1.
     *
     * @param te  тестируемый {@link TopicEnsurer}
     * @param key имя метрики
     * @return значение метрики или −1, если нет
     */
    private static long m(TopicEnsurer te, String key) {
        Long v = te.getMetrics().get(key);
        return (v == null) ? -1L : v;
    }

    /* ====================== ТЕСТЫ ====================== */

    /**
     * Сценарий: describe → UnknownTopic, затем успешный create.
     * Проверяем: метод возвращает true, счётчики фиксируют отсутствие темы и успешное создание.
     */
    @Test
    @DisplayName("ensureTopicOk: тема отсутствует → создаётся успешно (single-path)")
    void ensure_creates_when_missing() throws Exception {
        FakeAdmin fa = new FakeAdmin()
                .willDescribeNotFound("foo")
                .willCreateTopicSingle("foo", FakeAdmin.Action.OK);
        TopicEnsurer te = newEnsurer(fa, /*timeout*/ 25, 3, (short) 2,
                Collections.emptyMap(), /*backoff*/ 10);

        assertTrue(te.ensureTopicOk("foo"), "После успешного create тема должна считаться обеспеченной");
        assertEquals(1L, m(te, "exists.false"));
        assertEquals(1L, m(te, "create.ok"));
        assertEquals(0L, m(te, "create.fail"));
    }

    /**
     * Сценарий гонки: describe → UnknownTopic, create → {@link TopicExistsException}.
     * Ожидаем: трактуем как успех (тему уже создали параллельно), счётчик create.race увеличен.
     */
    @Test
    @DisplayName("ensureTopicOk: гонка при создании (TopicExists) трактуется как успех")
    void ensure_race_is_success() throws Exception {
        FakeAdmin fa = new FakeAdmin()
                .willDescribeNotFound("bar")
                .willCreateTopicSingle("bar", FakeAdmin.Action.EXISTS);
        TopicEnsurer te = newEnsurer(fa, 25, 1, (short) 1,
                Collections.emptyMap(), 10);

        assertTrue(te.ensureTopicOk("bar"), "TopicExists во время create должен считаться успешным исходом");
        assertEquals(1L, m(te, "create.race"));
        assertEquals(0L, m(te, "create.fail"));
    }

    /**
     * Негатив: некорректные имена тем (пустые, точки, слишком длинные) отвергаются до вызова админа.
     */
    @Test
    @DisplayName("ensureTopicOk: некорректные имена тем отвергаются (., .., пустое, пробелы, >249)")
    void invalid_topic_names_are_rejected() throws Exception {
        FakeAdmin fa = new FakeAdmin(); // неважно, админ не должен вызываться
        TopicEnsurer te = newEnsurer(fa, 10, 1, (short) 1, Collections.emptyMap(), 5);

        assertFalse(te.ensureTopicOk(null));
        assertFalse(te.ensureTopicOk(""));
        assertFalse(te.ensureTopicOk(" "));
        assertFalse(te.ensureTopicOk("."));
        assertFalse(te.ensureTopicOk(".."));

        char[] big = new char[260];
        Arrays.fill(big, 'a');
        assertFalse(te.ensureTopicOk(new String(big)), "Имена длиннее 249 символов недопустимы");
    }

    /**
     * Сценарий UNKNOWN: describe зависает (таймаут), тема помечается как UNKNOWN и попадает в backoff‑очередь.
     */
    @Test
    @DisplayName("ensureTopic: describe → таймаут = UNKNOWN, назначается короткий backoff")
    void unknown_triggers_backoff() throws Exception {
        FakeAdmin fa = new FakeAdmin().willDescribeTimeout("slow");
        TopicEnsurer te = newEnsurer(fa, /*timeout*/ 10, 1, (short) 1, Collections.emptyMap(), /*backoff*/ 20);

        te.ensureTopic("slow"); // приведёт к TimeoutException на get()
        assertEquals(1L, m(te, "exists.unknown"));
        assertTrue(m(te, "unknown.backoff.size") >= 1, "Должна появиться запись backoff для темы");
    }

    /**
     * Batch‑сценарий: t1 существует, t2 → UNKNOWN, t3 отсутствует и создаётся.
     * Проверяем метрики и отсутствие лишних create‑вызовов.
     */
    @Test
    @DisplayName("ensureTopics(batch): существующая/UNKNOWN/отсутствующая → создаём только отсутствующие")
    void batch_describe_and_create() throws Exception {
        FakeAdmin fa = new FakeAdmin()
                .willDescribeOk("t1")         // exists → cache
                .willDescribeTimeout("t2")     // unknown → backoff
                .willDescribeNotFound("t3")    // missing → create
                .willCreateOk("t3");
        TopicEnsurer te = newEnsurer(fa, 10, 2, (short) 1, Collections.emptyMap(), 15);

        te.ensureTopics(Arrays.asList("t1", "t2", "t3"));
        assertEquals(1L, m(te, "exists.true"));
        assertEquals(1L, m(te, "exists.unknown"));
        assertEquals(1L, m(te, "exists.false"));
        assertEquals(1L, m(te, "create.ok"));
        assertTrue(m(te, "unknown.backoff.size") >= 1);
    }

    /**
     * Закрытие компонента должно быть безопасным и повторяемым.
     */
    @Test
    @DisplayName("close(): закрытие безопасно и без исключений")
    void close_is_safe() throws Exception {
        FakeAdmin fa = new FakeAdmin();
        TopicEnsurer te = newEnsurer(fa, 5, 1, (short) 1, Collections.emptyMap(), 5);
        assertDoesNotThrow(te::close);
        // повторный close тоже не должен бросать
        assertDoesNotThrow(te::close);
    }
    /**
     * Позитив: существующая тема не создаётся повторно.
     * Проверяем, что ensureTopicOk возвращает true и create.ok не увеличивается.
     */
    @Test
    @DisplayName("ensureTopicOk: существующая тема → без create (idempotent)")
    void ensure_existing_topic_skips_creation() throws Exception {
        FakeAdmin fa = new FakeAdmin()
                .willDescribeOk("exists");
        TopicEnsurer te = newEnsurer(fa, 20, 2, (short) 1, Collections.emptyMap(), 10);

        assertTrue(te.ensureTopicOk("exists"));
        assertEquals(1L, m(te, "exists.true"));
        assertEquals(0L, m(te, "create.ok"));
    }

    /**
     * Негатив: ошибка создания (ExecutionException) приводит к возврату false.
     * Метрики фиксируют попытку (exists.false=1) и create.fail=1.
     */
    @Test
    @DisplayName("ensureTopicOk: ошибка create (FAIL) → false и счётчик fail")
    void ensure_create_failure_propagates_false() throws Exception {
        FakeAdmin fa = new FakeAdmin()
                .willDescribeNotFound("bad")
                .willCreateTopicSingle("bad", FakeAdmin.Action.FAIL);
        TopicEnsurer te = newEnsurer(fa, 25, 3, (short) 2, Collections.emptyMap(), 10);

        assertFalse(te.ensureTopicOk("bad"));
        assertEquals(1L, m(te, "exists.false"));
        assertEquals(1L, m(te, "create.fail"));
        assertEquals(0L, m(te, "create.ok"));
    }

    /**
     * Негатив: таймаут создания (TimeoutException) → false, без учёта как create.ok.
     */
    @Test
    @DisplayName("ensureTopicOk: таймаут при create → false, без create.ok")
    void ensure_create_timeout_returns_false() throws Exception {
        FakeAdmin fa = new FakeAdmin()
                .willDescribeNotFound("slowCreate")
                .willCreateTopicSingle("slowCreate", FakeAdmin.Action.TIMEOUT);
        TopicEnsurer te = newEnsurer(fa, 15, 2, (short) 1, Collections.emptyMap(), 10);

        assertFalse(te.ensureTopicOk("slowCreate"));
        assertEquals(1L, m(te, "exists.false"));
        assertEquals(0L, m(te, "create.ok"));
    }

    /**
     * Конструкторские значения по умолчанию передаются в NewTopic для одиночного пути.
     */
    @Test
    @DisplayName("ensureTopicOk: NewTopic использует partitions/replication из конструктора")
    void single_create_uses_ctor_defaults() throws Exception {
        FakeAdmin fa = new FakeAdmin()
                .willDescribeNotFound("pdef")
                .willCreateTopicSingle("pdef", FakeAdmin.Action.OK);
        int partitions = 4;
        short repl = 2;
        TopicEnsurer te = newEnsurer(fa, 25, partitions, repl, Collections.emptyMap(), 10);

        assertTrue(te.ensureTopicOk("pdef"));
        assertNotNull(fa.createdSingle, "Должен быть вызов createTopic(single)");
        assertEquals(partitions, fa.createdSingle.numPartitions());
        assertEquals(repl, fa.createdSingle.replicationFactor());
    }

    /**
     * Batch‑создание: для отсутствующих тем NewTopic получает значения из конструктора.
     */
    @Test
    @DisplayName("ensureTopics(batch): NewTopic получает partitions/replication из конструктора")
    void batch_create_uses_ctor_defaults_for_missing() throws Exception {
        FakeAdmin fa = new FakeAdmin()
                .willDescribeOk("t1")
                .willDescribeNotFound("t2")
                .willCreateOk("t2");
        int partitions = 6;
        short repl = 3;
        TopicEnsurer te = newEnsurer(fa, 20, partitions, repl, Collections.emptyMap(), 10);

        te.ensureTopics(Arrays.asList("t1", "t2"));
        // В батче должен быть один NewTopic — именно t2
        assertEquals(1, fa.createdBatch.size());
        NewTopic nt = fa.createdBatch.get(0);
        assertEquals("t2", nt.name());
        assertEquals(partitions, nt.numPartitions());
        assertEquals(repl, nt.replicationFactor());
    }

    /**
     * Batch: игнорируем некорректные имена и продолжаем обрабатывать валидные.
     */
    @Test
    @DisplayName("ensureTopics(batch): некорректные имена игнорируются, валидные обрабатываются")
    void batch_ignores_invalid_names() throws Exception {
        FakeAdmin fa = new FakeAdmin()
                .willDescribeOk("ok-exists")
                .willDescribeNotFound("ok-new")
                .willCreateOk("ok-new");
        TopicEnsurer te = newEnsurer(fa, 15, 1, (short) 1, Collections.emptyMap(), 10);

        te.ensureTopics(Arrays.asList("ok-exists", "", ".", "ok-new", " "));
        assertEquals(1L, m(te, "exists.true"));
        assertEquals(1L, m(te, "exists.false"));
        assertEquals(1L, m(te, "create.ok"));
    }
}