package kz.qazmarka.h2k.kafka.ensure;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kz.qazmarka.h2k.kafka.ensure.admin.KafkaTopicAdmin;
import kz.qazmarka.h2k.kafka.ensure.config.TopicEnsureConfig;
import kz.qazmarka.h2k.kafka.ensure.metrics.TopicEnsureState;
import kz.qazmarka.h2k.kafka.ensure.planner.TopicConfigPlanner;
import kz.qazmarka.h2k.kafka.ensure.planner.TopicDescribeHandler;
import kz.qazmarka.h2k.kafka.ensure.planner.TopicParams;
import kz.qazmarka.h2k.kafka.ensure.state.TopicBackoffManager;
import kz.qazmarka.h2k.kafka.ensure.util.TopicNameValidator;

/**
 * Внутренняя реализация ensure-логики Kafka-топиков.
 * Публичный API предоставляется обёрткой {@link TopicEnsurer}; данный класс держит бизнес-логику и состояние.
 */
final class TopicEnsureService implements AutoCloseable {
    static final Logger LOG = LoggerFactory.getLogger(TopicEnsureService.class);
    // Сообщение о некорректном имени топика (чтобы не дублировать литерал)
    private static final String WARN_INVALID_TOPIC =
            "Некорректное имя Kafka-топика '{}': допускаются [a-zA-Z0-9._-], длина 1..{}, запрещены '.' и '..'";
    /** Обёртка над AdminClient для unit‑тестируемости и стабильного API в классе. Может быть null, если ensureTopics=false. */
    private final KafkaTopicAdmin admin;
    /** Таймаут админских операций в миллисекундах (describe/create). */
    private final TopicEnsureConfig config;
    private final TopicEnsureState state;
    private final long adminTimeoutMs;
    /** Базовая величина backoff (мс) на «неуверенные» ошибки describe/create. */
    private final long unknownBackoffMs;
    /** Менеджер backoff-циклов (таймауты describe/create, повторные ensure). */
    private final TopicBackoffManager backoffManager;
    private final TopicDescribeHandler describeHandler;
    private final TopicConfigPlanner configPlanner;

    /** Минимальный интерфейс для админских вызовов Kafka, удобный для unit-тестов. */
    /**
     * Помечает тему как подтверждённую (существует/успешно создана) и снимает возможный backoff.
     *
     * Вызов безопасен при повторении; кеш и карта backoff будут приведены к согласованному состоянию.
     *
     * @param t имя Kafka‑топика
     */
    private void markEnsured(String t) {
        state.ensured.add(t);
        backoffManager.markSuccess(t);
    }
    /**
     * Внутренний конструктор: принимает уже нормализованные значения из H2kConfig.
     * Не выполняет повторной валидации числовых параметров.
     *
     * @param admin обёртка над AdminClient или {@code null}, если ensureTopics=false
     * @param config неизменяемый набор параметров ensureTopics
     * @param state кеши и метрики ensureTopics
     */
    TopicEnsureService(KafkaTopicAdmin admin, TopicEnsureConfig config, TopicEnsureState state) {
        this.admin = admin;
        this.config = config;
        this.state = state;
        this.adminTimeoutMs = config.adminTimeoutMs();
        this.unknownBackoffMs = config.unknownBackoffMs();
        this.backoffManager = new TopicBackoffManager(state, config.unknownBackoffMs());
        TopicParams params = TopicParams.from(config);
        this.describeHandler = new TopicDescribeHandler(admin, state, backoffManager, adminTimeoutMs);
        this.configPlanner = new TopicConfigPlanner(admin, params, state, backoffManager);
    }

    /**
     * Проверяет существование темы и при отсутствии — пытается создать её (идемпотентно).
     *
     * Быстрые ветки: пустое/некорректное имя → WARN и выход; кеш ensured; активный backoff.
     * При UNKNOWN‑ситуациях (таймаут/ACL/сеть) назначает короткий backoff с джиттером.
     *
     * @param topic имя Kafka‑топика
     */
    /**
     * Основной ensure-цикл для одной темы: нормализует имя, проверяет кеш и backoff, выполняет describe/create.
     */
    public void ensureTopic(String topic) {
        if (admin == null) return;
        state.ensureInvocations.increment();

        final String raw = (topic == null) ? "" : topic.trim();
        final String t = config.topicSanitizer().apply(raw);
        if (t.isEmpty()) {
            LOG.warn("Пустое имя Kafka-топика — пропускаю ensure");
            return;
        }
        if (!TopicNameValidator.isValid(t, config.topicNameMaxLen())) {
            LOG.warn(WARN_INVALID_TOPIC, t, config.topicNameMaxLen());
            return;
        }
        if (fastCacheHit(t)) return;       // уже успешно проверяли
        if (backoffManager.shouldSkip(t)) return; // действует backoff

        TopicDescribeHandler.TopicExistence ex = describeHandler.describeSingle(t);
        switch (ex) {
            case TRUE:
                onExistsTrue(t);
                break;
            case UNKNOWN:
                onExistsUnknown(t);
                break;
            case FALSE:
                // переходим к созданию
                tryCreateTopic(t);
                break;
        }
    }

    /**
     * Быстрая проверка кеша подтверждённых тем.
     *
     * @param t имя Kafka‑топика
     * @return {@code true}, если тема уже была подтверждена ранее и повторная проверка не требуется
     */
    private boolean fastCacheHit(String t) {
        if (state.ensured.contains(t)) {
            state.ensureHitCache.increment();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Kafka-топик '{}' уже проверен ранее — пропускаю ensure", t);
            }
            return true;
        }
        return false;
    }

    /**
     * Обработка кейса: тема существует (describeTopics успешен).
     *
     * @param t имя Kafka‑топика
     */
    private void onExistsTrue(String t) {
        state.existsTrue.increment();
        markEnsured(t);
        maybeEnsureUpgrades(t);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Kafka-топик '{}' уже существует — создание не требуется", t);
        }
    }

    /**
     * Обработка кейса: статус существования темы не определён (таймаут/ACL/сеть).
     * Планирует повторную попытку через короткий backoff.
     *
     * @param t имя Kafka‑топика
     */
    private void onExistsUnknown(String t) {
        state.existsUnknown.increment();
        backoffManager.scheduleRetry(t);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Не удалось надёжно определить существование Kafka-топика '{}'; повторю попытку после ~{} мс",
                    t, backoffManager.baseDelayMs());
        }
    }

    /**
     * Пытается создать тему, учитывая гонки и таймауты; обновляет метрики/кеш/логи.
     *
     * @param t имя Kafka‑топика
     */
    private void tryCreateTopic(String t) {
        try {
            configPlanner.createTopic(t, adminTimeoutMs);
        } catch (InterruptedException ie) {
            onCreateInterrupted(t, ie);
        } catch (ExecutionException e) {
            onCreateExecException(t, e);
        } catch (TimeoutException te) {
            onCreateTimeout(t, te);
        } catch (RuntimeException re) {
            onCreateRuntime(t, re);
        }
    }

    /**
     * Назначение
     *  - Обработка прерывания при создании темы: восстанавливает флаг прерывания,
     *    инкрементирует метрику неуспеха и пишет краткий WARN (полная трассировка — только в DEBUG).
     *
     * Контракт
     *  - Не бросает исключений.
     *  - Всегда вызывает Thread.currentThread().interrupt() для корректной сигнализации наверх.
     *
     * Параметры
     *  - t — имя Kafka-топика.
     *  - ie — перехваченное InterruptedException.
     *
     * Исключения
     *  - Нет.
     *
     * Замечания по производительности
     *  - Без аллокаций на горячем пути; дополнительная активность только при включённом DEBUG-логировании.
     */
    private void onCreateInterrupted(String t, InterruptedException ie) {
        Thread.currentThread().interrupt();
        configPlanner.recordFailure(t, ie);
        LOG.warn("Создание Kafka-топика '{}' было прервано", t);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Трассировка прерывания при создании темы '{}'", t, ie);
        }
    }

    /**
     * Назначение
     *  - Обработка ошибок выполнения при создании темы.
     *    TopicExists → фиксируем гонку и помечаем тему как подтверждённую;
     *    прочие ошибки → увеличиваем метрику и логируем кратко.
     *
     * Контракт
     *  - Не бросает исключений.
     *  - Идемпотентен относительно повторных вызовов.
     *
     * Параметры
     *  - t — имя Kafka-топика.
     *  - e — ExecutionException из AdminClient.
     *
     * Исключения
     *  - Нет.
     *
     * Замечания по производительности
     *  - Формирует краткое сообщение в WARN без stacktrace; полная трассировка — только в DEBUG.
     */
    private void onCreateExecException(String t, ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof TopicExistsException) {
            configPlanner.recordRace(t);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Kafka-топик '{}' уже существует (создан параллельно)", t);
            }
        } else {
            configPlanner.recordFailure(t, cause == null ? e : cause);
            LOG.warn("Не удалось создать Kafka-топик '{}': {}: {}",
                    t,
                    (cause == null ? e.getClass().getSimpleName() : cause.getClass().getSimpleName()),
                    (cause == null ? e.getMessage() : cause.getMessage()));
            if (LOG.isDebugEnabled()) {
                LOG.debug("Трассировка ошибки создания темы '{}'", t, e);
            }
        }
    }

    /**
     * Назначение
     *  - Обработка таймаута создания темы с записью краткого WARN и DEBUG-трассировки.
     *
     * Контракт
     *  - Не бросает исключений.
     *
     * Параметры
     *  - t — имя Kafka-топика.
     *  - te — TimeoutException из AdminClient.
     *
     * Исключения
     *  - Нет.
     *
     * Замечания по производительности
     *  - Минимальные аллокации; детальная трассировка только при DEBUG.
     */
    private void onCreateTimeout(String t, TimeoutException te) {
        configPlanner.recordFailure(t, te);
        LOG.warn("Не удалось создать Kafka-топик '{}': таймаут {} мс", t, adminTimeoutMs);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Трассировка таймаута создания темы '{}'", t, te);
        }
    }

    /**
     * Назначение
     *  - Унифицированная обработка непроверяемых исключений при создании темы.
     *
     * Контракт
     *  - Не бросает исключений.
     *
     * Параметры
     *  - t — имя Kafka-топика.
     *  - re — перехваченное RuntimeException.
     *
     * Исключения
     *  - Нет.
     *
     * Замечания по производительности
     *  - В WARN пишется только краткое сообщение; stacktrace уходит в DEBUG.
     */
    private void onCreateRuntime(String t, RuntimeException re) {
        configPlanner.recordFailure(t, re);
        LOG.warn("Не удалось создать Kafka-топик '{}' (runtime): {}", t, re.getMessage());
        if (LOG.isDebugEnabled()) {
            LOG.debug("Трассировка runtime при создании темы '{}'", t, re);
        }
    }

    /** Быстрая проверка с ensure; возвращает {@code true}, только если тема подтверждена после вызова. */
    public boolean ensureTopicOk(String topic) {
        if (admin == null) return false;               // ensureTopics=false
        final String raw = (topic == null) ? "" : topic.trim();
        final String t = config.topicSanitizer().apply(raw);
        if (t.isEmpty()) return false;
        // Быстрый путь: уже проверяли/создавали ранее — тема точно есть
        if (state.ensured.contains(t)) return true;
        // Иначе выполняем ensure и затем проверяем кеш
        ensureTopic(t);                                // ensureTopic сам валидирует имя и пр.
        return state.ensured.contains(t);
    }

    /** Batch-ensure нескольких тем с единичным describe и выборочным create. */
    public void ensureTopics(Collection<String> topics) {
        if (admin == null || topics == null || topics.isEmpty()) return;
        java.util.Set<String> toCheck = normalizeCandidates(topics);
        if (toCheck.isEmpty()) return;
        List<String> missing = describeAndCollectMissing(toCheck);
        if (missing.isEmpty()) return;
        createMissingTopics(missing);
    }

    /**
     * Нормализует входной набор имён: trim, валидация по правилам брокера,
     * учёт кеша и активного backoff.
     *
     * @param topics исходные имена
     * @return отфильтрованный набор имён, которые имеет смысл проверять у брокера
     */
    private LinkedHashSet<String> normalizeCandidates(Collection<String> topics) {
        LinkedHashSet<String> toCheck = new LinkedHashSet<>(topics.size());
        for (String raw : topics) {
            String base = (raw == null) ? "" : raw.trim();
            String t = config.topicSanitizer().apply(base);
            if (t.isEmpty()) {
                // пустые имена пропускаем молча
            } else if (!TopicNameValidator.isValid(t, config.topicNameMaxLen())) {
                LOG.warn(WARN_INVALID_TOPIC, t, config.topicNameMaxLen());
            } else if (state.ensured.contains(t)) {
                state.ensureHitCache.increment();
            } else if (backoffManager.shouldSkip(t)) {
                // действует backoff — пропускаем до следующего окна
            } else {
                toCheck.add(t);
            }
        }
        return toCheck;
    }

    /**
     * Вызывает describeTopics для набора имён и распределяет результаты:
     * существующие → кеш, отсутствующие → список на создание.
     *
     * @param toCheck имена тем, прошедших нормализацию
     * @return список отсутствующих тем
     */
    private List<String> describeAndCollectMissing(Set<String> toCheck) {
        return describeHandler.describeBatch(toCheck);
    }

    /**
     * При необходимости выполняет «скрытые» апгрейды для существующей темы:
     * увеличение числа партиций и/или приведение конфигов (diff-only).
     * Быстрый выход, если соответствующие флаги выключены.
     */
    private void maybeEnsureUpgrades(String t) {
        if ((!config.ensureIncreasePartitions() && !config.ensureDiffConfigs()) || admin == null) return;
        if (config.ensureIncreasePartitions()) {
            ensurePartitionsIfEnabled(t);
        }
        if (config.ensureDiffConfigs() && !config.topicConfigs().isEmpty()) {
            ensureConfigsIfEnabled(t);
        }
    }

    /** Увеличивает число партиций до конфигурационного значения, если текущее меньше. */
    private void ensurePartitionsIfEnabled(String t) {
        try {
            int cur = currentPartitionCount(t);
            decideAndMaybeIncreasePartitions(t, cur);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Проверка/увеличение партиций прерваны для '{}'", t, ie);
            } else {
                LOG.warn("Проверка/увеличение партиций прерваны для '{}'", t);
            }
        } catch (TimeoutException | ExecutionException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Не удалось проверить/увеличить партиции Kafka-топика '{}'", t, e);
            } else {
                LOG.warn("Не удалось проверить/увеличить партиции Kafka-топика '{}': {}", t, e.toString());
            }
        } catch (RuntimeException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Не удалось проверить/увеличить партиции Kafka-топика '{}' (runtime)", t, e);
            } else {
                LOG.warn("Не удалось проверить/увеличить партиции Kafka-топика '{}' (runtime): {}", t, e.toString());
            }
        }
    }

    private int currentPartitionCount(String t)
            throws InterruptedException, ExecutionException, TimeoutException {
        Map<String, KafkaFuture<TopicDescription>> m =
                admin.describeTopics(Collections.singleton(t));
        TopicDescription d =
                m.get(t).get(adminTimeoutMs, TimeUnit.MILLISECONDS);
        return d.partitions().size();
    }

    private void decideAndMaybeIncreasePartitions(String t, int cur)
            throws InterruptedException, ExecutionException, TimeoutException {
        if (cur < config.topicPartitions()) {
            LOG.info("Увеличиваю партиции Kafka-топика '{}' {}→{}", t, cur, config.topicPartitions());
            admin.increasePartitions(t, config.topicPartitions(), adminTimeoutMs);
        } else if (cur > config.topicPartitions()) {
            LOG.warn("Текущее число партиций Kafka-топика '{}' ({}) больше заданного ({}); уменьшение не поддерживается — оставляю как есть",
                    t, cur, config.topicPartitions());
        }
    }

    /** Приводит конфиги темы к заданным только по отличающимся ключам (incrementalAlterConfigs). */
    private void ensureConfigsIfEnabled(String t) {
        try {
            ConfigResource cr =
                    new ConfigResource(
                            ConfigResource.Type.TOPIC, t);
            Config cur = fetchCurrentTopicConfig(cr);

            List<AlterConfigOp> ops = diffConfigOps(cur, config.topicConfigs());
            if (!ops.isEmpty()) {
                applyConfigChanges(t, cr, ops);
            }
        } catch (InterruptedException ie) {
            onConfigsInterrupted(t, ie);
        } catch (TimeoutException te) {
            onConfigsTimeout(t, te);
        } catch (ExecutionException ee) {
            onConfigsExecution(t, ee);
        } catch (RuntimeException e) {
            onConfigsRuntime(t, e);
        }
    }

    private Config fetchCurrentTopicConfig(
            ConfigResource cr)
            throws InterruptedException, ExecutionException, TimeoutException {
        Map<ConfigResource, KafkaFuture<Config>> vals =
                admin.describeConfigs(Collections.singleton(cr));
        return vals.get(cr).get(adminTimeoutMs, TimeUnit.MILLISECONDS);
    }

    private List<AlterConfigOp> diffConfigOps(
            Config cur,
            Map<String, String> desired) {
        List<AlterConfigOp> ops = new ArrayList<>();
        if (desired == null || desired.isEmpty()) return ops;
        for (Map.Entry<String, String> e : desired.entrySet()) {
            String k = e.getKey();
            String v = e.getValue();
            ConfigEntry ce = cur.get(k);
            String curV = (ce == null) ? null : ce.value();
            if (!eq(curV, v)) {
                ops.add(new AlterConfigOp(
                        new ConfigEntry(k, v),
                        AlterConfigOp.OpType.SET));
            }
        }
        return ops;
    }

    private void applyConfigChanges(
            String topic,
            ConfigResource cr,
            List<AlterConfigOp> ops)
            throws InterruptedException, ExecutionException, TimeoutException {
        Map<ConfigResource, Collection<AlterConfigOp>> req =
                new LinkedHashMap<>(1);
        req.put(cr, ops);
        LOG.info("Привожу конфиги Kafka-топика '{}' ({} ключ(а/ей))", topic, ops.size());
        admin.incrementalAlterConfigs(req, adminTimeoutMs);
    }

    private void onConfigsInterrupted(String t, InterruptedException ie) {
        Thread.currentThread().interrupt();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Приведение конфигов прервано для Kafka-топика '{}'", t, ie);
        } else {
            LOG.warn("Приведение конфигов прервано для Kafka-топика '{}'", t);
        }
    }

    private void onConfigsTimeout(String t, TimeoutException te) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Не удалось привести конфиги Kafka-топика '{}' из-за таймаута {}", t, adminTimeoutMs, te);
        } else {
            LOG.warn("Не удалось привести конфиги Kafka-топика '{}': таймаут {} мс", t, adminTimeoutMs);
        }
    }

    private void onConfigsExecution(String t, ExecutionException e) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Не удалось привести конфиги Kafka-топика '{}'", t, e);
        } else {
            LOG.warn("Не удалось привести конфиги Kafka-топика '{}': {}", t, e.toString());
        }
    }

    private void onConfigsRuntime(String t, RuntimeException e) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Не удалось привести конфиги Kafka-топика '{}' (runtime)", t, e);
        } else {
            LOG.warn("Не удалось привести конфиги Kafka-топика '{}' (runtime): {}", t, e.toString());
        }
    }

    /** Utility: null-safe string equals. */
    private static boolean eq(String a, String b) {
        return Objects.equals(a, b);
    }

    /** Результат попытки создания темы (batch). */
    private enum CreateOutcome { OK, RACE, TIMEOUT, FAIL }

    /**
     * Пакетно создаёт отсутствующие темы и обрабатывает результаты по каждой.
     * Делает единичный повтор ТОЛЬКО для кейсов таймаута, с коротким backoff.
     *
     * @param missing имена отсутствующих тем
     */
    private void createMissingTopics(List<String> missing) {
        List<NewTopic> newTopics = configPlanner.planTopics(missing);
        Map<String, KafkaFuture<Void>> cvals = configPlanner.admin().createTopics(newTopics);
        ArrayList<String> timedOut = new ArrayList<>(missing.size());
        for (String t : missing) {
            CreateOutcome r = processCreateResult(cvals, t);
            if (r == CreateOutcome.TIMEOUT) timedOut.add(t);
        }
        if (timedOut.isEmpty()) return;
        long delayMs = backoffManager.computeDelayMillis();
        try {
            Thread.sleep(delayMs);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return;
        }
        List<NewTopic> retryTopics = configPlanner.planTopics(timedOut);
        Map<String, KafkaFuture<Void>> rvals = configPlanner.admin().createTopics(retryTopics);
        for (String t : timedOut) {
            processCreateResult(rvals, t);
        }
    }

    /**
     * Обрабатывает результат {@code createTopics} по одной теме: успех, гонка (TopicExists), таймаут, иные ошибки.
     *
     * @param cvals карта topic → future результата создания
     * @param t     имя Kafka‑топика
     * @return результат обработки для возможного повторного шага
     */
    private CreateOutcome processCreateResult(
            Map<String, KafkaFuture<Void>> cvals,
            String t) {
        try {
            cvals.get(t).get(adminTimeoutMs, TimeUnit.MILLISECONDS);
            configPlanner.recordSuccess(t);
            configPlanner.logTopicCreated(t);
            return CreateOutcome.OK;
        } catch (InterruptedException ie) {
            onCreateBatchInterrupted(t, ie);
            return CreateOutcome.FAIL;
        } catch (TimeoutException te) {
            onCreateBatchTimeout(t, te);
            return CreateOutcome.TIMEOUT;
        } catch (ExecutionException ee) {
            return handleCreateExecution(t, ee);
        }
    }

    /**
     * Обработчик ExecutionException при createTopics (batch).
     * Выделен отдельно для снижения когнитивной сложности основного метода.
     */
    private CreateOutcome handleCreateExecution(String t, ExecutionException ee) {
        final Throwable cause = ee.getCause();
        if (cause instanceof TopicExistsException) {
            configPlanner.recordRace(t);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Kafka-топик '{}' уже существует (создан параллельно)", t);
            }
            return CreateOutcome.RACE;
        }
        configPlanner.recordFailure(t, cause == null ? ee : cause);
        LOG.warn("Не удалось создать Kafka-топик '{}': {}: {}",
                t,
                (cause == null ? ee.getClass().getSimpleName() : cause.getClass().getSimpleName()),
                (cause == null ? ee.getMessage() : cause.getMessage()));
        return CreateOutcome.FAIL;
    }

    private void onCreateBatchInterrupted(String t, InterruptedException ie) {
        onCreateInterrupted(t, ie);
    }

    private void onCreateBatchTimeout(String t, TimeoutException te) {
        onCreateTimeout(t, te);
    }

    /**
     * Возвращает неизменяемый снимок внутренних счётчиков ensure-процесса.
     * Счётчики накопительные с момента создания инстанса; после перезапуска пира
     * значения обнуляются. Подсчёт размера очереди backoff выполняется без
     * лишних аллокаций через {@code state.unknownSize()}.
     *
     * @return неизменяемая карта «имя метрики → значение»
     */
    public Map<String, Long> getMetrics() {
        // 9 метрик → начальная ёмкость 13 (формула JDK для LinkedHashMap)
        Map<String, Long> m = new LinkedHashMap<>(13);
        m.put("ensure.invocations", state.ensureInvocations.longValue());
        m.put("ensure.cache.hit",   state.ensureHitCache.longValue());
        m.put("exists.true",        state.existsTrue.longValue());
        m.put("exists.false",       state.existsFalse.longValue());
        m.put("exists.unknown",     state.existsUnknown.longValue());
        m.put("create.ok",          state.createOk.longValue());
        m.put("create.race",        state.createRace.longValue());
        m.put("create.fail",        state.createFail.longValue());
        m.put("unknown.backoff.size", (long) state.unknownSize());
        return Collections.unmodifiableMap(m);
    }

    /**
     * Возвращает снимок очереди backoff: {@code topic → оставшееся время (мс)}.
     *
     * Контракт:
     *  - все значения считаются от единой точки времени {@code System.nanoTime()} (консистентность);
     *  - источник данных — немутируемая копия дедлайнов, см. {@link TopicEnsureState#snapshotUnknown()};
     *  - результат — неизменяемая карта; попытка модификации приводит к {@link UnsupportedOperationException};
     *  - сложность — {@code O(n)} по количеству элементов очереди; повторных снапшотов и лишних аллокаций нет.
     *
     * @return неизменяемая карта «topic → оставшиеся миллисекунды»
     */
    public Map<String, Long> getBackoffSnapshot() {
        final Map<String, Long> pending = state.snapshotUnknown();
        if (pending.isEmpty()) {
            return Collections.emptyMap();
        }
        final Map<String, Long> snapshot = new LinkedHashMap<>(pending.size());
        final long now = System.nanoTime();
        for (Map.Entry<String, Long> e : pending.entrySet()) {
            long remainingNs = e.getValue() - now;
            if (remainingNs < 0L) remainingNs = 0L;
            snapshot.put(e.getKey(), TimeUnit.NANOSECONDS.toMillis(remainingNs));
        }
        return Collections.unmodifiableMap(snapshot);
    }

    /** Закрывает AdminClient через обёртку KafkaTopicAdmin с таймаутом adminTimeoutMs. Безопасен к повторным вызовам. */
    @Override public void close() {
        if (admin != null) {
            if (LOG.isDebugEnabled()) LOG.debug("Закрываю Kafka AdminClient");
            admin.close(Duration.ofMillis(adminTimeoutMs));
        }
    }

    /**
     * Краткое диагностическое представление состояния TopicEnsurer.
     * Содержит только метаданные конфигурации и размеры кешей/метрик; без тяжёлых операций.
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(128);
        sb.append("TopicEnsurer{")
          .append("partitions=").append(config.topicPartitions())
          .append(", replication=").append(config.topicReplication())
          .append(", adminTimeoutMs=").append(adminTimeoutMs)
          .append(", unknownBackoffMs=").append(unknownBackoffMs)
          .append(", ensured.size=").append(state.ensured.size())
          .append(", metrics={")
          .append("ensure=").append(state.ensureInvocations.longValue())
          .append(", hit=").append(state.ensureHitCache.longValue())
          .append(", existsT=").append(state.existsTrue.longValue())
          .append(", existsF=").append(state.existsFalse.longValue())
          .append(", existsU=").append(state.existsUnknown.longValue())
          .append(", createOk=").append(state.createOk.longValue())
          .append(", createRace=").append(state.createRace.longValue())
          .append(", createFail=").append(state.createFail.longValue())
          .append("}}");
        return sb.toString();
    }
}
