package kz.qazmarka.h2k.kafka;

import java.security.SecureRandom;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kz.qazmarka.h2k.config.H2kConfig;

/**
 * Проверка/создание Kafka-топиков и применение per-topic конфигов из {@link H2kConfig}.
 *
 * Назначение
 *  - При старте/в рантайме убедиться, что целевые темы существуют; при отсутствии — создать с нужными
 *    параметрами (число партиций, фактор репликации, минимальный набор конфигов).
 *  - Ничего не «правит» у уже существующих тем — поведение идемпотентно и безопасно к гонкам.
 *
 * Производительность и потокобезопасность
 *  - Класс предназначен для использования из одного потока (в рамках RegionServer‑репликации),
 *    но внутренние структуры — неблокирующие (ConcurrentHashMap/LongAdder) и корректны при редких
 *    конкурентных вызовах.
 *  - На горячем пути не находится: обращения происходят редко (при первом упоминании темы/ошибках сети).
 *  - Есть кеш успешно проверенных/созданных тем (ensured) и короткий backoff для «неуверенных» ошибок.
 *
 * Логирование
 *  - INFO: только факты создания темы и явные ошибки.
 *  - DEBUG: повторные проверки, backoff‑решения, диагностические детали и сводка применённых конфигов.
 *
 * Конфигурация
 *  - Bootstrap/ClientId/таймауты берутся из {@link H2kConfig}; REQUEST_TIMEOUT_MS у AdminClient
 *    синхронизирован с adminTimeoutMs из конфига.
 *  - Включение/выключение: {@code h2k.ensure.topics} (true/false).
 *  - Параметры создаваемых тем: {@code h2k.topic.partitions} (≥1), {@code h2k.topic.replication} (≥1).
 *  - Дополнительные конфиги при создании: snapshot из {@link H2kConfig#getTopicConfigs()}.
 *  - Backoff на «неуверенные» ошибки: {@code h2k.ensure.unknown.backoff.ms} (используется с крипто‑джиттером).
 *  - Класс никогда не изменяет уже существующие темы (конфиги существующих тем остаются без изменений).
 */
public final class TopicEnsurer implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(TopicEnsurer.class);
    /**
     * Максимальная разрешённая длина имени Kafka‑топика (ограничение брокера).
     * На момент разработки: 249 символов.
     */
    private static final int TOPIC_NAME_MAX_LEN = 249;

    /** Ключи часто используемых конфигов темы, выводимых в краткой сводке (см. summarizeTopicConfigs). */
    private static final String CFG_RETENTION_MS       = "retention.ms";
    private static final String CFG_CLEANUP_POLICY     = "cleanup.policy";
    private static final String CFG_COMPRESSION_TYPE   = "compression.type";
    private static final String CFG_MIN_INSYNC_REPLICAS= "min.insync.replicas";

    // Сообщение о некорректном имени топика (чтобы не дублировать литерал)
    private static final String WARN_INVALID_TOPIC =
            "Некорректное имя Kafka-топика '{}': допускаются [a-zA-Z0-9._-], длина 1..{}, запрещены '.' и '..'";
    /** Обёртка над AdminClient для unit‑тестируемости и стабильного API в классе. Может быть null, если ensureTopics=false. */
    private final TopicAdmin admin;
    /** Таймаут админских операций в миллисекундах (describe/create). */
    private final long adminTimeoutMs;
    /** Число партиций для создаваемых тем (валидируется: минимум 1). */
    private final int topicPartitions;
    /** Фактор репликации для создаваемых тем (валидируется: минимум 1). */
    private final short topicReplication;
    /** Кеш тем, существование которых уже подтверждено (describe/create завершились успешно). */
    private final Set<String> ensured = ConcurrentHashMap.newKeySet();
    /** Конфиги, применяемые ТОЛЬКО при создании новой темы; для существующих тем не используются. */
    private final Map<String, String> topicConfigs;

    // ---- Лёгкие метрики (для отладки/наблюдаемости) ----
    private final LongAdder ensureInvocations = new LongAdder();
    private final LongAdder ensureHitCache   = new LongAdder();
    private final LongAdder existsTrue       = new LongAdder();
    private final LongAdder existsFalse      = new LongAdder();
    private final LongAdder existsUnknown    = new LongAdder();
    private final LongAdder createOk         = new LongAdder();
    private final LongAdder createRace       = new LongAdder();
    private final LongAdder createFail       = new LongAdder();

    /** Базовая величина backoff (мс) на «неуверенные» ошибки describe/create. */
    private final long unknownBackoffMs;
    /** Предвычисленный backoff в наносекундах (для быстрых расчётов задержки). */
    private final long unknownBackoffNs;
    /** Политика генерации задержки с джиттером (SecureRandom), без аллокаций на вызов. */
    private final BackoffPolicy backoff;

    /** Таймстемпы (nano) до которых мы не трогаем топик из-за предыдущего UNKNOWN-состояния. */
    private final ConcurrentHashMap<String, Long> unknownUntil = new ConcurrentHashMap<>();

    /**
     * Трёхзначный результат проверки существования темы.
     * TRUE    — тема подтверждена брокером (describeTopics без ошибок/таймаута).
     * FALSE   — брокер ответил, что темы нет (UnknownTopicOrPartitionException).
     * UNKNOWN — «неуверенная» ошибка (таймаут, прерывание, ACL/сеть/прочее) — включаем короткий backoff.
     */
    private enum TopicExistence { TRUE, FALSE, UNKNOWN }

    /** Минимальный интерфейс для админских вызовов Kafka, удобный для unit-тестов. */
    private interface TopicAdmin {
        /**
         * Описывает набор тем одним сетевым вызовом.
         * @param names имена тем
         * @return карта topic → KafkaFuture с TopicDescription; get() может бросить Timeout/ExecutionException
         */
        java.util.Map<String, org.apache.kafka.common.KafkaFuture<org.apache.kafka.clients.admin.TopicDescription>>
        describeTopics(java.util.Set<String> names);

        /**
         * Создаёт несколько тем батчем.
         * @param newTopics список спецификаций новых тем
         * @return карта topic → KafkaFuture<Void> для ожидания результатов по отдельности
         */
        java.util.Map<String, org.apache.kafka.common.KafkaFuture<Void>>
        createTopics(java.util.List<org.apache.kafka.clients.admin.NewTopic> newTopics);

        /**
         * Создаёт одну тему и блокирующе ожидает завершения с заданным таймаутом.
         * Может бросить InterruptedException/TimeoutException/ExecutionException.
         */
        void createTopic(org.apache.kafka.clients.admin.NewTopic topic, long timeoutMs)
                throws InterruptedException, java.util.concurrent.ExecutionException, java.util.concurrent.TimeoutException;

        /** Закрывает клиент с ожиданием до указанного таймаута. */
        void close(java.time.Duration timeout);
    }

    /** Реализация TopicAdmin поверх реального AdminClient. Не содержит бизнес‑логики, только адаптация API. */
    private static final class AdminFacade implements TopicAdmin {
        private final org.apache.kafka.clients.admin.AdminClient delegate;
        AdminFacade(org.apache.kafka.clients.admin.AdminClient delegate) { this.delegate = delegate; }

        @Override
        public java.util.Map<String, org.apache.kafka.common.KafkaFuture<org.apache.kafka.clients.admin.TopicDescription>>
        describeTopics(java.util.Set<String> names) {
            return delegate.describeTopics(names).values();
        }

        @Override
        public java.util.Map<String, org.apache.kafka.common.KafkaFuture<Void>>
        createTopics(java.util.List<org.apache.kafka.clients.admin.NewTopic> newTopics) {
            return delegate.createTopics(newTopics).values();
        }

        @Override
        public void createTopic(org.apache.kafka.clients.admin.NewTopic topic, long timeoutMs)
                throws InterruptedException, java.util.concurrent.ExecutionException, java.util.concurrent.TimeoutException {
            delegate.createTopics(java.util.Collections.singleton(topic))
                    .all()
                    .get(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
        }

        @Override
        public void close(java.time.Duration timeout) { delegate.close(timeout); }
    }

    /**
     * Помечает тему как подтверждённую (существует/успешно создана) и снимает возможный backoff.
     *
     * Вызов безопасен при повторении; кеш и карта backoff будут приведены к согласованному состоянию.
     *
     * @param t имя Kafka‑топика
     */
    private void markEnsured(String t) {
        ensured.add(t);
        unknownUntil.remove(t);
    }

    /**
     * Создаёт экземпляр {@link TopicEnsurer}, если включена опция ensureTopics.
     *
     * Параметры AdminClient (bootstrap, client.id, request.timeout.ms) берутся из {@link H2kConfig}.
     * Если bootstrap не задан — возвращает {@code null} и пишет предупреждение.
     *
     * @param cfg конфигурация H2K
     * @return инициализированный {@code TopicEnsurer} или {@code null}, если ensureTopics=false / отсутствует bootstrap
     */
    public static TopicEnsurer createIfEnabled(H2kConfig cfg) {
        if (!cfg.isEnsureTopics()) return null;
        final String bootstrap = cfg.getBootstrap();
        if (bootstrap == null || bootstrap.trim().isEmpty()) {
            LOG.warn("TopicEnsurer: не задан bootstrap Kafka — ensureTopics будет отключён");
            return null;
        }
        java.util.Properties ap = new java.util.Properties();
        ap.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        ap.put(AdminClientConfig.CLIENT_ID_CONFIG, cfg.getAdminClientId());
        // Синхронизируем таймаут запросов AdminClient с конфигурацией (ускоряет фейлы и снижает зависания)
        ap.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) cfg.getAdminTimeoutMs());
        AdminClient ac = AdminClient.create(ap);
        TopicAdmin admin = new AdminFacade(ac);
        return new TopicEnsurer(
            admin,
            cfg.getAdminTimeoutMs(),
            cfg.getTopicPartitions(),
            cfg.getTopicReplication(),
            cfg.getTopicConfigs(),
            cfg.getUnknownBackoffMs()
        );
    }

    /**
     * Внутренний конструктор. Выполняет валидацию входных параметров и подготавливает неизменяемые поля.
     *
     * @param admin обёртка над AdminClient или {@code null}, если ensureTopics=false
     * @param adminTimeoutMs таймаут админских вызовов, мс
     * @param topicPartitions число партиций (минимум 1)
     * @param topicReplication фактор репликации (минимум 1)
     * @param topicConfigs конфиги для создаваемых тем (read‑only snapshot)
     * @param unknownBackoffMs базовый backoff на «неуверенные» ошибки (мс)
     */
    private TopicEnsurer(TopicAdmin admin, long adminTimeoutMs, int topicPartitions, short topicReplication,
                        Map<String, String> topicConfigs, long unknownBackoffMs) {
        this.admin = admin;
        this.adminTimeoutMs = adminTimeoutMs;
        int parts = topicPartitions;
        if (parts < 1) {
            LOG.warn("Некорректное число партиций {}: принудительно устанавливаю 1", parts);
            parts = 1;
        }
        short repl = topicReplication;
        if (repl < 1) {
            LOG.warn("Некорректный фактор репликации {}: принудительно устанавливаю 1", repl);
            repl = 1;
        }
        this.topicPartitions = parts;
        this.topicReplication = repl;
        this.topicConfigs = (topicConfigs == null
                ? java.util.Collections.emptyMap()
                : java.util.Collections.unmodifiableMap(new java.util.HashMap<>(topicConfigs)));
        this.unknownBackoffMs = unknownBackoffMs;
        this.unknownBackoffNs = TimeUnit.MILLISECONDS.toNanos(unknownBackoffMs);
        this.backoff = new BackoffPolicy(TimeUnit.MILLISECONDS.toNanos(1), 20); // min=1ms, jitter≈20%
    }

    /**
     * Проверяет существование темы и при отсутствии — пытается создать её (идемпотентно).
     *
     * Быстрые ветки: пустое/некорректное имя → WARN и выход; кеш ensured; активный backoff.
     * При UNKNOWN‑ситуациях (таймаут/ACL/сеть) назначает короткий backoff с джиттером.
     *
     * @param topic имя Kafka‑топика
     */
    public void ensureTopic(String topic) {
        if (admin == null) return;
        ensureInvocations.increment();

        final String t = (topic == null) ? null : topic.trim();
        if (t == null || t.isEmpty()) {
            LOG.warn("Пустое имя Kafka-топика — пропускаю ensure");
            return;
        }
        if (!isValidTopicName(t)) {
            LOG.warn(WARN_INVALID_TOPIC, t, TOPIC_NAME_MAX_LEN);
            return;
        }
        if (fastCacheHit(t)) return;       // уже успешно проверяли
        if (respectBackoffIfAny(t)) return; // действует backoff

        TopicExistence ex = topicExists(t);
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
        if (ensured.contains(t)) {
            ensureHitCache.increment();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Kafka-топик '{}' уже проверен ранее — пропускаю ensure", t);
            }
            return true;
        }
        return false;
    }

    /**
     * Учитывает активный backoff после «неуверенной» ошибки по теме.
     *
     * @param t имя Kafka‑топика
     * @return {@code true}, если «окно ожидания» ещё не истекло и попытку следует отложить
     */
    private boolean respectBackoffIfAny(String t) {
        Long until = unknownUntil.get(t);
        if (until == null) return false;
        long now = System.nanoTime();
        if (now < until) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Пропускаю ensure Kafka-топика '{}' из-за backoff (осталось ~{} мс)",
                        t, TimeUnit.NANOSECONDS.toMillis(until - now));
            }
            return true;
        }
        unknownUntil.remove(t);
        return false;
    }

    /**
     * Обработка кейса: тема существует (describeTopics успешен).
     *
     * @param t имя Kafka‑топика
     */
    private void onExistsTrue(String t) {
        existsTrue.increment();
        markEnsured(t);
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
        existsUnknown.increment();
        scheduleUnknown(t);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Не удалось надёжно определить существование Kafka-топика '{}'; повторю попытку после ~{} мс",
                    t, unknownBackoffMs);
        }
    }

    /**
     * Пытается создать тему, учитывая гонки и таймауты; обновляет метрики/кеш/логи.
     *
     * @param t имя Kafka‑топика
     */
    private void tryCreateTopic(String t) {
        try {
            createTopic(t);
            createOk.increment();
            markEnsured(t);
        } catch (InterruptedException ie) {
            onCreateInterrupted(t, ie);
        } catch (java.util.concurrent.ExecutionException e) {
            onCreateExecException(t, e);
        } catch (java.util.concurrent.TimeoutException te) {
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
        createFail.increment();
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
    private void onCreateExecException(String t, java.util.concurrent.ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof org.apache.kafka.common.errors.TopicExistsException) {
            createRace.increment();
            markEnsured(t);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Kafka-топик '{}' уже существует (создан параллельно)", t);
            }
        } else {
            createFail.increment();
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
    private void onCreateTimeout(String t, java.util.concurrent.TimeoutException te) {
        createFail.increment();
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
        createFail.increment();
        LOG.warn("Не удалось создать Kafka-топик '{}' (runtime): {}", t, re.getMessage());
        if (LOG.isDebugEnabled()) {
            LOG.debug("Трассировка runtime при создании темы '{}'", t, re);
        }
    }

    /**
     * Убеждается, что тема существует, и возвращает итог.
     *
     * Возвращает {@code true}, если тема подтверждена (была ранее/создана сейчас/подтверждена describeTopics).
     * Возвращает {@code false}, если ensure отключён, имя пустое/некорректное, действует backoff либо произошла ошибка.
     * Метод никогда не бросает исключений.
     *
     * @param topic имя Kafka‑топика
     * @return {@code true}, если тема гарантированно существует; иначе {@code false}
     */
    public boolean ensureTopicOk(String topic) {
        if (admin == null) return false;               // ensureTopics=false
        if (topic == null) return false;
        final String t = topic.trim();
        if (t.isEmpty()) return false;
        // Быстрый путь: уже проверяли/создавали ранее — тема точно есть
        if (ensured.contains(t)) return true;
        // Иначе выполняем ensure и затем проверяем кеш
        ensureTopic(t);                                // ensureTopic сам валидирует имя и пр.
        return ensured.contains(t);
    }

    /**
     * Пакетная проверка/создание тем одним/двумя сетевыми вызовами.
     *
     * Нормализует имена (trim/валидация), исключает уже подтверждённые и попавшие в backoff,
     * затем вызывает describeTopics и createTopics для отсутствующих.
     *
     * @param topics коллекция имён Kafka‑топиков
     */
    public void ensureTopics(java.util.Collection<String> topics) {
        if (admin == null || topics == null || topics.isEmpty()) return;
        java.util.Set<String> toCheck = normalizeCandidates(topics);
        if (toCheck.isEmpty()) return;
        java.util.List<String> missing = describeAndCollectMissing(toCheck);
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
    private java.util.LinkedHashSet<String> normalizeCandidates(java.util.Collection<String> topics) {
        java.util.LinkedHashSet<String> toCheck = new java.util.LinkedHashSet<>(topics.size());
        for (String raw : topics) {
            String t = (raw == null) ? "" : raw.trim();
            if (t.isEmpty()) {
                // пустые имена пропускаем молча
            } else if (!isValidTopicName(t)) {
                LOG.warn(WARN_INVALID_TOPIC, t, TOPIC_NAME_MAX_LEN);
            } else if (ensured.contains(t)) {
                ensureHitCache.increment();
            } else if (respectBackoffIfAny(t)) {
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
    private java.util.ArrayList<String> describeAndCollectMissing(java.util.Set<String> toCheck) {
        java.util.ArrayList<String> missing = new java.util.ArrayList<>(toCheck.size());
        java.util.Map<String, org.apache.kafka.common.KafkaFuture<org.apache.kafka.clients.admin.TopicDescription>> fmap =
                admin.describeTopics(toCheck);
        for (String t : toCheck) {
            classifyDescribeTopic(fmap, t, missing);
        }
        return missing;
    }

    /**
     * Обрабатывает результат describeTopics по одной теме.
     *
     * UnknownTopic → добавляет в {@code missing}; Timeout/Execution (не UnknownTopic)/Interrupted → планирует backoff,
     * ведёт DEBUG‑лог и инкрементирует метрики.
     *
     * @param fmap   карта topic → future TopicDescription
     * @param t      имя Kafka‑топика
     * @param missing результирующий список отсутствующих тем
     */
    private void classifyDescribeTopic(
            java.util.Map<String, org.apache.kafka.common.KafkaFuture<org.apache.kafka.clients.admin.TopicDescription>> fmap,
            String t,
            java.util.List<String> missing) {
        try {
            fmap.get(t).get(adminTimeoutMs, TimeUnit.MILLISECONDS);
            existsTrue.increment();
            markEnsured(t);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Kafka-топик '{}' уже существует — создание не требуется (batch)", t);
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            existsUnknown.increment();
            scheduleUnknown(t);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Проверка Kafka-топика '{}' прервана (batch)", t, ie);
            }
        } catch (java.util.concurrent.TimeoutException te) {
            existsUnknown.increment();
            scheduleUnknown(t);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Проверка Kafka-топика '{}' превысила таймаут {} мс (batch)", t, adminTimeoutMs, te);
            }
        } catch (java.util.concurrent.ExecutionException ee) {
            Throwable cause = ee.getCause();
            if (cause instanceof UnknownTopicOrPartitionException) {
                existsFalse.increment();
                missing.add(t);
            } else {
                existsUnknown.increment();
                scheduleUnknown(t);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ошибка при проверке Kafka-топика '{}' (batch)", t, ee);
                }
            }
        }
    }

    /**
     * Пакетно создаёт отсутствующие темы и обрабатывает результаты по каждой.
     *
     * @param missing имена отсутствующих тем
     */
    private void createMissingTopics(java.util.List<String> missing) {
        java.util.List<NewTopic> newTopics = new java.util.ArrayList<>(missing.size());
        for (String t : missing) {
            NewTopic nt = new NewTopic(t, topicPartitions, topicReplication);
            if (!topicConfigs.isEmpty()) nt.configs(topicConfigs);
            newTopics.add(nt);
        }
        java.util.Map<String, org.apache.kafka.common.KafkaFuture<Void>> cvals = admin.createTopics(newTopics);
        for (String t : missing) {
            processCreateResult(cvals, t);
        }
    }

    /**
     * Обрабатывает результат {@code createTopics} по одной теме: успех, гонка (TopicExists), таймаут, иные ошибки.
     *
     * @param cvals карта topic → future результата создания
     * @param t     имя Kafka‑топика
     */
    private void processCreateResult(
            java.util.Map<String, org.apache.kafka.common.KafkaFuture<Void>> cvals,
            String t) {
        try {
            cvals.get(t).get(adminTimeoutMs, TimeUnit.MILLISECONDS);
            createOk.increment();
            markEnsured(t);
            LOG.info("Создал Kafka-топик '{}': partitions={}, replication={}", t, topicPartitions, topicReplication);
            if (LOG.isDebugEnabled() && !topicConfigs.isEmpty()) {
                LOG.debug("Конфиги Kafka-топика '{}': {}", t, summarizeTopicConfigs());
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            createFail.increment();
            LOG.warn("Создание Kafka-топика '{}' было прервано (batch)", t, ie);
        } catch (java.util.concurrent.TimeoutException te) {
            createFail.increment();
            LOG.warn("Не удалось создать Kafka-топик '{}': таймаут {} мс (batch)", t, adminTimeoutMs, te);
        } catch (java.util.concurrent.ExecutionException ee) {
            Throwable cause = ee.getCause();
            if (cause instanceof org.apache.kafka.common.errors.TopicExistsException) {
                createRace.increment();
                markEnsured(t);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Kafka-топик '{}' уже существует (создан параллельно, batch)", t);
                }
            } else {
                createFail.increment();
                LOG.warn("Не удалось создать Kafka-топик '{}' (batch)", t, ee);
            }
        }
    }

    /**
     * Назначает короткий backoff с крипто‑джиттером для указанной темы.
     *
     * @param topic имя Kafka‑топика
     */
    private void scheduleUnknown(String topic) {
        long delay = backoff.nextDelayNanos(unknownBackoffNs);
        unknownUntil.put(topic, System.nanoTime() + delay);
    }

    /**
     * Выполняет describeTopics для одной темы и маппит результат на {@link TopicExistence}.
     *
     * @param topic имя Kafka‑топика
     * @return {@code TRUE} — тема подтверждена; {@code FALSE} — брокер сообщил «не существует»;
     *         {@code UNKNOWN} — таймаут/прерывание/иная ошибка (будет назначен backoff)
     */
    private TopicExistence topicExists(String topic) {
        try {
            java.util.Map<String, org.apache.kafka.common.KafkaFuture<org.apache.kafka.clients.admin.TopicDescription>> m =
                    admin.describeTopics(java.util.Collections.singleton(topic));
            m.get(topic).get(adminTimeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
            return TopicExistence.TRUE;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOG.warn("Проверка Kafka-топика '{}' прервана", topic, ie);
            scheduleUnknown(topic);
            return TopicExistence.UNKNOWN;
        } catch (java.util.concurrent.TimeoutException te) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Проверка Kafka-топика '{}' превысила таймаут {} мс", topic, adminTimeoutMs, te);
            }
            scheduleUnknown(topic);
            return TopicExistence.UNKNOWN;
        } catch (java.util.concurrent.ExecutionException ee) {
            Throwable cause = ee.getCause();
            if (cause instanceof UnknownTopicOrPartitionException) {
                existsFalse.increment();
                return TopicExistence.FALSE;
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Ошибка при проверке Kafka-топика '{}'", topic, ee);
            }
            scheduleUnknown(topic);
            return TopicExistence.UNKNOWN;
        } catch (RuntimeException re) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Не удалось проверить Kafka-топик '{}' (runtime)", topic, re);
            }
            scheduleUnknown(topic);
            return TopicExistence.UNKNOWN;
        }
    }

    /**
     * Проверка имени Kafka‑топика по основным правилам брокера (без RegEx для минимальных аллокаций).
     *
     * @param topic имя Kafka‑топика
     * @return {@code true}, если имя валидно
     */
    private static boolean isValidTopicName(String topic) {
        if (topic == null) return false;
        int len = topic.length();
        if (len == 0 || len > TOPIC_NAME_MAX_LEN) return false;
        // Точки "." и ".." являются недопустимыми именами тем
        if (".".equals(topic) || "..".equals(topic)) return false;
        for (int i = 0; i < len; i++) {
            if (!isAllowedTopicChar(topic.charAt(i))) return false;
        }
        return true;
    }

    /**
     * Проверяет, входит ли символ в множество допустимых: {@code a-z}, {@code A-Z}, {@code 0-9}, {@code '.'}, {@code '_'}, {@code '-'}.
     *
     * @param c символ
     * @return {@code true}, если символ допустим в имени топика
     */
    private static boolean isAllowedTopicChar(char c) {
        if (c >= 'a' && c <= 'z') return true;
        if (c >= 'A' && c <= 'Z') return true;
        if (c >= '0' && c <= '9') return true;
        return c == '.' || c == '_' || c == '-';
    }

    /**
     * Формирует краткую человекочитаемую сводку ключевых конфигов темы для логов.
     *
     * @return строка с основными конфигами или «без явных конфигов»
     */
    private String summarizeTopicConfigs() {
        if (topicConfigs == null || topicConfigs.isEmpty()) return "без явных конфигов";
        StringBuilder sb = new StringBuilder(128);
        String retention = topicConfigs.get(CFG_RETENTION_MS);
        String cleanup   = topicConfigs.get(CFG_CLEANUP_POLICY);
        String comp      = topicConfigs.get(CFG_COMPRESSION_TYPE);
        String minIsr    = topicConfigs.get(CFG_MIN_INSYNC_REPLICAS);
        boolean first = true;
        first = appendConfig(sb, CFG_RETENTION_MS,        retention, first);
        first = appendConfig(sb, CFG_CLEANUP_POLICY,      cleanup,   first);
        first = appendConfig(sb, CFG_COMPRESSION_TYPE,    comp,      first);
        first = appendConfig(sb, CFG_MIN_INSYNC_REPLICAS, minIsr,    first);
        int known = countNonNull(retention, cleanup, comp, minIsr);
        int others = topicConfigs.size() - known;
        if (others > 0) {
            if (!first) sb.append(", ");
            sb.append("+").append(others).append(" др.");
        }
        return sb.toString();
    }

    /**
     * Считает количество ненулевых значений среди аргументов.
     *
     * @param vals значения
     * @return число ненулевых элементов
     */
    private static int countNonNull(Object... vals) {
        int n = 0;
        if (vals != null) {
            for (Object v : vals) if (v != null) n++;
        }
        return n;
    }

    /**
     * Добавляет пару {@code key=value} в summary с учётом разделителя.
     *
     * @param sb    буфер
     * @param key   ключ конфига
     * @param value значение конфига; если {@code null}, ничего не добавляется
     * @param first признак «первого элемента» (без разделителя)
     * @return новый признак «первого элемента» (всегда {@code false}, если была добавлена пара)
     */
    private static boolean appendConfig(StringBuilder sb, String key, String value, boolean first) {
        if (value == null) return first;
        if (!first) sb.append(", ");
        sb.append(key).append("=").append(value);
        return false;
    }

    /**
     * Создаёт одну тему и логирует факт/применённые конфиги.
     *
     * Бросает {@link InterruptedException}, {@link java.util.concurrent.ExecutionException},
     * {@link java.util.concurrent.TimeoutException}; вызывающая сторона отвечает за перевод в метрики/лог.
     *
     * @param topic имя Kafka‑топика
     * @throws InterruptedException если поток прерван
     * @throws java.util.concurrent.ExecutionException ошибка выполнения на стороне брокера
     * @throws java.util.concurrent.TimeoutException по истечении {@code adminTimeoutMs}
     */
    private void createTopic(String topic)
            throws InterruptedException, java.util.concurrent.ExecutionException, java.util.concurrent.TimeoutException {
        NewTopic nt = new NewTopic(topic, topicPartitions, topicReplication);
        if (!topicConfigs.isEmpty()) {
            nt.configs(topicConfigs);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Применяю конфиги для Kafka-топика '{}': {}", topic, topicConfigs);
            }
        }
        admin.createTopic(nt, adminTimeoutMs);
        LOG.info("Создал Kafka-топик '{}': partitions={}, replication={}", topic, topicPartitions, topicReplication);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Конфиги Kafka-топика '{}': {}", topic, summarizeTopicConfigs());
        }
    }

    /** Политика backoff с крипто-джиттером без смещения; без аллокаций на вызов. */
    private static final class BackoffPolicy {
        private static final SecureRandom SR = new SecureRandom();
        private final long minNanos;
        private final int jitterPercent; // 0..100

        /**
         * @param minNanos минимальная задержка в наносекундах
         * @param jitterPercent доля джиттера в процентах (0..100)
         */
        BackoffPolicy(long minNanos, int jitterPercent) {
            this.minNanos = minNanos;
            this.jitterPercent = jitterPercent;
        }

        /**
         * Возвращает задержку вокруг baseNanos с равномерным джиттером ±jitterPercent.
         * Минимум ограничен minNanos.
         */
        long nextDelayNanos(long baseNanos) {
            long jitter = Math.max(1L, (baseNanos * jitterPercent) / 100L);
            long delta = nextLongBetweenSecure(-jitter, jitter + 1);
            long d = baseNanos + delta;
            return (d < minNanos) ? minNanos : d;
        }

        /**
         * Генерирует псевдослучайное целое из полуинтервала [{@code originInclusive}; {@code boundExclusive}).
         *
         * @param originInclusive нижняя граница (включительно)
         * @param boundExclusive  верхняя граница (исключительно)
         * @return случайное значение из заданного диапазона; при некорректном диапазоне возвращает {@code originInclusive}
         */
        private static long nextLongBetweenSecure(long originInclusive, long boundExclusive) {
            long n = boundExclusive - originInclusive;
            if (n <= 0) return originInclusive;
            long bits;
            long val;
            do {
                bits = SR.nextLong() >>> 1;
                val  = bits % n;
            } while (bits - val + (n - 1) < 0L);
            return originInclusive + val;
        }
    }

    /**
     * Возвращает неизменяемый снимок внутренних счётчиков.
     * Счётчики являются накопительными с момента создания инстанса; перезапуск пира обнуляет значения.
     *
     * @return карта «имя метрики → значение»
     */
    public Map<String, Long> getMetrics() {
        java.util.Map<String, Long> m = new java.util.LinkedHashMap<>(13);
        m.put("ensure.invocations", ensureInvocations.longValue());
        m.put("ensure.cache.hit",   ensureHitCache.longValue());
        m.put("exists.true",        existsTrue.longValue());
        m.put("exists.false",       existsFalse.longValue());
        m.put("exists.unknown",     existsUnknown.longValue());
        m.put("create.ok",          createOk.longValue());
        m.put("create.race",        createRace.longValue());
        m.put("create.fail",        createFail.longValue());
        m.put("unknown.backoff.size", (long) unknownUntil.size());
        return java.util.Collections.unmodifiableMap(m);
    }

    /** Закрывает AdminClient через обёртку TopicAdmin с таймаутом adminTimeoutMs. Безопасен к повторным вызовам. */
    @Override public void close() {
        try {
            if (admin != null) {
                if (LOG.isDebugEnabled()) LOG.debug("Закрываю Kafka AdminClient");
                admin.close(Duration.ofMillis(adminTimeoutMs));
            }
        } catch (Exception ignore) { /* no-op */ }
    }

    /**
     * Краткое диагностическое представление состояния TopicEnsurer.
     * Содержит только метаданные конфигурации и размеры кешей/метрик; без тяжёлых операций.
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(128);
        sb.append("TopicEnsurer{")
          .append("partitions=").append(topicPartitions)
          .append(", replication=").append(topicReplication)
          .append(", adminTimeoutMs=").append(adminTimeoutMs)
          .append(", unknownBackoffMs=").append(unknownBackoffMs)
          .append(", ensured.size=").append(ensured.size())
          .append(", metrics={")
          .append("ensure=").append(ensureInvocations.longValue())
          .append(", hit=").append(ensureHitCache.longValue())
          .append(", existsT=").append(existsTrue.longValue())
          .append(", existsF=").append(existsFalse.longValue())
          .append(", existsU=").append(existsUnknown.longValue())
          .append(", createOk=").append(createOk.longValue())
          .append(", createRace=").append(createRace.longValue())
          .append(", createFail=").append(createFail.longValue())
          .append("}}");
        return sb.toString();
    }
}