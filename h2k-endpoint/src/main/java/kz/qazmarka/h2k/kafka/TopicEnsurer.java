package kz.qazmarka.h2k.kafka;

import java.security.SecureRandom;
import java.time.Duration;
import java.util.Collections;
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
 * Проверка/создание Kafka-топиков (+применение per-topic конфигов из H2kConfig).
 * Если ensureTopics=false — используем No-Op (admin=null).
 * Кэширует только УСПЕШНО проверенные/созданные топики (ensured), чтобы не повторять вызовы.
 * Валидирует имя топика (допустимые символы, длина, "."/"..").
 * Важно: класс только проверяет существование и при необходимости СОЗДАЁТ тему.
 * Ничего не «правит» у уже существующих тем (число партиций, фактор репликации, конфиги) — это осознанно.
 * Конфиги из H2kConfig применяются ТОЛЬКО при создании новой темы.
 * Поведение идемпотентно, учитывает гонки (TopicExistsException) и сетевые «неуверенные» ошибки через короткий backoff.
 * AdminClient получает client.id из H2kConfig; REQUEST_TIMEOUT_MS синхронизирован с adminTimeoutMs.
 * Лог‑уровни: подробные сообщения (повторные проверки, backoff, гоночные ситуации) пишутся на DEBUG; на INFO остаются только редкие события создания темы и предупреждения/ошибки.
 */
public final class TopicEnsurer implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(TopicEnsurer.class);
    /** Максимальная длина имени топика по Kafka. */
    private static final int TOPIC_NAME_MAX_LEN = 249;

    // Ключи часто используемых конфигов топика (для сводки в логах)
    private static final String CFG_RETENTION_MS       = "retention.ms";
    private static final String CFG_CLEANUP_POLICY     = "cleanup.policy";
    private static final String CFG_COMPRESSION_TYPE   = "compression.type";
    private static final String CFG_MIN_INSYNC_REPLICAS= "min.insync.replicas";

    // Сообщение о некорректном имени топика (чтобы не дублировать литерал)
    private static final String WARN_INVALID_TOPIC =
            "Некорректное имя Kafka-топика '{}': допускаются [a-zA-Z0-9._-], длина 1..{}, запрещены '.' и '..'";
    private final AdminClient admin;
    private final long adminTimeoutMs;
    private final int topicPartitions;
    private final short topicReplication;
    private final Set<String> ensured = ConcurrentHashMap.newKeySet();
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

    /** Backoff для повторной проверки/создания топика после неуверенной ошибки (таймаут/сеть/ACL). */
    private final long unknownBackoffMs;

    /** Предвычисленный backoff в наносекундах (для быстрых расчётов). */
    private final long unknownBackoffNs;

    /** Таймстемпы (nano) до которых мы не трогаем топик из-за предыдущего UNKNOWN-состояния. */
    private final ConcurrentHashMap<String, Long> unknownUntil = new ConcurrentHashMap<>();

    /** Трёхзначный результат проверки существования топика. */
    private enum TopicExistence { TRUE, FALSE, UNKNOWN }

    /**
     * Криптографически стойкий генератор для редкого джиттера backoff.
     * Один общий экземпляр на процесс: потокобезопасен и не создаёт лишних аллокаций.
     */
    private static final SecureRandom SR = new SecureRandom();

    /** Пометить тему как успешно проверенную/созданную и снять возможный backoff. */
    private void markEnsured(String t) {
        ensured.add(t);
        unknownUntil.remove(t);
    }

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
        AdminClient admin = AdminClient.create(ap);
        return new TopicEnsurer(
            admin,
            cfg.getAdminTimeoutMs(),
            cfg.getTopicPartitions(),
            cfg.getTopicReplication(),
            cfg.getTopicConfigs(),
            cfg.getUnknownBackoffMs()
        );
    }

    private TopicEnsurer(AdminClient admin, long adminTimeoutMs, int topicPartitions, short topicReplication,
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
    }

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

    /** Быстрый путь: если тема уже успешно проверялась/создавалась раньше. */
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

    /** Учитываем активный backoff после неуверенной ошибки. Возвращает true, если надо пропустить попытку сейчас. */
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

    /** Обработка случая: тема существует. */
    private void onExistsTrue(String t) {
        existsTrue.increment();
        markEnsured(t);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Kafka-топик '{}' уже существует — создание не требуется", t);
        }
    }

    /** Обработка случая: неизвестно (таймаут/ACL/сеть). */
    private void onExistsUnknown(String t) {
        existsUnknown.increment();
        scheduleUnknown(t);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Не удалось надёжно определить существование Kafka-топика '{}'; повторю попытку после ~{} мс",
                    t, unknownBackoffMs);
        }
    }

    /** Попытка создать тему с обработкой гонок/таймаута/исключений. */
    private void tryCreateTopic(String t) {
        try {
            createTopic(t);
            createOk.increment();
            markEnsured(t);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            createFail.increment();
            LOG.warn("Создание Kafka-топика '{}' было прервано", t, ie);
        } catch (java.util.concurrent.ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof org.apache.kafka.common.errors.TopicExistsException) {
                createRace.increment();
                markEnsured(t);
                if (LOG.isDebugEnabled()) LOG.debug("Kafka-топик '{}' уже существует (создан параллельно)", t);
            } else {
                createFail.increment();
                LOG.warn("Не удалось создать Kafka-топик '{}': ошибка выполнения", t, e);
            }
        } catch (java.util.concurrent.TimeoutException te) {
            createFail.increment();
            LOG.warn("Не удалось создать Kafka-топик '{}': таймаут {} мс", t, adminTimeoutMs, te);
        } catch (RuntimeException re) {
            createFail.increment();
            LOG.warn("Не удалось создать Kafka-топик '{}' (runtime)", t, re);
        }
    }

    /**
     * Ensure + вернуть статус существования темы.
     * Возвращает {@code true} только если тема точно существует (была ранее, создана сейчас,
     * либо подтверждена describeTopics). Возвращает {@code false}, если:
     *  - ensure выключен (admin == null),
     *  - имя пустое/некорректное,
     *  - текущее состояние не удалось определить (назначен короткий backoff),
     *  - произошла ошибка проверки/создания.
     * Метод не бросает исключений.
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
     * Пакетная проверка/создание нескольких топиков с минимальным числом сетевых вызовов.
     * Для уже проверенных ранее тем используется кеш (ensured), для неуверенных ошибок — короткий backoff.
     * Ничего не делает, если ensureTopics=false или список пуст.
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
     * Нормализует вход: trim, валидация имени, учёт кеша ensured и активного backoff.
     * Возвращает набор кандидатов для проверки у брокера одним вызовом.
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
     * Одним вызовом describeTopics определяет какие темы существуют, а какие отсутствуют.
     * Существующие попадают в кеш ensured, отсутствующие возвращаются в списке missing.
     */
    private java.util.ArrayList<String> describeAndCollectMissing(java.util.Set<String> toCheck) {
        java.util.ArrayList<String> missing = new java.util.ArrayList<>(toCheck.size());
        org.apache.kafka.clients.admin.DescribeTopicsResult dtr = admin.describeTopics(toCheck);
        java.util.Map<String, org.apache.kafka.common.KafkaFuture<org.apache.kafka.clients.admin.TopicDescription>> fmap = dtr.values();
        for (String t : toCheck) {
            classifyDescribeTopic(fmap, t, missing);
        }
        return missing;
    }

    /**
     * Обрабатывает результат describeTopics для одной темы: обновляет кеш/метрики или добавляет в missing.
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
            LOG.warn("Проверка Kafka-топика '{}' превысила таймаут {} мс (batch)", t, adminTimeoutMs, te);
        } catch (java.util.concurrent.ExecutionException ee) {
            Throwable cause = ee.getCause();
            if (cause instanceof UnknownTopicOrPartitionException) {
                existsFalse.increment();
                missing.add(t);
            } else {
                existsUnknown.increment();
                scheduleUnknown(t);
                LOG.warn("Ошибка при проверке Kafka-топика '{}' (batch)", t, ee);
            }
        }
    }

    /**
     * Создаёт отсутствующие темы одним батчем createTopics и обрабатывает гонки/таймауты.
     */
    private void createMissingTopics(java.util.List<String> missing) {
        java.util.List<NewTopic> newTopics = new java.util.ArrayList<>(missing.size());
        for (String t : missing) {
            NewTopic nt = new NewTopic(t, topicPartitions, topicReplication);
            if (!topicConfigs.isEmpty()) nt.configs(topicConfigs);
            newTopics.add(nt);
        }
        org.apache.kafka.clients.admin.CreateTopicsResult ctr = admin.createTopics(newTopics);
        java.util.Map<String, org.apache.kafka.common.KafkaFuture<Void>> cvals = ctr.values();
        for (String t : missing) {
            processCreateResult(cvals, t);
        }
    }

    /**
     * Завершает создание одной темы: учитывает гонки/таймауты, обновляет кеш и метрики.
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


    /** Возвращает случайное long в полуинтервале [originInclusive, boundExclusive) на базе SecureRandom без смещения. */
    private static long nextLongBetweenSecure(long originInclusive, long boundExclusive) {
        long n = boundExclusive - originInclusive;
        if (n <= 0) return originInclusive; // защита от некорректных диапазонов
        long bits;
        long val;
        do {
            bits = SR.nextLong() >>> 1;       // неотрицательное
            val  = bits % n;                  // минимизируем bias через rejection sampling
        } while (bits - val + (n - 1) < 0L);  // как в Random#nextInt
        return originInclusive + val;
    }

    /**
     * Jitter для backoff теперь берётся из SecureRandom, так что предупреждение S2245 неактуально.
     * Это по‑прежнему не security‑контекст, но SecureRandom здесь не на горячем пути и не влияет на TPS.
     */
    private void scheduleUnknown(String topic) {
        long base = unknownBackoffNs;
        long jitter = Math.max(1L, base / 5); // ≈20% от базового
        long delta = nextLongBetweenSecure(-jitter, jitter + 1); // [-jitter, +jitter]
        long delay = base + delta;
        long min = TimeUnit.MILLISECONDS.toNanos(1);
        if (delay < min) delay = min;
        unknownUntil.put(topic, System.nanoTime() + delay);
    }

    private TopicExistence topicExists(String topic) {
        try {
            admin.describeTopics(Collections.singleton(topic))
                .all()
                .get(adminTimeoutMs, TimeUnit.MILLISECONDS);
            return TopicExistence.TRUE;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOG.warn("Проверка Kafka-топика '{}' прервана", topic, ie);
            scheduleUnknown(topic);
            return TopicExistence.UNKNOWN;
        } catch (java.util.concurrent.TimeoutException te) {
            LOG.warn("Проверка Kafka-топика '{}' превысила таймаут {} мс", topic, adminTimeoutMs, te);
            scheduleUnknown(topic);
            return TopicExistence.UNKNOWN;
        } catch (java.util.concurrent.ExecutionException ee) {
            Throwable cause = ee.getCause();
            if (cause instanceof UnknownTopicOrPartitionException) {
                existsFalse.increment();
                return TopicExistence.FALSE;
            }
            LOG.warn("Ошибка при проверке Kafka-топика '{}'", topic, ee);
            scheduleUnknown(topic);
            return TopicExistence.UNKNOWN;
        } catch (RuntimeException re) {
            LOG.warn("Не удалось проверить Kafka-топик '{}' (runtime)", topic, re);
            scheduleUnknown(topic);
            return TopicExistence.UNKNOWN;
        }
    }

    /** Проверка имени Kafka-топика по основным правилам брокера (без RegEx для минимальных аллокаций). */
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

    /** Разрешённые символы имён топиков: a-z, A-Z, 0-9, '.', '_', '-'. */
    private static boolean isAllowedTopicChar(char c) {
        if (c >= 'a' && c <= 'z') return true;
        if (c >= 'A' && c <= 'Z') return true;
        if (c >= '0' && c <= '9') return true;
        return c == '.' || c == '_' || c == '-';
    }

    /** Короткая человекочитаемая сводка ключевых конфигов темы. */
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

    /** Считает количество ненулевых значений среди переданных. */
    private static int countNonNull(Object... vals) {
        int n = 0;
        if (vals != null) {
            for (Object v : vals) if (v != null) n++;
        }
        return n;
    }

    /** Добавляет пару key=value в summary, учитывая разделитель и флаг first. Возвращает новый флаг first. */
    private static boolean appendConfig(StringBuilder sb, String key, String value, boolean first) {
        if (value == null) return first;
        if (!first) sb.append(", ");
        sb.append(key).append("=").append(value);
        return false;
    }

    private void createTopic(String topic)
            throws InterruptedException, java.util.concurrent.ExecutionException, java.util.concurrent.TimeoutException {
        NewTopic nt = new NewTopic(topic, topicPartitions, topicReplication);
        if (!topicConfigs.isEmpty()) {
            nt.configs(topicConfigs);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Применяю конфиги для Kafka-топика '{}': {}", topic, topicConfigs);
            }
        }
        admin.createTopics(Collections.singleton(nt))
                .all()
                .get(adminTimeoutMs, TimeUnit.MILLISECONDS);
        LOG.info("Создал Kafka-топик '{}': partitions={}, replication={}", topic, topicPartitions, topicReplication);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Конфиги Kafka-топика '{}': {}", topic, summarizeTopicConfigs());
        }
    }

    /**
     * Снимок счётчиков (для диагностики/метрик).
     * Возвращает немодифицируемую Map<metricName, value>.
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