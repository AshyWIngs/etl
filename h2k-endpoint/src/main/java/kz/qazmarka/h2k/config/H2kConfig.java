package kz.qazmarka.h2k.config;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;

/**
 * Иммутабельная конфигурация эндпоинта, прочитанная один раз из HBase {@link Configuration}.
 *
 * Содержит:
 *  - Базовые параметры Kafka/CF и ограничение длины имени топика
 *  - Флаги формирования payload (rowkey/meta) и JSON (serializeNulls)
 *  - Параметры ожидания подтверждений отправок (awaitEvery/awaitTimeoutMs)
 *  - Фильтр по timestamp клеток целевого CF
 *  - Параметры автосоздания топиков (партиции/репликация/таймаут/backoff), client.id для AdminClient и произвольные topic-level конфиги
 *
 * Все поля неизменяемые (иммутабельные).
 *
 * Примечание по rowkey: по умолчанию rowkey кодируется в HEX. Если в конфигурации задано
 * {@code h2k.rowkey.encoding=base64}, будет использована Base64. Для быстрого ветвления в горячем
 * пути предусмотрен предвычисленный флаг {@link #isRowkeyBase64()}.
 */
public final class H2kConfig {
    private static final Pattern TOPIC_SANITIZE = Pattern.compile("[^a-zA-Z0-9._-]");
    /** Дефолтный лимит длины имени топика Kafka (совместимый со старыми брокерами). */
    private static final int DEFAULT_TOPIC_MAX_LENGTH = 249;
    private static final String PLACEHOLDER_TABLE = "${table}";
    private static final String PLACEHOLDER_NAMESPACE = "${namespace}";
    private static final String PLACEHOLDER_QUALIFIER = "${qualifier}";
    private static final String ROWKEY_ENCODING_HEX = "hex";
    private static final String ROWKEY_ENCODING_BASE64 = "base64";

    // ==== Ключи конфигурации (собраны в одном месте для устранения "хардкода") ====
    private static final String K_TOPIC_PATTERN = "h2k.topic.pattern";
    private static final String K_TOPIC_MAX_LENGTH = "h2k.topic.max.length";
    private static final String K_CF = "h2k.cf";
    private static final String K_PAYLOAD_INCLUDE_ROWKEY = "h2k.payload.include.rowkey";
    private static final String K_ROWKEY_ENCODING = "h2k.rowkey.encoding";
    private static final String K_PAYLOAD_INCLUDE_META = "h2k.payload.include.meta";
    private static final String K_PAYLOAD_INCLUDE_META_WAL = "h2k.payload.include.meta.wal";
    private static final String K_JSON_SERIALIZE_NULLS = "h2k.json.serialize.nulls";
    private static final String K_FILTER_WAL_MIN_TS = "h2k.filter.wal.min.ts";
    private static final String K_TOPIC_ENSURE = "h2k.topic.ensure";
    private static final String K_TOPIC_PARTITIONS = "h2k.topic.partitions";
    private static final String K_TOPIC_REPLICATION_FACTOR = "h2k.topic.replication.factor";
    private static final String K_ADMIN_TIMEOUT_MS = "h2k.admin.timeout.ms";
    private static final String K_ADMIN_CLIENT_ID = "h2k.admin.client.id";
    private static final String K_TOPIC_UNKNOWN_BACKOFF_MS = "h2k.topic.ensure.unknown.backoff.ms";
    private static final String K_PRODUCER_AWAIT_EVERY = "h2k.producer.await.every";
    private static final String K_PRODUCER_AWAIT_TIMEOUT_MS = "h2k.producer.await.timeout.ms";
    private static final String K_TOPIC_CONFIG_PREFIX = "h2k.topic.config.";

    /**
     * Публичные ключи конфигурации h2k.* для использования в других пакетах проекта
     * (исключаем дубли строковых литералов). Значения синхронизированы с приватными K_* выше.
     */
    public static final class Keys {
        private Keys() {}
        public static final String BOOTSTRAP = "h2k.kafka.bootstrap.servers";
        public static final String JSON_SERIALIZE_NULLS = "h2k.json.serialize.nulls";
        public static final String DECODE_MODE = "h2k.decode.mode";
        public static final String SCHEMA_PATH = "h2k.schema.path";
        public static final String PRODUCER_PREFIX = "h2k.producer.";
        public static final String TOPIC_CONFIG_PREFIX = "h2k.topic.config.";
        public static final String PRODUCER_BATCH_COUNTERS_ENABLED = "h2k.producer.batch.counters.enabled";
        public static final String PRODUCER_BATCH_DEBUG_ON_FAILURE = "h2k.producer.batch.debug.on.failure";
    }

    // ==== Значения по умолчанию (в одном месте) ====
    private static final String DEFAULT_CF_NAME = "0";
    private static final String DEFAULT_ADMIN_CLIENT_ID = "h2k-admin";
    private static final boolean DEFAULT_INCLUDE_ROWKEY = true;
    private static final boolean DEFAULT_INCLUDE_META = false;
    private static final boolean DEFAULT_INCLUDE_META_WAL = false;
    private static final boolean DEFAULT_JSON_SERIALIZE_NULLS = false;
    private static final int DEFAULT_TOPIC_PARTITIONS = 3;
    private static final short DEFAULT_TOPIC_REPLICATION = 1;
    private static final long DEFAULT_ADMIN_TIMEOUT_MS = 60000L;
    private static final long DEFAULT_UNKNOWN_BACKOFF_MS = 15000L;
    private static final int DEFAULT_AWAIT_EVERY = 500;
    private static final int DEFAULT_AWAIT_TIMEOUT_MS = 180000;
    private static final String K_PRODUCER_BATCH_COUNTERS_ENABLED   = "h2k.producer.batch.counters.enabled";
    private static final String K_PRODUCER_BATCH_DEBUG_ON_FAILURE   = "h2k.producer.batch.debug.on.failure";

    private static final boolean DEFAULT_PRODUCER_BATCH_COUNTERS_ENABLED = false;
    private static final boolean DEFAULT_PRODUCER_BATCH_DEBUG_ON_FAILURE = false;

    // ==== Базовые ====
    private final String bootstrap;
    private final String topicPattern;
    private final int topicMaxLength;
    private final byte[] cfBytes;
    private final String cfNameForPayload; // кэш для includeMeta

    // ==== Payload/метаданные/rowkey ====
    private final boolean includeRowKey;
    /** Кодирование rowkey: "hex" | "base64" */
    private final String rowkeyEncoding;
    /** Предвычисленный флаг: true — rowkey сериализуется в Base64, false — в HEX. */
    private final boolean rowkeyBase64;
    private final boolean includeMeta;
    private final boolean includeMetaWal;
    private final boolean jsonSerializeNulls;

    // ==== Фильтр по ts клеток целевого CF ====
    private final boolean filterByWalTs;
    private final long walMinTs;

    // ==== Автосоздание топиков ====
    private final boolean ensureTopics;
    private final int topicPartitions;
    private final short topicReplication;
    private final long adminTimeoutMs;
    /** client.id для AdminClient (для фильтрации в логах брокеров). */
    private final String adminClientId;
    /** Backoff (мс) при неопределённом ответе от AdminClient (UNKNOWN/таймаут/сеть). */
    private final long unknownBackoffMs;
    /** Каждые N отправок ожидаем подтверждения (батчевое ожидание). */
    private final int awaitEvery;
    /** Таймаут ожидания подтверждений батча, мс. */
    private final int awaitTimeoutMs;
    /** Включать ли диагностические счётчики BatchSender по умолчанию. */
    private final boolean producerBatchCountersEnabled;
    /** Логировать ли подробные причины неуспеха авто-сброса в DEBUG. */
    private final boolean producerBatchDebugOnFailure;
    /** Произвольные конфиги топика, собранные из h2k.topic.config.* */
    private final Map<String, String> topicConfigs;

    /**
     * Приватный конструктор: вызывается только билдером для инициализации
     * всех final‑полей за один проход. Сохраняет иммутабельность и избегает
     * длинного конструктора с множеством параметров.
     */
    private H2kConfig(Builder b) {
        this.bootstrap = b.bootstrap;
        this.topicPattern = b.topicPattern;
        this.topicMaxLength = b.topicMaxLength;
        if (b.cfBytes != null) {
            byte[] tmp = new byte[b.cfBytes.length];
            System.arraycopy(b.cfBytes, 0, tmp, 0, b.cfBytes.length);
            this.cfBytes = tmp; // защитная копия для иммутабельности
        } else {
            this.cfBytes = null;
        }
        this.cfNameForPayload = b.cfNameForPayload;
        this.includeRowKey = b.includeRowKey;
        this.rowkeyEncoding = b.rowkeyEncoding;
        this.rowkeyBase64 = b.rowkeyBase64;
        this.includeMeta = b.includeMeta;
        this.includeMetaWal = b.includeMetaWal;
        this.jsonSerializeNulls = b.jsonSerializeNulls;
        this.filterByWalTs = b.filterByWalTs;
        this.walMinTs = b.walMinTs;
        this.ensureTopics = b.ensureTopics;
        this.topicPartitions = b.topicPartitions;
        this.topicReplication = b.topicReplication;
        this.adminTimeoutMs = b.adminTimeoutMs;
        this.adminClientId = b.adminClientId;
        this.unknownBackoffMs = b.unknownBackoffMs;
        this.awaitEvery = b.awaitEvery;
        this.awaitTimeoutMs = b.awaitTimeoutMs;
        this.producerBatchCountersEnabled = b.producerBatchCountersEnabled;
        this.producerBatchDebugOnFailure = b.producerBatchDebugOnFailure;
        this.topicConfigs = java.util.Collections.unmodifiableMap(new java.util.HashMap<>(b.topicConfigs));
    }

    /**
     * Билдер для пошаговой сборки иммутабельной конфигурации без громоздкого конструктора.
     * Удобнее читать, безопаснее изменять, удовлетворяет правилу Sonar S107 (ограничение числа параметров).
     * Все поля имеют разумные значения по умолчанию; сеттеры возвращают this для чейнинга.
     */
    public static final class Builder {
        private String bootstrap;
        private String topicPattern = PLACEHOLDER_TABLE;
        private int topicMaxLength = DEFAULT_TOPIC_MAX_LENGTH;
        private byte[] cfBytes;
        private String cfNameForPayload; // может быть null, если includeMeta=false

        private boolean includeRowKey = true;
        private String rowkeyEncoding = ROWKEY_ENCODING_HEX;
        private boolean rowkeyBase64 = false;
        private boolean includeMeta = false;
        private boolean includeMetaWal = false;
        private boolean jsonSerializeNulls = false;

        private boolean filterByWalTs = false;
        private long walMinTs = Long.MIN_VALUE;

        private boolean ensureTopics = false;
        private int topicPartitions = DEFAULT_TOPIC_PARTITIONS;
        private short topicReplication = DEFAULT_TOPIC_REPLICATION;
        private long adminTimeoutMs = DEFAULT_ADMIN_TIMEOUT_MS;
        private String adminClientId = DEFAULT_ADMIN_CLIENT_ID;
        private long unknownBackoffMs = DEFAULT_UNKNOWN_BACKOFF_MS;

        private int awaitEvery = DEFAULT_AWAIT_EVERY;
        private int awaitTimeoutMs = DEFAULT_AWAIT_TIMEOUT_MS;

        boolean producerBatchCountersEnabled;
        boolean producerBatchDebugOnFailure;

        private Map<String, String> topicConfigs = java.util.Collections.emptyMap();

        /**
         * Создаёт билдер с обязательным адресом Kafka bootstrap.servers.
         * @param bootstrap список Kafka‑узлов в формате host:port[,host2:port2]
         */
        public Builder(String bootstrap) {
            this.bootstrap = bootstrap;
        }

        /**
         * Устанавливает шаблон имени Kafka‑топика. Поддерживаются плейсхолдеры
         * ${table}, ${namespace}, ${qualifier}.
         * @param v шаблон, например "${namespace}.${qualifier}"
         * @return this
         */
        public Builder topicPattern(String v) { this.topicPattern = v; return this; }
        /**
         * Ограничение длины имени топика (символов). Старые брокеры требуют ≤ 249.
         * @param v максимальная длина
         * @return this
         */
        public Builder topicMaxLength(int v) { this.topicMaxLength = v; return this; }
        /**
         * Устанавливает имя CF в виде UTF‑8 байтов (для быстрого доступа при формировании payload).
         * @param v байтовое представление имени CF
         * @return this
         */
        public Builder cfBytes(byte[] v) { this.cfBytes = v; return this; }
        /**
         * Явно задаёт имя CF, которое попадёт в метаданные payload. Если не указано и includeMeta=false,
         * может остаться null.
         * @param v имя CF
         * @return this
         */
        public Builder cfNameForPayload(String v) { this.cfNameForPayload = v; return this; }

        /**
         * Включать ли rowkey в формируемый payload.
         * @param v true — включать; false — нет
         * @return this
         */
        public Builder includeRowKey(boolean v) { this.includeRowKey = v; return this; }
        /**
         * Способ кодирования rowkey: "hex" (по умолчанию) или "base64".
         * @param v "hex" | "base64"
         * @return this
         */
        public Builder rowkeyEncoding(String v) { this.rowkeyEncoding = v; return this; }
        /**
         * Предвычисленный флаг: true — rowkey будет сериализован в Base64 (для горячего пути).
         * Обычно вычисляется автоматически на основе rowkeyEncoding.
         * @param v true для Base64, false для HEX
         * @return this
         */
        public Builder rowkeyBase64(boolean v) { this.rowkeyBase64 = v; return this; }
        /**
         * Добавлять ли метаданные ячеек (семейство столбцов/квалайфер/ts) в payload.
         * @param v флаг включения метаданных
         * @return this
         */
        public Builder includeMeta(boolean v) { this.includeMeta = v; return this; }
        /**
         * Включать ли в метаданные отметку о происхождении из WAL (write‑ahead log).
         * @param v флаг включения признака WAL
         * @return this
         */
        public Builder includeMetaWal(boolean v) { this.includeMetaWal = v; return this; }
        /**
         * Сериализовать ли null‑значения в JSON (иначе поля с null опускаются).
         * @param v флаг сериализации null
         * @return this
         */
        public Builder jsonSerializeNulls(boolean v) { this.jsonSerializeNulls = v; return this; }

        /**
         * Включает фильтрацию по минимальному timestamp клеток из WAL.
         * @param v true — включить фильтр
         * @return this
         */
        public Builder filterByWalTs(boolean v) { this.filterByWalTs = v; return this; }
        /**
         * Минимальный timestamp (epoch millis) клеток из WAL для включения в поток.
         * @param v минимальное значение ts
         * @return this
         */
        public Builder walMinTs(long v) { this.walMinTs = v; return this; }

        /**
         * Автоматически создавать недостающие топики при старте.
         * @param v true — создавать при необходимости
         * @return this
         */
        public Builder ensureTopics(boolean v) { this.ensureTopics = v; return this; }
        /**
         * Число партиций создаваемого топика (если ensureTopics=true).
         * @param v количество партиций (≥1)
         * @return this
         */
        public Builder topicPartitions(int v) { this.topicPartitions = v; return this; }
        /**
         * Фактор репликации создаваемого топика.
         * @param v фактор репликации (≥1)
         * @return this
         */
        public Builder topicReplication(short v) { this.topicReplication = v; return this; }
        /**
         * Таймаут операций AdminClient при ensureTopics, мс.
         * @param v таймаут в миллисекундах
         * @return this
         */
        public Builder adminTimeoutMs(long v) { this.adminTimeoutMs = v; return this; }
        /**
         * Значение client.id для AdminClient (удобно для фильтрации логов брокера).
         * @param v идентификатор клиента
         * @return this
         */
        public Builder adminClientId(String v) { this.adminClientId = v; return this; }
        /**
         * Backoff (мс) между повторами при неопределённом результате (UNKNOWN/timeout/сетевые ошибки).
         * @param v пауза между повторами в миллисекундах
         * @return this
         */
        public Builder unknownBackoffMs(long v) { this.unknownBackoffMs = v; return this; }

        /**
         * Каждые N отправок ждать подтверждения (батчевое ожидание) для ограничения памяти/pressure.
         * @param v размер батча N (≥1)
         * @return this
         */
        public Builder awaitEvery(int v) { this.awaitEvery = v; return this; }
        /**
         * Таймаут ожидания подтверждений батча, мс.
         * @param v таймаут в миллисекундах (≥1)
         * @return this
         */
        public Builder awaitTimeoutMs(int v) { this.awaitTimeoutMs = v; return this; }

        /**
         * Включить диагностические счётчики BatchSender по умолчанию.
         * @param v true — включить счётчики
         * @return this
         */
        public Builder producerBatchCountersEnabled(boolean v) { this.producerBatchCountersEnabled = v; return this; }
        /**
         * Включить DEBUG‑подробности ошибок авто‑сброса.
         * @param v true — включить подробный DEBUG
         * @return this
         */
        public Builder producerBatchDebugOnFailure(boolean v) { this.producerBatchDebugOnFailure = v; return this; }

        /**
         * Произвольные конфиги топика из префикса h2k.topic.config.* (см. {@link #readTopicConfigs}).
         * @param v карта ключ‑значение конфигураций топика
         * @return this
         */
        public Builder topicConfigs(Map<String, String> v) { this.topicConfigs = v; return this; }

        /**
         * Собирает неизменяемый объект конфигурации с текущими значениями билдера.
         * @return готовый {@link H2kConfig}
         */
        public H2kConfig build() { return new H2kConfig(this); }
    }

    /**
     * Строит {@link H2kConfig} из HBase {@link Configuration} и явного списка bootstrap‑серверов Kafka.
     * Проверяет обязательные параметры, подставляет значения по умолчанию, предвычисляет быстрые флаги.
     * @param cfg HBase‑конфигурация с параметрами вида h2k.*
     * @param bootstrap значение для kafka bootstrap.servers (host:port[,host2:port2]) — обязательно
     * @return полностью инициализированная иммутабельная конфигурация
     * @throws IllegalArgumentException если bootstrap пустой или не указан
     */
    public static H2kConfig from(Configuration cfg, String bootstrap) {
        if (bootstrap == null || bootstrap.trim().isEmpty()) {
            throw new IllegalArgumentException("Отсутствует обязательный параметр bootstrap.servers: h2k.kafka.bootstrap.servers пустой или не задан");
        }
        bootstrap = bootstrap.trim();

        String topicPattern = readTopicPattern(cfg);
        int topicMaxLength = readIntMin(cfg, K_TOPIC_MAX_LENGTH, DEFAULT_TOPIC_MAX_LENGTH, 1);
        String cfName = readCfName(cfg);
        byte[] cfBytes = cfName.getBytes(StandardCharsets.UTF_8);

        boolean includeRowKey = cfg.getBoolean(K_PAYLOAD_INCLUDE_ROWKEY, DEFAULT_INCLUDE_ROWKEY);
        String rowkeyEncoding = normalizeRowkeyEncoding(cfg.get(K_ROWKEY_ENCODING, ROWKEY_ENCODING_HEX));
        final boolean rowkeyBase64 = ROWKEY_ENCODING_BASE64.equals(rowkeyEncoding);
        boolean includeMeta = cfg.getBoolean(K_PAYLOAD_INCLUDE_META, DEFAULT_INCLUDE_META);
        boolean includeMetaWal = cfg.getBoolean(K_PAYLOAD_INCLUDE_META_WAL, DEFAULT_INCLUDE_META_WAL);
        boolean jsonSerializeNulls = cfg.getBoolean(K_JSON_SERIALIZE_NULLS, DEFAULT_JSON_SERIALIZE_NULLS);
        String cfNameForPayload = includeMeta ? cfName : null;

        WalFilter wf = readWalFilter(cfg);
        boolean filterByWalTs = wf.enabled;
        long walMinTs = wf.minTs;

        boolean ensureTopics = cfg.getBoolean(K_TOPIC_ENSURE, false);
        int topicPartitions = readIntMin(cfg, K_TOPIC_PARTITIONS, DEFAULT_TOPIC_PARTITIONS, 1);
        short topicReplication = (short) readIntMin(cfg, K_TOPIC_REPLICATION_FACTOR, DEFAULT_TOPIC_REPLICATION, 1);
        long adminTimeoutMs = readLong(cfg, K_ADMIN_TIMEOUT_MS, DEFAULT_ADMIN_TIMEOUT_MS);
        String adminClientId = buildAdminClientId(cfg);
        long unknownBackoffMs = readLong(cfg, K_TOPIC_UNKNOWN_BACKOFF_MS, DEFAULT_UNKNOWN_BACKOFF_MS);

        int awaitEvery = readIntMin(cfg, K_PRODUCER_AWAIT_EVERY, DEFAULT_AWAIT_EVERY, 1);
        int awaitTimeoutMs = readIntMin(cfg, K_PRODUCER_AWAIT_TIMEOUT_MS, DEFAULT_AWAIT_TIMEOUT_MS, 1);
        boolean producerBatchCountersEnabled = cfg.getBoolean(K_PRODUCER_BATCH_COUNTERS_ENABLED, DEFAULT_PRODUCER_BATCH_COUNTERS_ENABLED);
        boolean producerBatchDebugOnFailure = cfg.getBoolean(K_PRODUCER_BATCH_DEBUG_ON_FAILURE, DEFAULT_PRODUCER_BATCH_DEBUG_ON_FAILURE);

        Map<String, String> topicConfigs = readTopicConfigs(cfg);

        return new Builder(bootstrap)
                .topicPattern(topicPattern)
                .topicMaxLength(topicMaxLength)
                .cfBytes(cfBytes)
                .cfNameForPayload(cfNameForPayload)
                .includeRowKey(includeRowKey)
                .rowkeyEncoding(rowkeyEncoding)
                .rowkeyBase64(rowkeyBase64)
                .includeMeta(includeMeta)
                .includeMetaWal(includeMetaWal)
                .jsonSerializeNulls(jsonSerializeNulls)
                .filterByWalTs(filterByWalTs)
                .walMinTs(walMinTs)
                .ensureTopics(ensureTopics)
                .topicPartitions(topicPartitions)
                .topicReplication(topicReplication)
                .adminTimeoutMs(adminTimeoutMs)
                .adminClientId(adminClientId)
                .unknownBackoffMs(unknownBackoffMs)
                .awaitEvery(awaitEvery)
                .awaitTimeoutMs(awaitTimeoutMs)
                .producerBatchCountersEnabled(producerBatchCountersEnabled)
                .producerBatchDebugOnFailure(producerBatchDebugOnFailure)
                .topicConfigs(topicConfigs)
                .build();
    }
    /**
     * Считывает int из конфигурации с дефолтом и минимальным порогом (minVal).
     * Используется для параметров, требующих значения \u2265 1.
     */
    private static int readIntMin(Configuration cfg, String key, int defVal, int minVal) {
        int v = cfg.getInt(key, defVal);
        return (v < minVal) ? minVal : v;
    }

    /** Прочитать h2k.topic.config.* → Map&lt;конфиг, значение&gt;. */
    private static Map<String, String> readTopicConfigs(Configuration cfg) {
        Map<String, String> out = new HashMap<>();
        final String prefix = K_TOPIC_CONFIG_PREFIX;
        for (Map.Entry<String, String> e : cfg) {
            String k = e.getKey();
            if (k.startsWith(prefix)) {
                String real = k.substring(prefix.length()).trim();
                if (!real.isEmpty()) {
                    String v = e.getValue();
                    if (v != null) {
                        v = v.trim();
                        if (!v.isEmpty()) {
                            out.put(real, v);
                        }
                    }
                }
            }
        }
        return out;
    }

    /**
     * Читает и нормализует шаблон имени топика из конфигурации (с обрезкой пробелов).
     */
    private static String readTopicPattern(Configuration cfg) {
        String topicPattern = cfg.get(K_TOPIC_PATTERN, PLACEHOLDER_TABLE);
        return topicPattern == null ? PLACEHOLDER_TABLE : topicPattern.trim();
    }

    /**
     * Читает и нормализует имя семейства столбцов (CF); по умолчанию "0".
     */
    private static String readCfName(Configuration cfg) {
        String cfName = cfg.get(K_CF, DEFAULT_CF_NAME);
        return cfName == null ? DEFAULT_CF_NAME : cfName.trim();
    }

    /**
     * Нормализует способ кодирования rowkey до двух допустимых значений: "hex" или "base64".
     * По умолчанию используется "hex".
     */
    private static String normalizeRowkeyEncoding(String val) {
        if (val == null) return ROWKEY_ENCODING_HEX;
        String v = val.trim().toLowerCase(Locale.ROOT);
        return ROWKEY_ENCODING_BASE64.equals(v) ? ROWKEY_ENCODING_BASE64 : ROWKEY_ENCODING_HEX;
    }

    /**
     * Небольшой объект‑контейнер для параметров фильтра по timestamp из WAL.
     */
    private static final class WalFilter {
        final boolean enabled;
        final long minTs;
        WalFilter(boolean enabled, long minTs) { this.enabled = enabled; this.minTs = minTs; }
    }

    /**
     * Парсит параметры фильтра WAL: флаг включения и минимальный timestamp.
     */
    private static WalFilter readWalFilter(Configuration cfg) {
        String walMinStr = cfg.get(K_FILTER_WAL_MIN_TS);
        if (walMinStr == null) return new WalFilter(false, Long.MIN_VALUE);
        try {
            return new WalFilter(true, Long.parseLong(walMinStr.trim()));
        } catch (NumberFormatException nfe) {
            return new WalFilter(false, Long.MIN_VALUE);
        }
    }

    /**
     * Считывает long из конфигурации с мягкой деградацией к значению по умолчанию
     * при пустых или некорректных данных.
     */
    private static long readLong(Configuration cfg, String key, long defVal) {
        String v = cfg.get(key);
        if (v == null) return defVal;
        try {
            return Long.parseLong(v.trim());
        } catch (NumberFormatException nfe) {
            return defVal;
        }
    }

    /**
     * Формирует значение client.id для AdminClient. Если явно не задано — пытается
     * использовать имя хоста; при ошибке возвращает константу "h2k-admin".
     */
    private static String buildAdminClientId(Configuration cfg) {
        String adminClientId = cfg.get(K_ADMIN_CLIENT_ID);
        if (adminClientId != null) {
            adminClientId = adminClientId.trim();
            if (!adminClientId.isEmpty()) {
                return adminClientId;
            }
        }
        try {
            return DEFAULT_ADMIN_CLIENT_ID + "-" + java.net.InetAddress.getLocalHost().getHostName();
        } catch (java.net.UnknownHostException e) {
            return DEFAULT_ADMIN_CLIENT_ID;
        }
    }

    /**
     * Формирует имя Kafka‑топика по заданному шаблону {@link #topicPattern} с подстановкой
     * плейсхолдеров и санитизацией по правилам Kafka (замена недопустимых символов на "_",
     * обрезка до {@link #topicMaxLength}).
     * @param table таблица HBase (источник namespace и qualifier)
     * @return корректное имя Kafka‑топика
     */
    public String topicFor(TableName table) {
        String ns = table.getNamespaceAsString();
        String qn = table.getQualifierAsString();
        String base = topicPattern
                .replace(PLACEHOLDER_TABLE, ns + "_" + qn)
                .replace(PLACEHOLDER_NAMESPACE, ns)
                .replace(PLACEHOLDER_QUALIFIER, qn);
        String sanitized = TOPIC_SANITIZE.matcher(base).replaceAll("_");
        if (sanitized.length() > topicMaxLength) {
            sanitized = sanitized.substring(0, topicMaxLength);
        }
        return sanitized;
    }

    /**
     * Имя CF для метаданных payload.
     * Гарантированно non-null:
     *  - если явно задано — вернуть его;
     *  - иначе, если есть байты CF — вернуть их UTF-8 представление;
     *  - иначе — "0" (дефолт).
     */
    public String getCfNameForPayload() {
        if (this.cfNameForPayload != null) {
            return this.cfNameForPayload;
        }
        if (this.cfBytes != null) {
            return new String(this.cfBytes, StandardCharsets.UTF_8);
        }
        return DEFAULT_CF_NAME;
    }

    // ===== Итоговые геттеры (без дублирования и алиасов) =====
    /** @return список Kafka bootstrap.servers */
    public String getBootstrap() { return bootstrap; }
    /** @return шаблон имени Kafka‑топика с плейсхолдерами */
    public String getTopicPattern() { return topicPattern; }
    /** @return максимальная допустимая длина имени топика */
    public int getTopicMaxLength() { return topicMaxLength; }
    /** @return имя CF в виде UTF‑8 байтов (для быстрого доступа); массив только для чтения, не модифицировать */
    public byte[] getCfBytes() { return cfBytes; }

    /** @return флаг включения rowkey в payload */
    public boolean isIncludeRowKey() { return includeRowKey; }
    /** @return способ кодирования rowkey: "hex" или "base64" */
    public String getRowkeyEncoding() { return rowkeyEncoding; }
    /** @return true, если rowkey сериализуется в Base64; иначе false (HEX) */
    public boolean isRowkeyBase64() { return rowkeyBase64; }

    /** @return флаг включения метаданных ячеек в payload */
    public boolean isIncludeMeta() { return includeMeta; }
    /** @return флаг включения признака происхождения из WAL */
    public boolean isIncludeMetaWal() { return includeMetaWal; }
    /** @return сериализуются ли null‑значения в JSON */
    public boolean isJsonSerializeNulls() { return jsonSerializeNulls; }

    /** @return включена ли фильтрация по минимальному timestamp из WAL */
    public boolean isFilterByWalTs() { return filterByWalTs; }
    /** @return минимальный timestamp (epoch millis) для фильтра WAL */
    public long getWalMinTs() { return walMinTs; }

    /** @return создавать ли недостающие топики автоматически */
    public boolean isEnsureTopics() { return ensureTopics; }
    /** @return число партиций создаваемого топика */
    public int getTopicPartitions() { return topicPartitions; }
    /** @return фактор репликации создаваемого топика */
    public short getTopicReplication() { return topicReplication; }

    /** @return таймаут операций AdminClient при ensureTopics, мс */
    public long getAdminTimeoutMs() { return adminTimeoutMs; }
    /** @return значение client.id для AdminClient */
    public String getAdminClientId() { return adminClientId; }
    /** @return пауза между повторами при неопределённых ошибках AdminClient, мс */
    public long getUnknownBackoffMs() { return unknownBackoffMs; }

    /** @return размер батча отправок, после которого ожидаются подтверждения */
    public int getAwaitEvery() { return awaitEvery; }
    /** @return таймаут ожидания подтверждений батча, мс */
    public int getAwaitTimeoutMs() { return awaitTimeoutMs; }
    /** @return включены ли счётчики BatchSender по умолчанию */
    public boolean isProducerBatchCountersEnabled() { return producerBatchCountersEnabled; }
    /** @return включён ли DEBUG‑лог подробностей ошибок авто‑сброса */
    public boolean isProducerBatchDebugOnFailure() { return producerBatchDebugOnFailure; }

    /** @return карта дополнительных конфигураций топика (h2k.topic.config.*) */
    public Map<String, String> getTopicConfigs() { return topicConfigs; }
}