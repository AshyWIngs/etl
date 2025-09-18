package kz.qazmarka.h2k.config;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kz.qazmarka.h2k.util.Parsers;

/**
 * Иммутабельная конфигурация эндпоинта, прочитанная один раз из HBase {@link Configuration}.
 *
 * Содержит:
 *  - Базовые параметры Kafka/CF и ограничение длины имени топика
 *  - Флаги формирования payload (rowkey/meta) и JSON (serializeNulls)
 *  - Параметры ожидания подтверждений отправок (awaitEvery/awaitTimeoutMs)
 *  - Параметры автосоздания топиков (партиции/репликация/таймаут/backoff), client.id для AdminClient и произвольные topic-level конфиги
 *  - Табличные переопределения «соли» rowkey: параметр {@code h2k.salt.map} (TABLE[:BYTES])
 *
 * Все поля неизменяемые (иммутабельные).
 *
 * Примечание по rowkey: по умолчанию rowkey кодируется в HEX. Если в конфигурации задано
 * {@code h2k.rowkey.encoding=base64}, будет использована Base64. Для быстрого ветвления в горячем
 * пути предусмотрен предвычисленный флаг {@link #isRowkeyBase64()}.
 */
public final class H2kConfig {
    private static final Logger LOG = LoggerFactory.getLogger(H2kConfig.class);
    /**
     * Дефолтный лимит длины имени Kafka‑топика (символов).
     * Значение 249 совместимо со старыми версиями брокеров Kafka.
     */
    private static final int DEFAULT_TOPIC_MAX_LENGTH = 249;
    /** Плейсхолдер в шаблоне топика: будет заменён на "<namespace>_<qualifier>". */
    private static final String PLACEHOLDER_TABLE = "${table}";
    /** Плейсхолдер в шаблоне топика: будет заменён на имя namespace таблицы. */
    private static final String PLACEHOLDER_NAMESPACE = "${namespace}";
    /** Плейсхолдер в шаблоне топика: будет заменён на qualifier (имя таблицы без namespace). */
    private static final String PLACEHOLDER_QUALIFIER = "${qualifier}";
    /** Строковое значение способа кодирования rowkey по умолчанию — HEX. */
    private static final String ROWKEY_ENCODING_HEX = "hex";
    /** Строковое значение способа кодирования rowkey — Base64. */
    private static final String ROWKEY_ENCODING_BASE64 = "base64";
    /** Имя namespace HBase по умолчанию. */
    private static final String HBASE_DEFAULT_NS = "default";

    // ==== Ключи конфигурации (собраны в одном месте для устранения "хардкода") ====
    /**
     * Шаблон имени Kafka‑топика (поддерживаются плейсхолдеры ${table}, ${namespace}, ${qualifier}).
     * Используется в {@link #topicFor(TableName)}.
     */
    private static final String K_TOPIC_PATTERN = "h2k.topic.pattern";
    /** Максимально допустимая длина имени Kafka‑топика. */
    private static final String K_TOPIC_MAX_LENGTH = "h2k.topic.max.length";
    /** CSV‑список имён CF, подлежащих экспорту. */
    private static final String K_CF_LIST = "h2k.cf.list";
    /** Флаг включения rowkey в JSON‑payload. */
    private static final String K_PAYLOAD_INCLUDE_ROWKEY = "h2k.payload.include.rowkey";
    /** Способ кодирования rowkey: "hex" (по умолчанию) или "base64". */
    private static final String K_ROWKEY_ENCODING = "h2k.rowkey.encoding";
    /** Флаг добавления метаданных ячеек (cf/qualifier/ts) в payload. */
    private static final String K_PAYLOAD_INCLUDE_META = "h2k.payload.include.meta";
    /** Флаг добавления признака происхождения записи из WAL. */
    private static final String K_PAYLOAD_INCLUDE_META_WAL = "h2k.payload.include.meta.wal";
    /** Флаг автосоздания недостающих топиков. */
    private static final String K_ENSURE_TOPICS = "h2k.ensure.topics";
    /** Разрешать ли автоматическое увеличение числа партиций при ensureTopics. По умолчанию выключено. */
    private static final String K_ENSURE_INCREASE_PARTITIONS = "h2k.ensure.increase.partitions";
    /** Разрешать ли дифф‑применение topic‑конфигов (incrementalAlterConfigs) при ensureTopics. По умолчанию выключено. */
    private static final String K_ENSURE_DIFF_CONFIGS = "h2k.ensure.diff.configs";
    /** Целевое число партиций создаваемого топика. */
    private static final String K_TOPIC_PARTITIONS = "h2k.topic.partitions";
    /** Целевой фактор репликации создаваемого топика. */
    private static final String K_TOPIC_REPLICATION_FACTOR = "h2k.topic.replication";
    /** Таймаут операций Kafka AdminClient (мс) при ensureTopics. */
    private static final String K_ADMIN_TIMEOUT_MS = "h2k.admin.timeout.ms";
    /** Явное значение client.id для Kafka AdminClient (для читаемых логов брокера). */
    private static final String K_ADMIN_CLIENT_ID = "h2k.admin.client.id";
    /** Базовый backoff (мс) между повторами AdminClient при «неуверенных» ошибках. */
    private static final String K_ENSURE_UNKNOWN_BACKOFF_MS = "h2k.ensure.unknown.backoff.ms";
    /** Каждые N отправок ожидать подтверждение (ограничение памяти/pressure). */
    private static final String K_PRODUCER_AWAIT_EVERY = "h2k.producer.await.every";
    /** Таймаут ожидания подтверждения батча (мс). */
    private static final String K_PRODUCER_AWAIT_TIMEOUT_MS = "h2k.producer.await.timeout.ms";
    /** CSV‑карта переопределений длины соли rowkey в байтах по таблицам. */
    private static final String K_SALT_MAP = "h2k.salt.map";

    /**
     * Публичные ключи конфигурации h2k.* для использования в других пакетах проекта
     * (исключаем дубли строковых литералов). Значения синхронизированы с приватными K_* выше.
     */
    public static final class Keys {
        private Keys() {}
        /**
         * Список CF для экспорта в Kafka (CSV).
         * Пример: "d,b,0". Используется для фильтрации целевых семейств столбцов.
         */
        public static final String CF_LIST = "h2k.cf.list";
        /**
         * Адреса Kafka bootstrap.servers (обязательный параметр).
         * Формат: host:port[,host2:port2].
         */
        public static final String BOOTSTRAP = "h2k.kafka.bootstrap.servers";
        /**
         * Флаг сериализации null‑значений в JSON payload (true/false).
         * По умолчанию: false — поля с null опускаются.
         */
        public static final String JSON_SERIALIZE_NULLS = "h2k.json.serialize.nulls";
        /**
         * Режим декодирования значений из HBase: "simple" или "json-phoenix".
         * Используется при инициализации декодеров.
         */
        public static final String DECODE_MODE = "h2k.decode.mode";
        /**
         * Путь к JSON‑схеме (Schema Registry) для режимов, требующих типизации колонок.
         */
        public static final String SCHEMA_PATH = "h2k.schema.path";
        /**
         * Префикс для переопределения любых свойств Kafka Producer (например,
         * h2k.producer.acks, h2k.producer.linger.ms, и т.п.).
         */
        public static final String PRODUCER_PREFIX = "h2k.producer.";
        /**
         * Префикс для дополнительных конфигураций Kafka‑топика, собираемых в {@link #getTopicConfigs()}.
         */
        public static final String TOPIC_CONFIG_PREFIX = "h2k.topic.config.";
        /**
         * Включить диагностические счётчики BatchSender по умолчанию (true/false).
         */
        public static final String PRODUCER_BATCH_COUNTERS_ENABLED = "h2k.producer.batch.counters.enabled";
        /**
         * Включить подробный DEBUG‑лог при неуспехе автоматического сброса батча (true/false).
         */
        public static final String PRODUCER_BATCH_DEBUG_ON_FAILURE = "h2k.producer.batch.debug.on.failure";
        /**
         * Табличные переопределения соли rowkey в байтах.
         * Формат CSV: TABLE[:BYTES] | TABLE=BYTES | NS:TABLE[:BYTES] | NS:TABLE=BYTES [, ...].
         * Если BYTES не указан — берётся 1. Значение клиппится в диапазон 0..8.
         * Допускается полное имя 'namespace:qualifier' или просто 'qualifier'. Поиск — case-insensitive.
         */
        public static final String SALT_MAP = "h2k.salt.map";
        /**
         * Префикс подсказок ёмкости корневого JSON по таблицам.
         * Формат ключа: {@code h2k.capacity.hint.<TABLE> = <int>}
         * где TABLE — "namespace:qualifier" или просто "qualifier".
         */
        public static final String CAPACITY_HINT_PREFIX = "h2k.capacity.hint.";
        /**
         * CSV с подсказками ёмкости корневого JSON по таблицам.
         * Формат: h2k.capacity.hints = "TABLE=keys[,NS:TABLE=keys2,...]".
         * Значение "keys" — ожидаемое число не-null полей (см. README).
         */
        public static final String CAPACITY_HINTS = "h2k.capacity.hints";
    }

    // ==== Значения по умолчанию (в одном месте) ====
    /** Имя CF по умолчанию, если в конфигурации не задано явно. */
    private static final String DEFAULT_CF_NAME = "0";
    /** Базовое значение client.id для AdminClient (к нему добавляется hostname, если доступен). */
    private static final String DEFAULT_ADMIN_CLIENT_ID = "h2k-admin";
    /** По умолчанию rowkey в payload отключён. */
    private static final boolean DEFAULT_INCLUDE_ROWKEY = false;
    /** По умолчанию метаданные колонок в payload отключены. */
    private static final boolean DEFAULT_INCLUDE_META = false;
    /** По умолчанию признак происхождения из WAL отключён. */
    private static final boolean DEFAULT_INCLUDE_META_WAL = false;
    /** По умолчанию null-поля в JSON не сериализуются. */
    private static final boolean DEFAULT_JSON_SERIALIZE_NULLS = false;
    /** По умолчанию автосоздание топиков включено. */
    private static final boolean DEFAULT_ENSURE_TOPICS = true;
    /** По умолчанию увеличение партиций при ensureTopics отключено. */
    private static final boolean DEFAULT_ENSURE_INCREASE_PARTITIONS = false;
    /** По умолчанию дифф‑применение конфигов при ensureTopics отключено. */
    private static final boolean DEFAULT_ENSURE_DIFF_CONFIGS = false;
    /** Число партиций по умолчанию при создании топика. */
    private static final int DEFAULT_TOPIC_PARTITIONS = 3;
    /** Фактор репликации по умолчанию при создании топика. */
    private static final short DEFAULT_TOPIC_REPLICATION = 1;
    /** Таймаут операций AdminClient по умолчанию, мс. */
    private static final long DEFAULT_ADMIN_TIMEOUT_MS = 60000L;
    /** Пауза между повторами при неопределённых ошибках AdminClient по умолчанию, мс. */
    private static final long DEFAULT_UNKNOWN_BACKOFF_MS = 15000L;
    /**
     * Размер батча отправок по умолчанию (каждые N отправок ожидаем подтверждения),
     * баланс скорости и потребления памяти.
     */
    private static final int DEFAULT_AWAIT_EVERY = 500;
    /** Таймаут ожидания подтверждений батча по умолчанию, мс. */
    private static final int DEFAULT_AWAIT_TIMEOUT_MS = 180000;

    /** По умолчанию диагностические счётчики BatchSender отключены. */
    private static final boolean DEFAULT_PRODUCER_BATCH_COUNTERS_ENABLED = false;
    /** По умолчанию подробный DEBUG при неуспехе авто‑сброса отключён. */
    private static final boolean DEFAULT_PRODUCER_BATCH_DEBUG_ON_FAILURE = false;

    // ==== Базовые ====
    private final String bootstrap;
    private final String topicPattern;
    private final int topicMaxLength;
    /** Полный список имён CF, указанных в h2k.cf.list (в порядке конфигурации). */
    private final String[] cfNames;
    /** Те же CF в виде UTF‑8 байтов (для быстрого сравнения в горячем пути). */
    private final byte[][] cfBytes;

    // ==== Payload/метаданные/rowkey ====
    private final boolean includeRowKey;
    /** Кодирование rowkey: "hex" | "base64" */
    private final String rowkeyEncoding;
    /** Предвычисленный флаг: true — rowkey сериализуется в Base64, false — в HEX. */
    private final boolean rowkeyBase64;
    private final boolean includeMeta;
    private final boolean includeMetaWal;
    private final boolean jsonSerializeNulls;

    // ==== Автосоздание топиков ====
    private final boolean ensureTopics;
    /** Разрешено ли увеличение партиций при ensureTopics. */
    private final boolean ensureIncreasePartitions;
    /** Разрешено ли дифф‑применение topic‑конфигов при ensureTopics. */
    private final boolean ensureDiffConfigs;
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
    /** Переопределения длины соли rowkey в байтах по таблицам. 0 — соли нет. */
    private final Map<String, Integer> saltBytesByTable;
    /** Подсказки ёмкости корневого JSON по таблицам (ожидаемое число полей). */
    private final Map<String, Integer> capacityHintByTable;

    /**
     * Приватный конструктор: вызывается только билдером для инициализации
     * всех final‑полей за один проход. Сохраняет иммутабельность и избегает
     * длинного конструктора с множеством параметров.
     */
    private H2kConfig(Builder b) {
        this.bootstrap = b.bootstrap;
        this.topicPattern = b.topicPattern;
        this.topicMaxLength = b.topicMaxLength;
        this.cfNames = b.cfNames == null ? new String[]{DEFAULT_CF_NAME} : b.cfNames.clone();
        if (b.cfBytes != null) {
            // копируем внешний массив и каждую внутреннюю ссылку
            this.cfBytes = new byte[b.cfBytes.length][];
            for (int i = 0; i < b.cfBytes.length; i++) {
                byte[] src = b.cfBytes[i];
                byte[] dst = new byte[src.length];
                System.arraycopy(src, 0, dst, 0, src.length);
                this.cfBytes[i] = dst;
            }
        } else {
            this.cfBytes = new byte[][]{ DEFAULT_CF_NAME.getBytes(StandardCharsets.UTF_8) };
        }
        this.includeRowKey = b.includeRowKey;
        this.rowkeyEncoding = b.rowkeyEncoding;
        this.rowkeyBase64 = b.rowkeyBase64;
        this.includeMeta = b.includeMeta;
        this.includeMetaWal = b.includeMetaWal;
        this.jsonSerializeNulls = b.jsonSerializeNulls;
        this.ensureTopics = b.ensureTopics;
        this.ensureIncreasePartitions = b.ensureIncreasePartitions;
        this.ensureDiffConfigs = b.ensureDiffConfigs;
        this.topicPartitions = b.topicPartitions;
        this.topicReplication = b.topicReplication;
        this.adminTimeoutMs = b.adminTimeoutMs;
        this.adminClientId = b.adminClientId;
        this.unknownBackoffMs = b.unknownBackoffMs;
        this.awaitEvery = b.awaitEvery;
        this.awaitTimeoutMs = b.awaitTimeoutMs;
        this.producerBatchCountersEnabled = b.producerBatchCountersEnabled;
        this.producerBatchDebugOnFailure = b.producerBatchDebugOnFailure;
        this.topicConfigs = Collections.unmodifiableMap(new HashMap<>(b.topicConfigs));
        this.saltBytesByTable = Collections.unmodifiableMap(new HashMap<>(b.saltBytesByTable));
        this.capacityHintByTable = Collections.unmodifiableMap(new HashMap<>(b.capacityHintByTable));
    }

    /**
     * Билдер для пошаговой сборки иммутабельной конфигурации без громоздкого конструктора.
     * Удобнее читать, безопаснее изменять, удовлетворяет правилу Sonar S107 (ограничение числа параметров).
     * Все поля имеют разумные значения по умолчанию; сеттеры возвращают this для чейнинга.
     */
    public static final class Builder {
        /** Обязательный адрес(а) Kafka bootstrap.servers. */
        private String bootstrap;
        /** Шаблон имени топика (см. {@link H2kConfig#K_TOPIC_PATTERN}). */
        private String topicPattern = PLACEHOLDER_TABLE;
        /** Ограничение длины имени топика (символов). */
        private int topicMaxLength = DEFAULT_TOPIC_MAX_LENGTH;
        /** Список имён CF, указанных в конфигурации (оригинальный порядок). */
        private String[] cfNames = new String[]{DEFAULT_CF_NAME};
        /** Байтовые представления имён CF (UTF‑8) для быстрого сравнения. */
        private byte[][] cfBytes = new byte[][]{ DEFAULT_CF_NAME.getBytes(StandardCharsets.UTF_8) };

        /** Включать ли rowkey в payload. */
        private boolean includeRowKey = DEFAULT_INCLUDE_ROWKEY;
        /** Способ кодирования rowkey: "hex" или "base64". */
        private String rowkeyEncoding = ROWKEY_ENCODING_HEX;
        /** Предвычисленный флаг режима Base64 для горячего пути. */
        private boolean rowkeyBase64 = false;
        /** Включать ли метаданные ячеек (cf/qualifier/ts). */
        private boolean includeMeta = DEFAULT_INCLUDE_META;
        /** Включать ли признак происхождения записи из WAL. */
        private boolean includeMetaWal = DEFAULT_INCLUDE_META_WAL;
        /** Сериализовать ли null‑значения в JSON. */
        private boolean jsonSerializeNulls = DEFAULT_JSON_SERIALIZE_NULLS;
        /** Автоматически создавать недостающие топики. */
        private boolean ensureTopics = true;
        /** Разрешено ли автоматическое увеличение числа партиций. */
        private boolean ensureIncreasePartitions = DEFAULT_ENSURE_INCREASE_PARTITIONS;
        /** Разрешено ли дифф‑применение конфигов топика. */
        private boolean ensureDiffConfigs = DEFAULT_ENSURE_DIFF_CONFIGS;
        /** Целевое число партиций создаваемого топика. */
        private int topicPartitions = DEFAULT_TOPIC_PARTITIONS;
        /** Целевой фактор репликации создаваемого топика. */
        private short topicReplication = DEFAULT_TOPIC_REPLICATION;
        /** Таймаут операций AdminClient при ensureTopics (мс). */
        private long adminTimeoutMs = DEFAULT_ADMIN_TIMEOUT_MS;
        /** Значение client.id для AdminClient. */
        private String adminClientId = DEFAULT_ADMIN_CLIENT_ID;
        /** Базовый backoff между повторами при неопределённых ошибках (мс). */
        private long unknownBackoffMs = DEFAULT_UNKNOWN_BACKOFF_MS;

        /** Каждые N отправок ожидать подтверждение. */
        private int awaitEvery = DEFAULT_AWAIT_EVERY;
        /** Таймаут ожидания подтверждения батча (мс). */
        private int awaitTimeoutMs = DEFAULT_AWAIT_TIMEOUT_MS;

        /** Включены ли диагностические счётчики BatchSender по умолчанию. */
        boolean producerBatchCountersEnabled;
        /** Логировать ли подробности неуспешного авто‑сброса в DEBUG. */
        boolean producerBatchDebugOnFailure;

        /** Дополнительные конфиги топика, собранные из префикса h2k.topic.config.* */
        private Map<String, String> topicConfigs = Collections.emptyMap();
        /** Переопределения длины соли rowkey в байтах по таблицам. */
        private Map<String, Integer> saltBytesByTable = Collections.emptyMap();
        /** Подсказки ёмкости корневого JSON по таблицам. */
        private Map<String, Integer> capacityHintByTable = Collections.emptyMap();
        /**
         * Устанавливает карту переопределений соли по таблицам: имя → байты (0 — без соли).
         * Ожидается уже готовая карта (например, результат {@link Parsers#readSaltMap(Configuration, String)}).
         * @param v неизменяемая или копируемая карта name→bytes
         * @return this
         */
        public Builder saltBytesByTable(Map<String, Integer> v) { this.saltBytesByTable = v; return this; }

        /**
         * Устанавливает подсказки ёмкости корневого JSON по таблицам.
         * Ожидается уже готовая карта (например, результат {@link Parsers#readCapacityHints(Configuration, String, String)}).
         * @param v неизменяемая или копируемая карта name→capacity
         * @return this
         */
        public Builder capacityHintByTable(Map<String, Integer> v) { this.capacityHintByTable = v; return this; }

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
         * Ограничение длины имени топика (символов).
         * @param v максимальная длина (≥1)
         * @return this
         */
        public Builder topicMaxLength(int v) { this.topicMaxLength = v; return this; }
        /**
         * Устанавливает список имён CF, указанных через h2k.cf.list (CSV).
         * @param v массив имён CF
         * @return this
         */
        public Builder cfNames(String[] v) { this.cfNames = v; return this; }

        /**
         * Устанавливает байтовые представления имён CF (UTF‑8).
         * @param v массив байтовых имён CF
         * @return this
         */
        public Builder cfBytes(byte[][] v) { this.cfBytes = v; return this; }

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
         * Автоматически создавать недостающие топики при старте.
         * @param v true — создавать при необходимости
         * @return this
         */
        public Builder ensureTopics(boolean v) { this.ensureTopics = v; return this; }
        /**
         * Разрешить автоматическое увеличение числа партиций при ensureTopics.
         * @param v true — увеличивать партиции при необходимости
         * @return this
         */
        public Builder ensureIncreasePartitions(boolean v) { this.ensureIncreasePartitions = v; return this; }
        /**
         * Разрешить дифф‑применение конфигов топика (incrementalAlterConfigs) при ensureTopics.
         * @param v true — сравнивать и применять отличия конфигов
         * @return this
         */
        public Builder ensureDiffConfigs(boolean v) { this.ensureDiffConfigs = v; return this; }
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
         * Каждые N отправок ждать подтверждения (батчевое ожидание).
         * @param v размер батча N (≥1)
         * @return this
         */
        public Builder awaitEvery(int v) { this.awaitEvery = v; return this; }
        /**
         * Таймаут ожидания подтверждений батча.
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
         * Произвольные конфиги топика из префикса h2k.topic.config.* (см. {@link Parsers#readTopicConfigs(Configuration, String)}).
         * @param v карта ключ‑значение конфигураций топика
         * @return this
         */
        public Builder topicConfigs(Map<String, String> v) { this.topicConfigs = v; return this; }

        /**
         * Собирает неизменяемый объект конфигурации с текущими значениями билдера.
         *
         * Возвращаемый экземпляр {@link H2kConfig} иммутабелен и фиксирует копии/снимки переданных карт и массивов там, где это требуется.
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

        Map<String, Integer> saltMap = Parsers.readSaltMap(cfg, K_SALT_MAP);
        Map<String, Integer> capacityHints = Parsers.readCapacityHints(cfg, Keys.CAPACITY_HINTS, Keys.CAPACITY_HINT_PREFIX);

        String topicPattern = Parsers.readTopicPattern(cfg, K_TOPIC_PATTERN, PLACEHOLDER_TABLE);
        int topicMaxLength = Parsers.readIntMin(cfg, K_TOPIC_MAX_LENGTH, DEFAULT_TOPIC_MAX_LENGTH, 1);
        String[] cfNames = Parsers.readCfNames(cfg, K_CF_LIST, DEFAULT_CF_NAME);
        byte[][] cfBytes = Parsers.toUtf8Bytes(cfNames);

        boolean includeRowKey = cfg.getBoolean(K_PAYLOAD_INCLUDE_ROWKEY, DEFAULT_INCLUDE_ROWKEY);
        String rowkeyEncoding = Parsers.normalizeRowkeyEncoding(cfg.get(K_ROWKEY_ENCODING, ROWKEY_ENCODING_HEX));
        final boolean rowkeyBase64 = ROWKEY_ENCODING_BASE64.equals(rowkeyEncoding);
        boolean includeMeta = cfg.getBoolean(K_PAYLOAD_INCLUDE_META, DEFAULT_INCLUDE_META);
        boolean includeMetaWal = cfg.getBoolean(K_PAYLOAD_INCLUDE_META_WAL, DEFAULT_INCLUDE_META_WAL);
        boolean jsonSerializeNulls = cfg.getBoolean(Keys.JSON_SERIALIZE_NULLS, DEFAULT_JSON_SERIALIZE_NULLS);

        // По умолчанию автосоздание топиков включено (централизованный дефолт)
        boolean ensureTopics = cfg.getBoolean(K_ENSURE_TOPICS, DEFAULT_ENSURE_TOPICS);
        boolean ensureIncreasePartitions = cfg.getBoolean(K_ENSURE_INCREASE_PARTITIONS, DEFAULT_ENSURE_INCREASE_PARTITIONS);
        boolean ensureDiffConfigs = cfg.getBoolean(K_ENSURE_DIFF_CONFIGS, DEFAULT_ENSURE_DIFF_CONFIGS);
        int topicPartitions = cfg.getInt(K_TOPIC_PARTITIONS, DEFAULT_TOPIC_PARTITIONS);
        if (topicPartitions < 1) {
            LOG.warn("Некорректное значение {}={}, устанавливаю минимум: {}", K_TOPIC_PARTITIONS, topicPartitions, 1);
            topicPartitions = 1;
        }

        short topicReplication = (short) cfg.getInt(K_TOPIC_REPLICATION_FACTOR, DEFAULT_TOPIC_REPLICATION);
        if (topicReplication < 1) {
            LOG.warn("Некорректное значение {}={}, устанавливаю минимум: {}", K_TOPIC_REPLICATION_FACTOR, topicReplication, 1);
            topicReplication = 1;
        }
        long adminTimeoutMs = Parsers.readLong(cfg, K_ADMIN_TIMEOUT_MS, DEFAULT_ADMIN_TIMEOUT_MS);
        String adminClientId = Parsers.buildAdminClientId(cfg, K_ADMIN_CLIENT_ID, DEFAULT_ADMIN_CLIENT_ID);
        long unknownBackoffMs = cfg.getLong(K_ENSURE_UNKNOWN_BACKOFF_MS, DEFAULT_UNKNOWN_BACKOFF_MS);
        if (unknownBackoffMs < 1L) {
            LOG.warn("Некорректное значение {}={}, устанавливаю минимум: {} мс", K_ENSURE_UNKNOWN_BACKOFF_MS, unknownBackoffMs, 1);
            unknownBackoffMs = 1L;
        }
        int awaitEvery = Parsers.readIntMin(cfg, K_PRODUCER_AWAIT_EVERY, DEFAULT_AWAIT_EVERY, 1);
        int awaitTimeoutMs = Parsers.readIntMin(cfg, K_PRODUCER_AWAIT_TIMEOUT_MS, DEFAULT_AWAIT_TIMEOUT_MS, 1);
        boolean producerBatchCountersEnabled = cfg.getBoolean(Keys.PRODUCER_BATCH_COUNTERS_ENABLED, DEFAULT_PRODUCER_BATCH_COUNTERS_ENABLED);
        boolean producerBatchDebugOnFailure  = cfg.getBoolean(Keys.PRODUCER_BATCH_DEBUG_ON_FAILURE,  DEFAULT_PRODUCER_BATCH_DEBUG_ON_FAILURE);

        Map<String, String> topicConfigs = Parsers.readTopicConfigs(cfg, Keys.TOPIC_CONFIG_PREFIX);

        H2kConfig conf = new Builder(bootstrap)
                .topicPattern(topicPattern)
                .topicMaxLength(topicMaxLength)
                .cfNames(cfNames)
                .cfBytes(cfBytes)
                .includeRowKey(includeRowKey)
                .rowkeyEncoding(rowkeyEncoding)
                .rowkeyBase64(rowkeyBase64)
                .includeMeta(includeMeta)
                .includeMetaWal(includeMetaWal)
                .jsonSerializeNulls(jsonSerializeNulls)
                .ensureTopics(ensureTopics)
                .ensureIncreasePartitions(ensureIncreasePartitions)
                .ensureDiffConfigs(ensureDiffConfigs)
                .topicPartitions(topicPartitions)
                .topicReplication(topicReplication)
                .adminTimeoutMs(adminTimeoutMs)
                .adminClientId(adminClientId)
                .unknownBackoffMs(unknownBackoffMs)
                .awaitEvery(awaitEvery)
                .awaitTimeoutMs(awaitTimeoutMs)
                .producerBatchCountersEnabled(producerBatchCountersEnabled)
                .producerBatchDebugOnFailure(producerBatchDebugOnFailure)
                .saltBytesByTable(saltMap)
                .capacityHintByTable(capacityHints)
                .topicConfigs(topicConfigs)
                .build();
        LOG.info("Сконструирована конфигурация H2k: {}", conf);
        return conf;
    }

    /**
     * Для namespace "default" префикс не добавляется (см. правила по h2k.topic.pattern).
     * Формирует имя Kafka‑топика по заданному шаблону {@link #topicPattern} с подстановкой
     * плейсхолдеров и санитизацией по правилам Kafka (замена недопустимых символов на "_",
     * обрезка до {@link #topicMaxLength}).
     * @param table таблица HBase (источник namespace и qualifier)
     * @return корректное имя Kafka‑топика
     */
    public String topicFor(TableName table) {
        String ns = table.getNamespaceAsString();
        String qn = table.getQualifierAsString();
        // для namespace "default" префикс не пишем
        String tableAtom = HBASE_DEFAULT_NS.equals(ns) ? qn : (ns + "_" + qn);
        String nsAtom = HBASE_DEFAULT_NS.equals(ns) ? "" : ns;
        String base = topicPattern
                .replace(PLACEHOLDER_TABLE, tableAtom)
                .replace(PLACEHOLDER_NAMESPACE, nsAtom)
                .replace(PLACEHOLDER_QUALIFIER, qn);

        return sanitizeTopic(base);
    }

    /**
     * Санитизирует и нормализует произвольное «сырое» имя Kafka‑топика по тем же правилам,
     * что и {@link #topicFor(TableName)}: удаление ведущих/повторных разделителей, замена
     * недопустимых символов на '_', защита от "." и "..", обрезка до {@link #topicMaxLength}.
     *
     * Важно: эта функция — единая точка ответственности за правила формирования имени топика.
     * Используйте её при любых внешних/динамических именах, чтобы избежать расхождений.
     *
     * @implNote Делегирует нормализацию символов и разделителей вспомогательным методам {@code Parsers}; укорочение до {@code topicMaxLength} выполняется на финальном шаге.
     *
     * @param raw исходная строка (шаблон или произвольное имя)
     * @return корректное имя Kafka‑топика, соответствующее ограничениям брокера
     */
    public String sanitizeTopic(String raw) {
        String base = (raw == null) ? "" : raw;
        // убрать ведущие и повторные разделители
        String s = Parsers.topicCollapseRepeatedDelimiters(
                Parsers.topicStripLeadingDelimiters(base));
        // санитизация под допустимые символы Kafka
        String sanitized = Parsers.topicSanitizeKafkaChars(s);

        // защита от ".", ".." и пустой строки — используем универсальный безопасный placeholder
        if (sanitized.equals(".") || sanitized.equals("..") || sanitized.isEmpty()) {
            sanitized = "topic";
        }

        // обрезка по максимальной длине
        return (sanitized.length() > topicMaxLength)
                ? sanitized.substring(0, topicMaxLength)
                : sanitized;
    }


    // ===== Итоговые геттеры =====
    /** @return список Kafka bootstrap.servers */
    public String getBootstrap() { return bootstrap; }
    /** @return шаблон имени Kafka‑топика с плейсхолдерами */
    public String getTopicPattern() { return topicPattern; }
    /** @return максимальная допустимая длина имени топика */
    public int getTopicMaxLength() { return topicMaxLength; }
    /** @return имена CF в порядке, заданном конфигурацией (копия массива) */
    public String[] getCfNames() { return cfNames.clone(); }

    /**
     * @return байтовые представления имён CF (UTF‑8). ВАЖНО: возвращается ссылка на внутренний массив
     * для исключения лишних аллокаций на горячем пути. Не модифицируйте содержимое снаружи.
     */
    public byte[][] getCfFamiliesBytes() { return cfBytes; }

    /** @return CSV с именами CF — удобно для логов */
    public String getCfNamesCsv() { return String.join(",", cfNames); }

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

    /** @return создавать ли недостающие топики автоматически */
    public boolean isEnsureTopics() { return ensureTopics; }
    /**
     * Разрешено ли автоматическое увеличение числа партиций при ensureTopics.
     * По умолчанию false; управляется ключом h2k.ensure.increase.partitions.
     */
    public boolean isEnsureIncreasePartitions() { return ensureIncreasePartitions; }
    /**
     * Разрешено ли дифф‑применение конфигов топика (incrementalAlterConfigs) при ensureTopics.
     * По умолчанию false; управляется ключом h2k.ensure.diff.configs.
     */
    public boolean isEnsureDiffConfigs() { return ensureDiffConfigs; }
    /**
     * Число партиций для создаваемых Kafka-тем.
     * Значение нормализуется при построении конфигурации: минимум 1.
     */
    public int getTopicPartitions() { return topicPartitions; }
    /**
     * Фактор репликации для создаваемых Kafka-тем.
     * Значение нормализуется при построении конфигурации: минимум 1.
     */
    public short getTopicReplication() { return topicReplication; }
    /** @return таймаут операций AdminClient при ensureTopics, мс */
    public long getAdminTimeoutMs() { return adminTimeoutMs; }

    /**
     * Таймаут как int для API, принимающих миллисекунды 32‑битным целым.
     * Возвращает {@code Integer.MAX_VALUE}, если значение выходит за пределы int.
     */
    public int getAdminTimeoutMsAsInt() {
        long v = this.adminTimeoutMs;
        return (v > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) v;
    }

    /**
     * Готовые свойства для Kafka AdminClient.
     * Содержит как минимум bootstrap.servers и client.id.
     * Значения таймаутов намеренно не устанавливаются здесь и задаются на стороне TopicEnsurer.
     */
    public Properties kafkaAdminProps() {
        Properties p = new Properties();
        p.setProperty("bootstrap.servers", this.bootstrap);
        p.setProperty("client.id", this.adminClientId);
        return p;
    }
    /** @return значение client.id для AdminClient */
    public String getAdminClientId() { return adminClientId; }
    /**
     * Базовая задержка (в миллисекундах) для повторной попытки при «неуверенных» ошибках AdminClient.
     * Значение нормализуется при построении конфигурации: минимум 1 мс.
     */
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

    /**
     * Возвращает количество байт соли для заданной таблицы.
     * Поиск выполняется по полному имени (ns:qualifier), затем по одному qualifier.
     * @param table имя таблицы HBase
     * @return 0 если соль не используется; {@code >0} — число байт соли
     */
    public int getSaltBytesFor(TableName table) {
        String full = Parsers.up(table.getNameAsString()); // NS:QUALIFIER
        Integer v = saltBytesByTable.get(full);
        if (v != null) return v; // auto-unboxing
        v = saltBytesByTable.get(Parsers.up(table.getQualifierAsString()));
        return v == null ? 0 : v; // auto-unboxing
    }

    /** Удобный булев геттер: используется ли соль для таблицы. */
    public boolean isSalted(TableName table) { return getSaltBytesFor(table) > 0; }

    /** @return неизменяемая карта табличных переопределений соли (как задана в конфиге) */
    public Map<String, Integer> getSaltBytesByTable() { return saltBytesByTable; }

    /** @return неизменяемая карта подсказок ёмкости корневого JSON по таблицам */
    public Map<String, Integer> getCapacityHintByTable() { return capacityHintByTable; }

    /**
     * Возвращает подсказку ёмкости для заданной таблицы (если задана).
     * Поиск выполняется по полному имени (ns:qualifier), затем по одному qualifier.
     * @param table имя таблицы HBase
     * @return ожидаемое число полей в корневом JSON (0 — если подсказка не задана)
     */
    public int getCapacityHintFor(TableName table) {
        String full = Parsers.up(table.getNameAsString()); // NS:QUALIFIER
        Integer v = capacityHintByTable.get(full);
        if (v != null) return v; // auto-unboxing
        v = capacityHintByTable.get(Parsers.up(table.getQualifierAsString()));
        return v == null ? 0 : v; // auto-unboxing
    }
    /**
     * Строковое представление конфигурации с маскировкой bootstrap.servers.
     * Используется только для диагностических логов.
     */
    @Override
    public String toString() {
        final String maskedBootstrap = maskBootstrap(bootstrap);
        final String cfCsv = String.join(",", cfNames);
        return new StringBuilder(256)
                .append("H2kConfig{")
                .append("bootstrap=").append(maskedBootstrap)
                .append(", topicPattern='").append(topicPattern).append('\'')
                .append(", topicMaxLength=").append(topicMaxLength)
                .append(", cf=").append(cfCsv)
                .append(", includeRowKey=").append(includeRowKey)
                .append(", rowkeyEncoding='").append(rowkeyEncoding).append('\'')
                .append(", includeMeta=").append(includeMeta)
                .append(", includeMetaWal=").append(includeMetaWal)
                .append(", jsonSerializeNulls=").append(jsonSerializeNulls)
                .append(", ensureTopics=").append(ensureTopics)
                .append(", ensureIncreasePartitions=").append(ensureIncreasePartitions)
                .append(", ensureDiffConfigs=").append(ensureDiffConfigs)
                .append(", topicPartitions=").append(topicPartitions)
                .append(", topicReplication=").append(topicReplication)
                .append(", adminTimeoutMs=").append(adminTimeoutMs)
                .append(", adminClientId='").append(adminClientId).append('\'')
                .append(", unknownBackoffMs=").append(unknownBackoffMs)
                .append(", awaitEvery=").append(awaitEvery)
                .append(", awaitTimeoutMs=").append(awaitTimeoutMs)
                .append(", topicConfigs.size=").append(topicConfigs.size())
                .append(", saltBytesByTable.size=").append(saltBytesByTable.size())
                .append(", capacityHintByTable.size=").append(capacityHintByTable.size())
                .append('}')
                .toString();
    }

    /** Маскирует bootstrap.servers для логов: показывает только первый host и признак продолжения. */
    private static String maskBootstrap(String s) {
        if (s == null || s.isEmpty()) return "";
        String[] parts = s.split(",", 2);
        String first = parts[0];
        int colon = first.indexOf(':');
        String host = (colon >= 0 ? first.substring(0, colon) : first);
        String suffix = (parts.length > 1 ? ",..." : "");
        return host + suffix;
    }
}