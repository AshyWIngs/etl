package kz.qazmarka.h2k.payload.serializer.avro;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import org.apache.avro.Schema;
import org.apache.hadoop.hbase.TableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.payload.serializer.TableAwarePayloadSerializer;
import kz.qazmarka.h2k.schema.registry.avro.local.AvroSchemaRegistry;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

/**
 * Сериализатор Avro, использующий Confluent Schema Registry 5.3.x.
 * Формат вывода полностью совпадает с wire‑форматом Confluent: {@code 0x0} (magic byte)
 * плюс {@code int32} идентификатора схемы (big-endian) и бинарный Avro‑payload.
 * Класс читает локальную схему из {@link AvroSchemaRegistry}, при необходимости регистрирует её в Schema Registry
 * и кеширует {@link Schema#hashCode()} и schemaId для последующих отправок.
 */
public final class ConfluentAvroPayloadSerializer implements TableAwarePayloadSerializer {

    private static final Logger LOG = LoggerFactory.getLogger(ConfluentAvroPayloadSerializer.class);
    private static final byte MAGIC_BYTE = 0x0;
    private static final int MAGIC_HEADER_LENGTH = 5;
    private static final String STRATEGY_TABLE = "table";
    private static final String SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO = "schema.registry.basic.auth.user.info";
    private final AvroSchemaRegistry localRegistry;
    private final List<String> registryUrls;
    private final SchemaRegistryClient schemaRegistryClient;
    private final LongAdder schemaRegisterSuccess = new LongAdder();
    private final LongAdder schemaRegisterFailure = new LongAdder();
    private final String subjectStrategy;
    private final String subjectPrefix;
    private final String subjectSuffix;

    /** subject -> schema info. */
    private final ConcurrentHashMap<String, SchemaInfo> cache = new ConcurrentHashMap<>();

    /**
     * @param cfg конфигурация h2k с параметрами Avro/SR и путём к локальным схемам
     */
    public ConfluentAvroPayloadSerializer(H2kConfig cfg) {
        this(cfg, SchemaRegistryClientFactory.cached(), null);
    }

    ConfluentAvroPayloadSerializer(H2kConfig cfg, SchemaRegistryClient externalClient) {
        this(cfg, SchemaRegistryClientFactory.cached(), externalClient);
    }

    /**
     * @param cfg     конфигурация h2k
     * @param factory фабрика клиентов Schema Registry (используется для создания кэшируемого клиента)
     */
    public ConfluentAvroPayloadSerializer(H2kConfig cfg, SchemaRegistryClientFactory factory) {
        this(cfg, factory, null);
    }

    ConfluentAvroPayloadSerializer(H2kConfig cfg,
                                   SchemaRegistryClientFactory factory,
                                   SchemaRegistryClient externalClient) {
        Objects.requireNonNull(cfg, "cfg == null");
        Objects.requireNonNull(factory, "schemaRegistryClientFactory == null");
        if (cfg.getAvroMode() != H2kConfig.AvroMode.CONFLUENT) {
            throw new IllegalStateException("Avro: режим '" + cfg.getAvroMode() + "' не является Confluent");
        }

        Path baseDir;
        String dir = cfg.getAvroSchemaDir();
        if (dir == null || dir.trim().isEmpty()) {
            baseDir = Paths.get("conf", "avro");
        } else {
            baseDir = Paths.get(dir.trim());
        }
        this.localRegistry = new AvroSchemaRegistry(baseDir);

        List<String> urls = cfg.getAvroSchemaRegistryUrls();
        if (urls == null || urls.isEmpty()) {
            throw new IllegalStateException("Avro: не заданы адреса Schema Registry (h2k.avro.sr.urls)");
        }
        List<String> normalized = new ArrayList<>(urls.size());
        for (String u : urls) {
            if (u == null || u.trim().isEmpty()) continue;
            String trimmed = u.trim();
            if (trimmed.endsWith("/")) {
                trimmed = trimmed.substring(0, trimmed.length() - 1);
            }
            normalized.add(trimmed);
        }
        if (normalized.isEmpty()) {
            throw new IllegalStateException("Avro: список Schema Registry пуст после нормализации");
        }
        this.registryUrls = Collections.unmodifiableList(normalized);

        Map<String, String> auth = cfg.getAvroSrAuth();

        Map<String, String> avroProps = cfg.getAvroProps();
        this.subjectStrategy = prop(avroProps, "subject.strategy", STRATEGY_TABLE);
        this.subjectPrefix = prop(avroProps, "subject.prefix", "");
        this.subjectSuffix = prop(avroProps, "subject.suffix", "");
        this.schemaRegistryClient = externalClient != null
                ? externalClient
                : createClient(factory, auth, avroProps);
    }

    @Override
    /**
     * Сериализует payload в формат Confluent (magic byte + schema id + бинарный Avro).
     */
    public byte[] serialize(TableName table, Map<String, ?> obj) {
        Objects.requireNonNull(table, "не передано имя таблицы");
        Objects.requireNonNull(obj, "payload == null");

        final String subject = buildSubject(table);
        SchemaInfo info = cache.get(subject);
        if (info == null) {
            info = register(table, subject);
            SchemaInfo existing = cache.putIfAbsent(subject, info);
            if (existing != null) {
                info = existing;
            }
        }

        byte[] payload = info.serializer.serialize(obj);
        byte[] out = new byte[MAGIC_HEADER_LENGTH + payload.length];
        out[0] = MAGIC_BYTE;
        int id = info.schemaId;
        out[1] = (byte) ((id >>> 24) & 0xFF);
        out[2] = (byte) ((id >>> 16) & 0xFF);
        out[3] = (byte) ((id >>> 8) & 0xFF);
        out[4] = (byte) (id & 0xFF);
        System.arraycopy(payload, 0, out, MAGIC_HEADER_LENGTH, payload.length);
        return out;
    }

    @Override
    public String format() {
        return "avro-binary";
    }

    @Override
    public String contentType() {
        return "application/avro-binary";
    }

    /** Регистрирует схему в Confluent SR и возвращает информацию (schemaId + сериализатор). */
    private SchemaInfo register(TableName table, String subject) {
        String tableKey = table.getNameAsString();
        Schema schema = loadSchema(tableKey);
        int schemaId = registerSchema(subject, schema);
        LOG.debug("Avro Confluent: схема зарегистрирована: subject={}, id={}.", subject, schemaId);
        return new SchemaInfo(schemaId, new AvroSerializer(schema));
    }

    private Schema loadSchema(String tableKey) {
        try {
            return localRegistry.getByTable(tableKey);
        } catch (IllegalStateException ex) {
            throw new IllegalStateException(
                    "Avro: не удалось прочитать локальную схему для таблицы '" + tableKey + "': " + ex.getMessage(), ex);
        }
    }

    private String buildSubject(TableName table) {
        String base;
        String strategy = subjectStrategy.toLowerCase(Locale.ROOT);
        switch (strategy) {
            case STRATEGY_TABLE:
                base = tableNameForSubject(table, CaseMode.ORIGINAL);
                break;
            case "table-upper":
                base = tableNameForSubject(table, CaseMode.UPPER);
                break;
            case "table-lower":
                base = tableNameForSubject(table, CaseMode.LOWER);
                break;
            case "qualifier":
                base = table.getQualifierAsString();
                break;
            default:
                base = table.getQualifierAsString();
        }
        String sanitized = sanitizeSubject(base);
        return subjectPrefix + sanitized + subjectSuffix;
    }

    private enum CaseMode { ORIGINAL, UPPER, LOWER }

    private static String tableNameForSubject(TableName table, CaseMode mode) {
        String ns = table.getNamespaceAsString();
        String qualifier = table.getQualifierAsString();
        String base;
        if (ns != null && !ns.isEmpty() && !"default".equalsIgnoreCase(ns)) {
            base = ns + ":" + qualifier;
        } else {
            base = qualifier;
        }
        switch (mode) {
            case UPPER:
                return base.toUpperCase(Locale.ROOT);
            case LOWER:
                return base.toLowerCase(Locale.ROOT);
            default:
                return base;
        }
    }

    /** Приводит subject к допустимому виду (только латиница/цифры/._-). */
    private static String sanitizeSubject(String raw) {
        if (raw == null || raw.isEmpty()) {
            return "subject";
        }
        StringBuilder sb = new StringBuilder(raw.length());
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            if (Character.isLetterOrDigit(c) || c == '-' || c == '_' || c == '.') {
                sb.append(c);
            } else {
                sb.append('_');
            }
        }
        return sb.toString();
    }

    /** Конструирует клиента Schema Registry с учётом basic-auth и кеша схем. */
    private SchemaRegistryClient createClient(SchemaRegistryClientFactory factory,
                                              Map<String, String> auth,
                                              Map<String, String> avroProps) {
        Map<String, Object> clientConfig = new HashMap<>();
        String user = prop(auth, "basic.username", null);
        String pass = prop(auth, "basic.password", null);
        if (user != null && pass != null) {
            String credentials = user + ':' + pass;
            clientConfig.put(AbstractKafkaAvroSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
            clientConfig.put(AbstractKafkaAvroSerDeConfig.USER_INFO_CONFIG, credentials);
            clientConfig.put(SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO, credentials);
        }

        int identityMapCapacity = parsePositiveInt(
                prop(avroProps, "client.cache.capacity", null),
                AbstractKafkaAvroSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_DEFAULT);

        return factory.create(registryUrls, clientConfig, identityMapCapacity);
    }

    private static String prop(Map<String, String> props, String key, String def) {
        if (props == null) return def;
        String exact = props.get(key);
        if (exact != null) return exact;
        String lowerKey = key.toLowerCase(Locale.ROOT);
        for (Map.Entry<String, String> e : props.entrySet()) {
            if (e.getKey() != null && e.getKey().toLowerCase(Locale.ROOT).equals(lowerKey)) {
                return e.getValue();
            }
        }
        return def;
    }

    /** Простая структура для хранения schemaId и готового сериализатора. */
    private static final class SchemaInfo {
        final int schemaId;
        final AvroSerializer serializer;

        SchemaInfo(int schemaId, AvroSerializer serializer) {
            this.schemaId = schemaId;
            this.serializer = serializer;
        }
    }

    private static int parsePositiveInt(String value, int def) {
        if (value == null) {
            return def;
        }
        try {
            int parsed = Integer.parseInt(value.trim());
            return parsed > 0 ? parsed : def;
        } catch (NumberFormatException ex) {
            return def;
        }
    }

    /** Регистрирует схему в Schema Registry и возвращает присвоенный schemaId. */
    private int registerSchema(String subject, Schema schema) {
        try {
            int id = schemaRegistryClient.register(subject, schema);
            schemaRegisterSuccess.increment();
            return id;
        } catch (RestClientException ex) {
            schemaRegisterFailure.increment();
            throw new IllegalStateException("Avro: регистрация схемы в Schema Registry не удалась: "
                    + restError(ex) + ", subject=" + subject, ex);
        } catch (java.io.IOException ex) {
            schemaRegisterFailure.increment();
            throw new IllegalStateException("Avro: ошибка сетевого взаимодействия с Schema Registry: " + ex.getMessage(), ex);
        }
    }

    private static String restError(RestClientException ex) {
        return ex.getStatus() + " " + ex.getMessage();
    }

    /** Возвращает неизменяемый снимок счётчиков взаимодействия со Schema Registry. */
    /** @return неизменяемый снимок счётчиков регистрации схем. */
    public SchemaRegistryMetrics metrics() {
        return new SchemaRegistryMetrics(schemaRegisterSuccess.sum(), schemaRegisterFailure.sum());
    }

    /** Снимок метрик взаимодействия со Schema Registry (успех/ошибка регистрации схем). */
    public static final class SchemaRegistryMetrics {
        private final long registered;
        private final long failures;

        SchemaRegistryMetrics(long registered, long failures) {
            this.registered = registered;
            this.failures = failures;
        }

        /** @return количество успешных регистраций схем */
        public long registeredSchemas() { return registered; }

        /** @return количество ошибок регистрации схем */
        public long registrationFailures() { return failures; }
    }
}
