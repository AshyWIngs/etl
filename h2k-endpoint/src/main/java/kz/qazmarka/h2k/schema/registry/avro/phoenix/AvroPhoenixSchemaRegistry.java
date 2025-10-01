package kz.qazmarka.h2k.schema.registry.avro.phoenix;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.hadoop.hbase.TableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import kz.qazmarka.h2k.schema.registry.PhoenixTableMetadataProvider;
import kz.qazmarka.h2k.schema.registry.SchemaRegistry;
import kz.qazmarka.h2k.schema.registry.avro.local.AvroSchemaRegistry;

/**
 * Реестр Phoenix-метаданных, считываемых из Avro-схем.
 */
public final class AvroPhoenixSchemaRegistry implements SchemaRegistry, PhoenixTableMetadataProvider {

    private static final Logger LOG = LoggerFactory.getLogger(AvroPhoenixSchemaRegistry.class);

    private static final String PROP_PK = "h2k.pk";
    private static final String PROP_TYPE = "h2k.phoenixType";
    private static final String PROP_SALT = "h2k.saltBytes";
    private static final String PROP_CAPACITY = "h2k.capacityHint";
    private static final ObjectMapper PK_MAPPER = new ObjectMapper();

    private final AvroSchemaRegistry avroRegistry;
    private final SchemaRegistry fallback;
    private final ConcurrentMap<String, TableMetadata> cache = new ConcurrentHashMap<>();
    private final Set<String> missingSchemaWarned =
            Collections.newSetFromMap(new ConcurrentHashMap<>());

    /**
     * @param avroRegistry локальный реестр Avro-схем (обязательный)
     */
    public AvroPhoenixSchemaRegistry(AvroSchemaRegistry avroRegistry) {
        this(avroRegistry, null);
    }

    /**
     * @param avroRegistry локальный реестр Avro-схем
     * @param fallback     реестр, к которому выполняется фолбэк (JSON/legacy); может быть {@code null}
     */
    public AvroPhoenixSchemaRegistry(AvroSchemaRegistry avroRegistry, SchemaRegistry fallback) {
        if (avroRegistry == null) {
            throw new NullPointerException("avroRegistry == null");
        }
        this.avroRegistry = avroRegistry;
        this.fallback = fallback == null ? SchemaRegistry.NOOP : fallback;
    }

    @Override
    /** Возвращает тип Phoenix для указанной таблицы/колонки, считывая его из Avro-схемы. */
    public String columnType(TableName table, String qualifier) {
        TableMetadata meta = metadataFor(table);
        return meta.columnType(qualifier);
    }

    @Override
    /** Возвращает массив имён PK, заданный в Avro-схеме (или во fallback-реестре). */
    public String[] primaryKeyColumns(TableName table) {
        return metadataFor(table).primaryKey();
    }

    @Override
    /** Возвращает количество байт соли rowkey, указанное в Avro-схеме или во fallback. */
    public Integer saltBytes(TableName table) {
        return metadataFor(table).saltBytes();
    }

    @Override
    /** Возвращает подсказку ёмкости JSON, считанную из Avro-схемы или fallback. */
    public Integer capacityHint(TableName table) {
        return metadataFor(table).capacityHint();
    }

    /** Читает и кеширует метаданные таблицы, используя локальный .avsc и fallback. */
    private TableMetadata metadataFor(TableName table) {
        String key = table.getNameAsString().toUpperCase(Locale.ROOT);
        return cache.computeIfAbsent(key, k -> loadMetadataSafely(table, k));
    }

    /** Устойчиво загружает метаданные и выполняет фолбэк при ошибке. */
    private TableMetadata loadMetadataSafely(TableName table, String cacheKey) {
        try {
            return loadMetadata(table);
        } catch (RuntimeException ex) {
            if (fallback == SchemaRegistry.NOOP) {
                throw ex;
            }
            warnSchemaFallback(cacheKey, table, ex);
            return TableMetadata.fallbackOnly(table, fallback);
        }
    }

    private void warnSchemaFallback(String cacheKey, TableName table, RuntimeException ex) {
        if (missingSchemaWarned.add(cacheKey)) {
            if (LOG.isWarnEnabled()) {
                LOG.warn("Avro-схема Phoenix не загружена для таблицы {} — используем schema.json как фолбэк: {}",
                        table.getNameAsString(), ex.toString());
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Трассировка ошибки Avro Phoenix схемы {}", table.getNameAsString(), ex);
            }
        }
    }

    /** Загружает Avro-схему и извлекает из неё типы, PK, соль и подсказку ёмкости. */
    private TableMetadata loadMetadata(TableName table) {
        Schema schema = avroRegistry.getByTable(table.getNameAsString());
        Map<String, String> types = new HashMap<>(Math.max(8, schema.getFields().size() * 3));
        for (Field field : schema.getFields()) {
            String type = field.getProp(PROP_TYPE);
            if (type == null || type.trim().isEmpty()) {
                continue;
            }
            String trimmed = type.trim().toUpperCase(Locale.ROOT);
            String name = field.name();
            types.put(name, trimmed);
            types.put(name.toLowerCase(Locale.ROOT), trimmed);
            types.put(name.toUpperCase(Locale.ROOT), trimmed);
        }
        String[] pk = readPk(schema);
        Integer salt = readSaltBytes(schema, table);
        Integer capacity = readCapacityHint(schema, table);
        return new TableMetadata(table, types, pk, fallback, salt, capacity);
    }

    private Integer readSaltBytes(Schema schema, TableName table) {
        Object raw = firstNonNull(schema.getObjectProp(PROP_SALT), schema.getProp(PROP_SALT));
        if (raw == null) {
            return null;
        }
        Integer value = coerceInteger(raw);
        if (value == null) {
            logInvalidNumeric(table, PROP_SALT, raw);
            return null;
        }
        if (value < 0) {
            LOG.warn("Avro-схема {}: h2k.saltBytes={} < 0 — значение будет проигнорировано", table.getNameAsString(), value);
            return null;
        }
        if (value > 8) {
            LOG.warn("Avro-схема {}: h2k.saltBytes={} > 8 — значение будет ограничено 8", table.getNameAsString(), value);
            return 8;
        }
        return value;
    }

    private Integer readCapacityHint(Schema schema, TableName table) {
        Object raw = firstNonNull(schema.getObjectProp(PROP_CAPACITY), schema.getProp(PROP_CAPACITY));
        if (raw == null) {
            return null;
        }
        Integer value = coerceInteger(raw);
        if (value == null) {
            logInvalidNumeric(table, PROP_CAPACITY, raw);
            return null;
        }
        if (value < 0) {
            LOG.warn("Avro-схема {}: h2k.capacityHint={} < 0 — значение будет проигнорировано", table.getNameAsString(), value);
            return null;
        }
        return value;
    }

    private static String[] readPk(Schema schema) {
        Object raw = schema.getObjectProp(PROP_PK);
        if (raw instanceof com.fasterxml.jackson.databind.JsonNode) {
            return readPkFromJsonNode((com.fasterxml.jackson.databind.JsonNode) raw);
        }
        if (isLegacyJacksonNode(raw)) {
            return readPkFromLegacyJsonNode(raw);
        }
        if (raw instanceof java.util.List<?>) {
            java.util.List<?> list = (java.util.List<?>) raw;
            return copyPkFromList(list);
        }
        String json = schema.getProp(PROP_PK);
        if (json != null && !json.trim().isEmpty()) {
            return readPkFromString(json.trim());
        }
        return SchemaRegistry.EMPTY;
    }

    private static String normalizePkEntry(String raw) {
        if (raw == null) {
            return null;
        }
        String v = raw.trim();
        return v.isEmpty() ? null : v;
    }

    private static String[] readPkFromJsonNode(com.fasterxml.jackson.databind.JsonNode node) {
        if (!node.isArray()) {
            return SchemaRegistry.EMPTY;
        }
        String[] pk = new String[node.size()];
        int idx = 0;
        for (int i = 0; i < node.size(); i++) {
            String v = normalizePkEntry(node.get(i).asText());
            if (v != null) {
                pk[idx++] = v;
            }
        }
        return trimPkArray(pk, idx);
    }

    private static String[] copyPkFromList(java.util.List<?> list) {
        if (list.isEmpty()) {
            return SchemaRegistry.EMPTY;
        }
        String[] pk = new String[list.size()];
        int idx = 0;
        for (Object o : list) {
            String v = normalizePkEntry(String.valueOf(o));
            if (v != null) {
                pk[idx++] = v;
            }
        }
        return trimPkArray(pk, idx);
    }

    private static String[] readPkFromString(String json) {
        try {
            JsonNode node = PK_MAPPER.readTree(json);
            return readPkFromJsonNode(node);
        } catch (java.io.IOException e) {
            LOG.warn("Не удалось распарсить h2k.pk из Avro-схемы ({}): {}", json, e.toString());
            if (LOG.isDebugEnabled()) {
                LOG.debug("Трассировка парсинга h2k.pk", e);
            }
            return SchemaRegistry.EMPTY;
        }
    }

    private static String[] trimPkArray(String[] pk, int size) {
        if (size == 0) {
            return SchemaRegistry.EMPTY;
        }
        if (size == pk.length) {
            return pk;
        }
        String[] trimmed = new String[size];
        System.arraycopy(pk, 0, trimmed, 0, size);
        return trimmed;
    }

    private static boolean isLegacyJacksonNode(Object raw) {
        if (raw == null) {
            return false;
        }
        String name = raw.getClass().getName();
        return name.startsWith("org.codehaus.jackson.");
    }

    private static String[] readPkFromLegacyJsonNode(Object node) {
        try {
            java.lang.reflect.Method isArray = node.getClass().getMethod("isArray");
            if (!Boolean.TRUE.equals(isArray.invoke(node))) {
                return SchemaRegistry.EMPTY;
            }
            java.lang.reflect.Method sizeMethod = node.getClass().getMethod("size");
            int size = ((Number) sizeMethod.invoke(node)).intValue();
            String[] pk = new String[size];
            java.lang.reflect.Method getMethod = node.getClass().getMethod("get", int.class);
            int idx = 0;
            for (int i = 0; i < size; i++) {
                Object element = getMethod.invoke(node, i);
                if (element == null) {
                    continue;
                }
                java.lang.reflect.Method asText = element.getClass().getMethod("asText");
                String text = (String) asText.invoke(element);
                String v = normalizePkEntry(text);
                if (v != null) {
                    pk[idx++] = v;
                }
            }
            return trimPkArray(pk, idx);
        } catch (ReflectiveOperationException e) {
            LOG.warn("Не удалось прочитать h2k.pk из legacy Jackson: {}", e.toString());
            if (LOG.isDebugEnabled()) {
                LOG.debug("Трассировка legacy Jackson", e);
            }
            return SchemaRegistry.EMPTY;
        }
    }

    private static Object firstNonNull(Object a, Object b) {
        return (a != null) ? a : b;
    }

    private static Integer coerceInteger(Object raw) {
        if (raw instanceof Number) {
            return ((Number) raw).intValue();
        }
        if (raw instanceof String) {
            String s = ((String) raw).trim();
            if (s.isEmpty()) {
                return null;
            }
            try {
                return Integer.valueOf(s);
            } catch (NumberFormatException ex) {
                return null;
            }
        }
        return null;
    }

    private void logInvalidNumeric(TableName table, String prop, Object raw) {
        LOG.warn("Avro-схема {}: свойство {} имеет некорректное значение '{}' — будет проигнорировано",
                table.getNameAsString(), prop, raw);
    }

    private static final class TableMetadata {
        private final Map<String, String> columnTypes;
        private final String[] pk;
        private final SchemaRegistry fallback;
        private final PhoenixTableMetadataProvider fallbackMeta;
        private final TableName table;
        private final Integer saltBytes;
        private final Integer capacityHint;
        private final AtomicReference<String[]> fallbackPk = new AtomicReference<>();

        TableMetadata(TableName table,
                      Map<String, String> columnTypes,
                      String[] pk,
                      SchemaRegistry fallback,
                      Integer saltBytes,
                      Integer capacityHint) {
            this.table = table;
            if (columnTypes.isEmpty()) {
                this.columnTypes = Collections.emptyMap();
            } else {
                this.columnTypes = Collections.unmodifiableMap(new HashMap<>(columnTypes));
            }
            this.pk = normalizePk(pk);
            this.fallback = fallback;
            this.fallbackMeta = (fallback instanceof PhoenixTableMetadataProvider)
                    ? (PhoenixTableMetadataProvider) fallback
                    : PhoenixTableMetadataProvider.NOOP;
            this.saltBytes = saltBytes;
            this.capacityHint = capacityHint;
        }

        static TableMetadata fallbackOnly(TableName table, SchemaRegistry fallback) {
            return new TableMetadata(table,
                    Collections.<String, String>emptyMap(),
                    SchemaRegistry.EMPTY,
                    fallback,
                    null,
                    null);
        }

        String columnType(String name) {
            if (name == null) {
                return null;
            }
            String type = columnTypes.get(name);
            if (type != null) {
                return type;
            }
            type = columnTypes.get(name.toLowerCase(Locale.ROOT));
            if (type != null) {
                return type;
            }
            type = columnTypes.get(name.toUpperCase(Locale.ROOT));
            if (type != null) {
                return type;
            }
            return fallback.columnType(table, name);
        }

        String[] primaryKey() {
            if (pk.length > 0) {
                return pk.clone();
            }
            String[] cached = fallbackPk.get();
            if (cached == null) {
                String[] resolved = copyOrEmpty(fallback.primaryKeyColumns(table));
                if (fallbackPk.compareAndSet(null, resolved)) {
                    cached = resolved;
                } else {
                    cached = fallbackPk.get();
                }
            }
            return cached.length == 0 ? SchemaRegistry.EMPTY : cached.clone();
        }

        private static String[] normalizePk(String[] pk) {
            if (pk == null || pk.length == 0) {
                return SchemaRegistry.EMPTY;
            }
            String[] res = new String[pk.length];
            int idx = 0;
            for (String val : pk) {
                if (val == null) {
                    continue;
                }
                String trimmed = val.trim();
                if (!trimmed.isEmpty()) {
                    res[idx++] = trimmed;
                }
            }
            if (idx == 0) {
                return SchemaRegistry.EMPTY;
            }
            if (idx != res.length) {
                String[] trimmed = new String[idx];
                System.arraycopy(res, 0, trimmed, 0, idx);
                return trimmed;
            }
            return res;
        }

        private static String[] copyOrEmpty(String[] src) {
            if (src == null || src.length == 0) {
                return SchemaRegistry.EMPTY;
            }
            String[] copy = new String[src.length];
            System.arraycopy(src, 0, copy, 0, src.length);
            return copy;
        }

        Integer saltBytes() {
            if (saltBytes != null) {
                return saltBytes;
            }
            return fallbackMeta.saltBytes(table);
        }

        Integer capacityHint() {
            if (capacityHint != null) {
                return capacityHint;
            }
            return fallbackMeta.capacityHint(table);
        }
    }
}
