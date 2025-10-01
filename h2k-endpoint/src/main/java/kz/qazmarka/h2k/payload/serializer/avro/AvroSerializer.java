package kz.qazmarka.h2k.payload.serializer.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

import kz.qazmarka.h2k.payload.builder.PayloadFields;
import kz.qazmarka.h2k.payload.serializer.PayloadSerializer;

/**
 * Сериализатор Avro для «generic» режима (без Schema Registry).
 * Схема выбирается резолвером, как правило по полю {@link PayloadFields#TABLE}.
 * Экземпляр считается потокобезопасным, если каждый поток использует собственный объект.
 * Для экономии аллокаций применяются {@link ThreadLocal}-буферы (ByteArrayOutputStream + BinaryEncoder
 * и GenericDatumWriter).
 */
public final class AvroSerializer implements PayloadSerializer {

    private static final Object NO_DEFAULT = new Object();

    /** Резолвер Avro-схемы под конкретный payload. */
    @FunctionalInterface
    public interface SchemaResolver {
        /**
         * Определяет Avro-схему для переданного набора полей.
         *
         * @param fields исходные данные (ключи совпадают с полями Avro-схемы); не {@code null}
         * @return Avro-схема, совместимая с {@code fields}; не {@code null}
         * @throws IllegalStateException если схему невозможно подобрать (например, таблица неизвестна)
         */
        Schema resolve(Map<String, ?> fields);
    }

    private final SchemaResolver schemaResolver;

    // Небольшой внутренний кэш объектов на поток (Avro-энкодер не является потокобезопасным)
    private final ThreadLocal<ByteArrayOutputStream> localBaos = ThreadLocal.withInitial(() -> new ByteArrayOutputStream(512));
    private final ThreadLocal<BinaryEncoder> localEncoder = new ThreadLocal<>();
    private static final ConcurrentHashMap<Schema, ThreadLocal<GenericDatumWriter<GenericRecord>>> WRITER_CACHE = new ConcurrentHashMap<>(16);

    /**
     * @param fixedSchema фиксированная Avro-схема (подходит для одной таблицы/структуры)
     */
    public AvroSerializer(Schema fixedSchema) {
        Objects.requireNonNull(fixedSchema, "schema");
        this.schemaResolver = fields -> fixedSchema;
    }

    /**
     * @param resolver пользовательский резолвер схемы (например, из AvroSchemaRegistry)
     */
    public AvroSerializer(SchemaResolver resolver) {
        this.schemaResolver = Objects.requireNonNull(resolver, "schemaResolver");
    }

    /**
     * @param schemaByTable карта «имя таблицы → схема»; ожидается поле {@link PayloadFields#TABLE}
     */
    public AvroSerializer(final Map<String, Schema> schemaByTable) {
        Objects.requireNonNull(schemaByTable, "schemaByTable");
        this.schemaResolver = fields -> {
            Object t = fields.get(PayloadFields.TABLE);
            if (!(t instanceof String)) {
                throw new IllegalStateException("Avro: не удалось определить таблицу — отсутствует поле '" + PayloadFields.TABLE + "' или оно не строка");
            }
            final String table = t.toString();
            Schema s = schemaByTable.get(table);
            if (s == null) {
                throw new IllegalStateException("Avro: схема для таблицы '" + t + "' не найдена");
            }
            return s;
        };
    }

    // --- PayloadSerializer ---

    /** Формат, используемый для метрик и логов. */
    @Override
    public String format() { return "avro-binary"; }

    /** MIME-тип сериализованного представления. */
    @Override
    public String contentType() { return "application/avro-binary"; }

    @Override
    public byte[] serialize(Map<String, ?> fields) {
        Objects.requireNonNull(fields, "fields");
        final Schema schema = Objects.requireNonNull(schemaResolver.resolve(fields), "schema");

        final GenericRecord avroRecord = buildRecord(schema, fields);

        final ByteArrayOutputStream baos = localBaos.get();
        baos.reset();
        BinaryEncoder enc = localEncoder.get();
        enc = EncoderFactory.get().directBinaryEncoder(baos, enc);
        localEncoder.set(enc);

        final GenericDatumWriter<GenericRecord> writer = writerFor(schema);
        try {
            writer.write(avroRecord, enc);
            enc.flush();
        } catch (IOException e) {
            // не ожидается при ByteArrayOutputStream, но сохраним причину
            throw new IllegalStateException("Avro: ошибка сериализации: " + e.getMessage(), e);
        }
        return baos.toByteArray();
    }

    private static GenericDatumWriter<GenericRecord> writerFor(Schema schema) {
        ThreadLocal<GenericDatumWriter<GenericRecord>> tl = WRITER_CACHE.computeIfAbsent(schema,
                s -> ThreadLocal.withInitial(() -> new GenericDatumWriter<>(s)));
        GenericDatumWriter<GenericRecord> writer = tl.get();
        writer.setSchema(schema);
        return writer;
    }

    // --- coercion strategies (reduce cognitive complexity of coerceValue) ---

    /** Стратегия приведения Java-значения к совместимому с Avro типу. */
    private interface Coercer {
        Object apply(Schema schema, Object v, String path);
    }

    /** Реестр стратегий преобразования по типам Avro. */
    private static final EnumMap<Schema.Type, Coercer> COERCERS = new EnumMap<>(Schema.Type.class);
    static {
        // NULL
        COERCERS.put(Schema.Type.NULL, (schema, v, path) -> null);

        // BOOLEAN
        COERCERS.put(Schema.Type.BOOLEAN, (schema, v, path) -> {
            if (v == null) return null;
            if (v instanceof Boolean) return v;
            throw typeError(path, "BOOLEAN", v);
        });

        // INT
        COERCERS.put(Schema.Type.INT, (schema, v, path) -> {
            if (v == null) return null;
            if (v instanceof Number) return ((Number) v).intValue();
            throw typeError(path, "INT", v);
        });

        // LONG
        COERCERS.put(Schema.Type.LONG, (schema, v, path) -> {
            if (v == null) return null;
            if (v instanceof Number) return ((Number) v).longValue();
            throw typeError(path, "LONG", v);
        });

        // DOUBLE
        COERCERS.put(Schema.Type.DOUBLE, (schema, v, path) -> {
            if (v == null) return null;
            if (v instanceof Number) return ((Number) v).doubleValue();
            throw typeError(path, "DOUBLE", v);
        });

        // FLOAT (store as double to match previous behavior and Avro's numeric promotion)
        COERCERS.put(Schema.Type.FLOAT, (schema, v, path) -> {
            if (v == null) return null;
            if (v instanceof Number) return ((Number) v).doubleValue();
            throw typeError(path, "FLOAT", v);
        });

        // STRING
        COERCERS.put(Schema.Type.STRING, (schema, v, path) -> {
            if (v == null) return null;
            if (v instanceof CharSequence) return v.toString();
            throw typeError(path, "STRING", v);
        });

        // BYTES
        COERCERS.put(Schema.Type.BYTES, (schema, v, path) -> {
            if (v == null) return null;
            if (v instanceof byte[]) return ByteBuffer.wrap((byte[]) v);
            if (v instanceof ByteBuffer) return v;
            throw typeError(path, "BYTES", v);
        });

        // ARRAY
        COERCERS.put(Schema.Type.ARRAY, AvroSerializer::coerceArray);

        // MAP
        COERCERS.put(Schema.Type.MAP, AvroSerializer::coerceMap);

        // RECORD
        COERCERS.put(Schema.Type.RECORD, AvroSerializer::coerceRecord);

        // UNION (поддерживаем nullable-union: null + конкретный тип)
        COERCERS.put(Schema.Type.UNION, AvroSerializer::coerceUnion);

        // ENUM
        COERCERS.put(Schema.Type.ENUM, (schema, v, path) -> {
            if (v == null) return null;
            if (v instanceof String) return new GenericData.EnumSymbol(schema, (String) v);
            throw typeError(path, "ENUM", v);
        });

        // FIXED (фиксированный размер байтов)
        COERCERS.put(Schema.Type.FIXED, (schema, v, path) -> {
            if (v == null) return null;
            if (v instanceof byte[]) {
                byte[] arr = (byte[]) v;
                if (arr.length != schema.getFixedSize())
                    throw new IllegalStateException("Avro: размер fixed не совпадает для поля: " + path);
                return new GenericData.Fixed(schema, arr);
            }
            throw typeError(path, "FIXED(byte[])", v);
        });
    }

    // --- helpers ---

    static GenericRecord buildRecord(Schema schema, Map<String, ?> fields) {
        GenericData.Record avroRecord = new GenericData.Record(schema);
        GenericData genericData = GenericData.get();
        for (Schema.Field f : schema.getFields()) {
            final String fieldName = f.name();
            final Schema fieldSchema = f.schema();

            final boolean hasValue = fields.containsKey(fieldName);
            Object value = hasValue ? fields.get(fieldName) : null;

            if (!hasValue) {
                Object defaultValue = extractDefaultValue(genericData, f, fieldSchema);
                if (defaultValue != NO_DEFAULT) {
                    value = defaultValue;
                } else if (!allowsNull(fieldSchema)) {
                    throw new IllegalStateException("Avro: поле '" + fieldName + "' обязательно, но отсутствует в payload");
                }
            }

            Object coerced = coerceValue(fieldSchema, value, fieldName);
            if (coerced == null && !allowsNull(fieldSchema)) {
                throw new IllegalStateException("Avro: поле '" + fieldName + "' обязательно и не может быть null");
            }
            avroRecord.put(fieldName, coerced);
        }
        return avroRecord;
    }

    private static boolean allowsNull(Schema schema) {
        if (schema.getType() == Schema.Type.NULL) {
            return true;
        }
        if (schema.getType() == Schema.Type.UNION) {
            for (Schema option : schema.getTypes()) {
                if (option.getType() == Schema.Type.NULL) {
                    return true;
                }
            }
        }
        return false;
    }

    private static Object extractDefaultValue(GenericData genericData, Schema.Field field, Schema fieldSchema) {
        try {
            Object defaultValue = genericData.getDefaultValue(field);
            return genericData.deepCopy(fieldSchema, defaultValue);
        } catch (AvroRuntimeException ex) {
            return NO_DEFAULT;
        }
    }

    /** Проверка и копирование произвольной Map в типизированную Map<String,Object> (без unchecked cast). */
    private static Map<String, Object> toStringObjectMap(Object v, String path) {
        if (!(v instanceof Map)) {
            throw typeError(path, "RECORD(map)", v);
        }
        Map<?, ?> in = (Map<?, ?>) v;
        Map<String, Object> out = new HashMap<>(in.size());
        for (Map.Entry<?, ?> e : in.entrySet()) {
            Object k = e.getKey();
            if (!(k instanceof String)) {
                throw new IllegalStateException("Avro: ключи map должны быть STRING: " + path);
            }
            out.put((String) k, e.getValue());
        }
        return out;
    }

    private static List<Object> coerceArray(Schema fieldSchema, Object v, String path) {
        if (v == null) {
            throw new IllegalStateException("Avro: поле '" + path + "' обязательно и не может быть null");
        }
        if (!(v instanceof List)) throw typeError(path, "ARRAY", v);
        Schema elem = fieldSchema.getElementType();
        List<?> in = (List<?>) v;
        List<Object> arrOut = new ArrayList<>(in.size());
        for (int i = 0; i < in.size(); i++) {
            arrOut.add(coerceValue(elem, in.get(i), path + "[" + i + "]"));
        }
        return arrOut;
    }

    private static Map<String, Object> coerceMap(Schema fieldSchema, Object v, String path) {
        if (v == null) {
            throw new IllegalStateException("Avro: поле '" + path + "' обязательно и не может быть null");
        }
        Map<String, Object> in = toStringObjectMap(v, path);
        Schema vt = fieldSchema.getValueType();
        Map<String, Object> mapOut = new HashMap<>(in.size());
        for (Map.Entry<String, Object> e : in.entrySet()) {
            mapOut.put(e.getKey(), coerceValue(vt, e.getValue(), path + "." + e.getKey()));
        }
        return mapOut;
    }

    private static GenericRecord coerceRecord(Schema fieldSchema, Object v, String path) {
        if (v == null) return null;
        Map<String, Object> m = toStringObjectMap(v, path);
        return buildRecord(fieldSchema, m);
    }

    private static Object coerceUnion(Schema fieldSchema, Object v, String path) {
        Schema nullable = null;
        for (Schema option : fieldSchema.getTypes()) {
            if (option.getType() == Schema.Type.NULL) {
                nullable = option;
                break;
            }
        }
        if (v == null) {
            if (nullable != null) {
                return null;
            }
            throw new IllegalStateException("Avro: поле обязательно: " + path);
        }

        IllegalStateException lastError = null;
        for (Schema option : fieldSchema.getTypes()) {
            if (option.getType() == Schema.Type.NULL) {
                continue;
            }
            try {
                return coerceValue(option, v, path);
            } catch (IllegalStateException ex) {
                if (lastError == null) {
                    lastError = ex;
                }
            }
        }

        if (lastError != null) {
            throw new IllegalStateException("Avro: значение поля '" + path + "' не подходит ни к одной ветке union", lastError);
        }
        throw new IllegalStateException("Avro: значение поля '" + path + "' не подходит ни к одной ветке union");
    }

    /**
     * Приведение Java-значений из Map к типам Avro. Поддержаны примитивы, строки,
     * bytes, массивы, вложенные записи и простые union'ы вида ["null", T].
     */
    private static Object coerceValue(Schema fieldSchema, Object v, String path) {
        Coercer c = COERCERS.get(fieldSchema.getType());
        if (c == null) {
            throw new IllegalStateException("Avro: неподдержанный тип поля '" + path + "': " + fieldSchema.getType());
        }
        return c.apply(fieldSchema, v, path);
    }

    private static IllegalStateException typeError(String path, String expected, Object actual) {
        return new IllegalStateException(
                "Avro: поле '" + path + "' ожидает " + expected + ", получено: " +
                        (actual == null ? "null" : actual.getClass().getSimpleName()));
    }

    /**
     * Позволяет освободить ThreadLocal-буферы (рекомендуется вызывать при остановке потока-пула).
     */
    public void clearThreadLocals() {
        localBaos.remove();
        localEncoder.remove();
    }
}
