package kz.qazmarka.h2k.schema.phoenix;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.hbase.TableName;
import org.apache.phoenix.schema.types.PBinary;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PFloat;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PSmallint;
import org.apache.phoenix.schema.types.PTime;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.schema.types.PTinyint;
import org.apache.phoenix.schema.types.PUnsignedInt;
import org.apache.phoenix.schema.types.PUnsignedLong;
import org.apache.phoenix.schema.types.PUnsignedSmallint;
import org.apache.phoenix.schema.types.PUnsignedTinyint;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.types.PVarcharArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kz.qazmarka.h2k.schema.registry.SchemaRegistry;

/**
 * Регистрирует допустимые типы Phoenix и предоставляет нормализацию для декодера.
 * В проекте используется {@link kz.qazmarka.h2k.schema.decoder.ValueCodecPhoenix}.
 */
public final class PhoenixColumnTypeRegistry {

    /**
     * Создаёт реестр типов Phoenix поверх {@link SchemaRegistry}.
     * Не допускает {@code null}, чтобы предотвратить ленивые сбои в горячем пути сериализации.
     *
     * @param registry источник метаданных Phoenix-таблиц
     */
    public PhoenixColumnTypeRegistry(kz.qazmarka.h2k.schema.registry.SchemaRegistry registry) {
        if (registry == null) {
            throw new NullPointerException("SchemaRegistry не может быть null");
        }
        this.registry = registry;
    }

    private static final Logger LOG = LoggerFactory.getLogger(PhoenixColumnTypeRegistry.class);
    private static final String T_VARCHAR = "VARCHAR";

    private static final Map<String, PhoenixType> TYPE_MAP;
    private static final PhoenixType DEFAULT_TYPE = PhoenixType.of(PVarchar.INSTANCE);
    static {
        Map<String, PhoenixType> m = new HashMap<>(32);
        m.put(T_VARCHAR, PhoenixType.of(PVarchar.INSTANCE));
        m.put("CHAR", PhoenixType.of(PChar.INSTANCE));
        m.put("CHARACTER", PhoenixType.of(PChar.INSTANCE));
        m.put("UNSIGNED TINYINT", PhoenixType.of(PUnsignedTinyint.INSTANCE));
        m.put("UNSIGNED SMALLINT", PhoenixType.of(PUnsignedSmallint.INSTANCE));
        m.put("UNSIGNED INT", PhoenixType.of(PUnsignedInt.INSTANCE));
        m.put("UNSIGNED LONG", PhoenixType.of(PUnsignedLong.INSTANCE));
        m.put("TINYINT", PhoenixType.of(PTinyint.INSTANCE));
        m.put("SMALLINT", PhoenixType.of(PSmallint.INSTANCE));
        m.put("INTEGER", PhoenixType.of(PInteger.INSTANCE));
        m.put("INT", PhoenixType.of(PInteger.INSTANCE));
        m.put("BIGINT", PhoenixType.of(PLong.INSTANCE));
        m.put("FLOAT", PhoenixType.of(PFloat.INSTANCE));
        m.put("DOUBLE", PhoenixType.of(PDouble.INSTANCE));
        m.put("DECIMAL", PhoenixType.of(PDecimal.INSTANCE));
        m.put("BOOLEAN", PhoenixType.of(PBoolean.INSTANCE));
        m.put("TIMESTAMP", PhoenixType.of(PTimestamp.INSTANCE));
        m.put("TIME", PhoenixType.of(PTime.INSTANCE));
        m.put("DATE", PhoenixType.of(PDate.INSTANCE));
        m.put("VARCHAR ARRAY", PhoenixType.of(PVarcharArray.INSTANCE));
        m.put("CHARACTER VARYING ARRAY", PhoenixType.of(PVarcharArray.INSTANCE));
        m.put("STRING ARRAY", PhoenixType.of(PVarcharArray.INSTANCE));
        m.put("VARBINARY", PhoenixType.of(PVarbinary.INSTANCE));
        m.put("BINARY", PhoenixType.of(PBinary.INSTANCE));
        m.put("NUMERIC", PhoenixType.of(PDecimal.INSTANCE));
        m.put("NUMBER", PhoenixType.of(PDecimal.INSTANCE));
        m.put("STRING", DEFAULT_TYPE);
        m.put("CHARACTER VARYING", DEFAULT_TYPE);
        m.put("BINARY VARYING", PhoenixType.of(PVarbinary.INSTANCE));
        m.put("ANY", DEFAULT_TYPE);
        m.put("ARRAY", PhoenixType.of(PVarcharArray.INSTANCE));
        m.put("LONG", PhoenixType.of(PLong.INSTANCE));
        m.put("BOOL", PhoenixType.of(PBoolean.INSTANCE));
        TYPE_MAP = Collections.unmodifiableMap(m);
    }

    private final SchemaRegistry registry;
    private final ConcurrentMap<TableName, ConcurrentMap<String, PhoenixType>> cache = new ConcurrentHashMap<>();
    private final Set<ColKey> unknownTypeWarned =
            Collections.newSetFromMap(new ConcurrentHashMap<>());

    /**
     * Возвращает лениво кэшируемый дескриптор Phoenix-типа для конкретной колонки.
     * Дескриптор предоставляет только необходимые операции (размер, конвертация байтов),
     * не раскрывая wildcard-типов наружу.
     */
    public PhoenixType resolve(TableName table, String qualifier) {
        final ConcurrentMap<String, PhoenixType> byQualifier =
                cache.computeIfAbsent(table, t -> new ConcurrentHashMap<>());

        return byQualifier.computeIfAbsent(qualifier, q -> {
            final String raw = registry.columnType(table, q);
            final String norm = normalizeTypeName(raw == null ? T_VARCHAR : raw);

            final PhoenixType predefined = TYPE_MAP.get(norm);
            if (predefined != null) {
                return predefined;
            }

            final ColKey warnKey = new ColKey(table, q);
            if (unknownTypeWarned.add(warnKey)) {
                LOG.warn("Неизвестный тип Phoenix в реестре для колонки {}.{} -> '{}' (нормализовано '{}'). Будет использован VARCHAR по умолчанию.",
                        table.getNameAsString(), q, raw, norm);
            } else if (LOG.isDebugEnabled()) {
                LOG.debug("Повтор неизвестного типа Phoenix: {}.{} -> '{}' (нормализовано '{}')",
                        table.getNameAsString(), q, raw, norm);
            }
            return DEFAULT_TYPE;
        });
    }

    private static String normalizeTypeName(String typeName) {
        String t = typeName == null ? "" : typeName.trim().toUpperCase(Locale.ROOT);
        if (t.isEmpty()) return T_VARCHAR;

        t = stripParenParams(t);
        t = normalizeArraySyntax(t);
        t = t.replace('_', ' ');
        return collapseSpaces(t);
    }

    private static String stripParenParams(String t) {
        int p = t.indexOf('(');
        if (p < 0) return t;
        int q = t.indexOf(')', p + 1);
        if (q > p) {
            return (t.substring(0, p) + t.substring(q + 1)).trim();
        }
        return t.substring(0, p).trim();
    }

    private static String normalizeArraySyntax(String t) {
        if (t.endsWith("[]")) {
            String base = t.substring(0, t.length() - 2).trim();
            base = stripParenParams(base);
            return base + " ARRAY";
        }
        if (t.startsWith("ARRAY<") && t.endsWith(">")) {
            String inner = t.substring(6, t.length() - 1).trim();
            inner = stripParenParams(inner);
            return inner + " ARRAY";
        }
        return t;
    }

    private static String collapseSpaces(String t) {
        StringBuilder sb = new StringBuilder(t.length());
        boolean space = false;
        for (int i = 0; i < t.length(); i++) {
            char c = t.charAt(i);
            if (c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f') {
                if (!space) {
                    sb.append(' ');
                    space = true;
                }
            } else {
                sb.append(c);
                space = false;
            }
        }
        return sb.toString();
    }

    private static final class ColKey {
        final String ns;
        final String name;
        final String qual;
        final int hash;

        ColKey(TableName t, String qual) {
            this.ns = t.getNamespaceAsString();
            this.name = t.getNameAsString();
            this.qual = qual;
            this.hash = 31 * (31 * ns.hashCode() + name.hashCode()) + qual.hashCode();
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || o.getClass() != ColKey.class) return false;
            ColKey other = (ColKey) o;
            return this.hash == other.hash
                    && this.ns.equals(other.ns)
                    && this.name.equals(other.name)
                    && this.qual.equals(other.qual);
        }
    }

    /**
     * Минимальный обёрточный тип вокруг {@link PDataType}, чтобы избежать утечек wildcard-типа в публичном API.
     * Предоставляет только те операции, которые востребованы в коде проекта.
     */
    public static final class PhoenixType {
        private final PDataType<?> delegate;

        private PhoenixType(PDataType<?> delegate) {
            this.delegate = delegate;
        }

        static PhoenixType of(PDataType<?> delegate) {
            return new PhoenixType(delegate);
        }

        /** @return фиксированный размер в байтах или {@code null}, если тип переменной длины. */
        public Integer byteSize() {
            return delegate.getByteSize();
        }

        /** Проксирует {@link PDataType#toObject(byte[], int, int)}. */
        public Object toObject(byte[] value, int offset, int length) {
            return delegate.toObject(value, offset, length);
        }

        @Override
        public String toString() {
            return delegate.toString();
        }

        @Override
        public int hashCode() {
            return delegate.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || o.getClass() != PhoenixType.class) return false;
            PhoenixType other = (PhoenixType) o;
            return delegate.equals(other.delegate);
        }
    }
}
