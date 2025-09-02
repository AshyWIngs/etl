package kz.qazmarka.h2k.schema;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
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
import org.apache.phoenix.schema.types.PhoenixArray;

/**
 * ValueCodecPhoenix
 * Декод значений колонок через Phoenix PDataType, опираясь на реестр типов SchemaRegistry.
 * Обеспечивает точное соответствие семантике Phoenix (UNSIGNED-типы, TIMESTAMP/DATE/TIME, ARRAY и т.п.).
 *
 * Нормализация результата:
 * - TIMESTAMP/DATE/TIME -> миллисекунды epoch (long)
 * - Любой Phoenix ARRAY -> java.util.List&lt;Object&gt; (без копии для Object[], с минимальной копией для примитивных массивов)
 * - VARBINARY/BINARY -> byte[] как есть
 * - Прочие типы возвращаются как есть (String/Number/Boolean)
 */
public final class ValueCodecPhoenix implements Decoder {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(ValueCodecPhoenix.class);

    /** Каноническое имя строкового типа в Phoenix. Используем в нескольких местах, чтобы не дублировать литерал. */
    private static final String T_VARCHAR = "VARCHAR";

    private final SchemaRegistry registry;

    /** Локальный кэш сопоставлений (table, qualifier) -> PDataType для устранения повторной нормализации строк типов. */
    private final ConcurrentMap<ColKey, PDataType<?>> typeCache = new ConcurrentHashMap<>();

    /** Набор колонок, для которых уже выводили предупреждение об неизвестном типе (чтобы не шуметь). */
    private final java.util.Set<ColKey> unknownTypeWarned = java.util.concurrent.ConcurrentHashMap.newKeySet();

    /** Быстрый словарь соответствий: строковое имя типа -> PDataType. */
    private static final Map<String, PDataType<?>> TYPE_MAP;
    static {
        Map<String, PDataType<?>> m = new HashMap<>(64);
        m.put(T_VARCHAR, PVarchar.INSTANCE);
        m.put("CHAR", PChar.INSTANCE);
        m.put("UNSIGNED_TINYINT", PUnsignedTinyint.INSTANCE);
        m.put("UNSIGNED_SMALLINT", PUnsignedSmallint.INSTANCE);
        m.put("UNSIGNED_INT", PUnsignedInt.INSTANCE);
        m.put("UNSIGNED_LONG", PUnsignedLong.INSTANCE);
        // Варианты записи с пробелами (после нормализации подчёркиваний)
        m.put("UNSIGNED TINYINT", PUnsignedTinyint.INSTANCE);
        m.put("UNSIGNED SMALLINT", PUnsignedSmallint.INSTANCE);
        m.put("UNSIGNED INT", PUnsignedInt.INSTANCE);
        m.put("UNSIGNED LONG", PUnsignedLong.INSTANCE);
        m.put("TINYINT", PTinyint.INSTANCE);
        m.put("SMALLINT", PSmallint.INSTANCE);
        m.put("INTEGER", PInteger.INSTANCE);
        m.put("INT", PInteger.INSTANCE);
        m.put("BIGINT", PLong.INSTANCE);
        m.put("FLOAT", PFloat.INSTANCE);
        m.put("DOUBLE", PDouble.INSTANCE);
        m.put("DECIMAL", PDecimal.INSTANCE);
        m.put("BOOLEAN", PBoolean.INSTANCE);
        m.put("TIMESTAMP", PTimestamp.INSTANCE);
        m.put("TIME", PTime.INSTANCE);
        m.put("DATE", PDate.INSTANCE);
        m.put("VARCHAR ARRAY", PVarcharArray.INSTANCE);
        m.put("VARBINARY", PVarbinary.INSTANCE);
        m.put("BINARY", PBinary.INSTANCE);
        // Дополнительные синонимы/варианты записи, встречающиеся в реестрах/DDL
        m.put("NUMERIC", PDecimal.INSTANCE);             // синоним DECIMAL
        m.put("NUMBER", PDecimal.INSTANCE);              // частый синоним DECIMAL
        m.put("STRING", PVarchar.INSTANCE);              // синоним VARCHAR
        m.put("CHARACTER VARYING", PVarchar.INSTANCE);   // ANSI-форма VARCHAR
        m.put("BINARY VARYING", PVarbinary.INSTANCE);    // ANSI-форма VARBINARY
        TYPE_MAP = java.util.Collections.unmodifiableMap(m);
    }

    public ValueCodecPhoenix(SchemaRegistry registry) {
        this.registry = registry;
    }

    /** Быстрое разрешение PDataType с кэшированием по колонке. */
    private PDataType<?> resolvePType(TableName table, String qualifier) {
        ColKey key = new ColKey(table, qualifier);
        return typeCache.computeIfAbsent(key, k -> {
            String raw = registry.columnType(table, qualifier);
            String norm = normalizeTypeName(raw == null ? T_VARCHAR : raw);
            PDataType<?> pd = TYPE_MAP.get(norm);
            if (pd != null) return pd;
            if ("LONG".equals(norm)) return PLong.INSTANCE;    // синоним BIGINT
            if ("BOOL".equals(norm)) return PBoolean.INSTANCE; // синоним BOOLEAN
            // Неизвестный тип — предупредим один раз для этой колонки, дальше молчим (DEBUG)
            if (unknownTypeWarned.add(k)) {
                LOG.warn("Неизвестный тип Phoenix в реестре: {}.{} -> '{}' (нормализовано '{}'). Использую VARCHAR по умолчанию.",
                         table.getNameAsString(), qualifier, raw, norm);
            } else if (LOG.isDebugEnabled()) {
                LOG.debug("Повтор неизвестного типа Phoenix: {}.{} -> '{}' (нормализовано '{}')",
                          table.getNameAsString(), qualifier, raw, norm);
            }
            return PVarchar.INSTANCE;
        });
    }

    /** Компактный ключ кэша без ссылок на TableName-объекты (только строки). */
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

        @Override public int hashCode() { return hash; }

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
     * Декодирует значение колонки через Phoenix-тип из реестра.
     * Преобразования результата:
     * - TIMESTAMP/DATE/TIME -> миллисекунды epoch (long)
     * - Любой ARRAY -> List<Object> (без копии для Object[], минимальная копия для примитивов)
     * - VARBINARY/BINARY -> byte[] как есть
     * - Прочие типы возвращаются как есть
     *
     * @param table     имя таблицы (не null)
     * @param qualifier имя колонки (не null)
     * @param value     байты значения; null возвращается как null
     */
    @Override
    public Object decode(TableName table, String qualifier, byte[] value) {
        java.util.Objects.requireNonNull(table, "table");
        java.util.Objects.requireNonNull(qualifier, "qualifier");

        if (value == null) return null;

        // Получаем PDataType из локального кэша
        final PDataType<?> t = resolvePType(table, qualifier);

        // Преобразуем байты через Phoenix-тип, чтобы сохранить семантику Phoenix; добавляем диагностический контекст
        final Object obj;
        try {
            obj = t.toObject(value, 0, value.length);
        } catch (RuntimeException e) {
            throw new IllegalStateException("Не удалось преобразовать значение через Phoenix: " + table + "." + qualifier + ", тип=" + t, e);
        }

        // Единая нормализация времени: TIMESTAMP/DATE/TIME -> epoch millis
        if (obj instanceof java.sql.Timestamp) return ((java.sql.Timestamp) obj).getTime();
        if (obj instanceof java.sql.Date)      return ((java.sql.Date) obj).getTime();
        if (obj instanceof java.sql.Time)      return ((java.sql.Time) obj).getTime();

        // Массивы Phoenix конвертируем в List для удобства сериализации
        if (obj instanceof PhoenixArray) {
            return toListFromPhoenixArray((PhoenixArray) obj, table, qualifier);
        }
        return obj;
    }

    /** Преобразует PhoenixArray в List<Object>, оборачивая возможный SQLException в IllegalStateException. */
    private static java.util.List<Object> toListFromPhoenixArray(PhoenixArray pa, TableName table, String qualifier) {
        try {
            Object raw = pa.getArray();
            return toListFromRawArray(raw);
        } catch (SQLException e) {
            throw new IllegalStateException("Ошибка декодирования PhoenixArray для " + table + "." + qualifier, e);
        }
    }

    /** Универсальное преобразование массива (Object[] или примитивного) в List<Object> с минимальными аллокациями. */
    private static java.util.List<Object> toListFromRawArray(Object raw) {
        if (raw instanceof Object[]) {
            return Arrays.asList((Object[]) raw);
        }
        if (raw instanceof int[])     return boxIntArray((int[]) raw);
        if (raw instanceof long[])    return boxLongArray((long[]) raw);
        if (raw instanceof double[])  return boxDoubleArray((double[]) raw);
        if (raw instanceof float[])   return boxFloatArray((float[]) raw);
        if (raw instanceof short[])   return boxShortArray((short[]) raw);
        if (raw instanceof byte[])    return boxByteArray((byte[]) raw);
        if (raw instanceof boolean[]) return boxBooleanArray((boolean[]) raw);
        if (raw instanceof char[])    return boxCharArray((char[]) raw);
        // Фоллбек: на случай экзотических типов — отражение
        int n = java.lang.reflect.Array.getLength(raw);
        if (n == 0) return java.util.Collections.emptyList();
        ArrayList<Object> list = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            list.add(java.lang.reflect.Array.get(raw, i));
        }
        return list;
    }

    private static java.util.List<Object> boxIntArray(int[] a) {
        if (a.length == 0) return java.util.Collections.emptyList();
        ArrayList<Object> list = new ArrayList<>(a.length);
        for (int v : a) list.add(v);
        return list;
    }
    private static java.util.List<Object> boxLongArray(long[] a) {
        if (a.length == 0) return java.util.Collections.emptyList();
        ArrayList<Object> list = new ArrayList<>(a.length);
        for (long v : a) list.add(v);
        return list;
    }
    private static java.util.List<Object> boxDoubleArray(double[] a) {
        if (a.length == 0) return java.util.Collections.emptyList();
        ArrayList<Object> list = new ArrayList<>(a.length);
        for (double v : a) list.add(v);
        return list;
    }
    private static java.util.List<Object> boxFloatArray(float[] a) {
        if (a.length == 0) return java.util.Collections.emptyList();
        ArrayList<Object> list = new ArrayList<>(a.length);
        for (float v : a) list.add(v);
        return list;
    }
    private static java.util.List<Object> boxShortArray(short[] a) {
        if (a.length == 0) return java.util.Collections.emptyList();
        ArrayList<Object> list = new ArrayList<>(a.length);
        for (short v : a) list.add(v);
        return list;
    }
    private static java.util.List<Object> boxByteArray(byte[] a) {
        if (a.length == 0) return java.util.Collections.emptyList();
        ArrayList<Object> list = new ArrayList<>(a.length);
        for (byte v : a) list.add(v);
        return list;
    }
    private static java.util.List<Object> boxBooleanArray(boolean[] a) {
        if (a.length == 0) return java.util.Collections.emptyList();
        ArrayList<Object> list = new ArrayList<>(a.length);
        for (boolean v : a) list.add(v);
        return list;
    }
    private static java.util.List<Object> boxCharArray(char[] a) {
        if (a.length == 0) return java.util.Collections.emptyList();
        ArrayList<Object> list = new ArrayList<>(a.length);
        for (char v : a) list.add(v);
        return list;
    }

    /** Приводит имя типа к каноническому виду: верхний регистр, одинарные пробелы, ARRAY-формы к "<BASE> ARRAY". */
    private static String normalizeTypeName(String typeName) {
        String t = typeName == null ? "" : typeName.trim().toUpperCase(Locale.ROOT);
        if (t.isEmpty()) return T_VARCHAR;

        t = stripParenParams(t);
        t = normalizeArraySyntax(t);

        // Подчёркивания считаем пробелами (UNSIGNED_INT -> UNSIGNED INT)
        t = t.replace('_', ' ');

        // Схлопываем множественные пробелы без RegEx
        return collapseSpaces(t);
    }

    /** Удаляет параметры в круглых скобках у базового типа: VARCHAR(100) -> VARCHAR, DECIMAL(10,2) -> DECIMAL. */
    private static String stripParenParams(String t) {
        int p = t.indexOf('(');
        if (p < 0) return t;
        int q = t.indexOf(')', p + 1);
        if (q > p) {
            return (t.substring(0, p) + t.substring(q + 1)).trim();
        }
        return t.substring(0, p).trim();
    }

    /** Унифицирует записи массивов: T[] и ARRAY<T> -> "T ARRAY"; внутренний тип тоже очищается от параметров. */
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

    /** Схлопывает последовательности пробельных символов до одного пробела без использования RegEx. */
    private static String collapseSpaces(String t) {
        StringBuilder sb = new StringBuilder(t.length());
        boolean space = false;
        for (int i = 0; i < t.length(); i++) {
            char c = t.charAt(i);
            if (c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f') {
                if (!space) { sb.append(' '); space = true; }
            } else {
                sb.append(c);
                space = false;
            }
        }
        return sb.toString();
    }
}