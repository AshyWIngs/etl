package kz.qazmarka.h2k.schema;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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
    private final SchemaRegistry registry;

    /** Локальный кэш сопоставлений (table, qualifier) -> PDataType для устранения повторной нормализации строк типов. */
    private final ConcurrentMap<ColKey, PDataType<?>> typeCache = new ConcurrentHashMap<>();

    /** Быстрый словарь соответствий: строковое имя типа -> PDataType. */
    private static final Map<String, PDataType<?>> TYPE_MAP;
    static {
        Map<String, PDataType<?>> m = new HashMap<>();
        m.put("VARCHAR", PVarchar.INSTANCE);
        m.put("CHAR", PChar.INSTANCE);
        m.put("UNSIGNED_TINYINT", PUnsignedTinyint.INSTANCE);
        m.put("UNSIGNED_SMALLINT", PUnsignedSmallint.INSTANCE);
        m.put("UNSIGNED_INT", PUnsignedInt.INSTANCE);
        m.put("UNSIGNED_LONG", PUnsignedLong.INSTANCE);
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
        TYPE_MAP = java.util.Collections.unmodifiableMap(m);
    }

    public ValueCodecPhoenix(SchemaRegistry registry) {
        this.registry = registry;
    }

    /** Быстрое разрешение PDataType с кэшированием по колонке. */
    private PDataType<?> resolvePType(TableName table, String qualifier) {
        ColKey key = new ColKey(table, qualifier);
        return typeCache.computeIfAbsent(key, k -> map(registry.columnType(table, qualifier)));
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

    @Override
    public Object decode(TableName table, String qualifier, byte[] value) {
        if (value == null) return null;

        // Получаем PDataType из локального кэша
        final PDataType<?> t = resolvePType(table, qualifier);

        // Преобразуем байты через Phoenix-тип, чтобы сохранить семантику Phoenix; добавляем диагностический контекст
        final Object obj;
        try {
            obj = t.toObject(value, 0, value.length);
        } catch (RuntimeException e) {
            throw new IllegalStateException("Phoenix toObject failed for " + table + "." + qualifier + " ptype=" + t, e);
        }

        // Единая нормализация времени: TIMESTAMP/DATE/TIME -> epoch millis
        if (obj instanceof java.sql.Timestamp) return ((java.sql.Timestamp) obj).getTime();
        if (obj instanceof java.sql.Date)      return ((java.sql.Date) obj).getTime();
        if (obj instanceof java.sql.Time)      return ((java.sql.Time) obj).getTime();

        // Массивы Phoenix конвертируем в List для удобства сериализации
        if (obj instanceof PhoenixArray) {
            try {
                Object raw = ((PhoenixArray) obj).getArray(); // может кидать SQLException
                if (raw instanceof Object[]) {
                    // Нулевая копия
                    return Arrays.asList((Object[]) raw);
                }
                // Фоллбек для примитивных массивов (int[], long[] и т.п.)
                int n = java.lang.reflect.Array.getLength(raw);
                ArrayList<Object> list = new ArrayList<>(n);
                for (int i = 0; i < n; i++) {
                    list.add(java.lang.reflect.Array.get(raw, i)); // автобоксинг примитивов
                }
                return list;
            } catch (SQLException e) {
                throw new IllegalStateException("Ошибка декодирования PhoenixArray для " + table + "." + qualifier, e);
            }
        }
        return obj;
    }

    /**
     * Маппинг строкового имени типа Phoenix -> соответствующий PDataType.
     * Неизвестный тип трактуем как VARCHAR.
     */
    private static PDataType<?> map(String typeName) {
        if (typeName == null) return PVarchar.INSTANCE; // по умолчанию строка
        String t = typeName.trim().toUpperCase();
        PDataType<?> pd = TYPE_MAP.get(t);
        return (pd != null) ? pd : PVarchar.INSTANCE;
    }
}