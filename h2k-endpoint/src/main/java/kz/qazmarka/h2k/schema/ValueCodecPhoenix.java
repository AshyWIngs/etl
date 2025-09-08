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
 * ValueCodecPhoenix — декодер значений колонок через Phoenix {@link PDataType}, опираясь на {@link SchemaRegistry}.
 *
 * Назначение
 *  • Поддерживает семантику Phoenix для широкого набора типов (UNSIGNED‑типы, TIMESTAMP/DATE/TIME, ARRAY и т.п.).
 *  • Унифицирует результат: TIMESTAMP/DATE/TIME → epoch millis (long); любой Phoenix ARRAY → {@code List<Object>};
 *    VARBINARY/BINARY → {@code byte[]} как есть; прочие типы — как вернул {@code PDataType}.
 *
 * Строгая диагностика/исключения
 *  • Если тип колонки известен (получен из {@link SchemaRegistry}), но байты не соответствуют формату,
 *    метод {@link #decode(TableName, String, byte[])} выбрасывает {@link IllegalStateException} с контекстом
 *    (таблица, колонка, тип). Это позволяет рано обнаруживать «битые» данные и ошибки конфигурации.
 *  • Для фиксированных типов дополнительно проверяется длина байтового представления (getByteSize); при расхождении выбрасывается IllegalStateException.
 *  • Входные {@code table} и {@code qualifier} обязательны: при {@code null} выбрасывается {@link NullPointerException}.
 *  • Если тип колонки в реестре неизвестен, декодер один раз пишет WARN и использует {@code VARCHAR} как дефолт.
 *  • Для массивов с неизвестным типом (не распознанным реестром/словариём) будет использован дефолтный VARCHAR; это, как правило, приведёт к ошибке декодирования, которая будет выброшена как IllegalStateException (с контекстом).
 *
 * Производительность и GC
 *  • Двухуровневый кэш соответствий (table → qualifier → {@code PDataType}) минимизирует промахи в словарь типов
 *    и аллокации на «горячем пути».
 *  • Нормализация строкового имени типа выполняется ровно один раз на колонку (при первом доступе).
 *  • Конвертация массивов делает минимум аллокаций: {@code Object[]} не копируются, примитивы боксируются линейно.
 *  • В редких ветках (ошибки/неизвестные типы) формируются диагностические строки — это не влияет на быстрый путь.
 *
 * Логи
 *  • При неизвестном типе в реестре один раз для конкретной колонки пишет WARN и использует VARCHAR по умолчанию.
 *  • Повторные промахи фиксируются в DEBUG (если включён).
 *
 * Потокобезопасность
 *  • Класс использует только потокобезопасные структуры и безопасен для многопоточности в RegionServer.
 */
public final class ValueCodecPhoenix implements Decoder {
    /** Логгер класса; все сообщения — на русском языке. */
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(ValueCodecPhoenix.class);

    /** Каноническое имя строкового типа Phoenix, используемое как дефолт. */
    private static final String T_VARCHAR = "VARCHAR";

    /** Источник знаний о типах Phoenix для колонок (JSON/System Catalog и т.п.). */
    private final SchemaRegistry registry;

    /**
     * Двухуровневый потокобезопасный кэш:
     *  верхний уровень — по TableName,
     *  нижний — по qualifier (String).
     * Минимизирует аллокации ключей на горячем пути (избегаем создания ColKey/строк из TableName).
     */
    private final ConcurrentMap<TableName, ConcurrentMap<String, PDataType<?>>> typeCache = new ConcurrentHashMap<>();

    /**
     * Набор колонок, для которых уже был выведен WARN об неизвестном типе в реестре — чтобы не зашумлять логи.
     */
    private final java.util.Set<ColKey> unknownTypeWarned = java.util.concurrent.ConcurrentHashMap.newKeySet();

    /**
     * Быстрый словарь соответствий: каноническое строковое имя Phoenix‑типа → экземпляр {@link PDataType}.
     *
     * Примечания
     *  • Ключи должны быть уже нормализованы методом {@link #normalizeTypeName(String)} (UPPERCASE, схлопнутые пробелы,
     *    «UNSIGNED INT» и т.п.).
     *  • Допускаем общепринятые синонимы (NUMERIC/NUMBER → DECIMAL, STRING → VARCHAR, ANSI формы и пр.).
     *  • Расширять словарь безопасно: добавление новых ключей не влияет на существующее поведение.
     */
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
        m.put("CHARACTER VARYING ARRAY", PVarcharArray.INSTANCE); // ANSI-форма массива VARCHAR
        m.put("STRING ARRAY", PVarcharArray.INSTANCE);            // синонимичная форма массива VARCHAR
        m.put("VARBINARY", PVarbinary.INSTANCE);
        m.put("BINARY", PBinary.INSTANCE);
        // Дополнительные синонимы/варианты записи, встречающиеся в реестрах/DDL
        m.put("NUMERIC", PDecimal.INSTANCE);             // синоним DECIMAL
        m.put("NUMBER", PDecimal.INSTANCE);              // частый синоним DECIMAL
        m.put("STRING", PVarchar.INSTANCE);              // синоним VARCHAR
        m.put("CHARACTER VARYING", PVarchar.INSTANCE);   // ANSI-форма VARCHAR
        m.put("BINARY VARYING", PVarbinary.INSTANCE);    // ANSI-форма VARBINARY
        m.put("LONG", PLong.INSTANCE);     // синоним BIGINT
        m.put("BOOL", PBoolean.INSTANCE);  // синоним BOOLEAN
        TYPE_MAP = java.util.Collections.unmodifiableMap(m);
    }

    /**
     * @param registry реализация реестра типов Phoenix для колонок; не должна быть {@code null}
     */
    public ValueCodecPhoenix(SchemaRegistry registry) {
        this.registry = registry;
    }

    /**
     * Быстрое разрешение {@link PDataType} для колонки с кэшированием по (table, qualifier).
     * Применяет нормализацию строкового имени типа, использует {@link #TYPE_MAP}, поддерживает синонимы и разные записи массивов.
     * При неизвестном типе один раз пишет WARN (per колонка) и возвращает {@link PVarchar#INSTANCE}; последующие промахи идут в DEBUG.
     */
    private PDataType<?> resolvePType(TableName table, String qualifier) {
        // Верхний уровень по TableName — без аллокаций строк из TableName на каждом вызове
        final ConcurrentMap<String, PDataType<?>> byQualifier =
                typeCache.computeIfAbsent(table, t -> new ConcurrentHashMap<>());

        // Нижний уровень по строке qualifier (она уже есть) — никаких доп. объектов
        return byQualifier.computeIfAbsent(qualifier, q -> {
            final String raw  = registry.columnType(table, q);
            final String norm = normalizeTypeName(raw == null ? T_VARCHAR : raw);

            final PDataType<?> pd = TYPE_MAP.get(norm);
            if (pd != null) {
                return pd;
            }

            // Неизвестный тип — предупредим один раз для этой колонки, дальше молчим (DEBUG)
            final ColKey warnKey = new ColKey(table, q); // создаём только в редкой ветке
            if (unknownTypeWarned.add(warnKey)) {
                LOG.warn("Неизвестный тип Phoenix в реестре: {}.{} -> '{}' (нормализовано '{}'). Использую VARCHAR по умолчанию.",
                         table.getNameAsString(), q, raw, norm);
            } else if (LOG.isDebugEnabled()) {
                LOG.debug("Повтор неизвестного типа Phoenix: {}.{} -> '{}' (нормализовано '{}')",
                          table.getNameAsString(), q, raw, norm);
            }
            return PVarchar.INSTANCE;
        });
    }

    /**
     * Компактный ключ для «warn-once» по неизвестным типам.
     * Содержит имена namespace/таблицы/квалификатора в виде строк + предвычисленный хеш.
     * Почему не {@link TableName}:
     *  • не хотим держать лишние ссылки на тяжёлые объекты в долгоживущих структурах;
     *  • предвычисленный hash удешевляет {@link #hashCode()} и сравнение в наборах.
     */
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
     * Декодирует значение колонки согласно Phoenix‑типу из реестра.
     *
     * Унификация результата
     *  • TIMESTAMP/DATE/TIME → миллисекунды epoch (long);
     *  • любой Phoenix ARRAY → {@code List<Object>} (без копии для Object[], минимальная коробка для примитивов);
     *  • VARBINARY/BINARY → {@code byte[]} как есть;
     *  • прочие типы возвращаются как есть (строки/числа/Boolean), как их выдал {@link PDataType}.
     *
     * Контракты и ошибки
     *  • {@code value == null} → возвращается {@code null} без попытки декодирования;
     *  • при несовпадении объявленного типа и фактических байтов — {@link IllegalStateException} с контекстом;
     *  • {@code table} и {@code qualifier} обязательны (при {@code null} — {@link NullPointerException}).
     *
     * @param table     имя таблицы (не {@code null})
     * @param qualifier имя колонки (не {@code null})
     * @param value     байты значения; {@code null} возвращается как {@code null}
     * @return нормализованное значение в соответствии с правилами выше
     * @throws NullPointerException  если {@code table} или {@code qualifier} равны {@code null}
     * @throws IllegalStateException если {@link PDataType#toObject(byte[], int, int)} выбросил исключение
     */
    @Override
    public Object decode(TableName table, String qualifier, byte[] value) {
        java.util.Objects.requireNonNull(table, "table");
        java.util.Objects.requireNonNull(qualifier, "qualifier");

        if (value == null) return null;

        // Получаем PDataType из локального кэша
        final PDataType<?> t = resolvePType(table, qualifier);

        // Предварительная быстрая валидация длины для фиксированных типов Phoenix (int/long/boolean/...).
        // Это позволяет рано выявить ситуацию, когда байты принадлежат другому типу (например, VARCHAR),
        // и избежать "тихих" неверных декодирований. Накладные расходы нулевые на горячем пути.
        final Integer expectedSize = t.getByteSize();
        if (expectedSize != null && expectedSize != value.length) { // auto-unboxing, избыток intValue() не нужен
            throw new IllegalStateException(
                "Несоответствие длины значения для " + table + "." + qualifier
                + ": тип=" + t + " ожидает " + expectedSize + " байт(а), получено " + value.length
            );
        }

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

    /**
     * Безопасно извлекает массив из {@link PhoenixArray} и конвертирует его в {@code List<Object>}.
     * Любой {@link SQLException} заворачивается в {@link IllegalStateException} с контекстом.
     */
    private static java.util.List<Object> toListFromPhoenixArray(PhoenixArray pa, TableName table, String qualifier) {
        try {
            Object raw = pa.getArray();
            return toListFromRawArray(raw);
        } catch (SQLException e) {
            throw new IllegalStateException("Ошибка декодирования PhoenixArray для " + table + "." + qualifier, e);
        }
    }

    /**
     * Универсальная конвертация массивов (как объектных, так и примитивных) в {@code List<Object>} с минимальными
     * аллокациями. Для Object[] используется {@link Arrays#asList(Object[])}, для примитивов — коробка значений.
     */
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

    /** Быстрая коробка примитивного массива в {@code List<Object>} без лишних аллокаций. */
    private static java.util.List<Object> boxIntArray(int[] a) {
        if (a.length == 0) return java.util.Collections.emptyList();
        ArrayList<Object> list = new ArrayList<>(a.length);
        for (int v : a) list.add(v);
        return list;
    }
    /** Быстрая коробка примитивного массива в {@code List<Object>} без лишних аллокаций. */
    private static java.util.List<Object> boxLongArray(long[] a) {
        if (a.length == 0) return java.util.Collections.emptyList();
        ArrayList<Object> list = new ArrayList<>(a.length);
        for (long v : a) list.add(v);
        return list;
    }
    /** Быстрая коробка примитивного массива в {@code List<Object>} без лишних аллокаций. */
    private static java.util.List<Object> boxDoubleArray(double[] a) {
        if (a.length == 0) return java.util.Collections.emptyList();
        ArrayList<Object> list = new ArrayList<>(a.length);
        for (double v : a) list.add(v);
        return list;
    }
    /** Быстрая коробка примитивного массива в {@code List<Object>} без лишних аллокаций. */
    private static java.util.List<Object> boxFloatArray(float[] a) {
        if (a.length == 0) return java.util.Collections.emptyList();
        ArrayList<Object> list = new ArrayList<>(a.length);
        for (float v : a) list.add(v);
        return list;
    }
    /** Быстрая коробка примитивного массива в {@code List<Object>} без лишних аллокаций. */
    private static java.util.List<Object> boxShortArray(short[] a) {
        if (a.length == 0) return java.util.Collections.emptyList();
        ArrayList<Object> list = new ArrayList<>(a.length);
        for (short v : a) list.add(v);
        return list;
    }
    /** Быстрая коробка примитивного массива в {@code List<Object>} без лишних аллокаций. */
    private static java.util.List<Object> boxByteArray(byte[] a) {
        if (a.length == 0) return java.util.Collections.emptyList();
        ArrayList<Object> list = new ArrayList<>(a.length);
        for (byte v : a) list.add(v);
        return list;
    }
    /** Быстрая коробка примитивного массива в {@code List<Object>} без лишних аллокаций. */
    private static java.util.List<Object> boxBooleanArray(boolean[] a) {
        if (a.length == 0) return java.util.Collections.emptyList();
        ArrayList<Object> list = new ArrayList<>(a.length);
        for (boolean v : a) list.add(v);
        return list;
    }
    /** Быстрая коробка примитивного массива в {@code List<Object>} без лишних аллокаций. */
    private static java.util.List<Object> boxCharArray(char[] a) {
        if (a.length == 0) return java.util.Collections.emptyList();
        ArrayList<Object> list = new ArrayList<>(a.length);
        for (char v : a) list.add(v);
        return list;
    }

    /**
     * Канонизирует строковое имя Phoenix‑типа: UPPERCASE (Locale.ROOT), удаление параметров в скобках,
     * унификация массивов к форме "BASE ARRAY", замена подчёркиваний на пробелы и схлопывание пробелов.
     * Пустая или {@code null} строка даёт {@code VARCHAR}. Все операции — без RegEx и с минимумом аллокаций.
     */
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

    /** Убирает параметры в круглых скобках у базового типа: VARCHAR(100) → VARCHAR, DECIMAL(10,2) → DECIMAL. */
    private static String stripParenParams(String t) {
        int p = t.indexOf('(');
        if (p < 0) return t;
        int q = t.indexOf(')', p + 1);
        if (q > p) {
            return (t.substring(0, p) + t.substring(q + 1)).trim();
        }
        return t.substring(0, p).trim();
    }

    /** Приводит записи массивов T[] и ARRAY<T> к единому виду: "T ARRAY" (внутренний тип тоже очищается от параметров). */
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

    /** Схлопывает последовательности пробельных символов до одного пробела без RegEx и лишних аллокаций. */
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