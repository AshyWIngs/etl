package kz.qazmarka.h2k.schema.phoenix;

import java.lang.reflect.Array;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import org.apache.hadoop.hbase.TableName;
import org.apache.phoenix.schema.types.PhoenixArray;

/**
 * Набор утилит для нормализации значений Phoenix: время → epoch millis, массивы → {@code List<Object>}.
 */
public final class PhoenixValueNormalizer {

    private static final Map<Class<?>, UnaryOperator<Object>> TEMP_NORMALIZERS = buildTemporalNormalizers();

    private PhoenixValueNormalizer() {
    }

    /**
     * Приводит значение Phoenix к удобному виду: нормализует время и разворачивает массивы.
     *
     * @param value     исходное значение Phoenix
     * @param table     таблица (для сообщений об ошибках)
     * @param qualifier имя колонки (для сообщений об ошибках)
     * @return нормализованное значение (может быть {@code null})
     */
    public static Object normalizeValue(Object value, TableName table, String qualifier) {
        if (value == null) return null;
        Object temporal = normalizeTemporal(value);
        if (temporal instanceof PhoenixArray) {
            return toListFromPhoenixArray((PhoenixArray) temporal, table, qualifier);
        }
        return temporal;
    }

    /** Преобразует временные типы Phoenix (Timestamp/Date/Time) в epoch millis. */
    static Object normalizeTemporal(Object valueObj) {
        if (valueObj == null) return null;
        UnaryOperator<Object> f = TEMP_NORMALIZERS.get(valueObj.getClass());
        return (f != null) ? f.apply(valueObj) : valueObj;
    }

    private static Map<Class<?>, UnaryOperator<Object>> buildTemporalNormalizers() {
        java.util.HashMap<Class<?>, UnaryOperator<Object>> m = new java.util.HashMap<>(4);
        m.put(Timestamp.class, v -> ((Timestamp) v).getTime());
        m.put(Date.class, v -> ((Date) v).getTime());
        m.put(Time.class, v -> ((Time) v).getTime());
        return Collections.unmodifiableMap(m);
    }

    private static List<Object> toListFromPhoenixArray(PhoenixArray pa, TableName table, String qualifier) {
        try {
            Object raw = pa.getArray();
            return toListFromRawArray(raw);
        } catch (SQLException e) {
            throw new IllegalStateException("Ошибка декодирования PhoenixArray для " + table + "." + qualifier, e);
        }
    }

    /**
     * Преобразует массив Phoenix (объектный или примитивный) в список Java, минимизируя аллокации.
     */
    /** Преобразует массив Phoenix (объектный/примитивный) в список без лишних копий. */
    public static List<Object> toListFromRawArray(Object raw) {
        if (raw instanceof Object[]) {
            return Arrays.asList((Object[]) raw);
        }
        if (raw instanceof int[]) return boxIntArray((int[]) raw);
        if (raw instanceof long[]) return boxLongArray((long[]) raw);
        if (raw instanceof double[]) return boxDoubleArray((double[]) raw);
        if (raw instanceof float[]) return boxFloatArray((float[]) raw);
        if (raw instanceof short[]) return boxShortArray((short[]) raw);
        if (raw instanceof byte[]) return boxByteArray((byte[]) raw);
        if (raw instanceof boolean[]) return boxBooleanArray((boolean[]) raw);
        if (raw instanceof char[]) return boxCharArray((char[]) raw);
        int n = Array.getLength(raw);
        if (n == 0) return Collections.emptyList();
        ArrayList<Object> list = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            list.add(Array.get(raw, i));
        }
        return list;
    }

    private static List<Object> boxIntArray(int[] a) {
        if (a.length == 0) return Collections.emptyList();
        ArrayList<Object> list = new ArrayList<>(a.length);
        for (int v : a) list.add(v);
        return list;
    }

    private static List<Object> boxLongArray(long[] a) {
        if (a.length == 0) return Collections.emptyList();
        ArrayList<Object> list = new ArrayList<>(a.length);
        for (long v : a) list.add(v);
        return list;
    }

    private static List<Object> boxDoubleArray(double[] a) {
        if (a.length == 0) return Collections.emptyList();
        ArrayList<Object> list = new ArrayList<>(a.length);
        for (double v : a) list.add(v);
        return list;
    }

    private static List<Object> boxFloatArray(float[] a) {
        if (a.length == 0) return Collections.emptyList();
        ArrayList<Object> list = new ArrayList<>(a.length);
        for (float v : a) list.add(v);
        return list;
    }

    private static List<Object> boxShortArray(short[] a) {
        if (a.length == 0) return Collections.emptyList();
        ArrayList<Object> list = new ArrayList<>(a.length);
        for (short v : a) list.add(v);
        return list;
    }

    private static List<Object> boxByteArray(byte[] a) {
        if (a.length == 0) return Collections.emptyList();
        ArrayList<Object> list = new ArrayList<>(a.length);
        for (byte v : a) list.add(v);
        return list;
    }

    private static List<Object> boxBooleanArray(boolean[] a) {
        if (a.length == 0) return Collections.emptyList();
        ArrayList<Object> list = new ArrayList<>(a.length);
        for (boolean v : a) list.add(v);
        return list;
    }

    private static List<Object> boxCharArray(char[] a) {
        if (a.length == 0) return Collections.emptyList();
        ArrayList<Object> list = new ArrayList<>(a.length);
        for (char v : a) list.add(v);
        return list;
    }
}
