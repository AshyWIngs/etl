package kz.qazmarka.h2k.util;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * Утилита для высокопроизводительной записи JSON во {@link StringBuilder}.
 * Без внешних зависимостей. Сфокусирована на минимизации аллокаций
 * и корректном экранировании строк согласно JSON.
 *
 * Поддерживаемые типы значений:
 *  - null, CharSequence/String, Boolean
 *  - Числа: Byte, Short, Integer, Long, BigInteger, Float, Double, BigDecimal
 *  - Коллекции (Collection<?>) — сериализуются как JSON‑массив
 *  - Массивы примитивов и Object[] — сериализуются как JSON‑массив
 *  - Map<?, ?> — сериализуется как JSON‑объект; ключ приводится к String.valueOf(key)
 *  - Прочие типы — строкой через String.valueOf(v)
 *
 * Валидация входных данных намеренно минимальна ради скорости. Циклы в структурах
 * (self‑reference) не отслеживаются и могут привести к глубокой рекурсии.
 *
 * Потокобезопасность: класс не хранит состояния; все методы — статические.
 *
 * Формат экранирования строк:
 *  - Спецсимволы: \" \\ \b \f \n \r \t
 *  - Управляющие U+0000..U+001F печатаются как \\u00XX (латинские hex)
 */
public final class JsonWriter {

    private JsonWriter() {}

    // Таблица для быстрого вывода \\u00XX
    private static final char[] HEX = "0123456789abcdef".toCharArray();

    /* ===================== ПУБЛИЧНЫЕ API ===================== */

    /**
     * Печатает JSON‑объект вида {"k":"v",...} из пар ключ‑значение.
     * Ключ конвертируется через String.valueOf, значение — через writeAny.
     */
    public static void writeMap(StringBuilder sb, Map<?, ?> map) {
        sb.append('{');
        Iterator<? extends Map.Entry<?, ?>> it = map.entrySet().iterator();
        boolean first = true;
        while (it.hasNext()) {
            Map.Entry<?, ?> e = it.next();
            if (!first) sb.append(',');
            writeString(sb, String.valueOf(e.getKey()));
            sb.append(':');
            writeAny(sb, e.getValue());
            first = false;
        }
        sb.append('}');
    }

    /**
     * Печатает JSON‑массив из коллекции значений (элементы через writeAny).
     */
    public static void writeArray(StringBuilder sb, Collection<?> a) {
        sb.append('[');
        Iterator<?> it = a.iterator();
        boolean first = true;
        while (it.hasNext()) {
            if (!first) sb.append(',');
            writeAny(sb, it.next());
            first = false;
        }
        sb.append(']');
    }

    /** Печатает JSON‑массив из Object[]. */
    public static void writeArray(StringBuilder sb, Object[] a) {
        sb.append('[');
        for (int i = 0; i < a.length; i++) {
            if (i > 0) sb.append(',');
            writeAny(sb, a[i]);
        }
        sb.append(']');
    }

    /** Печатает JSON‑массивы примитивов. */
    public static void writeArray(StringBuilder sb, int[] a) {
        sb.append('[');
        for (int i = 0; i < a.length; i++) {
            if (i > 0) sb.append(',');
            sb.append(a[i]);
        }
        sb.append(']');
    }

    public static void writeArray(StringBuilder sb, long[] a) {
        sb.append('[');
        for (int i = 0; i < a.length; i++) {
            if (i > 0) sb.append(',');
            sb.append(a[i]);
        }
        sb.append(']');
    }

    public static void writeArray(StringBuilder sb, double[] a) {
        sb.append('[');
        for (int i = 0; i < a.length; i++) {
            if (i > 0) sb.append(',');
            double d = a[i];
            if (Double.isFinite(d)) sb.append(d); else sb.append("null");
        }
        sb.append(']');
    }

    public static void writeArray(StringBuilder sb, float[] a) {
        sb.append('[');
        for (int i = 0; i < a.length; i++) {
            if (i > 0) sb.append(',');
            float f = a[i];
            if (Float.isFinite(f)) sb.append(f); else sb.append("null");
        }
        sb.append(']');
    }

    public static void writeArray(StringBuilder sb, boolean[] a) {
        sb.append('[');
        for (int i = 0; i < a.length; i++) {
            if (i > 0) sb.append(',');
            sb.append(a[i]);
        }
        sb.append(']');
    }

    public static void writeArray(StringBuilder sb, byte[] a) {
        sb.append('[');
        for (int i = 0; i < a.length; i++) {
            if (i > 0) sb.append(',');
            sb.append(a[i]); // как число
        }
        sb.append(']');
    }

    public static void writeArray(StringBuilder sb, char[] a) {
        sb.append('[');
        for (int i = 0; i < a.length; i++) {
            if (i > 0) sb.append(',');
            writeString(sb, String.valueOf(a[i]));
        }
        sb.append(']');
    }

    /**
     * Универсальный диспетчер печати значения.
     * null → "null"; строки — через writeString; числа — через writeNumber;
     * Map/Collection/массивы — соответствующие методы; прочее — как строка.
     */
    public static void writeAny(StringBuilder sb, Object v) {
        if (v == null) { sb.append("null"); return; }
        if (v instanceof CharSequence) { writeString(sb, String.valueOf(v)); return; }
        if (v instanceof Number) { writeNumber(sb, (Number) v); return; }
        if (v instanceof Boolean) { sb.append(((Boolean) v).booleanValue()); return; }

        if (v instanceof Map<?, ?>) { writeMap(sb, (Map<?, ?>) v); return; }
        if (v instanceof Collection) { writeArray(sb, (Collection<?>) v); return; }

        // массивы
        if (v.getClass().isArray()) {
            writeAnyArray(sb, v);
            return;
        }

        // fallback — строковое представление
        writeString(sb, String.valueOf(v));
    }

    /* ===================== ПРИВАТНЫЕ ПОМОЩНИКИ ===================== */

    private static void writeAnyArray(StringBuilder sb, Object arr) {
        if (arr instanceof Object[])   { writeArray(sb, (Object[])   arr); return; }
        if (arr instanceof int[])      { writeArray(sb, (int[])      arr); return; }
        if (arr instanceof long[])     { writeArray(sb, (long[])     arr); return; }
        if (arr instanceof double[])   { writeArray(sb, (double[])   arr); return; }
        if (arr instanceof float[])    { writeArray(sb, (float[])    arr); return; }
        if (arr instanceof boolean[])  { writeArray(sb, (boolean[])  arr); return; }
        if (arr instanceof byte[])     { writeArray(sb, (byte[])     arr); return; }
        if (arr instanceof char[])     { writeArray(sb, (char[])     arr); return; }
        // неизвестный вид массива — напечатаем как строку (без исключений)
        writeString(sb, String.valueOf(arr));
    }

    /**
     * Печать числового значения с учётом специальных случаев.
     * BigDecimal — через toPlainString(); Double/Float — NaN/Infinity → null.
     */
    public static void writeNumber(StringBuilder sb, Number n) {
        // Защита от null на случай некорректного вызова; пишем валидный JSON-литерал.
        if (n == null) {
            sb.append("null");
            return;
        }
        if (n instanceof BigDecimal) {
            sb.append(((BigDecimal) n).toPlainString());
            return;
        }
        if (n instanceof Double) {
            // Без лишнего вызова .doubleValue(); автодебоксинг в присваивании
            double d = (Double) n;
            if (Double.isFinite(d)) sb.append(d); else sb.append("null");
            return;
        }
        if (n instanceof Float) {
            // Без лишнего вызова .floatValue(); автодебоксинг в присваивании
            float f = (Float) n;
            if (Float.isFinite(f)) sb.append(f); else sb.append("null");
            return;
        }
        // Byte, Short, Integer, Long, BigInteger и другие реализации Number
        sb.append(n.toString());
    }

    /**
     * Печать JSON‑строки c экранированием специальных и управляющих символов.
     */
    public static void writeString(StringBuilder sb, String s) {
        sb.append('"');
        final int n = s.length();
        for (int i = 0; i < n; i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"':  sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '\b': sb.append("\\b");  break;
                case '\f': sb.append("\\f");  break;
                case '\n': sb.append("\\n");  break;
                case '\r': sb.append("\\r");  break;
                case '\t': sb.append("\\t");  break;
                default:
                    if (c < 0x20) {
                        sb.append("\\u00");
                        sb.append(HEX[(c >>> 4) & 0xF]);
                        sb.append(HEX[c & 0xF]);
                    } else {
                        sb.append(c);
                    }
            }
        }
        sb.append('"');
    }
}
