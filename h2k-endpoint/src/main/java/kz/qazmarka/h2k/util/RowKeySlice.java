package kz.qazmarka.h2k.util;

import java.util.Objects;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Нуллекопийный срез rowkey.
 *
 * Назначение:
 *  - хранит ссылку на исходный массив байт, смещение и длину;
 *  - содержит предвычисленный хеш; подходит как компактный ключ в коллекциях
 *    (например, при группировке Cell по rowkey);
 *  - не копирует байты до явного запроса через {@link #toByteArray()}.
 *
 * Типичные сценарии:
 *  - ключ Map/Set при агрегации батча изменений в пределах одного rowkey;
 *  - быстрые сравнения/группировки без лишних аллокаций;
 *  - безопасная материализация ключа по требованию (для сериализации/логов).
 *
 * Безопасность и иммутабельность:
 *  - экземпляры неизменяемы;
 *  - объект хранит ССЫЛКУ на исходный массив — не модифицируйте его снаружи;
 *  - для долговременного хранения используйте {@link #toByteArray()}.
 *
 * Контракт равенства/хеша:
 *  - {@link #hashCode()} совместим с {@code org.apache.hadoop.hbase.util.Bytes#hashCode(byte[], int, int)};
 *  - {@link #equals(Object)} сравнивает содержимое срезов (быстрый отсев по хешу и длине,
 *    затем помасковое сравнение байтов); корректен даже при редких коллизиях хеша.
 *
 * Потокобезопасность: неизменяемость делает класс безопасным для публикации между потоками,
 * при условии, что исходный массив не модифицируется внешним кодом.
 *
 * Память/GC:
 *  - не создает копий байтов, пока не вызван {@link #toByteArray()};
 *  - есть общий пустой срез {@link #empty()} и общий пустой массив байт для минимизации аллокаций.
 */
public final class RowKeySlice {
    /**
     * Максимальное число байт для предпросмотра в {@link #toString()} (шестнадцатерично).
     * Небольшое значение защищает от избыточного вывода и лишних аллокаций при длинных ключах.
     */
    private static final int PREVIEW_MAX = 16;

    /**
     * Таблица символов для быстрого перевода байта в hex без промежуточных строк.
     * ВАЖНО: массив константный; не изменяйте его содержимое.
     */
    private static final char[] HEX = "0123456789abcdef".toCharArray();

    /**
     * Общий пустой массив для избежания лишних аллокаций при {@code length == 0}.
     * ВАЖНО: массив общий и должен рассматриваться как read‑only.
     */
    private static final byte[] EMPTY = new byte[0];

    /** Общий пустой срез (без аллокаций). */
    private static final RowKeySlice EMPTY_SLICE = new RowKeySlice(EMPTY, 0, 0);

    /**
     * Возвращает общий пустой срез (без аллокаций).
     * Полезно там, где ожидается «отсутствие ключа», но нужен объект.
     */
    public static RowKeySlice empty() { return EMPTY_SLICE; }

    /**
     * Удобная фабрика: срез на весь массив (без копирования).
     * @param array исходный массив (не копируется)
     * @return срез, покрывающий весь массив
     * @throws NullPointerException если {@code array == null}
     */
    public static RowKeySlice whole(byte[] array) { return new RowKeySlice(array, 0, array.length); }

    /**
     * Добавляет к буферу две hex‑цифры для байта {@code b}.
     * @param sb назначение вывода (ожидается не null)
     * @param b байт в виде беззнакового int [0..255]
     */
    private static void appendByteHex(StringBuilder sb, int b) {
        sb.append(HEX[(b >>> 4) & 0xF]).append(HEX[b & 0xF]);
    }

    /** Ссылка на исходный массив rowkey (НЕ копия). */
    private final byte[] array;
    /** Смещение начала среза в массиве. */
    private final int offset;
    /** Длина среза. */
    private final int length;
    /** Предвычисленный хеш (совместим с Bytes.hashCode). */
    private final int hash;

    /**
     * Создаёт новый срез поверх переданного массива без копирования.
     * @param array исходный массив (не копируется)
     * @param offset смещение начала среза в массиве
     * @param length длина среза
     * @throws NullPointerException если {@code array == null}
     * @throws IllegalArgumentException если выход за границы массива
     */
    public RowKeySlice(byte[] array, int offset, int length) {
        Objects.requireNonNull(array, "array");
        if (offset < 0 || length < 0 || offset + length > array.length) {
            throw new IllegalArgumentException(
                "offset/length out of bounds: offset=" + offset + ", length=" + length + ", array.length=" + array.length);
        }
        this.array = array;
        this.offset = offset;
        this.length = length;
        this.hash = Bytes.hashCode(array, offset, length);
    }

    /**
     * Возвращает ССЫЛКУ на исходный массив rowkey (без копирования).
     * <strong>ВАЖНО:</strong> возвращаемый массив нельзя модифицировать — это приведёт к нарушению
     * инвариантов (предвычисленный хеш останется старым). Метод предназначен для низкоуровневых
     * сценариев, где нужна совместимость с нативными API.
     */
    public byte[] getArray() { return array; }

    /** @return смещение начала среза в массиве rowkey. */
    public int getOffset() { return offset; }

    /** @return длина среза rowkey. */
    public int getLength() { return length; }

    /** @return предвычисленный хеш (совместим с Bytes.hashCode на [array, offset, length]). */
    public int getHash() { return hash; }

    /**
     * Хеш содержимого среза; вычисляется один раз в конструкторе.
     * Совместим с {@code Bytes.hashCode(array, offset, length)}.
     */
    @Override
    public int hashCode() { return hash; }

    /**
     * Равенство по содержимому: быстрый путь по хешу, затем проверка длины и побайтовое сравнение.
     * Сохраняет корректность даже при коллизиях хеша (редких): финальная проверка сравнивает байты.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RowKeySlice)) return false;
        RowKeySlice other = (RowKeySlice) o;
        // Быстрый путь: если хэши различаются — объекты точно не равны
        if (this.hash != other.hash) return false;
        // Затем проверяем длину и, только если она совпадает, сравниваем байтовые массивы
        return this.length == other.length
                && Bytes.equals(this.array, this.offset, this.length, other.array, other.offset, other.length);
    }

    /** @return {@code true}, если длина среза равна нулю. */
    public boolean isEmpty() { return length == 0; }

    /**
     * Создаёт копию байтов этого среза.
     * Полезно для безопасного хранения ключа за пределами жизни исходного массива.
     * @return новый массив байт; при нулевой длине возвращается общий {@link #EMPTY}
     */
    public byte[] toByteArray() {
        if (length == 0) return EMPTY;
        byte[] copy = new byte[length];
        System.arraycopy(array, offset, copy, 0, length);
        return copy;
    }

    /**
     * Краткое диагностическое представление: длина, смещение, хеш (hex) и предпросмотр
     * первых байт rowkey в шестнадцатеричном виде, ограниченный {@link #PREVIEW_MAX} байт.
     * Формат предпросмотра: `[xx xx ..]`, где `..` означает усечение.
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(64);
        sb.append("RowKeySlice{")
          .append("len=").append(length)
          .append(", off=").append(offset)
          .append(", hash=0x").append(Integer.toHexString(hash));
        sb.append(", preview=");
        final int n = Math.min(length, PREVIEW_MAX);
        sb.append('[');
        for (int i = 0; i < n; i++) {
            int b = array[offset + i] & 0xFF;
            if (i > 0) sb.append(' ');
            appendByteHex(sb, b);
        }
        if (length > n) sb.append(" ..");
        sb.append(']');
        sb.append('}');
        return sb.toString();
    }
}