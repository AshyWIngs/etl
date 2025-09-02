package kz.qazmarka.h2k.util;

import java.util.Objects;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Нуллекопийный срез rowkey: хранит ссылку на исходный массив байт, смещение и длину,
 * плюс предвычисленный hash. Идеален как компактный ключ в коллекциях (например, при
 * группировке Cell по rowkey).
 *
 * Иммутабельность и потокобезопасность: экземпляры неизменяемы.
 * Важно: содержит ссылку на исходный массив; не храните и не передавайте такие
 * экземпляры за пределы обработки одного WAL.Entry.
 *
 * Публичный API — только стандартные «говорящие» геттеры:
 * {@link #getArray()}, {@link #getOffset()}, {@link #getLength()}, {@link #getHash()}.
 * Сеттеров нет умышленно — объект иммутабельный.
 *
 * Контракт и безопасность: объект хранит ссылку на исходный массив и не делает копий.
 * Для долгого хранения вне обработки одного WAL.Entry используйте {@link #toByteArray()}.
 */
public final class RowKeySlice {
    /** Максимальное число байт для предпросмотра в toString() (шестнадцатерично). */
    private static final int PREVIEW_MAX = 16;

    /** Набор символов для быстрого перевода байта в hex без аллокаций строк. */
    private static final char[] HEX = "0123456789abcdef".toCharArray();

    /** Общий пустой массив для избежания аллокаций при length==0 (независим от HBase Bytes). */
    private static final byte[] EMPTY = new byte[0];

    /** Добавляет к StringBuilder две hex-цифры для байта b. */
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
     * Создаёт новый срез поверх массива байт без копирования.
     *
     * @param array исходный массив (не копируется)
     * @param offset смещение начала среза в массиве
     * @param length длина среза
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

    /** Ссылка на исходный массив rowkey (без копирования). */
    public byte[] getArray() { return array; }

    /** Смещение начала среза в массиве rowkey. */
    public int getOffset() { return offset; }

    /** Длина среза rowkey. */
    public int getLength() { return length; }

    /** Предвычисленный хеш (совместим с Bytes.hashCode). */
    public int getHash() { return hash; }

    @Override
    public int hashCode() { return hash; }

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

    /**
     * Возвращает true, если срез пуст (length == 0).
     */
    public boolean isEmpty() { return length == 0; }

    /**
     * Создаёт копию байтов этого среза. Полезно, если нужно безопасно хранить ключ дольше
     * времени жизни исходного массива (например, вне обработки одного WAL.Entry).
     */
    public byte[] toByteArray() {
        if (length == 0) return EMPTY;
        byte[] copy = new byte[length];
        System.arraycopy(array, offset, copy, 0, length);
        return copy;
    }

    /**
     * Краткое диагностическое представление: длина, смещение, хеш и предпросмотр первых байт rowkey.
     * Для безопасности и производительности выводится не более 16 байт в шестнадцатеричном виде.
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