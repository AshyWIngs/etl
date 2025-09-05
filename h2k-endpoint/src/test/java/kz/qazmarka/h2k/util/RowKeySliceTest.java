package kz.qazmarka.h2k.util;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Юнит‑тесты для {@link RowKeySlice}.
 *
 * Назначение:
 *  • Зафиксировать контракт «нуллекопийного» среза rowkey: неизменяемость, корректный equals/hashCode,
 *    отсутствие аллокаций при обычном использовании и предсказуемое поведение вспомогательных методов.
 *  • Быстро выявлять регрессии при изменениях представления rowkey.
 *
 * Что проверяем:
 *  • empty(): одиночный экземпляр пустого среза и корректные длины;
 *  • whole(byte[]): охват всего массива и совпадение hashCode с Bytes.hashCode;
 *  • equals/hashCode при разных смещениях одного массива;
 *  • toByteArray(): возврат копии, не зависящей от исходного массива;
 *  • toString(): компактный превью‑вывод с усечением;
 *  • проверку границ конструктора (offset/length).
 *
 * Эти тесты не зависят от HBase‑окружения и выполняются за миллисекунды.
 *
 * @see RowKeySlice
 */
class RowKeySliceTest {

    /**
     * Проверяет контракт {@code empty()}:
     *  • singleton‑экземпляр;
     *  • нулевая длина и пустой массив у {@code toByteArray()}.
     */
    @Test
    void emptySingletonAndLength() {
        RowKeySlice e1 = RowKeySlice.empty();
        RowKeySlice e2 = RowKeySlice.empty();
        assertSame(e1, e2, "empty() должен возвращать один и тот же экземпляр");
        assertTrue(e1.isEmpty());
        assertEquals(0, e1.getLength());
        assertEquals(0, e1.toByteArray().length);
    }

    /**
     * Проверяет фабрику {@code whole(byte[])}:
     *  • срез покрывает весь массив (offset=0, length=array.length);
     *  • предвычисленный hashCode совпадает с {@code Bytes.hashCode(array,0,len)}.
     */
    @Test
    void wholeCoversArrayAndHashMatchesHBase() {
        byte[] a = new byte[] {1,2,3,4};
        RowKeySlice s = RowKeySlice.whole(a);
        assertSame(a, s.getArray());
        assertEquals(0, s.getOffset());
        assertEquals(a.length, s.getLength());
        assertEquals(Bytes.hashCode(a, 0, a.length), s.hashCode());
    }

    /**
     * Равенство/неравенство по содержимому:
     *  • два среза одного диапазона равны и имеют одинаковый hashCode;
     *  • сдвиг диапазона даёт неравенство.
     */
    @Test
    void equalsByContentWithDifferentOffsets() {
        byte[] a = new byte[] {9, 1, 2, 3, 4, 9};
        RowKeySlice s1 = new RowKeySlice(a, 1, 4); // [1,2,3,4]
        RowKeySlice s2 = new RowKeySlice(a, 1, 4); // тот же диапазон
        assertEquals(s1, s2);
        assertEquals(s1.hashCode(), s2.hashCode());

        RowKeySlice s3 = new RowKeySlice(a, 2, 4); // [2,3,4,9] — другой контент
        assertNotEquals(s1, s3);
    }

    /**
     * {@code toByteArray()} возвращает копию:
     *  • дальнейшая модификация исходного массива не влияет на возвращённый байтовый массив.
     */
    @Test
    void toByteArrayCopies() {
        byte[] a = new byte[] {7,7,7};
        RowKeySlice s = new RowKeySlice(a, 1, 2); // [7,7]
        byte[] copy = s.toByteArray();
        assertArrayEquals(new byte[] {7,7}, copy);
        // меняем исходный массив — копия не должна измениться
        a[1] = 9;
        assertArrayEquals(new byte[] {7,7}, copy);
    }

    /**
     * {@code toString()} формирует компактное превью:
     *  • присутствует маркер превью и признак усечения, чтобы лог не был «шумным».
     */
    @Test
    void toStringPreviewIsBounded() {
        byte[] a = new byte[64];
        for (int i = 0; i < a.length; i++) a[i] = (byte) i;
        RowKeySlice s = RowKeySlice.whole(a);
        String text = s.toString();
        // просто проверка, что строка строится и есть признак усечения
        assertTrue(text.contains("preview=["));
        assertTrue(text.contains(".."));
    }

    /**
     * Валидация границ конструктора:
     *  • отрицательный offset, выход за пределы массива и переполнение offset+length приводят к {@link IllegalArgumentException}.
     */
    @Test
    void boundsCheck() {
        byte[] a = new byte[] {1,2,3};
        assertThrows(IllegalArgumentException.class, () -> new RowKeySlice(a, -1, 1));
        assertThrows(IllegalArgumentException.class, () -> new RowKeySlice(a, 0, 4));
        assertThrows(IllegalArgumentException.class, () -> new RowKeySlice(a, 3, 1));
    }
}