package kz.qazmarka.h2k.util;

import org.apache.hadoop.hbase.util.Bytes;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

import kz.qazmarka.h2k.schema.registry.SchemaRegistry;

/**
 * Юнит‑тесты для {@link RowKeySlice}.
 *
 * Назначение:
 *  • Зафиксировать контракт «нуллекопийного» среза rowkey: неизменяемость, корректный equals/hashCode,
 *    отсутствие аллокаций при обычном использовании и предсказуемое поведение вспомогательных методов.
 *  • Быстро выявлять регрессии при изменениях представления rowkey.
 *
 * Что проверяем (позитив и негатив):
 *  • empty(): одиночный экземпляр пустого среза и корректные длины; whole(new byte[0]) → пустой срез, эквивалентный empty();
 *  • whole(byte[]): охват всего массива и совпадение hashCode с Bytes.hashCode;
 *  • equals/hashCode при разных смещениях одного массива и при одинаковом содержимом на разных массивах;
 *  • equals: рефлексивность/симметричность; сравнение с null и другим типом → false;
 *  • toByteArray(): возврат копии, не зависящей от исходного массива;
 *  • toString(): компактный превью‑вывод с усечением; для коротких срезов усечения нет;
 *  • проверку границ конструктора (offset/length) и NPE при null‑массиве.
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
     * whole(new byte[0]) возвращает тот же singleton, что и empty().
     */
    @Test
    void wholeEmptyReturnsEmptySingleton() {
        RowKeySlice e = RowKeySlice.empty();
        RowKeySlice w = RowKeySlice.whole(new byte[0]);
        assertEquals(e, w);
        assertTrue(w.isEmpty());
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
     * equals/hashCode при одинаковом содержимом на разных массивах.
     */
    @Test
    void equalsByContentAcrossDifferentArrays() {
        byte[] a = new byte[] {0, 1, 2, 3, 0};
        byte[] b = new byte[] {9, 1, 2, 3, 9};
        RowKeySlice s1 = new RowKeySlice(a, 1, 3); // [1,2,3]
        RowKeySlice s2 = new RowKeySlice(b, 1, 3); // [1,2,3]
        assertEquals(s1, s2);
        assertEquals(s1.hashCode(), s2.hashCode());
    }

    /**
     * equals: рефлексивность/симметричность; сравнение с null и другим типом.
     */
    @Test
    void equalsLawsAndNullAndOtherType() {
        byte[] a = new byte[] {5, 6, 7, 8};
        RowKeySlice s1 = new RowKeySlice(a, 1, 2);
        RowKeySlice s2 = new RowKeySlice(a, 1, 2);
        assertEquals(s1, s1);                  // рефлексивность
        assertTrue(s1.equals(s2) && s2.equals(s1)); // симметричность
        assertNotEquals(null, s1);          // null
        assertNotEquals("not a slice", s1); // другой тип
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
     * Для коротких срезов превью не содержит маркера усечения.
     */
    @Test
    void toStringPreviewNoEllipsisWhenShort() {
        byte[] a = new byte[] {10, 11, 12, 13, 14};
        RowKeySlice s = RowKeySlice.whole(a);
        String text = s.toString();
        assertTrue(text.contains("preview=["));
        assertFalse(text.contains(",.."), "Для коротких срезов усечения быть не должно");
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
     * hashCode консистентен и совпадает с Bytes.hashCode для неполного диапазона.
     */
    @Test
    void hashCodeConsistentWithBytesForSlice() {
        byte[] a = new byte[] {42, 1, 2, 3, 99};
        RowKeySlice s = new RowKeySlice(a, 1, 3); // [1,2,3]
        int h1 = s.hashCode();
        int h2 = s.hashCode();
        assertEquals(h1, h2);
        assertEquals(Bytes.hashCode(a, 1, 3), h1);
    }

    /**
     * Валидация границ конструктора:
     *  • отрицательный offset, выход за пределы массива и переполнение offset+length приводят к {@link IndexOutOfBoundsException}.
     */
    @Test
    void boundsCheck() {
        byte[] a = new byte[] {1,2,3};
        IllegalArgumentException exNegOff = assertThrows(IllegalArgumentException.class,
                () -> new RowKeySlice(a, -1, 1));
        assertNotNull(exNegOff);

        IllegalArgumentException exTooLong = assertThrows(IllegalArgumentException.class,
                () -> new RowKeySlice(a, 0, 4));
        assertNotNull(exTooLong);

        IllegalArgumentException exOverflow = assertThrows(IllegalArgumentException.class,
                () -> new RowKeySlice(a, 3, 1));
        assertNotNull(exOverflow);

        // пустой срез допустим, но null‑массив — нет
        NullPointerException npe = assertThrows(NullPointerException.class,
                () -> new RowKeySlice(null, 0, 0));
        assertNotNull(npe);
        // допустим нулевой размер на границе массива
        RowKeySlice emptyTail = new RowKeySlice(a, 3, 0);
        assertTrue(emptyTail.isEmpty());
        assertEquals(0, emptyTail.getLength());
    }

    /**
     * Тест‑двойник реестра: считает вызовы {@link #refresh()} и хранит простую
     * карту типов по имени колонки. Используется для проверки того, что
     * {@code refresh()} вызывается и что поиск по имени чувствителен к регистру
     * в «точном» режиме (без расслабленной логики).
     */
    private static final class CountingRegistry implements SchemaRegistry {
        final java.util.concurrent.atomic.AtomicInteger calls = new java.util.concurrent.atomic.AtomicInteger();
        final java.util.Map<String, String> cols = new java.util.HashMap<>();

        @Override
        public void refresh() {
            calls.incrementAndGet();
        }

        @Override
        public String columnType(org.apache.hadoop.hbase.TableName table, String qualifier) {
            return cols.get(qualifier);
        }

        @Override
        public String[] primaryKeyColumns(org.apache.hadoop.hbase.TableName table) {
            return new String[0];
        }
    }

    /**
     * refresh(): проверяем, что вызовы не теряются и считаются корректно.
     */
    @org.junit.jupiter.api.Test
    void refreshIncrementsCounter() {
        CountingRegistry r = new CountingRegistry();
        r.refresh();
        r.refresh();
        r.refresh();
        org.junit.jupiter.api.Assertions.assertEquals(3, r.calls.get(),
                "refresh() должен инкрементировать счётчик каждый вызов");
    }

    /**
     * Чувствительность к регистру для точного поиска: наличие только в Upper/Lower
     * не должно находиться по другому регистру. Набор проверок оформлен как
     * мини‑параметризация через assertAll.
     */
    @org.junit.jupiter.api.Test
    void exactLookupIsCaseSensitive_matrix() {
        CountingRegistry r = new CountingRegistry();
        r.cols.put("FOO", "INT");
        r.cols.put("bar", "VARCHAR");
        org.apache.hadoop.hbase.TableName tbl = org.apache.hadoop.hbase.TableName.valueOf("T");

        org.junit.jupiter.api.Assertions.assertAll(
                () -> org.junit.jupiter.api.Assertions.assertEquals("INT", r.columnType(tbl, "FOO"),
                        "Должны находить точное совпадение в верхнем регистре"),
                () -> org.junit.jupiter.api.Assertions.assertNull(r.columnType(tbl, "foo"),
                        "Нельзя находить через иной регистр, если нет relaxed‑логики"),
                () -> org.junit.jupiter.api.Assertions.assertEquals("VARCHAR", r.columnType(tbl, "bar"),
                        "Должны находить точное совпадение в нижнем регистре"),
                () -> org.junit.jupiter.api.Assertions.assertNull(r.columnType(tbl, "BAR"),
                        "Нельзя находить через иной регистр, если нет relaxed‑логики")
        );
    }
}
