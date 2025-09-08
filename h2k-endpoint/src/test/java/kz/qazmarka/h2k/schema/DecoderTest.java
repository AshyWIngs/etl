package kz.qazmarka.h2k.schema;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

import org.apache.hadoop.hbase.TableName;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

/**
 * Юнит‑тесты контракта интерфейса {@link Decoder}.
 *
 * Цели:
 *  • Зафиксировать поведение «быстрой» перегрузки с срезами:
 *    - qualifier всегда строится как String в кодировке UTF‑8;
 *    - value всегда копируется (изоляция от внешних мутаций), независимо от смещений.
 *  • Проверить удобные обёртки {@link Decoder#decode(org.apache.hadoop.hbase.TableName, byte[], byte[])}
 *    и {@link Decoder#decodeOrDefault(org.apache.hadoop.hbase.TableName, String, byte[], Object)}.
 *
 * Контракт:
 * • Параметры table и qualifier обязательны (не null) для всех перегрузок; при null — NPE (fail‑fast).
 *
 * Эти тесты не касаются конкретной логики декодирования значений — они проверяют только
 * корректную работу перегрузок, копирование массивов и стабильность строкового qualifier.
 */
class DecoderTest {

    private static final TableName TBL = TableName.valueOf("DEFAULT:UT_DECODER");

    /**
     * Вспомогательный контейнер для проверки того, что базовая перегрузка
     * {@link Decoder#decode(org.apache.hadoop.hbase.TableName, String, byte[])}
     * получила ожидаемые данные.
     *
     * qualifier — строка, собранная из байтов UTF‑8;
     * value — копия фрагмента исходного массива value.
     */
    private static final class Holder {
        final String qualifier;
        final byte[] value;
        Holder(String q, byte[] v) { this.qualifier = q; this.value = v; }
    }

    /**
     * Тестовый декодер‑«эхо».
     * Возвращает {@link Holder} с теми же qualifier и value, которые получил,
     * что позволяет прозрачно проверить прокидывание параметров всеми перегрузками.
     */
    private static final class EchoDecoder implements Decoder {
        @Override
        public Object decode(org.apache.hadoop.hbase.TableName table, String qualifier, byte[] value) {
            Objects.requireNonNull(table, "table");
            Objects.requireNonNull(qualifier, "qualifier");
            return new Holder(qualifier, value);
        }
    }

    /**
     * Тестовый декодер, всегда возвращающий {@code null}.
     * Используется для проверки поведения {@link Decoder#decodeOrDefault(org.apache.hadoop.hbase.TableName, String, byte[], Object)}.
     */
    private static final class NullDecoder implements Decoder {
        @Override
        public Object decode(org.apache.hadoop.hbase.TableName table, String qualifier, byte[] value) {
            return null;
        }
    }

    /**
     * Проверяет, что «быстрая» перегрузка
     * {@link Decoder#decode(org.apache.hadoop.hbase.TableName, byte[], int, int, byte[], int, int)}
     * при передаче полных массивов:
     *  • собирает qualifier как строку UTF‑8;
     *  • копирует весь массив value (другая ссылка);
     *  • копия не меняется при последующей мутации исходного массива.
     */
    @Test
    void fastOverload_copiesWholeArrays_andBuildsQualifierString() {
        Decoder d = new EchoDecoder();

        byte[] qual = "colX".getBytes(StandardCharsets.UTF_8);
        byte[] val  = new byte[] {1, 2, 3, 4};

        Holder h = (Holder) d.decode(TBL, qual, 0, qual.length, val, 0, val.length);

        assertEquals("colX", h.qualifier, "Qualifier должен быть собран в String (UTF-8).");
        assertNotSame(val, h.value, "Value должно быть скопировано (другая ссылка).");
        assertArrayEquals(new byte[]{1,2,3,4}, h.value, "Контент копии совпадает с исходным.");

        // Мутируем исходник — копия в Holder не должна измениться
        val[0] = 9;
        assertArrayEquals(new byte[]{1,2,3,4}, h.value, "Копия изолирована от мутаций исходного массива.");
    }

    /**
     * Проверяет корректность работы срезов в «быстрой» перегрузке:
     *  • qualifier строится из подмассива байт (qOff,qLen);
     *  • value копируется из подмассива (vOff,vLen);
     *  • дальнейшие изменения исходного массива не влияют на копию.
     */
    @Test
    void fastOverload_copiesSlices_correctly() {
        Decoder d = new EchoDecoder();

        byte[] qual = "abcde".getBytes(StandardCharsets.UTF_8); // возьмём "bcd"
        // В этом массиве будем проверять срез индексов 1..3 (значения 20, 30, 40)
        byte[] val  = new byte[] {10, 20, 30, 40, 50};

        Holder h = (Holder) d.decode(TBL, qual, 1, 3, val, 1, 3);

        assertEquals("bcd", h.qualifier);
        assertArrayEquals(new byte[]{20,30,40}, h.value);

        // Мутируем исходный массив value в зоне ранее скопированного среза
        val[1] = 99;
        assertArrayEquals(new byte[]{20,30,40}, h.value, "Копия среза изолирована от последующих мутаций.");
    }

    /**
     * Проверяет удобную перегрузку {@link Decoder#decode(org.apache.hadoop.hbase.TableName, byte[], byte[])},
     * что она делегирует «быстрой» версии, формирует строковый qualifier и копирует массив value.
     */
    @Test
    void convenienceOverload_withWholeArrays_delegatesToFastAndCopies() {
        Decoder d = new EchoDecoder();

        byte[] qual = "q".getBytes(StandardCharsets.UTF_8);
        byte[] val  = new byte[] {42};

        Holder h = (Holder) d.decode(TBL, qual, val);
        assertEquals("q", h.qualifier);
        assertNotSame(val, h.value);
        assertArrayEquals(new byte[]{42}, h.value);
    }

    /**
     * Проверяет, что {@link Decoder#decodeOrDefault(org.apache.hadoop.hbase.TableName, String, byte[], Object)}
     * возвращает значение по умолчанию, если декодер вернул {@code null}.
     */
    @Test
    void decodeOrDefault_returnsFallbackWhenDecoderReturnsNull() {
        Decoder d = new NullDecoder();
        Object res = d.decodeOrDefault(TBL, "ignored", new byte[]{1}, 777);
        assertEquals(777, res);
    }

    /**
     * Негатив: fail‑fast NPE при null table/qualifier для удобных перегрузок.
     */
    @Test
    void negative_failFast_onNullArgs() {
        Decoder d = new EchoDecoder();
        byte[] bytes = new byte[] {1};

        NullPointerException ex1 = assertThrows(NullPointerException.class, () -> d.decode(null, "q", bytes));
        assertTrue(ex1.getMessage() == null || ex1.getMessage().toLowerCase().contains("table"),
                "Сообщение NPE должно указывать на параметр table");

        NullPointerException ex2 = assertThrows(NullPointerException.class, () -> d.decode(TBL, (String) null, bytes));
        assertTrue(ex2.getMessage() == null || ex2.getMessage().toLowerCase().contains("qualifier"),
                "Сообщение NPE должно указывать на параметр qualifier");

        byte[] qual = "q".getBytes(StandardCharsets.UTF_8);
        NullPointerException ex3 = assertThrows(NullPointerException.class, () -> d.decode(null, qual, bytes));
        assertTrue(ex3.getMessage() == null || ex3.getMessage().toLowerCase().contains("table"),
                "Сообщение NPE должно указывать на параметр table");

        NullPointerException ex4 = assertThrows(NullPointerException.class, () -> d.decode(TBL, (byte[]) null, bytes));
        assertTrue(ex4.getMessage() == null || ex4.getMessage().toLowerCase().contains("qualifier"),
                "Сообщение NPE должно указывать на параметр qualifier");
    }

    // -------- Typed API (Decoder.Typed) — объединено сюда из DecoderTypedSliceTest --------

    /**
     * Дополнение: whole(new byte[0]) возвращает срез с тем же массивом, off=0, len=0 (zero‑copy).
     */
    @Test
    void typed_slice_whole_emptyArray_zeroLenZeroOff() {
        byte[] arr = new byte[0];
        Decoder.Typed.Slice s = Decoder.Typed.Slice.whole(arr);
        assertSame(arr, s.a, "Должна сохраниться та же ссылка на массив");
        assertEquals(0, s.off);
        assertEquals(0, s.len);
    }

    /**
     * Если декодер возвращает null и defaultValue == null, результат тоже null.
     */
    @Test
    void typed_decodeOrDefault_nullDefault_staysNull() {
        Decoder.Typed<String> nullTyped =
                (table, qa, qo, ql, va, vo, vl) -> null;

        Decoder.Typed.Slice q = Decoder.Typed.Slice.whole("q".getBytes(StandardCharsets.UTF_8));
        Decoder.Typed.Slice v = Decoder.Typed.Slice.EMPTY;

        String res = nullTyped.decodeOrDefault(TBL, q, v, null);
        assertNull(res);
    }

    /**
     * typed.decodeOrDefault корректно передаёт off/len внутрь декодера:
     * из массива "qualifier" берём подстроку "ali".
     */
    @Test
    void typed_decodeOrDefault_respectsOffsets() {
        Decoder.Typed<String> toStringSlice =
                (table, qa, qo, ql, va, vo, vl) -> qa == null ? null : new String(qa, qo, ql, StandardCharsets.UTF_8);

        byte[] src = "qualifier".getBytes(StandardCharsets.UTF_8);
        // подстрока "ali" начинается с индекса 2 и длиной 3 — собираем подмассив и передаём как Slice
        byte[] ali = new byte[3];
        System.arraycopy(src, 2, ali, 0, 3);
        Decoder.Typed.Slice q = Decoder.Typed.Slice.whole(ali);
        Decoder.Typed.Slice v = Decoder.Typed.Slice.EMPTY;

        String res = toStringSlice.decodeOrDefault(TBL, q, v, "fallback");
        assertEquals("ali", res);
    }

    /**
     * typed.decodeOrDefault: при ненулевом значении возвращается оно, а не default.
     */
    @Test
    void typed_decodeOrDefault_propagatesNonNullValue() {
        Decoder.Typed<String> qualifierAsString =
                (table, qa, qo, ql, va, vo, vl) -> qa == null ? null : new String(qa, qo, ql, StandardCharsets.UTF_8);

        Decoder.Typed.Slice q = Decoder.Typed.Slice.whole("qq".getBytes(StandardCharsets.UTF_8));
        Decoder.Typed.Slice v = Decoder.Typed.Slice.EMPTY;

        String res = qualifierAsString.decodeOrDefault(TBL, q, v, "fallback");
        assertEquals("qq", res);
    }

    /**
     * typed.decodeOrDefault: если декодер вернул null — отдаём default.
     */
    @Test
    void typed_decodeOrDefault_returnsDefaultOnNull() {
        Decoder.Typed<String> nullTyped =
                (table, qa, qo, ql, va, vo, vl) -> null;

        Decoder.Typed.Slice q = Decoder.Typed.Slice.whole("x".getBytes(StandardCharsets.UTF_8));
        Decoder.Typed.Slice v = Decoder.Typed.Slice.EMPTY;

        String res = nullTyped.decodeOrDefault(TBL, q, v, "fallback");
        assertEquals("fallback", res);
    }
}