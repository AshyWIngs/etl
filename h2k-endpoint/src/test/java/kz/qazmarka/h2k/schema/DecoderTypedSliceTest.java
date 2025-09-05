package kz.qazmarka.h2k.schema;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Юнит‑тесты для вспомогательных типов {@link Decoder.Typed} и {@link Decoder.Typed.Slice}.
 *
 * Что проверяем
 *  • Контракт {@code Slice.EMPTY}: массив равен {@code null}, смещение и длина равны нулю; объект используется как синглтон.
 *  • Контракт фабрики {@code Slice.whole(byte[])}:
 *    — для {@code null} возвращает ровно {@code Slice.EMPTY};
 *    — для непустого массива не делает копий, выставляет {@code off=0}, {@code len=array.length}.
 *  • Обёртку {@link Decoder.Typed#decodeOrDefault(TableName, byte[], int, int, byte[], int, int, Object)}:
 *    — если декодер возвращает {@code null}, возвращается {@code defaultValue};
 *    — если возвращает ненулевое значение, оно прокидывается без изменений.
 *
 * Зачем это нужно
 *  • Эти утилиты используются в горячем пути сборки JSON‑payload, важно сохранять простую и предсказуемую семантику.
 *  • Тесты «фиксируют» API, чтобы будущие рефакторинги не меняли поведение незаметно.
 *
 * Замечания по производительности
 *  • Тесты не создают лишних копий массивов; проверяются только ссылки, смещения и длины.
 *  • Строки создаются лишь там, где это необходимо для проверки.
 */
class DecoderTypedSliceTest {

    /**
     * Проверяет инварианты пустого среза и поведение фабрики {@code whole(...)}.
     * Убеждаемся, что:
     *  • {@code EMPTY} хранит {@code null}-массив и нулевые {@code off}/{@code len};
     *  • {@code whole(null)} возвращает ровно {@code EMPTY} (та же ссылка);
     *  • {@code whole(byte[])} не копирует массив и выставляет {@code off=0}, {@code len=array.length}.
     */
    @Test
    void slice_emptyAndWhole_behaveAsDocumented() {
        Decoder.Typed.Slice empty = Decoder.Typed.Slice.EMPTY;
        assertNull(empty.a);
        assertEquals(0, empty.off);
        assertEquals(0, empty.len);

        assertSame(Decoder.Typed.Slice.EMPTY, Decoder.Typed.Slice.whole(null),
                "Для null должен вернуться EMPTY.");

        byte[] arr = new byte[]{1,2,3};
        Decoder.Typed.Slice whole = Decoder.Typed.Slice.whole(arr);
        assertSame(arr, whole.a);
        assertEquals(0, whole.off);
        assertEquals(3, whole.len);
    }

    /**
     * Проверяет, что {@code decodeOrDefault(...)} возвращает значение по умолчанию,
     * когда реализация {@link Decoder.Typed} возвращает {@code null}.
     */
    @Test
    void typed_decodeOrDefault_returnsDefaultOnNull() {
        // Декодер, который всегда возвращает null
        Decoder.Typed<String> nullTyped =
                (table, qa, qo, ql, va, vo, vl) -> null;

        Decoder.Typed.Slice q = Decoder.Typed.Slice.whole("q".getBytes(StandardCharsets.UTF_8));
        Decoder.Typed.Slice v = Decoder.Typed.Slice.whole("v".getBytes(StandardCharsets.UTF_8));

        String res = nullTyped.decodeOrDefault(null, q, v, "fallback");
        assertEquals("fallback", res);
    }

    /**
     * Проверяет, что {@code decodeOrDefault(...)} прокидывает ненулевое значение без изменений.
     * Для наглядности декодер собирает {@code qualifier} как строку из переданного среза.
     */
    @Test
    void typed_decodeOrDefault_propagatesNonNullValue() {
        // Декодер, который собирает qualifier как String
        Decoder.Typed<String> qualifierAsString =
                (table, qa, qo, ql, va, vo, vl) ->
                        (qa == null ? null : new String(qa, qo, ql, StandardCharsets.UTF_8));

        Decoder.Typed.Slice q = Decoder.Typed.Slice.whole("name".getBytes(StandardCharsets.UTF_8));
        Decoder.Typed.Slice v = Decoder.Typed.Slice.EMPTY; // не используется

        String res = qualifierAsString.decodeOrDefault(null, q, v, "fallback");
        assertEquals("name", res);
    }
}