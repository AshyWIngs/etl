package kz.qazmarka.h2k.schema;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

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
 * Допущения:
 *  • В тестах в качестве TableName передаётся null — это допустимо по контракту и исключает
 *    побочные эффекты инициализации HBase/Log4j.
 *
 * Эти тесты не касаются конкретной логики декодирования значений — они проверяют только
 * корректную работу перегрузок, копирование массивов и стабильность строкового qualifier.
 */
class DecoderTest {

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

        Holder h = (Holder) d.decode(null, qual, 0, qual.length, val, 0, val.length);

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

        Holder h = (Holder) d.decode(null, qual, 1, 3, val, 1, 3);

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

        Holder h = (Holder) d.decode(null, qual, val);
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
        Object res = d.decodeOrDefault(null, "ignored", new byte[]{1}, 777);
        assertEquals(777, res);
    }
}