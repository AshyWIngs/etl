package kz.qazmarka.h2k.schema;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.hadoop.hbase.TableName;

/**
 * Decoder
 * Унифицированный интерфейс для декодирования значений ячеек
 * (Cell value bytes) из HBase/Phoenix в прикладные объекты Java.
 *
 * Производительность. Для горячих путей предусмотрена быстрая перегрузка
 * decode(TableName, byte[], int, int, byte[], int, int) — она позволяет
 * избегать создания строк/копий и работать напрямую со слайсами байтов.
 *
 * Потокобезопасность. Реализация должна быть потокобезопасной
 * или иметь неизменяемое состояние, так как вызывается из параллельных потоков.
 *
 * Контракт по входным данным. Входные массивы рассматриваются как
 * только для чтения; реализация не должна модифицировать их содержимое
 * и не должна кешировать ссылку на них (при необходимости — делайте копию).
 *
 * Исключения. Checked‑исключения не выбрасываются.
 * При непарсируемом значении допустимо вернуть null либо
 * бросить непроверяемое исключение с кратким контекстом (таблица, колонка,
 * длина/префикс значения) для облегчения диагностики.
 */
@FunctionalInterface
public interface Decoder {

    /**
     * Медленная совместимая перегрузка: декодирует значение по строковому qualifier.
     * Примечание: преобразование в строку производится в UTF‑8.
     *
     * @param table     имя таблицы (для выбора типа по схеме)
     * @param qualifier имя колонки (Phoenix qualifier, как в WAL)
     * @param value     сырые байты значения (может быть null)
     * @return декодированное значение или null
     */
    Object decode(TableName table, String qualifier, byte[] value);

    /**
     * Быстрая перегрузка без создания строк и копий:
     * позволяет работать с слайсами qualifier/value напрямую.
     * По умолчанию вызывает медленную перегрузку, создав строку и копию value.
     * Реализациям рекомендуется переопределять этот метод для нулевых аллокаций.
     *
     * @param table  имя таблицы
     * @param qual   байты qualifier (может быть null)
     * @param qOff   смещение qualifier
     * @param qLen   длина qualifier
     * @param value  байты значения (может быть null)
     * @param vOff   смещение значения
     * @param vLen   длина значения
     * @return декодированное значение или null
     */
    default Object decode(TableName table,
                          byte[] qual, int qOff, int qLen,
                          byte[] value, int vOff, int vLen) {
        final String qualifier =
                (qual == null ? null : new String(qual, qOff, qLen, StandardCharsets.UTF_8));
        final byte[] valCopy =
                (value == null ? null : Arrays.copyOfRange(value, vOff, vOff + vLen));
        return decode(table, qualifier, valCopy);
    }

    /**
     * Удобная обёртка: вернуть значение или дефолт, если null.
     * Замечание по производительности: для горячих путей предпочтительнее
     * использовать быструю перегрузку и самостоятельно применять дефолт, чтобы
     * избежать лишних аллокаций.
     *
     * @param table        имя таблицы
     * @param qualifier    имя колонки (как строка)
     * @param value        байты значения
     * @param defaultValue значение по умолчанию при null
     * @return декодированное значение или defaultValue, если декодирование вернуло null
     */
    default Object decodeOrDefault(TableName table, String qualifier, byte[] value, Object defaultValue) {
        Object v = decode(table, qualifier, value);
        return (v != null) ? v : defaultValue;
    }

    /**
     * Опциональный типобезопасный декодер без автобоксинга.
     * Рекомендуется для колонок с примитивными типами (long/int/boolean и т.д.), чтобы
     * избежать аллокаций обёрток. Экземпляры типизированных декодеров можно хранить
     * в реестре схем (например, на уровне SchemaRegistry) для мгновенной выдачи
     * подходящего декодера под колонку.
     */
    @FunctionalInterface
    interface Typed<T> {
        /**
         * Быстрая типобезопасная перегрузка (нулевые аллокации при корректной реализации).
         */
        T decode(TableName table,
                 byte[] qual, int qOff, int qLen,
                 byte[] value, int vOff, int vLen);

        /**
         * Небольшая обёртка для среза байтов (массив + смещение + длина).
         * Используется для снижения количества параметров в сигнатурах (см. S107).
         * В горячих путях можно не использовать и вызывать низкоуровневые перегрузки.
         */
        final class Slice {
            public final byte[] a;
            public final int off;
            public final int len;

            private Slice(byte[] a, int off, int len) {
                this.a = a;
                this.off = off;
                this.len = len;
            }

            /** Фабрика с null‑проверкой: допустимо передавать null массив. */
            public static Slice of(byte[] a, int off, int len) {
                return new Slice(a, off, len);
            }
        }

        /**
         * Удобная обёртка с дефолтом и компактной сигнатурой (2 среза вместо 6 параметров).
         * Предпочтительная для статического анализа версия (уменьшает количество параметров).
         * Для максимальной производительности можно напрямую вызывать
         * метод decode(TableName, byte[], int, int, byte[], int, int).
         */
        default T decodeOrDefault(TableName table,
                                  Slice qual,
                                  Slice value,
                                  T defaultValue) {
            final byte[] qa = (qual == null ? null : qual.a);
            final int    qo = (qual == null ? 0    : qual.off);
            final int    ql = (qual == null ? 0    : qual.len);

            final byte[] va = (value == null ? null : value.a);
            final int    vo = (value == null ? 0    : value.off);
            final int    vl = (value == null ? 0    : value.len);

            T v = decode(table, qa, qo, ql, va, vo, vl);
            return (v != null) ? v : defaultValue;
        }
    }
}