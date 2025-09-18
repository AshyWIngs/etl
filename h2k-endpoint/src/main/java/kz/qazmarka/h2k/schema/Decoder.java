package kz.qazmarka.h2k.schema;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;

import org.apache.hadoop.hbase.TableName;

import kz.qazmarka.h2k.util.RowKeySlice;

/**
 * Унифицированный интерфейс декодирования значений ячеек (Cell value bytes)
 * из HBase/Phoenix в прикладные объекты Java.
 *
 * Назначение
 *  - предоставить общий контракт для схематических декодеров (Simple/Phoenix и т.д.);
 *  - поддержать как «медленный совместимый путь» (через строковый qualifier),
 *    так и «быстрый путь без аллокаций» (через срезы byte[]).
 *  - обеспечить декодирование составного {@code rowkey} в именованные поля значения
 *    для таблиц с составным PK; см. {@link #decodeRowKey(TableName, RowKeySlice, int, Map)}.
 *
 * Производительность
 *  - для горячего пути используйте перегрузку с срезами байтов
 *    {@link #decode(TableName, byte[], int, int, byte[], int, int)} —
 *    она позволяет не создавать промежуточные строки/копии;
 *  - дефолтная реализация «быстрой» перегрузки намеренно делает безопасные копии/преобразования,
 *    сохраняя прежнюю семантику и изоляцию от внешних модификаций массивов; конкретные реализации
 *    могут переопределить метод для нулевых аллокаций.
 *
 * Потокобезопасность
 *  - реализации должны быть неизменяемыми либо корректно синхронизированными,
 *    так как вызываются из параллельных потоков репликации.
 *
 * Контракт по входным данным
 *  - входные массивы рассматриваются как read‑only; реализация не должна их модифицировать
 *    и не должна кешировать ссылки дольше времени вызова (при необходимости делайте копию).
 *  - параметры table и qualifier являются обязательными: при {@code null} выбрасывается {@link NullPointerException}.
 *
 * Исключения
 *  - checked‑исключения интерфейс не объявляет; при непарсируемых значениях допустимо
 *    вернуть {@code null} либо бросить непроверяемое исключение с коротким контекстом
 *    (таблица, колонка, длина/префикс значения) для диагностики.
 *
 * Контракты
 *  - строгая проверка фиксированных типов/размеров для Phoenix-типов — обязанность реализации
 *    «Phoenix-кодека»; при несоответствии следует бросать {@link IllegalStateException}
 *    с диагностикой на русском;
 *  - все ссылочные аргументы считаются обязательными; при {@code null} — {@link NullPointerException};
 *  - отсутствует I/O и избыточные аллокации на горячем пути; при необходимости конкретные реализации
 *    могут переопределять дефолтные методы для нулевых аллокаций.
 *  - имена полей PK сохраняются такими, как заданы в схеме (переименование, например в {@code *_ms}, на уровне интерфейса не производится);
 *  - нормализация временных типов Phoenix (TIMESTAMP/DATE/TIME → миллисекунды эпохи) выполняется конкретной реализацией декодера; в стандартной поставке это делает {@code ValueCodecPhoenix}.
 */
@FunctionalInterface
public interface Decoder {

    /**
     * Совместимая «медленная» перегрузка: декодирует значение по строковому имени колонки.
     * Преобразование qualifier в строку предполагает UTF‑8 на стороне вызывающего кода.
     *
     * Контракт: параметры {@code table} и {@code qualifier} обязательны и не могут быть {@code null}.
     *
     * @param table     имя таблицы (может использоваться для выбора кодека по схеме), не {@code null}
     * @param qualifier имя колонки (Phoenix qualifier), не {@code null}
     * @param value     сырые байты значения, допускается {@code null}
     * @return декодированное значение или {@code null}, если не удалось распознать
     * @throws NullPointerException если {@code table} или {@code qualifier} равны {@code null}
     */
    Object decode(TableName table, String qualifier, byte[] value);

    /**
     * Быстрая перегрузка без создания строк и лишних копий: работает с срезами массивов.
     * По умолчанию метод создаёт строку для qualifier (UTF‑8) и копию value, сохраняя совместимость
     * и изоляцию от потенциальных внешних модификаций исходных массивов.
     *
     * Важно: по умолчанию копия массива value создаётся всегда — даже если передан «целый» массив
     * (off=0, len=value.length). Это гарантирует изоляцию контракта. Конкретные реализации
     * могут переопределить метод для нулевых аллокаций (не строить String для qualifier и не копировать value),
     * если это безопасно для их контракта и вызывающая сторона не мутирует массив.
     *
     * Предусловия к параметрам-срезам:
     *  — {@code qual != null} и диапазон {@code [qOff, qOff+qLen)} должен полностью попадать в {@code qual.length};
     *  — если {@code value != null}, то диапазон {@code [vOff, vOff+vLen)} должен полностью попадать в {@code value.length};
     *    при {@code value == null} параметры {@code vOff}/{@code vLen} игнорируются.
     *
     * @param table  имя таблицы
     * @param qual   массив байт qualifier, не {@code null}
     * @param qOff   смещение qualifier в массиве (≥ 0)
     * @param qLen   длина qualifier (≥ 0)
     * @param value  массив байт значения; допускается {@code null}
     * @param vOff   смещение значения в массиве (≥ 0, если {@code value != null})
     * @param vLen   длина значения (≥ 0, если {@code value != null})
     * @return декодированное значение или {@code null}
     * @throws NullPointerException     если {@code table} или {@code qual} равны {@code null}
     * @throws IndexOutOfBoundsException если любой из срезов выходит за границы соответствующего массива
     */
    default Object decode(TableName table,
                          byte[] qual, int qOff, int qLen,
                          byte[] value, int vOff, int vLen) {
        if (table == null) {
            throw new NullPointerException("Аргумент 'table' (имя таблицы) не может быть null");
        }
        // Предусловия к срезам (ручные проверки совместимые с Java 8)
        if (qual == null) {
            throw new NullPointerException("Аргумент 'qualifier' (имя колонки) не может быть null");
        }
        if (qOff < 0 || qLen < 0 || qOff > qual.length - qLen) {
            throw new IndexOutOfBoundsException(
                    "срез qualifier вне границ массива: off=" + qOff + " len=" + qLen + " cap=" + qual.length);
        }
        if (value != null && (vOff < 0 || vLen < 0 || vOff > value.length - vLen)) {
            throw new IndexOutOfBoundsException(
                    "срез value вне границ массива: off=" + vOff + " len=" + vLen + " cap=" + value.length);
        }
        final String qualifier;
        if (qOff == 0 && qLen == qual.length) {
            // Быстрый путь: используем весь массив
            qualifier = new String(qual, StandardCharsets.UTF_8);
        } else {
            qualifier = new String(qual, qOff, qLen, StandardCharsets.UTF_8);
        }
        final byte[] valCopy;
        if (value == null) {
            valCopy = null;
        } else if (vOff == 0 && vLen == value.length) {
            // Быстрый путь: копируем весь массив целиком
            valCopy = Arrays.copyOf(value, vLen);
        } else {
            // Обычный путь: копия указанного среза
            valCopy = Arrays.copyOfRange(value, vOff, vOff + vLen);
        }
        return decode(table, qualifier, valCopy);
    }

    /**
     * Удобная перегрузка с целыми массивами {@code qualifier}/{@code value}.
     * Делегирует на «быструю» перегрузку с оффсетами. Семантика копирования {@code value} по умолчанию
     * сохраняется в вызываемой перегрузке.
     *
     * Предусловия:
     *  — {@code table} и {@code qual} обязательны и не могут быть {@code null};
     *  — {@code value} допускается быть {@code null}.
     *
     * @param table имя таблицы, не {@code null}
     * @param qual  массив байт qualifier, не {@code null}
     * @param value массив байт значения; допускается {@code null}
     * @return декодированное значение или {@code null}
     * @throws NullPointerException если {@code table} или {@code qual} равны {@code null}
     */
    default Object decode(TableName table, byte[] qual, byte[] value) {
        if (table == null) {
            throw new NullPointerException("Аргумент 'table' (имя таблицы) не может быть null");
        }
        if (qual == null) {
            throw new NullPointerException("Аргумент 'qualifier' (имя колонки) не может быть null");
        }
        int qLen = qual.length;
        int vLen = (value == null ? 0 : value.length);
        return decode(table, qual, 0, qLen, value, 0, vLen);
    }

    /**
     * Удобная обёртка: вернуть значение или дефолт при {@code null}.
     * Замечание по производительности: в горячем пути предпочтительнее вызывать «быструю» перегрузку
     * и применять дефолт самостоятельно, чтобы исключить лишние аллокации.
     *
     * @param table        имя таблицы
     * @param qualifier    имя колонки (как строка), не {@code null}
     * @param value        байты значения
     * @param defaultValue значение по умолчанию
     * @return результат декодирования или {@code defaultValue}, если декодер вернул {@code null}
     * @throws NullPointerException если {@code table} или {@code qualifier} равны {@code null}
     */
    default Object decodeOrDefault(TableName table, String qualifier, byte[] value, Object defaultValue) {
        Object v = decode(table, qualifier, value);
        return (v != null) ? v : defaultValue;
    }

    /**
     * Типобезопасный декодер без автобоксинга для примитивных/конкретных типов.
     * Позволяет избежать аллокаций обёрток и работать напрямую с целевым типом {@code T}.
     * Тип {@code T} может быть как контейнером примитива (например, {@code Long}),
     * так и пользовательским POJO. Возврат {@code null} допустим как сигнал «не распознано».
     *
     * Потокобезопасность: реализации должны быть неизменяемыми или синхронизированными.
     * Контракт по входу: массивы считаются read‑only и могут быть {@code null}.
     */
    @FunctionalInterface
    interface Typed<T> {
        /**
         * Быстрая типобезопасная перегрузка (нулевые аллокации при корректной реализации).
         *
         * Предусловия:
         *  — если {@code qual != null}, диапазон {@code [qOff, qOff + qLen)} обязан полностью
         *    находиться в пределах {@code qual.length};
         *  — если {@code value != null}, диапазон {@code [vOff, vOff + vLen)} обязан полностью
         *    находиться в пределах {@code value.length}.
         *  Валидация диапазонов может не выполняться реализацией — ответственность вызывающей стороны;
         *  при нарушении возможен {@link IndexOutOfBoundsException}.
         *
         * @param table имя таблицы
         * @param qual  массив байт qualifier (или {@code null})
         * @param qOff  смещение qualifier
         * @param qLen  длина qualifier
         * @param value массив байт значения (или {@code null})
         * @param vOff  смещение значения
         * @param vLen  длина значения
         * @return декодированное значение типа {@code T} или {@code null}
         * @throws IndexOutOfBoundsException если указанные срезы выходят за границы соответствующих массивов
         */
        T decode(TableName table,
                 byte[] qual, int qOff, int qLen,
                 byte[] value, int vOff, int vLen);

        /**
         * Компактный срез массива байт (массив + смещение + длина).
         * Уменьшает количество параметров в сигнатурах и облегчает статический анализ.
         * Валидность границ **не** проверяется на уровне фабрик — ответственность вызывающей стороны.
         */
        final class Slice {
            /** Исходный массив (допускается {@code null}). */
            public final byte[] a;
            /** Смещение начала среза в массиве. */
            public final int off;
            /** Длина среза в байтах. */
            public final int len;
            /** Пустой срез (эквивалент {@code a=null, off=0, len=0}). */
            public static final Slice EMPTY = new Slice(null, 0, 0);

            private Slice(byte[] a, int off, int len) {
                this.a = a;
                this.off = off;
                this.len = len;
            }

            /**
             * Фабрика без проверок: допустимы {@code null} массив и любые границы.
             * Вызов обязан гарантировать корректность диапазона на стороне клиента.
             */
            public static Slice of(byte[] a, int off, int len) {
                return new Slice(a, off, len);
            }

            /**
             * Срез всего массива (или {@link #EMPTY}, если массив {@code null}).
             */
            public static Slice whole(byte[] a) {
                return (a == null) ? EMPTY : new Slice(a, 0, a.length);
            }
        }

        /**
         * Удобная обёртка с дефолтом и компактной сигнатурой (2 среза вместо 6 параметров).
         * Для максимальной производительности возможно прямое обращение к
         * {@link #decode(TableName, byte[], int, int, byte[], int, int)}.
         *
         * @param table        имя таблицы
         * @param qual         срез qualifier (или {@code null})
         * @param value        срез значения (или {@code null})
         * @param defaultValue значение по умолчанию
         * @return результат декодирования или {@code defaultValue}, если декодер вернул {@code null}
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

    /**
     * Декодирует составной {@code rowkey} в именованные поля значения.
     * Реализация по умолчанию — no-op (для простых декодеров, которые не знают о составе PK).
     *
     * Контракт:
     * - Метод не бросает исключения; при отсутствии описания PK или невозможности декодирования — ничего не делает.
     * - Реализация, понимающая схему Phoenix, обязана учитывать соль ({@code saltBytes}) и корректно сдвигать/обрезать ключ.
     *   Имена ключей PK помещаются в {@code out} без переименования (как в схеме).
     *   Если PK-колонка имеет временной тип Phoenix, нормализация значения (например, в миллисекунды эпохи)
     *   выполняется реализацией декодера (в стандартной реализации — {@code ValueCodecPhoenix}).
     *
     * — Коллекция {@code out} не очищается реализацией; записи добавляются/переопределяются по месту.
     */
    default void decodeRowKey(TableName table,
                              RowKeySlice rk,
                              int saltBytes,
                              Map<String, Object> out) {
        // no-op по умолчанию
    }
}