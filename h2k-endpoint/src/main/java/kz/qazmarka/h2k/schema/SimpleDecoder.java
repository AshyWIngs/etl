package kz.qazmarka.h2k.schema;

import org.apache.hadoop.hbase.TableName;
import java.util.Arrays;

/**
 * Простейший декодер значений HBase без знания схемы (raw/zero‑copy).
 *
 * Назначение:
 *  • Вернуть исходный массив байт без каких‑либо преобразований и копирований.
 *  • Использоваться там, где значения трактуются/переносятся «как есть».
 *
 * Основные свойства:
 *  • Zero‑copy: метод возвращает ту же ссылку на {@code byte[]}, что и получил.
 *  • Поддерживает {@code null} вход — вернёт {@code null}.
 *  • Без аллокаций и без синхронизации; потокобезопасен и пригоден для переиспользования.
 *
 * Производительность и GC:
 *  • Экземпляр создаётся один раз (см. {@link #INSTANCE}) и переиспользуется.
 *  • Вызовы {@link #decode(TableName, String, byte[])} и {@link #decode(TableName, byte[], byte[])} — O(1), выделений памяти нет.
 *
 * Как использовать:
 *  • {@code SimpleDecoder.INSTANCE.decode(table, qualifier, valueBytes)}
 *  • Или передайте {@link #INSTANCE} туда, где требуется {@link Decoder}.
 *
 * Когда не подходит:
 *  • Если нужно типизировать значение согласно Phoenix/JSON‑схеме — используйте профильные декодеры
 *    (например, {@code ValueCodecPhoenix}).
 *
 * Соглашения:
 *  • Возвращаемый массив считается «логически неизменяемым». Не модифицируйте его,
 *    если массив планируется переиспользовать в других частях пайплайна.
 *  • Параметры table и qualifier обязательны; при null выбрасывается {@link NullPointerException}.
 *
 * @since 0.0.1
 * @see Decoder
 */
public final class SimpleDecoder implements Decoder {

    /** Единственный экземпляр декодера — переиспользуйте его повсюду. */
    public static final Decoder INSTANCE = new SimpleDecoder();

    /** Закрытый конструктор — экземпляры создаются только внутри класса. */
    private SimpleDecoder() {
        // no-op
    }

    /**
     * Возвращает переданный массив байт как есть.
     *
     * Параметры {@code table} и {@code qualifier} присутствуют для унификации API и в данном декодере
     * не используются, но проходят валидацию на {@code null} согласно контракту {@link Decoder}.
     *
     * @param unusedTable     имя таблицы; не используется, но не может быть {@code null}
     * @param unusedQualifier имя колонки; не используется, но не может быть {@code null}
     * @param value           сырые байты значения; допускается {@code null}
     * @return тот же объект {@code byte[]} без копирования, либо {@code null}
     * @throws NullPointerException если {@code unusedTable} или {@code unusedQualifier} равны {@code null}
     */
    @Override
    public Object decode(TableName unusedTable, String unusedQualifier, byte[] value) {
        // Валидация обязательных параметров согласно контракту Decoder.
        if (unusedTable == null) {
            throw new NullPointerException("table");
        }
        if (unusedQualifier == null) {
            throw new NullPointerException("qualifier");
        }
        return value;
    }

    /**
     * Быстрая перегрузка с байтовым qualifier: возвращает тот же массив значения (zero‑copy).
     *
     * @param table имя таблицы, не {@code null}
     * @param qual  массив байт qualifier, не {@code null}
     * @param value массив байт значения; допускается {@code null}
     * @return тот же объект {@code byte[]} без копирования, либо {@code null}
     * @throws NullPointerException если {@code table} или {@code qual} равны {@code null}
     */
    @Override
    public Object decode(TableName table, byte[] qual, byte[] value) {
        if (table == null) {
            throw new NullPointerException("table");
        }
        if (qual == null) {
            throw new NullPointerException("qualifier");
        }
        return value;
    }

    /**
     * Перегрузка со срезами: если срез покрывает весь массив, возвращает исходный массив (zero‑copy),
     * иначе возвращает копию указанного диапазона.
     *
     * @param table имя таблицы, не {@code null}
     * @param qual  массив байт qualifier, не {@code null}
     * @param qOff  смещение qualifier (игнорируется)
     * @param qLen  длина qualifier (игнорируется)
     * @param value массив байт значения; допускается {@code null}
     * @param vOff  смещение значения
     * @param vLen  длина значения
     * @return {@code byte[]} исходный массив при полном покрытии или копия указанного диапазона; либо {@code null}
     * @throws NullPointerException если {@code table} или {@code qual} равны {@code null}
     * @throws IllegalArgumentException при некорректных границах {@code vOff}/{@code vLen}
     */
    @Override
    public Object decode(TableName table, byte[] qual, int qOff, int qLen, byte[] value, int vOff, int vLen) {
        if (table == null) {
            throw new NullPointerException("table");
        }
        if (qual == null) {
            throw new NullPointerException("qualifier");
        }
        if (value == null) {
            return null;
        }
        // Проверка границ
        if (vOff < 0 || vLen < 0 || vOff > value.length || vOff + vLen > value.length) {
            throw new IllegalArgumentException("invalid value slice: off=" + vOff + ", len=" + vLen +
                    ", value.length=" + value.length);
        }
        // Zero‑copy если запрошен весь массив
        if (vOff == 0 && vLen == value.length) {
            return value;
        }
        // Иначе — создать копию диапазона (семантически корректно для срезов)
        return Arrays.copyOfRange(value, vOff, vOff + vLen);
    }
}