package kz.qazmarka.h2k.schema;

import org.apache.hadoop.hbase.TableName;

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
 *  • Вызов {@link #decode(TableName, String, byte[])} — O(1), выделений памяти нет.
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
     * Параметры {@code table} и {@code qualifier} присутствуют для унификации API
     * и в данном декодере не используются.
     *
     * @param table     имя таблицы (не используется)
     * @param qualifier имя колонки (не используется)
     * @param value     сырые байты значения; допускается {@code null}
     * @return тот же объект {@code byte[]} без копирования, либо {@code null}
     */
    @Override
    public Object decode(TableName unusedTable, String unusedQualifier, byte[] value) {
        return value;
    }
}