package kz.qazmarka.h2k.payload.serializer;

import java.util.Map;

import org.apache.hadoop.hbase.TableName;

/**
 * Расширение {@link PayloadSerializer}, которому требуется контекст таблицы при сериализации.
 *
 * Назначение
 *  - Позволить форматам с таблично-зависимыми схемами (например, Avro) безопасно выбирать схему.
 *  - Сохранить обратную совместимость: вызывающая сторона может опционально
 *    определить, что сериализатор поддерживает данный интерфейс и передать таблицу.
 *
 * Контракт
 *  - Реализации должны быть потокобезопасны.
 *  - Метод {@link #serialize(TableName, Map)} обязан принимать те же данные, что и базовый
 *    {@link PayloadSerializer#serialize(Map)}, но дополнительно получает {@link TableName}.
 *  - Дефолтная реализация {@link #serialize(Map)} делегирует на табличный метод,
 *    прокидывая {@code null} и ожидая, что реализация обработает этот случай
 *    (обычно — выбросит исключение).
 */
public interface TableAwarePayloadSerializer extends PayloadSerializer {

    /**
     * Сериализует полезную нагрузку с учётом таблицы-источника.
     *
     * @param table имя таблицы HBase (не {@code null})
     * @param obj   корневая карта payload (не {@code null})
     * @return байтовое представление значения
     */
    byte[] serialize(TableName table, Map<String, ?> obj);

    @Override
    default byte[] serialize(Map<String, ?> obj) {
        throw new UnsupportedOperationException(
                "Сериализатор требует контекст таблицы: используйте serialize(TableName, Map)"
        );
    }
}
