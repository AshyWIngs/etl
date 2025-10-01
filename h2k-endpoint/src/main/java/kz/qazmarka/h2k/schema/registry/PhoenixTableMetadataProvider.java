package kz.qazmarka.h2k.schema.registry;

import org.apache.hadoop.hbase.TableName;

/**
 * Поставщик табличных метаданных Phoenix, не привязанный к конкретному формату конфигурации.
 * Позволяет получать параметры, которые зависят от таблицы (байты соли, подсказки ёмкости и т.п.).
 */
public interface PhoenixTableMetadataProvider {

    /** Пустой поставщик, всегда возвращающий {@code null}. */
    PhoenixTableMetadataProvider NOOP = new PhoenixTableMetadataProvider() {
        @Override
        public Integer saltBytes(TableName table) { return null; }

        @Override
        public Integer capacityHint(TableName table) { return null; }
    };

    /**
     * @param table таблица Phoenix/HBase
     * @return количество байт соли rowkey или {@code null}, если значение неизвестно
     */
    Integer saltBytes(TableName table);

    /**
     * @param table таблица Phoenix/HBase
     * @return подсказка ёмкости корневого JSON (ожидаемое число полей) или {@code null}, если значение неизвестно
     */
    Integer capacityHint(TableName table);
}
