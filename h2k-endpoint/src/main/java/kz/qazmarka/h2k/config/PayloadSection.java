package kz.qazmarka.h2k.config;

import org.apache.hadoop.conf.Configuration;

import kz.qazmarka.h2k.util.Parsers;

/**
 * Группа настроек формирования payload: включение rowkey/метаданных, формат JSON/Avro и SPI-сериализатор.
 * Выделена в отдельный объект при построении {@link H2kConfig}.
 */
public final class PayloadSection {
    final boolean includeRowKey;
    final String rowkeyEncoding;
    final boolean rowkeyBase64;
    final boolean includeMeta;
    final boolean includeMetaWal;
    final boolean jsonSerializeNulls;
    final H2kConfig.PayloadFormat payloadFormat;
    final String serializerFactoryClass;

    private PayloadSection(Builder b) {
        this.includeRowKey = b.includeRowKey;
        this.rowkeyEncoding = b.rowkeyEncoding;
        this.rowkeyBase64 = b.rowkeyBase64;
        this.includeMeta = b.includeMeta;
        this.includeMetaWal = b.includeMetaWal;
        this.jsonSerializeNulls = b.jsonSerializeNulls;
        this.payloadFormat = b.payloadFormat;
        this.serializerFactoryClass = b.serializerFactoryClass;
    }

    static final class Builder {
        boolean includeRowKey;
        String rowkeyEncoding;
        boolean rowkeyBase64;
        boolean includeMeta;
        boolean includeMetaWal;
        boolean jsonSerializeNulls;
        H2kConfig.PayloadFormat payloadFormat;
        String serializerFactoryClass;

        Builder includeRowKey(boolean v) { this.includeRowKey = v; return this; }
        Builder rowkeyEncoding(String v) { this.rowkeyEncoding = v; return this; }
        Builder rowkeyBase64(boolean v) { this.rowkeyBase64 = v; return this; }
        Builder includeMeta(boolean v) { this.includeMeta = v; return this; }
        Builder includeMetaWal(boolean v) { this.includeMetaWal = v; return this; }
        Builder jsonSerializeNulls(boolean v) { this.jsonSerializeNulls = v; return this; }
        Builder payloadFormat(H2kConfig.PayloadFormat v) { this.payloadFormat = v; return this; }
        Builder serializerFactoryClass(String v) { this.serializerFactoryClass = v; return this; }

        PayloadSection build() { return new PayloadSection(this); }
    }

    /**
     * Читает ключи {@code h2k.payload.*} и возвращает иммутабельный блочный конфиг.
     *
     * @param cfg конфигурация Hadoop, из которой извлекаются настройки payload
     * @return секция payload, готовая к использованию в {@link H2kConfig}
     */
    static PayloadSection from(Configuration cfg) {
        boolean includeRowKey = cfg.getBoolean(H2kConfig.K_PAYLOAD_INCLUDE_ROWKEY, H2kConfig.DEFAULT_INCLUDE_ROWKEY);
        String rowkeyEncoding = Parsers.normalizeRowkeyEncoding(cfg.get(H2kConfig.K_ROWKEY_ENCODING, H2kConfig.ROWKEY_ENCODING_HEX));
        boolean rowkeyBase64 = H2kConfig.ROWKEY_ENCODING_BASE64.equals(rowkeyEncoding);
        boolean includeMeta = cfg.getBoolean(H2kConfig.K_PAYLOAD_INCLUDE_META, H2kConfig.DEFAULT_INCLUDE_META);
        boolean includeMetaWal = cfg.getBoolean(H2kConfig.K_PAYLOAD_INCLUDE_META_WAL, H2kConfig.DEFAULT_INCLUDE_META_WAL);
        boolean jsonSerializeNulls = cfg.getBoolean(H2kConfig.Keys.JSON_SERIALIZE_NULLS, H2kConfig.DEFAULT_JSON_SERIALIZE_NULLS);
        H2kConfig.PayloadFormat payloadFormat = Parsers.readPayloadFormat(cfg, H2kConfig.K_PAYLOAD_FORMAT, H2kConfig.PayloadFormat.JSON_EACH_ROW);
        String serializerFactoryClass = cfg.get(H2kConfig.K_PAYLOAD_SERIALIZER_FACTORY, null);
        return new Builder()
                .includeRowKey(includeRowKey)
                .rowkeyEncoding(rowkeyEncoding)
                .rowkeyBase64(rowkeyBase64)
                .includeMeta(includeMeta)
                .includeMetaWal(includeMetaWal)
                .jsonSerializeNulls(jsonSerializeNulls)
                .payloadFormat(payloadFormat)
                .serializerFactoryClass(serializerFactoryClass)
                .build();
    }
}
