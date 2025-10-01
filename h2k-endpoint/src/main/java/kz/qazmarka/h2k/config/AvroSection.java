package kz.qazmarka.h2k.config;

import java.util.List;
import java.util.Map;

import kz.qazmarka.h2k.util.Parsers;

import org.apache.hadoop.conf.Configuration;

/**
 * Снимок Avro-конфигурации: режим (generic/confluent), директория локальных схем и параметры Schema Registry.
 * Заполняется один раз из {@link org.apache.hadoop.conf.Configuration} и далее используется при сборке {@link H2kConfig}.
 */
public final class AvroSection {
    final H2kConfig.AvroMode mode;
    final String schemaDir;
    final List<String> schemaRegistryUrls;
    final Map<String, String> auth;
    final Map<String, String> props;

    private AvroSection(H2kConfig.AvroMode mode,
                        String schemaDir,
                        List<String> schemaRegistryUrls,
                        Map<String, String> auth,
                        Map<String, String> props) {
        this.mode = mode;
        this.schemaDir = schemaDir;
        this.schemaRegistryUrls = schemaRegistryUrls;
        this.auth = auth;
        this.props = props;
    }

    /**
     * Читает блок ключей {@code h2k.avro.*} и возвращает иммутабельный объект с результатами парсинга.
     *
     * @param cfg исходная конфигурация HBase/endpoint
     * @return готовый объект с приведёнными значениями Avro-настроек
     */
    static AvroSection from(Configuration cfg) {
        H2kConfig.AvroMode mode = Parsers.readAvroMode(cfg, H2kConfig.K_AVRO_MODE, H2kConfig.DEFAULT_AVRO_MODE);
        String schemaDir = Parsers.readStringOrDefault(cfg, H2kConfig.K_AVRO_SCHEMA_DIR, H2kConfig.DEFAULT_AVRO_SCHEMA_DIR);
        List<String> srUrls = Parsers.readCsvListFirstNonEmpty(cfg,
                H2kConfig.K_AVRO_SR_URLS,
                H2kConfig.K_AVRO_SR_URLS_LEGACY,
                H2kConfig.K_AVRO_SR_URL_LEGACY);
        Map<String, String> auth = Parsers.readWithPrefix(cfg, H2kConfig.K_AVRO_SR_AUTH_PREFIX);
        Map<String, String> props = Parsers.readWithPrefix(cfg, H2kConfig.K_AVRO_PREFIX);
        props.remove("mode");
        props.remove("schema.dir");
        props.remove("sr.urls");
        props.remove("schema.registry");
        props.remove("schema.registry.url");
        props.keySet().removeIf(k -> k.startsWith("sr.auth."));
        return new AvroSection(mode, schemaDir, srUrls, auth, props);
    }
}
