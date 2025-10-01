package kz.qazmarka.h2k.endpoint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.schema.decoder.Decoder;
import kz.qazmarka.h2k.schema.decoder.SimpleDecoder;
import kz.qazmarka.h2k.schema.decoder.ValueCodecPhoenix;

class KafkaReplicationEndpointDecoderTest {

    @Test
    void shouldUseSimpleDecoderWhenModeNotSpecified() throws Exception {
        KafkaReplicationEndpoint endpoint = new KafkaReplicationEndpoint();
        Method m = KafkaReplicationEndpoint.class.getDeclaredMethod("chooseDecoder", Configuration.class);
        m.setAccessible(true);

        Configuration cfg = new Configuration(false);
        Decoder decoder = (Decoder) m.invoke(endpoint, cfg);

        assertSame(SimpleDecoder.INSTANCE, decoder);
    }

    @Test
    void shouldUsePhoenixAvroDecoderWhenConfigured() throws Exception {
        KafkaReplicationEndpoint endpoint = new KafkaReplicationEndpoint();
        Method m = KafkaReplicationEndpoint.class.getDeclaredMethod("chooseDecoder", Configuration.class);
        m.setAccessible(true);

        Configuration cfg = new Configuration(false);
        cfg.set(H2kConfig.Keys.DECODE_MODE, "phoenix-avro");
        cfg.set(H2kConfig.Keys.AVRO_SCHEMA_DIR, Paths.get("src", "test", "resources", "avro").toString());

        Decoder decoder = (Decoder) m.invoke(endpoint, cfg);

        assertEquals(ValueCodecPhoenix.class, decoder.getClass());
    }

    @Test
    void shouldUseJsonPhoenixDecoderWhenLegacyMode(@TempDir Path tempDir) throws Exception {
        KafkaReplicationEndpoint endpoint = new KafkaReplicationEndpoint();
        Method m = KafkaReplicationEndpoint.class.getDeclaredMethod("chooseDecoder", Configuration.class);
        m.setAccessible(true);

        Path schema = tempDir.resolve("schema.json");
        String json = "{\n  \"LEGACY\": { \"columns\": {} }\n}";
        Files.write(schema, json.getBytes(StandardCharsets.UTF_8));

        Configuration cfg = new Configuration(false);
        cfg.set(H2kConfig.Keys.DECODE_MODE, "json-phoenix");
        cfg.set(H2kConfig.Keys.SCHEMA_PATH, schema.toString());

        Decoder decoder = (Decoder) m.invoke(endpoint, cfg);

        assertEquals(ValueCodecPhoenix.class, decoder.getClass());
    }
}
