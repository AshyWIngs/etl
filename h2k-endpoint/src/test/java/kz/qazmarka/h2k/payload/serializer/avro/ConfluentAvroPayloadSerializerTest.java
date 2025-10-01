package kz.qazmarka.h2k.payload.serializer.avro;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.payload.builder.PayloadBuilder;
import kz.qazmarka.h2k.payload.serializer.PayloadSerializer;
import kz.qazmarka.h2k.payload.serializer.avro.ConfluentAvroPayloadSerializer.SchemaRegistryMetrics;
import kz.qazmarka.h2k.schema.decoder.Decoder;
import kz.qazmarka.h2k.schema.registry.avro.local.AvroSchemaRegistry;
import kz.qazmarka.h2k.util.RowKeySlice;

/**
 * Тесты режима Confluent Schema Registry (avro-binary).
 */
class ConfluentAvroPayloadSerializerTest {

    private static final TableName TABLE = TableName.valueOf("T_AVRO");
    private static final TableName TABLE_ESCAPE = TableName.valueOf("T_AVRO_ESCAPE");
    private static final Decoder STRING_DECODER = (table, qualifier, value) ->
            value == null ? null : new String(value, StandardCharsets.UTF_8);

    private static final ThreadLocal<SchemaRegistryClient> TEST_CLIENT = new ThreadLocal<>();
    private static final ThreadLocal<ConfluentAvroPayloadSerializer> LAST_SERIALIZER = new ThreadLocal<>();

    /**
     * Фабрика сериализатора, которая подменяет SchemaRegistryClient на тестовый.
     */
    public static final class TestConfluentFactory implements PayloadBuilder.PayloadSerializerFactory {
        @Override
        public PayloadSerializer create(H2kConfig cfg) {
            SchemaRegistryClient client = TEST_CLIENT.get();
            if (client == null) {
                return null; // fallback к стандартной фабрике
            }
            ConfluentAvroPayloadSerializer serializer = new ConfluentAvroPayloadSerializer(cfg, client);
            LAST_SERIALIZER.set(serializer);
            return serializer;
        }
    }

    private static final class FailingSchemaRegistryClient extends MockSchemaRegistryClient {
        @Override
        public int register(String subject, Schema schema) throws RestClientException {
            throw new RestClientException("boom", 500, 500);
        }
    }

    @Test
    @DisplayName("Confluent: бинарный вывод с заголовком magic/id")
    void confluentBinaryProducesHeader() throws Exception {
        MockSchemaRegistryClient client = new MockSchemaRegistryClient();
        TEST_CLIENT.set(client);
        try {
            H2kConfig cfg = buildConfig("avro-binary");
            PayloadBuilder builder = new PayloadBuilder(STRING_DECODER, cfg);
            long ts = 555L;
            byte[] rowKey = Bytes.toBytes("rk1");
            Cell cell = new KeyValue(rowKey, Bytes.toBytes("d"), Bytes.toBytes("value"), ts, Bytes.toBytes("OK"));
            List<Cell> cells = Collections.singletonList(cell);

            byte[] out = builder.buildRowPayloadBytes(TABLE, cells, RowKeySlice.whole(rowKey), ts, ts);

            assertEquals(0x0, out[0]);
            int schemaId = ((out[1] & 0xFF) << 24) | ((out[2] & 0xFF) << 16) | ((out[3] & 0xFF) << 8) | (out[4] & 0xFF);
            assertEquals(1, schemaId);

            byte[] payload = Arrays.copyOfRange(out, 5, out.length);

            AvroSchemaRegistry registry = new AvroSchemaRegistry(Paths.get("src", "test", "resources", "avro"));
            Schema schema = registry.getByTable(TABLE.getNameAsString());
            GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
            GenericRecord avroRecord = reader.read(null, DecoderFactory.get().binaryDecoder(payload, null));
            assertEquals("OK", avroRecord.get("value").toString());

            java.util.Collection<String> subjects = subjects(client);
            assertTrue(subjects.contains("T_AVRO"));
            assertEquals(1, subjects.size());

            SchemaRegistryMetrics metrics = requireSerializer().metrics();
            assertEquals(1, metrics.registeredSchemas());
            assertEquals(0, metrics.registrationFailures());
        } finally {
            TEST_CLIENT.remove();
            LAST_SERIALIZER.remove();
        }
    }

    @Test
    @DisplayName("Confluent: регистрация проходит для схемы с управляющими символами")
    void confluentSchemaPayloadEscaping() throws Exception {
        MockSchemaRegistryClient client = new MockSchemaRegistryClient();
        TEST_CLIENT.set(client);
        try {
            H2kConfig cfg = buildConfig("avro-binary");
            PayloadBuilder builder = new PayloadBuilder(STRING_DECODER, cfg);
            byte[] rowKey = Bytes.toBytes("rk_escape");
            Cell cell = new KeyValue(rowKey, Bytes.toBytes("d"), Bytes.toBytes("value"), 1L, Bytes.toBytes("ESC"));
            List<Cell> cells = Collections.singletonList(cell);
            builder.buildRowPayloadBytes(TABLE_ESCAPE, cells, RowKeySlice.whole(rowKey), 1L, 1L);

            String registered = client.getLatestSchemaMetadata("T_AVRO_ESCAPE").getSchema();
            assertTrue(registered.contains("\\n") || registered.contains("\n"));

            SchemaRegistryMetrics metrics = requireSerializer().metrics();
            assertEquals(1, metrics.registeredSchemas());
            assertEquals(0, metrics.registrationFailures());
        } finally {
            TEST_CLIENT.remove();
            LAST_SERIALIZER.remove();
        }
    }

    @Test
    @DisplayName("Confluent: ошибки Schema Registry приводят к IllegalStateException")
    void confluentRegisterFails() {
        SchemaRegistryClient client = new FailingSchemaRegistryClient();
        TEST_CLIENT.set(client);
        try {
            H2kConfig cfg = buildConfig("avro-binary");
            PayloadBuilder builder = new PayloadBuilder(STRING_DECODER, cfg);
            byte[] rowKey = Bytes.toBytes("rk_fail");
            Cell cell = new KeyValue(rowKey, Bytes.toBytes("d"), Bytes.toBytes("value"), 1L, Bytes.toBytes("fail"));
            List<Cell> cells = Collections.singletonList(cell);
            RowKeySlice rowKeySlice = RowKeySlice.whole(rowKey);

            IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                    builder.buildRowPayloadBytes(TABLE, cells, rowKeySlice, 1L, 1L)
            );
            assertTrue(ex.getMessage().contains("Schema Registry"));

            SchemaRegistryMetrics metrics = requireSerializer().metrics();
            assertEquals(0, metrics.registeredSchemas());
            assertEquals(1, metrics.registrationFailures());
        } finally {
            TEST_CLIENT.remove();
            LAST_SERIALIZER.remove();
        }
    }

    @Test
    @DisplayName("Confluent: формат avro-json запрещён")
    void confluentJsonNotAllowed() {
        H2kConfig cfg = buildConfigWithoutFactory("avro-json");
        PayloadBuilder builder = new PayloadBuilder(STRING_DECODER, cfg);
        byte[] rowKey = Bytes.toBytes("rk");
        Cell cell = new KeyValue(rowKey, Bytes.toBytes("d"), Bytes.toBytes("value"), 1L, Bytes.toBytes("x"));
        List<Cell> cells = Collections.singletonList(cell);
        RowKeySlice rowKeySlice = RowKeySlice.whole(rowKey);
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                builder.buildRowPayloadBytes(TABLE, cells, rowKeySlice, 1L, 1L)
        );
        assertTrue(ex.getMessage().contains("confluent"));
    }

    @Test
    @DisplayName("Confluent: стратегия table-upper + suffix формирует ожидаемый subject")
    void confluentSubjectStrategyUpper() {
        MockSchemaRegistryClient client = new MockSchemaRegistryClient();
        TEST_CLIENT.set(client);
        try {
            Configuration c = baseConfigurationWithFactory("avro-binary");
            c.set("h2k.avro.subject.strategy", "table-upper");
            c.set("h2k.avro.subject.suffix", "-value");
            H2kConfig cfg = H2kConfig.from(c, "dummy:9092");

            PayloadBuilder builder = new PayloadBuilder(STRING_DECODER, cfg);
            byte[] rowKey = Bytes.toBytes("rk_subject");
            Cell cell = new KeyValue(rowKey, Bytes.toBytes("d"), Bytes.toBytes("value"), 1L, Bytes.toBytes("x"));
            builder.buildRowPayloadBytes(TABLE, Collections.singletonList(cell), RowKeySlice.whole(rowKey), 1L, 1L);

            assertTrue(subjects(client).contains("T_AVRO-value"));
            SchemaRegistryMetrics metrics = requireSerializer().metrics();
            assertEquals(1, metrics.registeredSchemas());
        } finally {
            TEST_CLIENT.remove();
            LAST_SERIALIZER.remove();
        }
    }

    @Test
    @DisplayName("Confluent: таблицы с одинаковым qualifier в разных namespace получают разные subject")
    void confluentSubjectsIsolatedByNamespace() throws Exception {
        MockSchemaRegistryClient client = new MockSchemaRegistryClient();
        TEST_CLIENT.set(client);
        try {
            Path tmp = Files.createTempDirectory("sr-subject");
            byte[] schemaBytes = Files.readAllBytes(Paths.get("src", "test", "resources", "avro", "t_avro.avsc"));
            Files.write(tmp.resolve("common.avsc"), schemaBytes);
            Files.write(tmp.resolve("analytics:common.avsc"), schemaBytes);

            Configuration c = baseConfigurationWithFactory("avro-binary");
            c.set("h2k.avro.schema.dir", tmp.toString());
            c.set("h2k.avro.subject.strategy", "table");
            H2kConfig cfg = H2kConfig.from(c, "dummy:9092");

            PayloadBuilder builder = new PayloadBuilder(STRING_DECODER, cfg);

            TableName tableDefault = TableName.valueOf("COMMON");
            TableName tableAnalytics = TableName.valueOf("analytics", "COMMON");
            byte[] rowKey = Bytes.toBytes("rk");
            Cell cell = new KeyValue(rowKey, Bytes.toBytes("d"), Bytes.toBytes("value"), 1L, Bytes.toBytes("ok"));

            builder.buildRowPayloadBytes(tableDefault, Collections.singletonList(cell), RowKeySlice.whole(rowKey), 1L, 1L);
            builder.buildRowPayloadBytes(tableAnalytics, Collections.singletonList(cell), RowKeySlice.whole(rowKey), 1L, 1L);

            java.util.Collection<String> subjects = subjects(client);
            assertTrue(subjects.contains("COMMON"));
            assertTrue(subjects.contains("analytics_COMMON"));

            SchemaRegistryMetrics metrics = requireSerializer().metrics();
            assertEquals(2, metrics.registeredSchemas());
            assertEquals(0, metrics.registrationFailures());
        } finally {
            TEST_CLIENT.remove();
            LAST_SERIALIZER.remove();
        }
    }

    private static Configuration baseConfigurationWithFactory(String format) {
        Configuration c = new Configuration(false);
        c.set("h2k.cf.list", "d");
        c.set("h2k.payload.format", format);
        c.set("h2k.avro.mode", "confluent");
        c.set("h2k.avro.schema.dir", Paths.get("src", "test", "resources", "avro").toAbsolutePath().toString());
        c.set("h2k.avro.sr.urls", "http://mock");
        c.set("h2k.payload.serializer.factory", TestConfluentFactory.class.getName());
        return c;
    }

    private static H2kConfig buildConfig(String format) {
        return H2kConfig.from(baseConfigurationWithFactory(format), "dummy:9092");
    }

    private static H2kConfig buildConfigWithoutFactory(String format) {
        Configuration c = new Configuration(false);
        c.set("h2k.cf.list", "d");
        c.set("h2k.payload.format", format);
        c.set("h2k.avro.mode", "confluent");
        c.set("h2k.avro.schema.dir", Paths.get("src", "test", "resources", "avro").toAbsolutePath().toString());
        c.set("h2k.avro.sr.urls", "http://mock");
        return H2kConfig.from(c, "dummy:9092");
    }

    private static ConfluentAvroPayloadSerializer requireSerializer() {
        ConfluentAvroPayloadSerializer serializer = LAST_SERIALIZER.get();
        if (serializer == null) {
            throw new IllegalStateException("Сериализатор Confluent не был создан в тесте");
        }
        return serializer;
    }

    private static java.util.Collection<String> subjects(SchemaRegistryClient client) {
        try {
            return client.getAllSubjects();
        } catch (IOException | RestClientException e) {
            throw new IllegalStateException("Не удалось получить список subject'ов из тестового клиента", e);
        }
    }
}
