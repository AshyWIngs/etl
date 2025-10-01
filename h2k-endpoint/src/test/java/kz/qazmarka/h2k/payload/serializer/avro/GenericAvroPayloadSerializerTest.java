package kz.qazmarka.h2k.payload.serializer.avro;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.payload.builder.PayloadBuilder;
import kz.qazmarka.h2k.schema.decoder.Decoder;
import kz.qazmarka.h2k.schema.registry.avro.local.AvroSchemaRegistry;
import kz.qazmarka.h2k.util.RowKeySlice;

/**
 * Интеграционные тесты Avro-сериализации в режиме generic.
 */
class GenericAvroPayloadSerializerTest {

    private static final TableName TABLE = TableName.valueOf("T_AVRO");
    private static final TableName TABLE_UNION = TableName.valueOf("T_AVRO_UNION");
    private static final TableName TABLE_COLLECTIONS = TableName.valueOf("T_AVRO_COLLECTIONS");
    private static final Decoder STRING_DECODER = (table, qualifier, value) ->
            value == null ? null : new String(value, StandardCharsets.UTF_8);

    private static final String SHARED_SCHEMA_DIR = Paths.get("src", "test", "resources", "avro").toAbsolutePath().toString();

    private static H2kConfig prepareConfig(String format) {
        Configuration conf = new Configuration(false);
        conf.set("h2k.cf.list", "d");
        conf.set("h2k.payload.format", format);
        conf.set("h2k.avro.mode", "generic");
        conf.set("h2k.avro.schema.dir", SHARED_SCHEMA_DIR);
        return H2kConfig.from(conf, "dummy:9092");
    }

    @Test
    @DisplayName("Avro generic: nullable коллекции сохраняют null и значения")
    void avroNullableCollectionsRoundTrip() throws Exception {
        H2kConfig cfg = prepareConfig("avro-binary");
        Decoder decoder = new Decoder() {
            @Override
            public Object decode(TableName table, String qualifier, byte[] value) {
                if (value == null) {
                    return null;
                }
                String q = qualifier.toLowerCase();
                String text = new String(value, StandardCharsets.UTF_8);
                if ("tags".equals(q)) {
                    if ("NULL".equalsIgnoreCase(text)) {
                        return null;
                    }
                    return Arrays.asList(text.split(","));
                }
                if ("metrics".equals(q)) {
                    Map<String, Object> map = new HashMap<>();
                    for (String token : text.split(";")) {
                        if (token.isEmpty()) continue;
                        String[] kv = token.split("=");
                        map.put(kv[0], Long.valueOf(kv[1]));
                    }
                    return map;
                }
                if ("numbers".equals(q)) {
                    if (text.isEmpty()) {
                        return Collections.emptyList();
                    }
                    String[] parts = text.split(",");
                    List<Long> out = new ArrayList<>(parts.length);
                    for (String part : parts) {
                        if (part.isEmpty()) continue;
                        out.add(Long.valueOf(part));
                    }
                    return out;
                }
                return STRING_DECODER.decode(table, qualifier, value);
            }

            @Override
            public void decodeRowKey(TableName table, RowKeySlice rk, int saltBytes, Map<String, Object> out) {
                // no-op
            }
        };

        PayloadBuilder builder = new PayloadBuilder(decoder, cfg);
        AvroSchemaRegistry registry = new AvroSchemaRegistry(Paths.get(cfg.getAvroSchemaDir()));
        Schema schema = registry.getByTable(TABLE_COLLECTIONS.getNameAsString());
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);

        byte[] rowKeyValues = Bytes.toBytes("rk-coll-values");
        List<Cell> valueCells = Arrays.asList(
                new KeyValue(rowKeyValues, Bytes.toBytes("d"), Bytes.toBytes("tags"), 1L, "ALPHA,BETA".getBytes(StandardCharsets.UTF_8)),
                new KeyValue(rowKeyValues, Bytes.toBytes("d"), Bytes.toBytes("metrics"), 1L, "rps=10;errors=2".getBytes(StandardCharsets.UTF_8)),
                new KeyValue(rowKeyValues, Bytes.toBytes("d"), Bytes.toBytes("numbers"), 1L, "1,2,3".getBytes(StandardCharsets.UTF_8))
        );
        byte[] bytesValues = builder.buildRowPayloadBytes(TABLE_COLLECTIONS, valueCells, RowKeySlice.whole(rowKeyValues), 1L, 1L);
        GenericRecord recordValues = reader.read(null, DecoderFactory.get().binaryDecoder(bytesValues, null));

        assertEquals(Arrays.asList("ALPHA", "BETA"), toStringList(recordValues.get("tags")));
        Map<?, ?> metricsRaw = (Map<?, ?>) recordValues.get("metrics");
        assertNotNull(metricsRaw);
        Map<String, Long> metrics = new HashMap<>();
        metricsRaw.forEach((k, v) -> {
            if (v != null) {
                metrics.put(k.toString(), ((Number) v).longValue());
            }
        });
        assertEquals(Long.valueOf(10L), metrics.get("rps"));
        assertEquals(Long.valueOf(2L), metrics.get("errors"));
        assertEquals(Arrays.asList(1L, 2L, 3L), toLongList(recordValues.get("numbers")));

        byte[] rowKeyNulls = Bytes.toBytes("rk-coll-nulls");
        List<Cell> nullCells = Collections.emptyList();
        byte[] bytesNulls = builder.buildRowPayloadBytes(TABLE_COLLECTIONS, nullCells, RowKeySlice.whole(rowKeyNulls), 1L, 1L);
        GenericRecord recordNulls = reader.read(null, DecoderFactory.get().binaryDecoder(bytesNulls, null));

        assertNull(recordNulls.get("tags"));
        assertNull(recordNulls.get("metrics"));
        assertNull(recordNulls.get("numbers"));
    }

    @Test
    @DisplayName("Avro generic: union-поля поддерживают несколько веток")
    void avroUnionSupportsMultipleBranches() throws Exception {
        H2kConfig cfg = prepareConfig("avro-binary");
        Decoder decoder = new Decoder() {
            @Override
            public Object decode(TableName table, String qualifier, byte[] value) {
                if (value == null) {
                    return null;
                }
                String q = qualifier.toLowerCase();
                if ("value_union".equals(q)) {
                    return value.length == Long.BYTES ? Bytes.toLong(value) : new String(value, StandardCharsets.UTF_8);
                }
                String text = new String(value, StandardCharsets.UTF_8);
                if ("aux_union".equals(q)) {
                    HashMap<String, Object> nested = new HashMap<>();
                    nested.put("flag", Boolean.valueOf(text));
                    return nested;
                }
                return text;
            }

            @Override
            public void decodeRowKey(TableName table, RowKeySlice rk, int saltBytes, Map<String, Object> out) {
                // no-op
            }
        };

        PayloadBuilder builder = new PayloadBuilder(decoder, cfg);
        AvroSchemaRegistry registry = new AvroSchemaRegistry(Paths.get(cfg.getAvroSchemaDir()));
        Schema schema = registry.getByTable(TABLE_UNION.getNameAsString());
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);

        // Строковая ветка
        byte[] rowString = Bytes.toBytes("rk-union-str");
        List<Cell> stringCells = Arrays.asList(
                new KeyValue(rowString, Bytes.toBytes("d"), Bytes.toBytes("value_union"), 1L, "STRING".getBytes(StandardCharsets.UTF_8)),
                new KeyValue(rowString, Bytes.toBytes("d"), Bytes.toBytes("aux_union"), 1L, "true".getBytes(StandardCharsets.UTF_8))
        );
        byte[] bytesString = builder.buildRowPayloadBytes(TABLE_UNION, stringCells, RowKeySlice.whole(rowString), 1L, 1L);
        GenericRecord recordString = reader.read(null, DecoderFactory.get().binaryDecoder(bytesString, null));
        assertEquals("STRING", recordString.get("value_union").toString());
        GenericRecord auxRecord = (GenericRecord) recordString.get("aux_union");
        assertNotNull(auxRecord);
        assertEquals(Boolean.TRUE, auxRecord.get("flag"));

        // Числовая ветка
        byte[] rowLong = Bytes.toBytes("rk-union-long");
        List<Cell> longCells = Arrays.asList(
                new KeyValue(rowLong, Bytes.toBytes("d"), Bytes.toBytes("value_union"), 1L, Bytes.toBytes(42L))
        );
        byte[] bytesLong = builder.buildRowPayloadBytes(TABLE_UNION, longCells, RowKeySlice.whole(rowLong), 1L, 1L);
        GenericRecord recordLong = reader.read(null, DecoderFactory.get().binaryDecoder(bytesLong, null));
        assertEquals(42L, ((Long) recordLong.get("value_union")).longValue());
        assertNull(recordLong.get("aux_union"));
    }

    @Test
    @DisplayName("Avro generic: бинарный вывод соответствует схеме")
    void avroBinary_matchesSchema() throws Exception {
        H2kConfig configBinary = prepareConfig("avro-binary");
        PayloadBuilder builder = new PayloadBuilder(STRING_DECODER, configBinary);
        long ts = 123L;
        byte[] rowKey = Bytes.toBytes("rk1");
        Cell cell = new KeyValue(rowKey, Bytes.toBytes("d"), Bytes.toBytes("value"), ts, Bytes.toBytes("OK"));
        List<Cell> cells = Collections.singletonList(cell);
        byte[] bytes = builder.buildRowPayloadBytes(TABLE, cells, RowKeySlice.whole(rowKey), ts, ts);

        AvroSchemaRegistry registry = new AvroSchemaRegistry(Paths.get(configBinary.getAvroSchemaDir()));
        Schema schema = registry.getByTable(TABLE.getNameAsString());

        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        org.apache.avro.io.Decoder avroDecoder = DecoderFactory.get().binaryDecoder(bytes, null);
        GenericRecord avroRecord = reader.read(null, avroDecoder);

        assertEquals("OK", avroRecord.get("value").toString());
        assertEquals(ts, ((Long) avroRecord.get("_event_ts")).longValue());
    }

    @Test
    @DisplayName("Avro generic: JSON вывод разбирается обратным преобразованием")
    void avroJson_roundTrip() throws Exception {
        H2kConfig configJson = prepareConfig("avro-json");
        PayloadBuilder builder = new PayloadBuilder(STRING_DECODER, configJson);
        long ts = 321L;
        byte[] rowKey = Bytes.toBytes("rk2");
        Cell cell = new KeyValue(rowKey, Bytes.toBytes("d"), Bytes.toBytes("value"), ts, Bytes.toBytes("JSON"));
        List<Cell> cells = Collections.singletonList(cell);
        byte[] bytes = builder.buildRowPayloadBytes(TABLE, cells, RowKeySlice.whole(rowKey), ts, ts);

        AvroSchemaRegistry registry = new AvroSchemaRegistry(Paths.get(configJson.getAvroSchemaDir()));
        Schema schema = registry.getByTable(TABLE.getNameAsString());

        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        org.apache.avro.io.Decoder avroDecoder = DecoderFactory.get().jsonDecoder(schema, new ByteArrayInputStream(bytes));
        GenericRecord avroRecord = reader.read(null, avroDecoder);

        assertEquals("JSON", avroRecord.get("value").toString());
        assertEquals(ts, ((Long) avroRecord.get("_event_ts")).longValue());
        assertEquals('\n', (char) bytes[bytes.length - 1], "JSON вывод должен заканчиваться переводом строки");
    }

    @Test
    @DisplayName("Avro generic: отсутствие схемы даёт понятную ошибку")
    void avroMissingSchemaFails() {
        Configuration c = new Configuration(false);
        c.set("h2k.cf.list", "d");
        c.set("h2k.payload.format", "avro-binary");
        c.set("h2k.avro.mode", "generic");
        c.set("h2k.avro.schema.dir", Paths.get("src", "test", "resources", "avro-missing").toAbsolutePath().toString());
        H2kConfig cfg = H2kConfig.from(c, "dummy:9092");

        PayloadBuilder builder = new PayloadBuilder(STRING_DECODER, cfg);
        byte[] rowKey = Bytes.toBytes("rk3");
        Cell cell = new KeyValue(rowKey, Bytes.toBytes("d"), Bytes.toBytes("value"), 1L, Bytes.toBytes("oops"));
        List<Cell> cells = Collections.singletonList(cell);

        RowKeySlice rowKeySlice = RowKeySlice.whole(rowKey);
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                builder.buildRowPayloadBytes(TABLE, cells, rowKeySlice, 1L, 1L)
        );
        assertTrue(ex.getMessage().contains("не удалось загрузить схему"));
    }

    @Test
    @DisplayName("Avro generic: тип не соответствует схеме — бросается IllegalStateException")
    void avroSchemaTypeMismatch() throws Exception {
        Path tmp = Files.createTempDirectory("avro-mismatch");
        writeSchema(tmp, "t_avro.avsc", "{\n"
                + "  \"type\": \"record\",\n"
                + "  \"name\": \"T_AVRO\",\n"
                + "  \"fields\": [\n"
                + "    {\"name\": \"value\", \"type\": [\"null\", \"long\"], \"default\": null}\n"
                + "  ]\n"
                + "}");

        Configuration c = new Configuration(false);
        c.set("h2k.cf.list", "d");
        c.set("h2k.payload.format", "avro-binary");
        c.set("h2k.avro.mode", "generic");
        c.set("h2k.avro.schema.dir", tmp.toString());
        H2kConfig cfg = H2kConfig.from(c, "dummy:9092");

        PayloadBuilder builder = new PayloadBuilder(STRING_DECODER, cfg);
        byte[] rowKey = Bytes.toBytes("rk-mismatch");
        Cell cell = new KeyValue(rowKey, Bytes.toBytes("d"), Bytes.toBytes("value"), 1L, Bytes.toBytes("NOT_A_NUMBER"));
        List<Cell> cells = Collections.singletonList(cell);

        RowKeySlice rowKeySlice = RowKeySlice.whole(rowKey);
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                builder.buildRowPayloadBytes(TABLE, cells, rowKeySlice, 1L, 1L));
        String msg = ex.getMessage();
        assertTrue(msg.contains("поле") || msg.contains("поля"), "Сообщение должно указывать на проблемное поле: " + msg);
    }

    @Test
    @DisplayName("Avro generic: схема с optional полем поддерживается без изменений payload")
    void avroSchemaOptionalField() throws Exception {
        Path tmp = Files.createTempDirectory("avro-optional");
        writeSchema(tmp, "t_avro.avsc", "{\n"
                + "  \"type\": \"record\",\n"
                + "  \"name\": \"T_AVRO\",\n"
                + "  \"fields\": [\n"
                + "    {\"name\": \"value\", \"type\": [\"null\", \"string\"], \"default\": null},\n"
                + "    {\"name\": \"extra\", \"type\": [\"null\", \"string\"], \"default\": null}\n"
                + "  ]\n"
                + "}");

        Configuration c = new Configuration(false);
        c.set("h2k.cf.list", "d");
        c.set("h2k.payload.format", "avro-binary");
        c.set("h2k.avro.mode", "generic");
        c.set("h2k.avro.schema.dir", tmp.toString());
        H2kConfig cfg = H2kConfig.from(c, "dummy:9092");

        PayloadBuilder builder = new PayloadBuilder(STRING_DECODER, cfg);
        byte[] rowKey = Bytes.toBytes("rk-optional");
        Cell cell = new KeyValue(rowKey, Bytes.toBytes("d"), Bytes.toBytes("value"), 1L, Bytes.toBytes("OK"));
        List<Cell> cells = Collections.singletonList(cell);

        byte[] out = builder.buildRowPayloadBytes(TABLE, cells, RowKeySlice.whole(rowKey), 1L, 1L);

        Schema schema = new AvroSchemaRegistry(tmp).getByTable(TABLE.getNameAsString());
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        GenericRecord avroRecord = reader.read(null, DecoderFactory.get().binaryDecoder(out, null));
        assertEquals("OK", avroRecord.get("value").toString());
        assertNull(avroRecord.get("extra"), "Дополнительное поле должно сериализоваться как null по умолчанию");
    }

    @Test
    @DisplayName("Avro generic: обязательный массив не принимает null")
    void avroRequiredArrayRejectsNull() {
        Schema schema = new Schema.Parser().parse("{\n"
                + "  \"type\": \"record\",\n"
                + "  \"name\": \"ARRAY_REQUIRED\",\n"
                + "  \"fields\": [\n"
                + "    {\"name\": \"items\", \"type\": {\"type\": \"array\", \"items\": \"string\"}}\n"
                + "  ]\n"
                + "}");

        AvroSerializer serializer = new AvroSerializer(schema);
        Map<String, Object> payload = new HashMap<>();
        payload.put("items", null);

        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> serializer.serialize(payload));
        assertTrue(ex.getMessage().contains("обязательно"));
    }

    private static List<String> toStringList(Object value) {
        if (value == null) {
            return Collections.emptyList();
        }
        List<?> src = (List<?>) value;
        List<String> out = new ArrayList<>(src.size());
        for (Object o : src) {
            out.add(o.toString());
        }
        return out;
    }

    private static List<Long> toLongList(Object value) {
        if (value == null) {
            return Collections.emptyList();
        }
        List<?> src = (List<?>) value;
        List<Long> out = new ArrayList<>(src.size());
        for (Object o : src) {
            out.add(((Number) o).longValue());
        }
        return out;
    }

    private static void writeSchema(Path dir, String fileName, String schemaJson) throws Exception {
        Files.write(dir.resolve(fileName), schemaJson.getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }
}
