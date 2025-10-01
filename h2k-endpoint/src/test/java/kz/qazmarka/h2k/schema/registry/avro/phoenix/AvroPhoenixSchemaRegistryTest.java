package kz.qazmarka.h2k.schema.registry.avro.phoenix;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.hadoop.hbase.TableName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import kz.qazmarka.h2k.schema.registry.PhoenixTableMetadataProvider;
import kz.qazmarka.h2k.schema.registry.SchemaRegistry;
import kz.qazmarka.h2k.schema.registry.json.JsonSchemaRegistry;
import kz.qazmarka.h2k.schema.registry.avro.local.AvroSchemaRegistry;

class AvroPhoenixSchemaRegistryTest {

    private static final TableName TABLE_AVRO = TableName.valueOf("T_AVRO");

    @Test
    void shouldReadTypesFromAvroSchema() {
        AvroSchemaRegistry avro = new AvroSchemaRegistry(Paths.get("src", "test", "resources", "avro"));
        AvroPhoenixSchemaRegistry registry = new AvroPhoenixSchemaRegistry(avro);

        assertEquals("VARCHAR", registry.columnType(TABLE_AVRO, "value"));
        assertNull(registry.columnType(TABLE_AVRO, "missing"));
        assertArrayEquals(SchemaRegistry.EMPTY, registry.primaryKeyColumns(TABLE_AVRO));
        PhoenixTableMetadataProvider provider = registry;
        assertEquals(Integer.valueOf(1), provider.saltBytes(TABLE_AVRO));
        assertEquals(Integer.valueOf(4), provider.capacityHint(TABLE_AVRO));
    }

    @Test
    void shouldFallbackToJsonForMissingFieldType(@TempDir Path tempDir) throws Exception {
        String json = "{\n"
                + "  \"T_AVRO\": {\n"
                + "    \"columns\": {\n"
                + "      \"value\": \"VARCHAR\",\n"
                + "      \"_event_ts\": \"TIMESTAMP\"\n"
                + "    },\n"
                + "    \"pk\": [\"value\"]\n"
                + "  }\n"
                + "}";
        Path schemaJson = tempDir.resolve("schema.json");
        Files.write(schemaJson, json.getBytes(StandardCharsets.UTF_8));

        AvroSchemaRegistry avro = new AvroSchemaRegistry(Paths.get("src", "test", "resources", "avro"));
        SchemaRegistry fallback = new JsonSchemaRegistry(schemaJson.toString());
        AvroPhoenixSchemaRegistry registry = new AvroPhoenixSchemaRegistry(avro, fallback);

        assertEquals("TIMESTAMP", registry.columnType(TABLE_AVRO, "_event_ts"));
        assertArrayEquals(new String[]{"value"}, registry.primaryKeyColumns(TABLE_AVRO));
        PhoenixTableMetadataProvider provider = registry;
        assertEquals(Integer.valueOf(1), provider.saltBytes(TABLE_AVRO));
        assertEquals(Integer.valueOf(4), provider.capacityHint(TABLE_AVRO));
    }

    @Test
    void shouldUseFallbackWhenAvroSchemaMissing(@TempDir Path tempDir) throws Exception {
        Path avroDir = Files.createDirectory(tempDir.resolve("avro"));
        AvroSchemaRegistry avro = new AvroSchemaRegistry(avroDir);

        String json = "{\n"
                + "  \"T_EMPTY\": {\n"
                + "    \"columns\": {\n"
                + "      \"c\": \"VARCHAR\"\n"
                + "    },\n"
                + "    \"pk\": [\"c\"]\n"
                + "  }\n"
                + "}";
        Path schemaJson = tempDir.resolve("schema.json");
        Files.write(schemaJson, json.getBytes(StandardCharsets.UTF_8));

        SchemaRegistry fallback = new JsonSchemaRegistry(schemaJson.toString());
        AvroPhoenixSchemaRegistry registry = new AvroPhoenixSchemaRegistry(avro, fallback);
        TableName table = TableName.valueOf("T_EMPTY");

        assertEquals("VARCHAR", registry.columnType(table, "c"));
        assertArrayEquals(new String[]{"c"}, registry.primaryKeyColumns(table));
        PhoenixTableMetadataProvider provider = registry;
        assertNull(provider.saltBytes(table));
        assertNull(provider.capacityHint(table));
    }
}
