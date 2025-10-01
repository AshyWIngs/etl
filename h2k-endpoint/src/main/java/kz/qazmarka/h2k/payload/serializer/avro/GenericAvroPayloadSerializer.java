package kz.qazmarka.h2k.payload.serializer.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.hadoop.hbase.TableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.payload.serializer.TableAwarePayloadSerializer;
import kz.qazmarka.h2k.schema.registry.avro.local.AvroSchemaRegistry;

/**
 * Сериализатор Avro для режима {@code generic}: читает локальные {@code *.avsc}
 * и кэширует готовые сериализаторы по таблицам.
 *
 * Основные характеристики
 *  - Потокобезопасен: использует {@link ConcurrentHashMap} для кэша.
 *  - Минимум аллокаций: повторно использует {@link AvroSerializer}, который
 *    сам хранит буферы в {@code ThreadLocal}.
 *  - Поддерживает вывод в бинарном формате Avro и в JSON (авро-вариант).
 */
public final class GenericAvroPayloadSerializer implements TableAwarePayloadSerializer {

    private static final Logger LOG = LoggerFactory.getLogger(GenericAvroPayloadSerializer.class);

    /** Поддерживаемые варианты вывода. */
    public enum Encoding { BINARY, JSON }

    private final AvroSchemaRegistry registry;
    private final ConcurrentHashMap<String, SerializerHolder> serializerByTable = new ConcurrentHashMap<>();
    private final Encoding encoding;
    private final EncoderFactory encoderFactory = EncoderFactory.get();

    /**
     * @param cfg конфигурация (режим Avro должен быть GENERIC)
     * @param encoding вариант сериализации: бинарный Avro или Avro JSON
     */
    public GenericAvroPayloadSerializer(H2kConfig cfg, Encoding encoding) {
        Objects.requireNonNull(cfg, "cfg");
        if (cfg.getAvroMode() != H2kConfig.AvroMode.GENERIC) {
            throw new IllegalStateException("Avro: режим '" + cfg.getAvroMode() + "' не поддерживается этим сериализатором");
        }
        this.encoding = Objects.requireNonNull(encoding, "encoding");

        final String dir = cfg.getAvroSchemaDir();
        Path baseDir;
        if (dir == null || dir.trim().isEmpty()) {
            baseDir = Paths.get("conf", "avro");
        } else {
            baseDir = Paths.get(dir.trim());
        }
        this.registry = new AvroSchemaRegistry(baseDir);
        LOG.debug("Avro: режим generic, каталог схем={}, абсолютный путь={}", baseDir, baseDir.toAbsolutePath());
    }

    /** Сериализует payload указанной таблицы, используя локальный AvroSchemaRegistry. */
    @Override
    public byte[] serialize(TableName table, Map<String, ?> obj) {
        Objects.requireNonNull(table, "table == null");
        Objects.requireNonNull(obj, "payload == null");
        final String tableKey = table.getNameAsString();

        SerializerHolder holder = serializerByTable.get(tableKey);
        if (holder == null) {
            holder = createSerializer(tableKey);
            SerializerHolder prev = serializerByTable.putIfAbsent(tableKey, holder);
            if (prev != null) {
                holder = prev; // гонка: используем существующий
            }
        }

        if (encoding == Encoding.BINARY) {
            return holder.serializer.serialize(obj);
        }
        return serializeJson(holder, obj);
    }

    /** Создаёт и кеширует сериализатор для конкретной таблицы. */
    private SerializerHolder createSerializer(String tableKey) {
        Schema schema;
        try {
            schema = registry.getByTable(tableKey);
        } catch (IllegalStateException ex) {
            throw new IllegalStateException(
                    "Avro: не удалось загрузить схему для таблицы '" + tableKey + "': " + ex.getMessage(), ex);
        }
        LOG.debug("Avro: схема загружена для таблицы='{}', количество полей={}.", tableKey, schema.getFields().size());
        return new SerializerHolder(new AvroSerializer(schema), schema);
    }

    /** Сериализует payload в Avro JSON (используется только для тестовых сценариев/отладки). */
    private byte[] serializeJson(SerializerHolder holder, Map<String, ?> obj) {
        GenericRecord rec = AvroSerializer.buildRecord(holder.schema, obj);

        ByteArrayOutputStream baos = new ByteArrayOutputStream(256);
        try {
            Encoder jsonEncoder = encoderFactory.jsonEncoder(holder.schema, baos);
            GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(holder.schema);
            writer.write(rec, jsonEncoder);
            jsonEncoder.flush();
            baos.write(0x0A); // JSONEachRow: отдельная строка на событие
        } catch (IOException io) {
            throw new IllegalStateException("Avro: ошибка сериализации в JSON: " + io.getMessage(), io);
        }
        return baos.toByteArray();
    }

    @Override
    public String format() {
        return encoding == Encoding.BINARY ? "avro-binary" : "avro-json";
    }

    @Override
    public String contentType() {
        return encoding == Encoding.BINARY ? "application/avro-binary" : "application/json";
    }

    /** Пара «сериализатор + схема» для конкретной таблицы. */
    private static final class SerializerHolder {
        final AvroSerializer serializer;
        final Schema schema;

        SerializerHolder(AvroSerializer serializer, Schema schema) {
            this.serializer = serializer;
            this.schema = schema;
        }
    }

}
