package kz.qazmarka.h2k.payload.builder;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.payload.serializer.PayloadSerializer;
import kz.qazmarka.h2k.payload.serializer.TableAwarePayloadSerializer;
import kz.qazmarka.h2k.payload.serializer.internal.SerializerResolver;
import kz.qazmarka.h2k.payload.serializer.avro.ConfluentAvroPayloadSerializer;
import kz.qazmarka.h2k.payload.serializer.avro.SchemaRegistryClientFactory;
import kz.qazmarka.h2k.schema.decoder.Decoder;
import kz.qazmarka.h2k.util.RowKeySlice;

/**
 * Управляет построением карты payload и выбором сериализатора. Вся тяжёлая логика сборки
 * вынесена в {@link RowPayloadAssembler}, что упрощает поддержку и уменьшает нагрузку на горячем пути.
 */
public final class PayloadBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(PayloadBuilder.class);

    private final H2kConfig cfg;
    private final AtomicReference<PayloadSerializer> cachedSerializer = new AtomicReference<>();
    private final RowPayloadAssembler assembler;
    private final SerializerResolver serializerResolver;

    /**
     * @param decoder декодер Phoenix, предоставляющий значения колонок и PK
     * @param cfg     неизменяемая конфигурация h2k
     */
    public PayloadBuilder(Decoder decoder, H2kConfig cfg) {
        this(decoder, cfg, SchemaRegistryClientFactory.cached());
    }

    /**
     * @param decoder                     декодер Phoenix
     * @param cfg                         конфигурация h2k
     * @param schemaRegistryClientFactory фабрика клиентов Schema Registry (для режима Confluent)
     */
    public PayloadBuilder(Decoder decoder,
                          H2kConfig cfg,
                          SchemaRegistryClientFactory schemaRegistryClientFactory) {
        this.cfg = Objects.requireNonNull(cfg, "конфигурация h2k");
        Objects.requireNonNull(schemaRegistryClientFactory, "schemaRegistryClientFactory");
        this.serializerResolver = new SerializerResolver(LOG, cfg, schemaRegistryClientFactory);
        this.assembler = new RowPayloadAssembler(decoder, cfg);
    }

    /**
     * Собирает payload и сериализует результат в байты, используя сериализатор из конфигурации.
     */
    public byte[] buildRowPayloadBytes(TableName table,
                                       List<Cell> cells,
                                       RowKeySlice rowKey,
                                       long walSeq,
                                       long walWriteTime) {
        PayloadSerializer ser = resolveSerializerFromConfig();
        return buildRowPayloadBytes(table, cells, rowKey, walSeq, walWriteTime, ser);
    }

    /**
     * Формирует payload и сериализует его указанным сериализатором (переопределяет сериализатор из конфига).
     */
    public byte[] buildRowPayloadBytes(TableName table,
                                       List<Cell> cells,
                                       RowKeySlice rowKey,
                                       long walSeq,
                                       long walWriteTime,
                                       PayloadSerializer serializer) {
        Objects.requireNonNull(table, "таблица");
        Objects.requireNonNull(serializer, "serializer");
        Map<String, Object> obj = buildRowPayload(table, cells, rowKey, walSeq, walWriteTime);
        if (serializer instanceof TableAwarePayloadSerializer) {
            return ((TableAwarePayloadSerializer) serializer).serialize(table, obj);
        }
        return serializer.serialize(obj);
    }

    /**
     * Формирует карту payload без сериализации — удобно для тестов и альтернативных сериализаторов.
     */
    public Map<String, Object> buildRowPayload(TableName table,
                                               List<Cell> cells,
                                               RowKeySlice rowKey,
                                               long walSeq,
                                               long walWriteTime) {
        Objects.requireNonNull(table, "таблица");
        return assembler.assemble(table, cells, rowKey, walSeq, walWriteTime);
    }

    public long schemaRegistryRegisteredCount() {
        PayloadSerializer serializer = cachedSerializer.get();
        if (serializer instanceof ConfluentAvroPayloadSerializer) {
            return ((ConfluentAvroPayloadSerializer) serializer).metrics().registeredSchemas();
        }
        return 0L;
    }

    public long schemaRegistryFailedCount() {
        PayloadSerializer serializer = cachedSerializer.get();
        if (serializer instanceof ConfluentAvroPayloadSerializer) {
            return ((ConfluentAvroPayloadSerializer) serializer).metrics().registrationFailures();
        }
        return 0L;
    }

    /** Минимальная фабрика сериализаторов для расширяемости. */
    public interface PayloadSerializerFactory {
        PayloadSerializer create(H2kConfig cfg);
    }

    /**
     * Возвращает сериализатор, выбранный по текущей конфигурации (с lazy-кэшированием).
     */
    private PayloadSerializer resolveSerializerFromConfig() {
        PayloadSerializer existing = cachedSerializer.get();
        if (existing != null) {
            return existing;
        }
        PayloadSerializer created = serializerResolver.resolve();
        if (cachedSerializer.compareAndSet(null, created)) {
            return created;
        }
        return cachedSerializer.get();
    }

    /**
     * Человекочитаемое описание активного сериализатора и ключевых параметров Avro.
     */
    public String describeSerializer() {
        PayloadSerializer serializer = resolveSerializerFromConfig();
        StringBuilder sb = new StringBuilder(160);

        H2kConfig.PayloadFormat fmt = cfg.getPayloadFormat();
        if (fmt == null) {
            sb.append("payload.format=JSON_EACH_ROW (default)");
        } else {
            sb.append("payload.format=").append(fmt.name());
        }

        String factoryClass = cfg.getSerializerFactoryClass();
        if (factoryClass != null && !factoryClass.trim().isEmpty()) {
            sb.append(", serializer.factory=").append(factoryClass.trim());
        }

        sb.append(", serializer.class=").append(serializer.getClass().getName());
        sb.append(", tableAware=").append(serializer instanceof TableAwarePayloadSerializer);

        boolean avroFormat = fmt == H2kConfig.PayloadFormat.AVRO_BINARY
                || fmt == H2kConfig.PayloadFormat.AVRO_JSON;
        if (avroFormat) {
            H2kConfig.AvroMode mode = cfg.getAvroMode();
            sb.append(", avro.mode=").append(mode);
            if (mode == H2kConfig.AvroMode.GENERIC) {
                sb.append(", avro.schema.dir=").append(cfg.getAvroSchemaDir());
            } else {
                sb.append(", avro.schema.registry.urls=").append(cfg.getAvroSchemaRegistryUrls());
                sb.append(", avro.schema.registry.auth=")
                        .append(cfg.getAvroSrAuth().isEmpty() ? "disabled" : "configured");
            }
            if (!cfg.getAvroProps().isEmpty()) {
                sb.append(", avro.props.keys=").append(cfg.getAvroProps().keySet());
            }
        }

        return sb.toString();
    }
}
