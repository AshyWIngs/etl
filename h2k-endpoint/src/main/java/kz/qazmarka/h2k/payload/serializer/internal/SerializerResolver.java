package kz.qazmarka.h2k.payload.serializer.internal;

import org.slf4j.Logger;

import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.payload.builder.PayloadBuilder;
import kz.qazmarka.h2k.payload.serializer.JsonEachRowSerializer;
import kz.qazmarka.h2k.payload.serializer.PayloadSerializer;
import kz.qazmarka.h2k.payload.serializer.avro.ConfluentAvroPayloadSerializer;
import kz.qazmarka.h2k.payload.serializer.avro.GenericAvroPayloadSerializer;
import kz.qazmarka.h2k.payload.serializer.avro.SchemaRegistryClientFactory;

/**
 * Инкапсулирует выбор и создание {@link PayloadSerializer} на основе конфигурации.
 */
public final class SerializerResolver {

    private final Logger log;
    private final H2kConfig cfg;
    private final SchemaRegistryClientFactory schemaRegistryClientFactory;

    /**
     * @param log   логгер, в который будут писаться предупреждения о кастомных сериализаторах
     * @param cfg   конфигурация h2k
     * @param schemaRegistryClientFactory фабрика клиентов Schema Registry (для режима confluent)
     */
    public SerializerResolver(Logger log, H2kConfig cfg, SchemaRegistryClientFactory schemaRegistryClientFactory) {
        this.log = log;
        this.cfg = cfg;
        this.schemaRegistryClientFactory = schemaRegistryClientFactory;
    }

    /**
     * Подбирает сериализатор на основании конфигурации: сначала кастомная фабрика, затем стандартные режимы.
     */
    public PayloadSerializer resolve() {
        PayloadSerializer custom = customSerializer();
        if (custom != null) {
            return custom;
        }

        H2kConfig.PayloadFormat fmt = cfg.getPayloadFormat();
        if (fmt == null || fmt == H2kConfig.PayloadFormat.JSON_EACH_ROW) {
            return new JsonEachRowSerializer();
        }

        switch (fmt) {
            case AVRO_BINARY:
                return avroBinarySerializer();
            case AVRO_JSON:
                return avroJsonSerializer();
            default:
                throw new IllegalStateException("Неизвестный формат payload: " + fmt);
        }
    }

    /**
     * Пытается создать сериализатор через пользовательский класс (фабрика или прямой сериализатор).
     *
     * @return сериализатор или {@code null}, если использовать стандартную конфигурацию
     */
    private PayloadSerializer customSerializer() {
        String fqcn = cfg.getSerializerFactoryClass();
        if (fqcn == null || fqcn.trim().isEmpty()) {
            return null;
        }
        try {
            Class<?> cls = Class.forName(fqcn);
            if (PayloadBuilder.PayloadSerializerFactory.class.isAssignableFrom(cls)) {
                PayloadBuilder.PayloadSerializerFactory factory =
                        (PayloadBuilder.PayloadSerializerFactory) cls.getDeclaredConstructor().newInstance();
                PayloadSerializer created = factory.create(cfg);
                if (created != null) {
                    return created;
                }
                log.warn("Сериализатор '{}': фабрика вернула null, использую h2k.payload.format={}.", fqcn, cfg.getPayloadFormat());
                return null;
            }
            if (PayloadSerializer.class.isAssignableFrom(cls)) {
                return cls.asSubclass(PayloadSerializer.class).getDeclaredConstructor().newInstance();
            }
            log.warn("Сериализатор '{}': класс не реализует PayloadSerializer/Factory. Использую h2k.payload.format={}.",
                    fqcn, cfg.getPayloadFormat());
        } catch (ReflectiveOperationException | ClassCastException ex) {
            log.warn("Сериализатор '{}': не удалось создать экземпляр ({}). Использую h2k.payload.format={}.",
                    fqcn, ex, cfg.getPayloadFormat());
            if (log.isDebugEnabled()) {
                log.debug("Трассировка ошибки сериализатора '{}'", fqcn, ex);
            }
        }
        return null;
    }

    private PayloadSerializer avroBinarySerializer() {
        if (cfg.getAvroMode() == H2kConfig.AvroMode.CONFLUENT) {
            return new ConfluentAvroPayloadSerializer(cfg, schemaRegistryClientFactory);
        }
        return new GenericAvroPayloadSerializer(cfg, GenericAvroPayloadSerializer.Encoding.BINARY);
    }

    /** Возвращает JSON Avro сериализатор (только для режима generic). */
    private PayloadSerializer avroJsonSerializer() {
        if (cfg.getAvroMode() == H2kConfig.AvroMode.CONFLUENT) {
            throw new IllegalStateException("Avro: режим confluent поддерживает только avro-binary");
        }
        return new GenericAvroPayloadSerializer(cfg, GenericAvroPayloadSerializer.Encoding.JSON);
    }
}
