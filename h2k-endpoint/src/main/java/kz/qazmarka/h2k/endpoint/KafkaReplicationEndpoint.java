package kz.qazmarka.h2k.endpoint;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.nio.file.Paths;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.replication.BaseReplicationEndpoint;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.config.H2kConfig.Keys;
import kz.qazmarka.h2k.endpoint.internal.TopicManager;
import kz.qazmarka.h2k.endpoint.internal.WalEntryProcessor;
import kz.qazmarka.h2k.kafka.ensure.TopicEnsurer;
import kz.qazmarka.h2k.kafka.producer.BatchSender;
import kz.qazmarka.h2k.payload.builder.PayloadBuilder;
import kz.qazmarka.h2k.schema.decoder.Decoder;
import kz.qazmarka.h2k.schema.decoder.SimpleDecoder;
import kz.qazmarka.h2k.schema.decoder.ValueCodecPhoenix;
import kz.qazmarka.h2k.schema.registry.PhoenixTableMetadataProvider;
import kz.qazmarka.h2k.schema.registry.SchemaRegistry;
import kz.qazmarka.h2k.schema.registry.json.JsonSchemaRegistry;
import kz.qazmarka.h2k.schema.registry.avro.phoenix.AvroPhoenixSchemaRegistry;
import kz.qazmarka.h2k.schema.registry.avro.local.AvroSchemaRegistry;

/**
 * Репликация изменений из HBase 1.4.13 в Kafka (производитель 2.x).
 *
 * Назначение
 *  - Принимать батчи WAL-записей от фреймворка репликации HBase и надёжно публиковать их в Kafka.
 *  - Формировать JSON-представление строки по набору ячеек с учётом выбранного декодера.
 *
 * Производительность и потокобезопасность
 *  - Горячий путь максимально прямолинейный: группировка по rowkey → сборка карты → сериализация выбранным
 *    {@link kz.qazmarka.h2k.payload.serializer.PayloadSerializer} → send в Kafka.
 *  - Все тяжёлые инициализации выполняются один раз в init()/doStart(): декодер, конфиг, KafkaProducer, PayloadBuilder.
 *  - Класс используется из потока репликации HBase, дополнительных потоков не создаёт.
 *
 * Логирование
 *  - INFO: только существенные ошибки и факты остановки/старта.
 *  - DEBUG: подробности инициализации, трассировка ошибок отправки, диагностические параметры.
 *  - Все сообщения на русском для удобства сопровождения.
 *
 * Конфигурация
 *  - Основные ключи h2k.* см. в {@link kz.qazmarka.h2k.config.H2kConfig H2kConfig}.
 *  - В частности: bootstrap серверов Kafka, выбор режима декодера, шаблон имён топиков.
 */
public final class KafkaReplicationEndpoint extends BaseReplicationEndpoint {

    /** Логгер класса. Все сообщения — на русском языке. */
    private static final Logger LOG = LoggerFactory.getLogger(KafkaReplicationEndpoint.class);

    // ==== Дефолты локального класса ====
    /** Значение по умолчанию для режима декодирования значений. */
    private static final String DEFAULT_DECODE_MODE = "simple";

    // ядро
    /** Kafka Producer для отправки событий; ключ/значение сериализуются как байты. */
    private Producer<byte[], byte[]> producer;

    // вынесенная конфигурация и сервисы
    /** Иммутабельный снимок настроек h2k.* с предвычисленными флагами для горячего пути. */
    private H2kConfig h2k;
    private TopicManager topicManager;
    private WalEntryProcessor walEntryProcessor;
    // CF-фильтр
    private boolean doFilter;
    private byte[][] cfFamilies;
    private PhoenixTableMetadataProvider tableMetadataProvider = PhoenixTableMetadataProvider.NOOP;

    /**
     * Инициализация эндпоинта: чтение конфигурации, подготовка продьюсера, декодера и сборщика payload.
     * Выполняется один раз на старте RegionServer.
     * @param context контекст от HBase
     * @throws IOException если отсутствует обязательный параметр h2k.kafka.bootstrap.servers
     */
    @Override
    public void init(ReplicationEndpoint.Context context) throws IOException {
        super.init(context);
        final Configuration cfg = context.getConfiguration();

        // обязательный параметр
        final String bootstrap = readBootstrapOrThrow(cfg);

        // producer
        setupProducer(cfg, bootstrap);

        // decoder
        Decoder decoder = chooseDecoder(cfg);

        // immut-конфиг, билдер и энсюрер
        this.h2k = H2kConfig.from(cfg, bootstrap, tableMetadataProvider);
        logSaltSummary();
        final PayloadBuilder payload = new PayloadBuilder(decoder, h2k);
        final TopicEnsurer topicEnsurer = TopicEnsurer.createIfEnabled(h2k);
        this.topicManager = new TopicManager(h2k, topicEnsurer);
        this.walEntryProcessor = new WalEntryProcessor(payload, topicManager, producer);
        registerMetrics(payload, walEntryProcessor);
        initCfFilter();
        logPayloadSerializer(payload);
        logInitSummary();
    }

    /**
     * Печатает сводку по карте «соли» rowkey (h2k.salt.map) в DEBUG. Ошибки не критичны.
     */
    private void logSaltSummary() {
        if (!LOG.isDebugEnabled()) {
            return;
        }
        try {
            final Map<String, Integer> saltMap = h2k.getSaltBytesByTable();
            if (saltMap == null || saltMap.isEmpty()) {
                LOG.debug("Соль rowkey не задана (h2k.salt.map).");
            } else {
                LOG.debug("Соль rowkey (h2k.salt.map): {} ({} записей)", saltMap, saltMap.size());
            }
        } catch (Exception t) {
            LOG.debug("Не удалось вывести сводку h2k.salt.map (не критично)", t);
        }
    }

    /**
     * Инициализирует фильтрацию по CF из конфигурации h2k.cf.list. Выполняется один раз.
     */
    private void initCfFilter() {
        if (!h2k.isCfFilterExplicit()) {
            this.cfFamilies = null;
            this.doFilter = false;
            logCfFilterState();
            return;
        }

        final byte[][] families = h2k.getCfFamiliesBytes();
        if (families.length == 0) {
            this.cfFamilies = null;
            this.doFilter = false;
        } else {
            this.cfFamilies = families;
            this.doFilter = true;
        }
        logCfFilterState();
    }

    private void logCfFilterState() {
        if (!LOG.isDebugEnabled()) {
            return;
        }
        final int count = (cfFamilies == null) ? 0 : cfFamilies.length;
        LOG.debug("CF-фильтр: enabled={}, cf.count={}, cf.list={}", doFilter, count, h2k.getCfNamesCsv());
    }

    /**
     * Итоговая сводка параметров инициализации в DEBUG.
     */
    private void logInitSummary() {
        if (!LOG.isDebugEnabled()) {
            return;
        }
        LOG.debug("Инициализация завершена: шаблон_топика={}, cf_список={}, проверять_топики={}, включать_rowkey={}, кодировка_rowkey={}, включать_meta={}, включать_meta_WAL={}, счетчики_батчей={}, debug_батчей_при_ошибке={}",
                h2k.getTopicPattern(),
                h2k.getCfNamesCsv(),
                h2k.isEnsureTopics(), h2k.isIncludeRowKey(), h2k.getRowkeyEncoding(), h2k.isIncludeMeta(), h2k.isIncludeMetaWal(),
                h2k.isProducerBatchCountersEnabled(), h2k.isProducerBatchDebugOnFailure());
    }

    /**
     * Выводит в INFO итоговый режим сериализации payload (JSON/AVRO, локальные схемы или Schema Registry).
     * Видно даже при стандартном уровне логирования.
     */
    private void logPayloadSerializer(PayloadBuilder payload) {
        if (!LOG.isInfoEnabled()) {
            return;
        }
        try {
            LOG.info("Payload: {}", payload.describeSerializer());
        } catch (RuntimeException ex) {
            LOG.warn("Не удалось определить активный сериализатор payload: {}", ex.toString());
            if (LOG.isDebugEnabled()) {
                LOG.debug("Трассировка ошибки сериализатора", ex);
            }
        }
    }

    private void registerMetrics(PayloadBuilder payload, WalEntryProcessor walProcessor) {
        try {
            topicManager.registerMetric("wal.entries.total", walProcessor::entriesTotal);
            topicManager.registerMetric("wal.rows.total", walProcessor::rowsTotal);
            topicManager.registerMetric("wal.cells.total", walProcessor::cellsTotal);
            topicManager.registerMetric("wal.rows.filtered", walProcessor::rowsFilteredTotal);
            topicManager.registerMetric("schema.registry.register.success", payload::schemaRegistryRegisteredCount);
            topicManager.registerMetric("schema.registry.register.failures", payload::schemaRegistryFailedCount);
        } catch (RuntimeException ex) {
            LOG.warn("Не удалось зарегистрировать метрики TopicManager: {}", ex.toString());
            if (LOG.isDebugEnabled()) {
                LOG.debug("Трассировка ошибки при регистрации метрик", ex);
            }
        }
    }
    /**
     * Возвращает строку bootstrap‑серверов Kafka из конфигурации или бросает {@link IOException}, если параметр отсутствует.
     * Пустая строка после {@code trim()} считается отсутствующим параметром.
     */
    private static String readBootstrapOrThrow(Configuration cfg) throws IOException {
        final String bootstrap = cfg.get(Keys.BOOTSTRAP, "").trim();
        if (bootstrap.isEmpty()) {
            throw new IOException("Отсутствует обязательный параметр конфигурации: " + Keys.BOOTSTRAP);
        }
        return bootstrap;
    }

    /**
     * Выбирает и настраивает декодер значений по конфигурации.
     * Режимы:
     *  - {@code simple} — простой декодер без схемы (по умолчанию);
     *  - {@code phoenix-avro} — считывает типы и PK из локальных `.avsc` (атрибуты {@code h2k.phoenixType}, {@code h2k.pk})
     *    с необязательным фолбэком на {@code schema.json};
     *  - {@code json-phoenix} — декодер на основе Phoenix-схемы из {@code h2k.schema.path}.
     * При ошибке инициализации схемы выполняется безопасный фолбэк на {@link SimpleDecoder#INSTANCE}.
     *
     * @param cfg HBase-конфигурация
     * @return выбранный декодер
     */
    private Decoder chooseDecoder(Configuration cfg) {
        String mode = cfg.get(Keys.DECODE_MODE, DEFAULT_DECODE_MODE);
        if (mode != null) {
            mode = mode.trim();
        }
        tableMetadataProvider = PhoenixTableMetadataProvider.NOOP;
        if ("json-phoenix".equalsIgnoreCase(mode)) {
            return initJsonPhoenixDecoder(cfg);
        }
        if ("phoenix-avro".equalsIgnoreCase(mode) || "phoenix".equalsIgnoreCase(mode)) {
            return initAvroPhoenixDecoder(cfg);
        }
        LOG.debug("Режим декодирования: simple");
        return SimpleDecoder.INSTANCE;
    }

    private Decoder initJsonPhoenixDecoder(Configuration cfg) {
        final String rawPath = cfg.get(Keys.SCHEMA_PATH);
        if (rawPath == null || rawPath.trim().isEmpty()) {
            throw new IllegalStateException(
                "Включён режим json-phoenix, но не задан обязательный параметр h2k.schema.path");
        }
        final String path = rawPath.trim();
        try {
            SchemaRegistry schema = new JsonSchemaRegistry(path);
            LOG.debug("Режим декодирования: json-phoenix, схема={}", path);
            if (schema instanceof PhoenixTableMetadataProvider) {
                tableMetadataProvider = (PhoenixTableMetadataProvider) schema;
            }
            return new ValueCodecPhoenix(schema);
        } catch (Exception e) {
            LOG.warn("Не удалось инициализировать JsonSchemaRegistry ({}): {} — переключаюсь на простой декодер",
                    path, e.toString());
            if (LOG.isDebugEnabled()) {
                LOG.debug("Трассировка ошибки JsonSchemaRegistry", e);
            }
            tableMetadataProvider = PhoenixTableMetadataProvider.NOOP;
            return SimpleDecoder.INSTANCE;
        }
    }

    private Decoder initAvroPhoenixDecoder(Configuration cfg) {
        final String configuredDir = cfg.getTrimmed(H2kConfig.Keys.AVRO_SCHEMA_DIR);
        final String schemaDir = (configuredDir == null || configuredDir.isEmpty())
                ? H2kConfig.DEFAULT_AVRO_SCHEMA_DIR
                : configuredDir;
        final SchemaRegistry legacy = tryInitLegacyJsonRegistry(cfg.get(Keys.SCHEMA_PATH));
        try {
            AvroSchemaRegistry avro = new AvroSchemaRegistry(Paths.get(schemaDir));
            SchemaRegistry registry = new AvroPhoenixSchemaRegistry(avro, legacy);
            if (legacy == SchemaRegistry.NOOP) {
                LOG.debug("Режим декодирования: phoenix-avro, каталог={}", schemaDir);
            } else {
                LOG.debug("Режим декодирования: phoenix-avro, каталог={}, legacy schema.json=включён", schemaDir);
            }
            if (registry instanceof PhoenixTableMetadataProvider) {
                tableMetadataProvider = (PhoenixTableMetadataProvider) registry;
            } else {
                tableMetadataProvider = PhoenixTableMetadataProvider.NOOP;
            }
            return new ValueCodecPhoenix(registry);
        } catch (Exception e) {
            LOG.warn("Не удалось инициализировать AvroPhoenixSchemaRegistry ({}): {} — переключаюсь на простой декодер",
                    schemaDir, e.toString());
            if (LOG.isDebugEnabled()) {
                LOG.debug("Трассировка ошибки AvroPhoenixSchemaRegistry", e);
            }
            tableMetadataProvider = PhoenixTableMetadataProvider.NOOP;
            return SimpleDecoder.INSTANCE;
        }
    }

    private SchemaRegistry tryInitLegacyJsonRegistry(String rawPath) {
        if (rawPath == null) {
            return SchemaRegistry.NOOP;
        }
        final String path = rawPath.trim();
        if (path.isEmpty()) {
            return SchemaRegistry.NOOP;
        }
        try {
            return new JsonSchemaRegistry(path);
        } catch (Exception e) {
            LOG.warn("Фолбэк schema.json не инициализирован ({}): {} — будет проигнорирован", path, e.toString());
            if (LOG.isDebugEnabled()) {
                LOG.debug("Трассировка ошибки schema.json", e);
            }
            return SchemaRegistry.NOOP;
        }
    }

    /**
     * Конфигурация и создание {@link KafkaProducer}.
     * Заполняет обязательные параметры сериализации ключа/значения, включает идемпотентность и другие
     * безопасные дефолты, затем применяет любые переопределения из префикса {@code h2k.producer.*}.
     *
     * @param cfg HBase-конфигурация (источник префиксных параметров)
     * @param bootstrap список брокеров Kafka (формат host:port[,host2:port2])
     * @implNote Сборка properties вынесена в {@code ProducerPropsFactory.build(...)}. Конфигурация cfg уже включает peer-CONFIG; приоритет peer над hbase-site.xml сохраняется.
     */
    private void setupProducer(Configuration cfg, String bootstrap) {
        Properties props = ProducerPropsFactory.build(cfg, bootstrap);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Kafka‑producer: client.id={}, брокеры={}, acks={}, компрессия={}, linger.ms={}, batch.size={}, идемпотентность={}",
                    props.get(ProducerConfig.CLIENT_ID_CONFIG),
                    props.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG),
                    props.get(ProducerConfig.ACKS_CONFIG),
                    props.get(ProducerConfig.COMPRESSION_TYPE_CONFIG),
                    props.get(ProducerConfig.LINGER_MS_CONFIG),
                    props.get(ProducerConfig.BATCH_SIZE_CONFIG),
                    props.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
        }

        this.producer = new KafkaProducer<>(props);
    }

    /**
     * Основной цикл обработки партии WAL‑записей: группировка по rowkey, сборка JSON и асинхронная отправка сообщений в Kafka
     * с последующим ожиданием подтверждений.
     *
     * @param ctx контекст с WAL‑записями
     * @return {@code true} — продолжать репликацию; {@code false} — попросить HBase повторить партию
     * @implNote Прерывание потока конвертируется в {@code false} с восстановлением флага прерывания.
     */
    @Override
    public boolean replicate(ReplicationEndpoint.ReplicateContext ctx) {
        try {
            final List<WAL.Entry> entries = ctx.getEntries();
            if (entries == null || entries.isEmpty()) return true;

            final int awaitEvery = h2k.getAwaitEvery();
            final int awaitTimeoutMs = h2k.getAwaitTimeoutMs();
            final boolean includeWalMeta = h2k.isIncludeMetaWal();

            if (LOG.isDebugEnabled()) {
                LOG.debug("Репликация: записей={}, awaitEvery={}, awaitTimeoutMs={}, включать WAL‑мета={}",
                        entries.size(), awaitEvery, awaitTimeoutMs, includeWalMeta);
            }

            try (BatchSender sender = new BatchSender(
                    awaitEvery,
                    awaitTimeoutMs,
                    h2k.isProducerBatchCountersEnabled(),
                    h2k.isProducerBatchDebugOnFailure())) {
                for (WAL.Entry entry : entries) {
                    processEntry(entry, sender, includeWalMeta, this.doFilter, this.cfFamilies);
                }
                // Строгий flush выполнится при закрытии sender (AutoCloseable)
                return true;
            }

        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOG.warn("Репликация: поток прерван; запросим повтор партии", ie);
            return false;
        } catch (java.util.concurrent.ExecutionException ee) {
            LOG.error("Репликация: ошибка при ожидании подтверждений Kafka", ee);
            return false;
        } catch (java.util.concurrent.TimeoutException te) {
            LOG.error("Репликация: таймаут ожидания подтверждений Kafka", te);
            return false;
        } catch (org.apache.kafka.common.KafkaException ke) {
            LOG.error("Репликация: ошибка продьюсера Kafka", ke);
            return false;
        } catch (Exception e) {
            // сюда могут попасть исключения из try-with-resources (sender.close())
            LOG.error("Репликация: непредвиденная ошибка", e);
            return false;
        }
    }

    /**
     * Обрабатывает один {@link WAL.Entry}: вычисляет имя топика, безопасно проверяет/создаёт тему,
     * формирует и отправляет сообщения по строкам.
     *
     * @param entry         запись WAL
     * @param sender        батч‑отправитель для ожидания ack по порогам
     * @param includeWalMeta включать ли метаданные WAL в payload
     * @param doFilter      включена ли фильтрация по WAL‑timestamp
     * @param cfFamilies    список целевых CF (в байтах) для фильтра; {@code null}, если фильтр выключен
     */
    private void processEntry(WAL.Entry entry,
                              BatchSender sender,
                              boolean includeWalMeta,
                              boolean doFilter,
                              byte[][] cfFamilies) {
        walEntryProcessor.process(entry, sender, includeWalMeta, doFilter, cfFamilies);
    }

    /** В HBase 1.4 {@code Context} не предоставляет getPeerUUID(); сигнатура метода требуется API базового класса.
     *  Для совместимости с этой версией возвращаем {@code null} (допустимое значение для данного API).
     */
    @Override public UUID getPeerUUID() { return null; }
    /**
     * Сообщает фреймворку об успешном старте эндпоинта.
     */
    @Override
    protected void doStart() {
        notifyStarted();
    }
    /**
     * Корректное завершение работы: сброс и закрытие Kafka‑producer и TopicEnsurer, затем уведомление о стопе.
     * Исключения при закрытии не пробрасываются и логируются на уровне DEBUG.
     */
    @Override protected void doStop() {
        try {
            if (producer != null) {
                producer.flush();
                producer.close();
            }
        } catch (Exception e) {
            // При завершении работы проглатываем исключение, но выводим debug для диагностики
            if (LOG.isDebugEnabled()) {
                LOG.debug("doStop(): ошибка при закрытии Kafka producer (игнорируется при завершении работы)", e);
            }
        }
        if (topicManager != null) {
            topicManager.closeQuietly();
        }
        notifyStopped();
    }
    /**
     * Построитель настроек {@link KafkaProducer}. Выполняется один раз при старте; не участвует в горячем пути.
     * Собирает безопасные дефолты и применяет переопределения из префикса {@code h2k.producer.*}
     * без перезаписи уже заданных ключей.
     */
    private static final class ProducerPropsFactory {
        private ProducerPropsFactory() {}

        /**
         * Формирует {@link Properties} для {@link KafkaProducer}:
         *  - базовые обязательные параметры (bootstrap, сериализаторы);
         *  - безопасные дефолты (acks=all, enable.idempotence=true, retries и т.д.);
         *  - pass-through: только валидные {@code h2k.producer.*} прокидываются как «родные» ключи продьюсера, если не заданы выше;
         *  - {@code client.id}: по умолчанию префикс + hostname + короткий случайный суффикс, фолбэк — UUID.
         *
         * @param cfg       HBase-конфигурация
         * @param bootstrap список брокеров Kafka (host:port[,host2:port2])
         * @return заполненный набор свойств для конструктора продьюсера
         */
        static Properties build(Configuration cfg, String bootstrap) {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");

            // дефолты с возможностью переопределения через h2k.producer.*
            props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, cfg.get("h2k.producer.enable.idempotence", "true"));
            props.put(ProducerConfig.ACKS_CONFIG, cfg.get("h2k.producer.acks", "all"));
            props.put(ProducerConfig.RETRIES_CONFIG, cfg.get("h2k.producer.retries", String.valueOf(Integer.MAX_VALUE)));
            props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, cfg.get("h2k.producer.delivery.timeout.ms", "180000"));
            props.put(ProducerConfig.LINGER_MS_CONFIG, cfg.get("h2k.producer.linger.ms", "50"));
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, cfg.get("h2k.producer.batch.size", "65536"));
            props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, cfg.get("h2k.producer.compression.type", "lz4"));
            props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, cfg.get("h2k.producer.max.in.flight", "1"));

            // Набор валидных ключей Kafka‑продьюсера (используем для фильтрации pass‑through)
            final java.util.Set<String> kafkaValidKeys = org.apache.kafka.clients.producer.ProducerConfig.configNames();
            // Наши «служебные» ключи, которые не должны попадать в конфиг Kafka‑продьюсера
            final java.util.Set<String> h2kInternalKeys = new java.util.HashSet<>(
                    java.util.Arrays.asList(
                            "await.every",
                            "await.timeout.ms",
                            "batch.counters.enabled",
                            "batch.debug.on.failure"
                    )
            );

            // client.id: базово per‑host; для дефолтного значения добавляем короткий случайный суффикс,
            // чтобы избежать коллизии MBean "kafka.producer:type=app-info,id=<client.id>" при нескольких пирах в одном JVM.
            String computedDefaultClientId;
            try {
                String host = java.net.InetAddress.getLocalHost().getHostName();
                computedDefaultClientId = (host == null || host.isEmpty())
                        ? (H2kConfig.DEFAULT_ADMIN_CLIENT_ID + '-' + java.util.UUID.randomUUID())
                        : (H2kConfig.DEFAULT_ADMIN_CLIENT_ID + '-' + host);
            } catch (java.net.UnknownHostException ignore) {
                computedDefaultClientId = H2kConfig.DEFAULT_ADMIN_CLIENT_ID + '-' + java.util.UUID.randomUUID();
            }
            // Пользователь может явно задать h2k.producer.client.id — тогда уважаем его как есть.
            String requestedClientId = cfg.get("h2k.producer.client.id", computedDefaultClientId);
            String clientIdToUse = requestedClientId;
            if (requestedClientId.equals(computedDefaultClientId)) {
                // Добавляем короткий суффикс только для дефолтного значения
                String rnd8 = java.util.UUID.randomUUID().toString().substring(0, 8);
                clientIdToUse = requestedClientId + '-' + rnd8;
            }
            props.put(org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG, clientIdToUse);

            // pass‑through: только «родные» ключи Kafka‑продьюсера; все прочие игнорируем
            final String prefix = Keys.PRODUCER_PREFIX;
            final int prefixLen = prefix.length();
            for (java.util.Map.Entry<String, String> e : cfg) {
                final String k = e.getKey();
                final boolean hasPrefix = k.startsWith(prefix);
                final boolean isAlias   = "h2k.producer.max.in.flight".equals(k);
                if (hasPrefix && !isAlias) {
                    final String real = k.substring(prefixLen);
                    final boolean isInternal = h2kInternalKeys.contains(real);
                    final boolean isKafkaKey = kafkaValidKeys.contains(real);
                    if (!isInternal && isKafkaKey) {
                        props.putIfAbsent(real, e.getValue());
                    }
                }
            }
            return props;
        }
    }
}
