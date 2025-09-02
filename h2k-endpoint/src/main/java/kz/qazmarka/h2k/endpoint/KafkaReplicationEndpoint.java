package kz.qazmarka.h2k.endpoint;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.replication.BaseReplicationEndpoint;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.kafka.BatchSender;
import kz.qazmarka.h2k.kafka.TopicEnsurer;
import kz.qazmarka.h2k.payload.PayloadBuilder;
import kz.qazmarka.h2k.schema.Decoder;
import kz.qazmarka.h2k.schema.JsonSchemaRegistry;
import kz.qazmarka.h2k.schema.SchemaRegistry;
import kz.qazmarka.h2k.schema.SimpleDecoder;
import kz.qazmarka.h2k.schema.ValueCodecPhoenix;
import kz.qazmarka.h2k.util.RowKeySlice;

/**
 * Репликация изменений из HBase (1.4.13) в Kafka (2.3.1).
 *
 * Высокоуровневый поток выполнения:
 * 1) HBase вызывает {@link #init(ReplicationEndpoint.Context)} — читаем конфигурацию, создаём Kafka Producer,
 *    настраиваем декодер значений и конструктор payload.
 * 2) Для каждой партии WAL-записей HBase вызывает {@link #replicate(ReplicationEndpoint.ReplicateContext)} —
 *    группируем клетки по rowkey, опционально фильтруем по WAL timestamp, собираем JSON и отправляем в Kafka.
 * 3) {@link #doStart()} и {@link #doStop()} управляют жизненным циклом (старт/остановка, корректное закрытие ресурсов).
 *
 * Основные конфигурационные ключи:
 * - h2k.kafka.bootstrap.servers — адрес(а) брокеров Kafka (обязательный параметр).
 * - h2k.json.serialize.nulls — сериализовать ли null-поля в JSON (по умолчанию false).
 * - h2k.decode.mode — режим декодирования значений: simple (по умолчанию) или json-phoenix.
 * - h2k.schema.path — путь к JSON-схеме для режима json-phoenix.
 * - см. также параметры из {@link kz.qazmarka.h2k.config.H2kConfig H2kConfig} с префиксом h2k.*.
 *
 * Сообщения логов и ошибок в этом классе ведутся на русском языке для удобства сопровождения.
 */
public final class KafkaReplicationEndpoint extends BaseReplicationEndpoint {

    /** Логгер класса. Все сообщения — на русском языке. */
    private static final Logger LOG = LoggerFactory.getLogger(KafkaReplicationEndpoint.class);

    // ==== Дефолты локального класса ====
    private static final String DEFAULT_DECODE_MODE = "simple";
    private static final String DEFAULT_CLIENT_ID   = "hbase1-repl";

    // ядро
    /** Kafka Producer: отправка сообщений (ключ/значение — байты). */
    private Producer<byte[], byte[]> producer;
    /** Gson для сериализации payload в JSON (disableHtmlEscaping, опционально serializeNulls). */
    private Gson gson;

    // вынесенная конфигурация и сервисы
    /** Иммутабельная конфигурация h2k.*, предвычисленные флаги для горячего пути. */
    private H2kConfig h2k;
    /** Сборщик JSON-представления строки (ячейки → объект), учитывает режим декодирования. */
    private PayloadBuilder payload;
    /** Сервис проверки/создания топиков (может быть null, если ensureTopics=false). */
    private TopicEnsurer topicEnsurer; // может быть null, если ensureTopics=false

    /** Последний успешно проверенный/созданный топик (для подавления повторных ensure в одном и том же потоке). */
    private String lastEnsuredTopic;

    /**
     * Инициализация эндпоинта репликации.
     * Читает HBase-конфигурацию, валидирует обязательные параметры, создаёт Kafka Producer,
     * настраивает JSON-сериализацию и декодер значений, формирует неизменяемую {@link H2kConfig}.
     * @param context контекст репликации от HBase
     * @throws IOException если не указан обязательный параметр h2к.kafka.bootstrap.servers
     */
    @Override
    public void init(ReplicationEndpoint.Context context) throws IOException {
        super.init(context);
        final Configuration cfg = context.getConfiguration();

        // обязательный параметр
        final String bootstrap = readBootstrapOrThrow(cfg);

        // producer
        setupProducer(cfg, bootstrap);

        // gson
        this.gson = buildGson(cfg);

        // decoder
        Decoder decoder = chooseDecoder(cfg);

        // immut-конфиг, билдер и энсюрер
        this.h2k = H2kConfig.from(cfg, bootstrap);
        this.payload = new PayloadBuilder(decoder, h2k);
        this.topicEnsurer = TopicEnsurer.createIfEnabled(h2k);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Инициализация завершена: topicPattern={}, cf={}, ensureTopics={}, filterByWalTs={}, includeRowKey={}, rowkeyEncoding={}, includeMeta={}, includeMetaWal={}, h2k.isProducerBatchCountersEnabled(), h2k.isProducerBatchDebugOnFailure()",
                    h2k.getTopicPattern(),
                    h2k.getCfNameForPayload(),
                    h2k.isEnsureTopics(), h2k.isFilterByWalTs(), h2k.isIncludeRowKey(), h2k.getRowkeyEncoding(), h2k.isIncludeMeta(), h2k.isIncludeMetaWal());
        }
    }
    /** Читает bootstrap и валидирует обязательность параметра. */
    private static String readBootstrapOrThrow(Configuration cfg) throws IOException {
        final String bootstrap = cfg.get(kz.qazmarka.h2k.config.H2kConfig.Keys.BOOTSTRAP, "").trim();
        if (bootstrap.isEmpty()) {
            throw new IOException("Отсутствует обязательный параметр конфигурации: " + kz.qazmarka.h2k.config.H2kConfig.Keys.BOOTSTRAP);
        }
        return bootstrap;
    }

    /** Создаёт Gson с учётом флага сериализации null-полей. */
    private static Gson buildGson(Configuration cfg) {
        boolean serializeNulls = cfg.getBoolean(kz.qazmarka.h2k.config.H2kConfig.Keys.JSON_SERIALIZE_NULLS, false);
        GsonBuilder gb = new GsonBuilder().disableHtmlEscaping();
        if (serializeNulls) gb.serializeNulls();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Gson: serializeNulls={}", serializeNulls);
        }
        return gb.create();
    }

    /** Выбирает и настраивает декодер значений по конфигурации. */
    private Decoder chooseDecoder(Configuration cfg) {
        String mode = cfg.get(kz.qazmarka.h2k.config.H2kConfig.Keys.DECODE_MODE, DEFAULT_DECODE_MODE);
        if ("json-phoenix".equalsIgnoreCase(mode)) {
            String schemaPath = cfg.get(kz.qazmarka.h2k.config.H2kConfig.Keys.SCHEMA_PATH);
            if (schemaPath == null || schemaPath.trim().isEmpty()) {
                LOG.warn("Включён режим json-phoenix, но не задан путь схемы (h2k.schema.path) — переключаюсь на простой декодер");
                return SimpleDecoder.INSTANCE;
            }
            SchemaRegistry schema = new JsonSchemaRegistry(schemaPath.trim());
            LOG.debug("Режим декодирования: json-phoenix, схема={}", schemaPath);
            return new ValueCodecPhoenix(schema);
        }
        LOG.debug("Режим декодирования: simple");
        return SimpleDecoder.INSTANCE;
    }

    /**
     * Подбирает начальную ёмкость LinkedHashMap под ожидаемое число пар (учитывая loadFactor 0.75).
     * Эквивалент ⌈expected/0.75⌉, но без операций с плавающей точкой.
     */
    private static int capacityFor(int expected) {
        if (expected <= 0) return 16; // дефолтная начальная ёмкость
        long cap = (expected * 4L) / 3L + 1L;
        return cap >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) cap;
    }

    /**
     * Конфигурация и создание Kafka Producer.
     * Заполняет обязательные параметры сериализации ключа/значения, включает идемпотентность и другие
     * безопасные дефолты, затем применяет любые переопределения из префикса h2k.producer.*.
     * @param cfg HBase-конфигурация (источник префиксных параметров)
     * @param bootstrap список брокеров Kafka (host:port[,host2:port2])
     */
    private void setupProducer(Configuration cfg, String bootstrap) {
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

        try {
            String host = InetAddress.getLocalHost().getHostName();
            String fallback = (host == null || host.isEmpty())
                    ? (DEFAULT_CLIENT_ID + '-' + java.util.UUID.randomUUID())
                    : (DEFAULT_CLIENT_ID + '-' + host);
            props.put(ProducerConfig.CLIENT_ID_CONFIG, cfg.get("h2k.producer.client.id", fallback));
        } catch (java.net.UnknownHostException ignore) {
            String fallback = DEFAULT_CLIENT_ID + '-' + java.util.UUID.randomUUID();
            props.put(ProducerConfig.CLIENT_ID_CONFIG, cfg.get("h2k.producer.client.id", fallback));
        }

        final String prefix = kz.qazmarka.h2k.config.H2kConfig.Keys.PRODUCER_PREFIX;
        final int prefixLen = prefix.length();
        for (Map.Entry<String, String> e : cfg) {
            final String k = e.getKey();
            if (k.startsWith(prefix)) {
                final String real = k.substring(prefixLen);
                props.putIfAbsent(real, e.getValue());
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Kafka producer: client.id={}, bootstrap={}, acks={}, compression={}, linger.ms={}, batch.size={}, idempotence={}",
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
     * Основной цикл репликации для партии WAL-записей.
     * Группирует клетки по rowkey, фильтрует при необходимости, собирает JSON и отправляет в Kafka
     * с батчевым ожиданием подтверждений (см. {@link H2kConfig#getAwaitEvery()} и {@link H2kConfig#getAwaitTimeoutMs()}).
     * @param ctx контекст с WAL-записями
     * @return true — продолжать репликацию; false — повторить/переотправить партию (HBase может вызвать снова)
     */
    @Override
    public boolean replicate(ReplicationEndpoint.ReplicateContext ctx) {
        try {
            final List<WAL.Entry> entries = ctx.getEntries();
            if (entries == null || entries.isEmpty()) return true;

            final int awaitEvery = h2k.getAwaitEvery();
            final int awaitTimeoutMs = h2k.getAwaitTimeoutMs();
            final boolean includeWalMeta = h2k.isIncludeMetaWal();
            final boolean doFilter = h2k.isFilterByWalTs();
            final byte[] cfBytes = doFilter ? h2k.getCfBytes() : null;
            final long minTs = doFilter ? h2k.getWalMinTs() : -1L;

            try (BatchSender sender = new BatchSender(
                    awaitEvery,
                    awaitTimeoutMs,
                    h2k.isProducerBatchCountersEnabled(),
                    h2k.isProducerBatchDebugOnFailure())) {
                for (WAL.Entry entry : entries) {
                    processEntry(entry, sender, includeWalMeta, doFilter, cfBytes, minTs);
                }
                // Строгий flush выполнится при закрытии sender (AutoCloseable)
                return true;
            }

        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOG.warn("replicate(): поток был прерван; попробуем ещё раз", ie);
            return false;
        } catch (java.util.concurrent.ExecutionException ee) {
            LOG.error("replicate(): ошибка при ожидании подтверждений Kafka", ee);
            return false;
        } catch (java.util.concurrent.TimeoutException te) {
            LOG.error("replicate(): таймаут ожидания подтверждений Kafka", te);
            return false;
        } catch (org.apache.kafka.common.KafkaException ke) {
            LOG.error("replicate(): ошибка продьюсера Kafka", ke);
            return false;
        } catch (Exception e) {
            // сюда могут попасть исключения из try-with-resources (sender.close())
            LOG.error("replicate(): непредвиденная ошибка", e);
            return false;
        }
    }

    /** Обработка одного WAL.Entry. */
    private void processEntry(WAL.Entry entry,
                            BatchSender sender,
                            boolean includeWalMeta,
                            boolean doFilter,
                            byte[] cfBytes,
                            long minTs) {
        TableName table = entry.getKey().getTablename();
        String topic = h2k.topicFor(table);
        WalMeta wm = includeWalMeta ? readWalMeta(entry) : WalMeta.EMPTY;
        ensureTopicSafely(topic);
        for (Map.Entry<RowKeySlice, List<Cell>> rowEntry : filteredRows(entry, doFilter, cfBytes, minTs)) {
            sendRow(topic, table, wm, rowEntry, sender);
        }
    }

    /**
     * Возвращает только те строки, которые проходят фильтр по WAL timestamp для целевого CF.
     * Если фильтр выключен (h2k.filterByWalTs() == false), возвращает все строки без копирования.
     */
    private Iterable<Map.Entry<RowKeySlice, List<Cell>>> filteredRows(WAL.Entry entry,
                                                                    boolean doFilter,
                                                                    byte[] cfBytes,
                                                                    long minTs) {
        final Map<RowKeySlice, List<Cell>> byRow = groupByRow(entry);
        if (!doFilter) {
            // Фильтр выключен — возвращаем «живое» представление без копирования
            return byRow.entrySet();
        }
        // Ленивая аллокация результата — создаём список только если нашлись подходящие строки
        java.util.List<Map.Entry<RowKeySlice, List<Cell>>> out = null;
        for (Map.Entry<RowKeySlice, List<Cell>> e : byRow.entrySet()) {
            if (passWalTsFilter(e.getValue(), cfBytes, minTs)) {
                if (out == null) {
                    // типичный порядок — небольшое число строк попадает под фильтр
                    out = new java.util.ArrayList<>(Math.min(byRow.size(), 16));
                }
                out.add(e);
            }
        }
        return (out != null) ? out : java.util.Collections.<Map.Entry<RowKeySlice, List<Cell>>>emptyList();
    }

    /**
     * Построить payload для одной строки и отправить в Kafka.
     * Предполагается, что фильтрация по WAL ts уже выполнена (см. filteredRows()).
     */
    private void sendRow(String topic,
                         TableName table,
                         WalMeta wm,
                         Map.Entry<RowKeySlice, List<Cell>> rowEntry,
                         BatchSender sender) {
        final List<Cell> cells = rowEntry.getValue();
        final byte[] keyBytes = rowEntry.getKey().toByteArray();
        final Map<String,Object> obj =
                payload.buildRowPayload(table, cells, rowEntry.getKey(), wm.seq, wm.writeTime);
        sender.add(send(topic, keyBytes, obj));
    }

    /**
     * Чтение sequenceId/writeTime из WAL.Entry.Key (если доступно в HBase 1.4).
     * Вызывается только если включён вывод метаданных WAL (h2k.includeMetaWal=true).
     * При отсутствии метода/значений возвращает "-1".
     */
    private static WalMeta readWalMeta(WAL.Entry entry) {
        long walSeq = -1L;
        long walWriteTime = -1L;
        try { walSeq = entry.getKey().getSequenceId(); } catch (NoSuchMethodError | Exception ignore) {
            // В HBase 1.4 метод может отсутствовать — безопасно игнорируем
        }
        try { walWriteTime = entry.getKey().getWriteTime(); } catch (NoSuchMethodError | Exception ignore) {
            // В HBase 1.4 метод может отсутствовать — безопасно игнорируем
        }
        return new WalMeta(walSeq, walWriteTime);
    }

    /**
     * Быстрая проверка по фильтру WAL ts относительно целевого CF.
     * Микро-оптимизация: кэшируем часто используемые значения в локальные переменные.
     */
    private static boolean passWalTsFilter(List<Cell> cells, byte[] cfBytes, long minTs) {
        // Когда фильтр выключен (cfBytes == null), пропускаем все строки
        if (cfBytes == null) {
            return true;
        }
        for (Cell c : cells) {
            if (org.apache.hadoop.hbase.CellUtil.matchingFamily(c, cfBytes) && c.getTimestamp() >= minTs) {
                return true;
            }
        }
        return false;
    }

    /**
     * Безопасно проверяет/создаёт топик в Kafka через TopicEnsurer.
     * Любые ошибки логируются и не прерывают репликацию.
     * Кэширует последний успешно проверенный топик для подавления повторных ensure в одном потоке.
     */
    private void ensureTopicSafely(String topic) {
        if (topicEnsurer == null) return;
        // Если тот же самый топик уже успешно проверяли/создавали, подавляем повтор
        if (topic.equals(lastEnsuredTopic)) return;
        try {
            topicEnsurer.ensureTopic(topic);
            lastEnsuredTopic = topic; // кэшируем успешную проверку
        } catch (Exception e) {
            LOG.warn("Не удалось проверить/создать топик '{}': {}. Продолжаю без прерывания репликации", topic, e.toString());
            if (LOG.isDebugEnabled()) {
                LOG.debug("Трассировка ошибки ensureTopic()", e);
            }
        }
    }

    /** Мини-контейнер метаданных WAL. */
    private static final class WalMeta {
        static final WalMeta EMPTY = new WalMeta(-1L, -1L);
        final long seq;
        final long writeTime;
        WalMeta(long seq, long writeTime) { this.seq = seq; this.writeTime = writeTime; }
    }

    /** Группируем клетки по rowkey (без копий) с сохранением порядка. */
    private Map<RowKeySlice, List<Cell>> groupByRow(WAL.Entry entry) {
        final List<Cell> cells = entry.getEdit().getCells();
        final Map<RowKeySlice, List<Cell>> byRow = new LinkedHashMap<>(capacityFor(cells.size()));
        for (Cell c : cells) {
            RowKeySlice key = new RowKeySlice(c.getRowArray(), c.getRowOffset(), c.getRowLength());
            List<Cell> list = byRow.computeIfAbsent(key, k -> new java.util.ArrayList<>(8));
            list.add(c);
        }
        return byRow;
    }

    /** Отправка одной записи в Kafka. */
    private Future<RecordMetadata> send(String topic, byte[] key, Map<String, Object> obj) {
        byte[] val = gson.toJson(obj).getBytes(StandardCharsets.UTF_8);
        return producer.send(new ProducerRecord<>(topic, key, val));
    }


    /**
     * В HBase 1.4 {@code Context} не предоставляет getPeerUUID(), возвращаем null (допустимо для данного API).
     */
    @Override public UUID getPeerUUID() { return null; }
    /** Сообщает фреймворку об успешном старте эндпоинта. */
    @Override protected void doStart() { notifyStarted(); }
    /** Корректное завершение работы: сброс и закрытие Producer/TopicEnsurer, уведомление о стопе. */
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
        try {
            if (topicEnsurer != null) topicEnsurer.close();
        } catch (Exception e) {
            // При завершении работы проглатываем исключение, но выводим debug для диагностики
            if (LOG.isDebugEnabled()) {
                LOG.debug("doStop(): ошибка при закрытии TopicEnsurer (игнорируется при завершении работы)", e);
            }
        }
        notifyStopped();
    }
}