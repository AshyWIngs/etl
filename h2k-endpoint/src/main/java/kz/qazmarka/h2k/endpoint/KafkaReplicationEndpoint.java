package kz.qazmarka.h2k.endpoint;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.replication.BaseReplicationEndpoint;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.hadoop.hbase.util.Bytes;
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

import kz.qazmarka.h2k.endpoint.PhoenixRowKeyDecoder.Pk;
import kz.qazmarka.h2k.schema.Decoder;
import kz.qazmarka.h2k.schema.JsonSchemaRegistry;
import kz.qazmarka.h2k.schema.SchemaRegistry;
import kz.qazmarka.h2k.schema.SimpleDecoder;
import kz.qazmarka.h2k.schema.ValueCodecPhoenix;

/**
 * Реализация ReplicationEndpoint (HBase 1.4.13) → Kafka 2.3.1.
 * Читает WALEdit, группирует по rowkey, декодирует PK (c,t,opd_ms) и отправляет JSON в Kafka.
 *
 * Конфиги (hbase-site.xml/peer):
 *  - h2k.kafka.bootstrap.servers
 *  - h2k.topic.pattern (по умолчанию ${table})
 *  - h2k.cf (по умолчанию 0)
 *  - h2k.salted (false), h2k.salt.bytes (1)
 *  - h2k.producer.acks=1, linger.ms=50, batch.size=65536, compression.type=snappy
 */
public class KafkaReplicationEndpoint extends BaseReplicationEndpoint {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaReplicationEndpoint.class);

    /** 
     * Kafka producer. Создаётся в {@link #init(ReplicationEndpoint.Context)} и закрывается в {@link #doStop()}.
     * Используется бинарная сериализация key/value (byte[]) для полного контроля над форматом сообщений.
     */
    private Producer<byte[], byte[]> producer;

    /** 
     * Шаблон для вычисления имени Kafka-топика.
     * Поддерживает плейсхолдеры: ${table} (namespace_qualifier), ${namespace}, ${qualifier}.
     */
    private String topicPattern;

    /** Предвычисленные байты имени CF (UTF-8), чтобы не аллоцировать в каждом батче. */
    private byte[] cfBytes;

    /** Признак SALT в rowkey. */
    private boolean salted;

    /** Длина SALT (в байтах), если {@link #salted} = true. */
    private int saltBytes;

    /** JSON-сериализатор для значений. Конфигурируется опцией h2k.json.serialize.nulls. */
    private Gson gson;

    /** Декодер значений колонок (простой или Phoenix-aware по схеме). */
    private Decoder decoder;

    /** Конфигурация HBase, сохранённая из контекста. */
    private Configuration conf;

    /**
     * Инициализация эндпоинта: читает конфиг HBase, создаёт KafkaProducer, настраивает декодер и параметры.
     * Обязательный ключ: h2k.kafka.bootstrap.servers.
     *
     * @param context контекст репликации (даёт доступ к конфигурации)
     * @throws IOException если не задан обязательный параметр конфигурации
     */
    @Override
    public void init(ReplicationEndpoint.Context context) throws IOException {
        super.init(context);
        // Сохраняем конфигурацию для последующего использования (например, в replicate()).
        this.conf = context.getConfiguration();
        final Configuration cfg = this.conf;

        // -------- Kafka producer configuration --------
        final String bootstrap = cfg.get("h2k.kafka.bootstrap.servers", "").trim();
        if (bootstrap.isEmpty()) {
            // Без этого параметра продьюсер не сможет подключиться — явно падаем на старте.
            throw new IOException("Отсутствует обязательный параметр конфигурации: h2k.kafka.bootstrap.servers");
        }

        Properties props = new Properties();
        // Бинарные сериализаторы: ключ/значение отправляем как byte[]
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");

        // Надёжные дефолты (их можно переопределить через h2k.producer.*):
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, cfg.get("h2k.producer.enable.idempotence", "true"));
        props.put(ProducerConfig.ACKS_CONFIG, cfg.get("h2k.producer.acks", "all"));
        props.put(ProducerConfig.RETRIES_CONFIG, cfg.get("h2k.producer.retries", String.valueOf(Integer.MAX_VALUE)));
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, cfg.get("h2k.producer.delivery.timeout.ms", "180000"));
        props.put(ProducerConfig.LINGER_MS_CONFIG, cfg.get("h2k.producer.linger.ms", "50"));
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, cfg.get("h2k.producer.batch.size", "65536"));
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, cfg.get("h2k.producer.compression.type", "snappy"));
        // Порядок при ретраях: 1 запрос в полёте гарантирует отсутствие пересортировки записей
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, cfg.get("h2k.producer.max.in.flight", "1"));

        // client.id: осмысленный идентификатор процесса
        try {
            String cid = "hbase1-repl-" + InetAddress.getLocalHost().getHostName();
            props.put(ProducerConfig.CLIENT_ID_CONFIG, cfg.get("h2k.producer.client.id", cid));
        } catch (java.net.UnknownHostException ignore) {
            props.put(ProducerConfig.CLIENT_ID_CONFIG, cfg.get("h2k.producer.client.id", "hbase1-repl"));
        }

        // Pass-through: всё, что начинается с h2k.producer.*, прокидываем как нативные проперти Kafka
        for (Map.Entry<String, String> e : cfg) {
            final String k = e.getKey();
            if (k.startsWith("h2k.producer.")) {
                final String real = k.substring("h2k.producer.".length());
                // не затираем уже выставленные значения
                props.putIfAbsent(real, e.getValue());
            }
        }

        this.producer = new KafkaProducer<>(props);

        // -------- Прочие настройки рантайма --------
        this.topicPattern = cfg.get("h2k.topic.pattern", "${table}");
        final String cfName = cfg.get("h2k.cf", "0");
        this.cfBytes = cfName.getBytes(StandardCharsets.UTF_8);
        this.salted = cfg.getBoolean("h2k.salted", false);
        this.saltBytes = cfg.getInt("h2k.salt.bytes", 1);

        // Конфигурируем сериализацию null-полей в JSON при необходимости
        boolean serializeNulls = cfg.getBoolean("h2k.json.serialize.nulls", false);
        GsonBuilder gb = new GsonBuilder().disableHtmlEscaping();
        if (serializeNulls) gb.serializeNulls();
        this.gson = gb.create();

        // Выбор декодера значений: simple (по умолчанию) или json-phoenix по схеме
        String decodeMode = cfg.get("h2k.decode.mode", "simple");
        if ("json-phoenix".equalsIgnoreCase(decodeMode)) {
            String schemaPath = cfg.get("h2k.schema.path");
            if (schemaPath == null || schemaPath.trim().isEmpty()) {
                LOG.warn("Включён режим json-phoenix, но не задан путь схемы (h2k.schema.path) — переключаюсь на простой декодер");
                this.decoder = new SimpleDecoder();
            } else {
                SchemaRegistry schema = new JsonSchemaRegistry(schemaPath.trim());
                this.decoder = new ValueCodecPhoenix(schema);
                LOG.info("Режим декодирования: json-phoenix, схема={}", schemaPath);
            }
        } else {
            this.decoder = new SimpleDecoder();
            LOG.info("Режим декодирования: simple");
        }

        LOG.info("Инициализация завершена: topicPattern={}, cf={}, salted={}, saltBytes={}",
                topicPattern, cfName, salted, saltBytes);
    }

    /**
     * Основной цикл: принимает батч записей WAL, группирует изменения по rowkey, собирает JSON и отправляет в Kafka.
     * Поддерживает дозированное ожидание подтверждений от Kafka, чтобы не переполнять очередь.
     *
     * @param ctx контекст с батчем записей WAL
     * @return true при успешной обработке, false — если стоит повторить (HBase перезапустит задачу)
     */
    @Override
    public boolean replicate(ReplicationEndpoint.ReplicateContext ctx) {
        try {
            List<WAL.Entry> entries = ctx.getEntries();
            if (entries == null || entries.isEmpty()) {
                return true; // нечего реплицировать
            }

            // Дозированное ожидание подтверждений от Kafka
            final int awaitEvery = conf.getInt("h2k.producer.await.every", 500);
            final int awaitTimeoutMs = conf.getInt("h2k.producer.await.timeout.ms", 180000);

            List<Future<RecordMetadata>> sent = new ArrayList<>(awaitEvery);
            int inFlight = 0;

            for (WAL.Entry entry : entries) {
                TableName table = entry.getKey().getTablename();
                String topic = topicFor(table);
                Map<RowKeySlice, List<Cell>> byRow = groupByRow(entry);

                for (Map.Entry<RowKeySlice, List<Cell>> row : byRow.entrySet()) {
                    // Декодируем PK напрямую из среза rowkey (без копии).
                    RowKeySlice s = row.getKey();
                    Pk pk = PhoenixRowKeyDecoder.decodePk(s.arr, s.off, s.len, salted, saltBytes);

                    // Копию rowkey делаем только для ключа сообщения Kafka.
                    byte[] rowKey = copyRowKey(s);

                    RowBuildResult r = buildRowPayload(table, pk, row.getValue(), this.cfBytes);
                    sent.add(send(topic, rowKey, r.obj));

                    if (++inFlight >= awaitEvery) {
                        // ждём подтверждений для накопленной пачки
                        waitAll(sent, awaitTimeoutMs);
                        sent.clear();
                        inFlight = 0;
                    }
                }
            }

            // дожидаемся «хвоста»
            if (!sent.isEmpty()) {
                waitAll(sent, awaitTimeoutMs);
            }
            return true;

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
            LOG.error("replicate(): непредвиденная ошибка", e);
            return false;
        }
    }

    /** В HBase 1.4 у Context нет getPeerUUID(); возвращаем null (цикл-репликация не используется). */
    @Override
    public UUID getPeerUUID() { return null; }

    @Override
    protected void doStart() { notifyStarted(); }

    @Override
    protected void doStop() {
        try {
            if (producer != null) {
                producer.flush();
                producer.close();
            }
        } catch (Exception ignore) {
            // no-op
        } finally {
            notifyStopped();
        }
    }

    /**
     * Быстрый "срез" поверх массива байт rowkey без промежуточных копий и без создания String.
     * Содержит ссылку на исходный массив + offset/length. Хеш предвычисляется один раз.
     * Предназначен только для краткоживущих Map-ключей в пределах обработки одного WALEntry.
     */
    private static final class RowKeySlice {
        final byte[] arr;
        final int off;
        final int len;
        final int hash; // precomputed: стабильный и быстрый, совпадает с Bytes.hashCode(..)

        RowKeySlice(byte[] arr, int off, int len) {
            this.arr = arr;
            this.off = off;
            this.len = len;
            this.hash = Bytes.hashCode(arr, off, len);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof RowKeySlice)) return false;
            RowKeySlice other = (RowKeySlice) o;
            return this.len == other.len
                && Bytes.equals(this.arr, this.off, this.len, other.arr, other.off, other.len);
        }
    }

    /**
     * Небольшой контейнер результата построения JSON по одной строке rowkey.
     */
    private static final class RowBuildResult {
        final Map<String, Object> obj;
        RowBuildResult(Map<String, Object> obj) {
            this.obj = obj;
        }
    }

    /**
     * Строит имя Kafka-топика по шаблону из конфигурации.
     * Поддерживаются плейсхолдеры: ${table}, ${namespace}, ${qualifier}.
     *
     * @param table имя таблицы HBase
     * @return название топика после подстановки и санитизации
     */
    private String topicFor(TableName table) {
        // Разделяем namespace и имя (qualifier) и позволяем использовать их в шаблоне
        String ns = table.getNamespaceAsString();
        String qn = table.getQualifierAsString();

        String base = topicPattern
            .replace("${table}", ns + "_" + qn)
            .replace("${namespace}", ns)
            .replace("${qualifier}", qn);

        // Санитизация под правила Kafka: только [a-zA-Z0-9._-]
        base = base.replace(':', '_');
        return base.replaceAll("[^a-zA-Z0-9._-]", "_");
    }

    /**
     * Группирует клетки WALEntry по rowkey (без копий) с сохранением порядка.
     * Использует RowKeySlice как ключ, чтобы избегать аллокаций.
     *
     * @param entry запись WAL
     * @return карта: rowkey-срез → список клеток этой строки
     */
    private Map<RowKeySlice, List<Cell>> groupByRow(WAL.Entry entry) {
        // Используем LinkedHashMap, чтобы сохранить естественный порядок появления строк из WAL.
        // Ключ — нуллекопийный срез RowKeySlice (byte[] + offset/len) с предвычисленным hash.
        final List<Cell> cells = entry.getEdit().getCells();
        final Map<RowKeySlice, List<Cell>> byRow = new LinkedHashMap<>(cells.size());
        for (Cell c : cells) {
            RowKeySlice key = new RowKeySlice(c.getRowArray(), c.getRowOffset(), c.getRowLength());
            // computeIfAbsent: один lookup без ветвления; лямбда выполнится только при отсутствии ключа
            List<Cell> list = byRow.computeIfAbsent(key, k -> new ArrayList<>(8));
            list.add(c);
        }
        return byRow;
    }

    /**
     * Строит JSON-представление одной строки по списку клеток.
     * Вычисляет event_version как максимум timestamp по целевому CF и устанавливает флаг delete при наличии метки удаления.
     *
     * @param table  таблица
     * @param pk     распарсенный PK (c,t,opd_ms) из rowkey (получен без копии байтов)
     * @param cells  клетки строки (все CF)
     * @param cfBytes целевой CF в байтах
     * @return результат с готовой Map для сериализации
     */
    private RowBuildResult buildRowPayload(TableName table, Pk pk, List<Cell> cells, byte[] cfBytes) {
        // Собираем итоговый JSON в LinkedHashMap, чтобы сохранить стабильный порядок полей.
        Map<String, Object> obj = new LinkedHashMap<>();

        // PK уже распарсен из среза rowkey — здесь лишь публикуем поля
        obj.put("c", pk.c);
        obj.put("t", pk.t);
        obj.put("opd_ms", pk.opdMs);

        long maxTs = 0L;          // event_version: максимальный timestamp по нужному CF
        boolean hasDelete = false; // delete=true, если пришла метка удаления в нашем CF

        for (Cell cell : cells) {
            // Быстрый пропуск чужих CF без continue — вложенная ветка
            if (CellUtil.matchingFamily(cell, cfBytes)) {
                long ts = cell.getTimestamp();
                if (ts > maxTs) {
                    maxTs = ts;
                }

                if (CellUtil.isDelete(cell)) {
                    hasDelete = true;
                } else {
                    // Ключ JSON нам всё равно нужен как строка:
                    String q = new String(cell.getQualifierArray(), cell.getQualifierOffset(),
                                          cell.getQualifierLength(), StandardCharsets.UTF_8);

                    // Декодирование — быстрый путь без копий (если реализация его переопределяет):
                    Object decoded = decoder.decode(
                            table,
                            cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(),
                            cell.getValueArray(),      cell.getValueOffset(),     cell.getValueLength()
                    );
                    obj.put(q, decoded);
                }
            }
        }

        obj.put("event_version", maxTs);
        if (hasDelete) {
            obj.put("delete", true);
        }

        return new RowBuildResult(obj);
    }

    /**
     * Ожидает завершения всех отправок в пределах заданного общего таймаута.
     * Таймаут применяется «на весь набор»: каждый последующий future получает остаток времени.
     */
    private static void waitAll(List<Future<RecordMetadata>> futures, int timeoutMs)
            throws InterruptedException, java.util.concurrent.ExecutionException, java.util.concurrent.TimeoutException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        for (Future<RecordMetadata> f : futures) {
            long leftNs = deadline - System.nanoTime();
            if (leftNs <= 0L) {
                throw new java.util.concurrent.TimeoutException("Таймаут отправки в Kafka");
            }
            f.get(leftNs, TimeUnit.NANOSECONDS);
        }
    }

    /**
     * Отправляет одну запись в Kafka.
     *
     * @param topic Имя Kafka-топика (после применения шаблона и санитизации)
     * @param key   Ключ сообщения — исходный rowkey (byte[])
     * @param obj   Тело события (Map), будет сериализовано в JSON через Gson
     * @return Future с метаданными записи в Kafka (partition/offset/timestamp)
     */
    private Future<RecordMetadata> send(String topic, byte[] key, Map<String, Object> obj) {
        byte[] val = gson.toJson(obj).getBytes(StandardCharsets.UTF_8);
        return producer.send(new ProducerRecord<>(topic, key, val));
    }

    /**
     * Делает компактную копию rowkey по срезу; одна аллокация/копирование на строку.
     * Это дешевле, чем сначала создавать String, а потом снова кодировать в bytes.
     */
    private static byte[] copyRowKey(RowKeySlice s) {
        byte[] out = new byte[s.len];
        System.arraycopy(s.arr, s.off, out, 0, s.len);
        return out;
    }
}