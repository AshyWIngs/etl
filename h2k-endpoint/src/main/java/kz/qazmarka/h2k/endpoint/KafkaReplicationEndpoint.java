package kz.qazmarka.h2k.endpoint;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
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
import kz.qazmarka.h2k.config.H2kConfig.Keys;
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
 * Репликация изменений из HBase 1.4.13 в Kafka (производитель 2.x).
 *
 * Назначение
 *  - Принимать батчи WAL-записей от фреймворка репликации HBase и надёжно публиковать их в Kafka.
 *  - Формировать JSON-представление строки по набору ячеек с учётом выбранного декодера.
 *
 * Производительность и потокобезопасность
 *  - Горячий путь максимально прямолинейный: группировка по rowkey → сборка JSON → send в Kafka.
 *  - Все тяжёлые инициализации выполняются один раз в init()/doStart(): GSON, декодер, конфиг, KafkaProducer.
 *  - Класс используется из потока репликации HBase, дополнительных потоков не создаёт.
 *  - JSON кодируется без промежуточной строки, переиспользуются {@code jsonOut}/{@code jsonWriter}
 *    (поток один — синхронизация не требуется).
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
    /** Префикс client.id продьюсера; к нему добавляется hostname или UUID. */
    private static final String DEFAULT_CLIENT_ID   = "hbase1-repl";

    // ядро
    /** Kafka Producer для отправки событий; ключ/значение сериализуются как байты. */
    private Producer<byte[], byte[]> producer;
    /** Экземпляр Gson для сериализации payload в JSON (disableHtmlEscaping, опциональная сериализация null). */
    private Gson gson;
    /** Буфер для кодирования JSON без промежуточной строки (уменьшаем аллокации на горячем пути). */
    private final ByteArrayOutputStream jsonOut = new ByteArrayOutputStream(4096);
    /** Переиспользуемый writer поверх {@link #jsonOut}. Поток репликации один, поэтому синхронизация не требуется. */
    private final OutputStreamWriter jsonWriter = new OutputStreamWriter(jsonOut, StandardCharsets.UTF_8);

    // вынесенная конфигурация и сервисы
    /** Иммутабельный снимок настроек h2k.* с предвычисленными флагами для горячего пути. */
    private H2kConfig h2k;
    /** Сборщик JSON-объекта строки на основе набора ячеек и выбранного декодера. */
    private PayloadBuilder payload;
    /** Сервис проверки/создания топиков Kafka; может быть null, если ensureTopics=false. */
    private TopicEnsurer topicEnsurer;
    /** Множество топиков, успешно проверенных/созданных за время жизни пира (для подавления повторных ensure).
     *  При ошибке ensure топик удаляется из множества, чтобы повторить попытку при следующем обращении. */
    private final Set<String> ensuredTopics = new HashSet<>(8);
    /** Кэш соответствий таблица -> топик для устранения повторных вычислений topicPattern.
     *  TableName имеет стабильные equals/hashCode, кэш корректен на время жизни пира.
     */
    private final Map<TableName, String> topicCache = new HashMap<>(8);

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

        // gson
        this.gson = buildGson(cfg);

        // decoder
        Decoder decoder = chooseDecoder(cfg);

        // immut-конфиг, билдер и энсюрер
        this.h2k = H2kConfig.from(cfg, bootstrap);
        // Сводка по карте «соли» rowkey (h2k.salt.map) — только на DEBUG, чтобы не шуметь
        try {
            if (LOG.isDebugEnabled()) {
                final Map<String, Integer> saltMap = h2k.getSaltBytesByTable();
                if (saltMap == null || saltMap.isEmpty()) {
                    LOG.debug("Соль rowkey не задана (h2k.salt.map).");
                } else {
                    LOG.debug("Соль rowkey (h2k.salt.map): {} ({} записей)", saltMap, saltMap.size());
                }
            }
        } catch (Exception t) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Не удалось вывести сводку h2k.salt.map (не критично)", t);
            }
        }
        this.payload = new PayloadBuilder(decoder, h2k);
        this.topicEnsurer = TopicEnsurer.createIfEnabled(h2k);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Инициализация завершена: шаблон_топика={}, cf_список={}, проверять_топики={}, включать_rowkey={}, кодировка_rowkey={}, включать_meta={}, включать_meta_WAL={}, счетчики_батчей={}, debug_батчей_при_ошибке={}",
                    h2k.getTopicPattern(),
                    h2k.getCfNamesCsv(),
                    h2k.isEnsureTopics(), h2k.isIncludeRowKey(), h2k.getRowkeyEncoding(), h2k.isIncludeMeta(), h2k.isIncludeMetaWal(),
                    h2k.isProducerBatchCountersEnabled(), h2k.isProducerBatchDebugOnFailure());
        }
    }
    /**
     * Возвращает строку bootstrap‑серверов Kafka из конфигурации или бросает {@link IOException}, если параметр отсутствует.
     * Пустая строка после {@code trim()} считается отсутствующим параметром.
     *
     * @param cfg HBase‑конфигурация
     * @return непустая строка с bootstrap‑серверами (формат host:port[,host2:port2])
     * @throws IOException если параметр {@code h2k.kafka.bootstrap.servers} не задан
     */
    private static String readBootstrapOrThrow(Configuration cfg) throws IOException {
        final String bootstrap = cfg.get(Keys.BOOTSTRAP, "").trim();
        if (bootstrap.isEmpty()) {
            throw new IOException("Отсутствует обязательный параметр конфигурации: " + Keys.BOOTSTRAP);
        }
        return bootstrap;
    }

    /**
     * Создаёт и настраивает {@link Gson} с учётом флага сериализации {@code null}‑полей.
     * HTML‑escaping отключён для компактности и читаемости JSON.
     * На DEBUG выводится итоговый флаг serializeNulls.
     *
     * @param cfg HBase‑конфигурация (читается флаг {@code h2k.json.serialize.nulls})
     * @return готовый экземпляр {@link Gson}
     */
    private static Gson buildGson(Configuration cfg) {
        boolean serializeNulls = cfg.getBoolean(Keys.JSON_SERIALIZE_NULLS, false);
        GsonBuilder gb = new GsonBuilder().disableHtmlEscaping();
        if (serializeNulls) gb.serializeNulls();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Настройки Gson: serializeNulls={}", serializeNulls);
        }
        return gb.create();
    }

    /**
     * Выбирает и настраивает декодер значений по конфигурации.
     * Режимы:
     *  - {@code simple} — простой декодер без схемы (по умолчанию);
     *  - {@code json-phoenix} — декодер на основе Phoenix-схемы из {@code h2k.schema.path}.
     * При ошибке инициализации схемы выполняется безопасный фолбэк на {@link SimpleDecoder#INSTANCE}.
     *
     * @param cfg HBase-конфигурация
     * @return выбранный декодер
     */
    private Decoder chooseDecoder(Configuration cfg) {
        String mode = cfg.get(Keys.DECODE_MODE, DEFAULT_DECODE_MODE);
        if ("json-phoenix".equalsIgnoreCase(mode)) {
            String schemaPath = cfg.get(Keys.SCHEMA_PATH);
            if (schemaPath == null || schemaPath.trim().isEmpty()) {
                throw new IllegalStateException(
                    "Включён режим json-phoenix, но не задан обязательный параметр h2k.schema.path");
            }
            try {
                SchemaRegistry schema = new JsonSchemaRegistry(schemaPath.trim());
                LOG.debug("Режим декодирования: json-phoenix, схема={}", schemaPath);
                return new ValueCodecPhoenix(schema);
            } catch (Exception e) {
                LOG.warn("Не удалось инициализировать JsonSchemaRegistry ({}): {} — переключаюсь на простой декодер",
                        schemaPath, e.toString());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Трассировка ошибки JsonSchemaRegistry", e);
                }
                return SimpleDecoder.INSTANCE;
            }
        }
        LOG.debug("Режим декодирования: simple");
        return SimpleDecoder.INSTANCE;
    }

    /**
     * Подбирает начальную ёмкость для {@link LinkedHashMap} под ожидаемое число элементов
     * с учётом коэффициента загрузки 0.75. Эквивалент выражению {@code 1 + ceil(expected/0.75)}
     * (= {@code 1 + ceil(4*expected/3)}) без операций с плавающей точкой, чтобы исключить double/ceil на горячем пути.
     * Для неположительных значений {@code expected} возвращает 16 (дефолтную начальную ёмкость);
     * верхний кэп — {@code 1<<30}.
     *
     * @param expected ожидаемое число пар ключ‑значение
     * @return рекомендуемая начальная ёмкость
     */
    private static int capacityFor(int expected) {
        if (expected <= 0) return 16; // дефолтная начальная ёмкость
        // 1 + ceil(4*expected/3) в целочисленной форме: 1 + (4*n + 2)/3
        long cap = 1L + (4L * expected + 2L) / 3L;
        // безопасный кэп для HashMap в Java 8
        return cap > (1L << 30) ? (1 << 30) : (int) cap;
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
            final boolean doFilter = false;
            final byte[][] cfFamilies = null;
            final long minTs = -1L;

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
                    processEntry(entry, sender, includeWalMeta, doFilter, cfFamilies, minTs);
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
     * @param minTs         минимальный timestamp для допуска через фильтр
     */
    private void processEntry(WAL.Entry entry,
                              BatchSender sender,
                              boolean includeWalMeta,
                              boolean doFilter,
                              byte[][] cfFamilies,
                              long minTs) {
        TableName table = entry.getKey().getTablename();
        String topic = topicCache.computeIfAbsent(table, h2k::topicFor);
        WalMeta wm = includeWalMeta ? readWalMeta(entry) : WalMeta.EMPTY;
        ensureTopicSafely(topic);
        int rowsSent = 0;
        int cellsSent = 0;
        for (Map.Entry<RowKeySlice, List<Cell>> rowEntry : filteredRows(entry, doFilter, cfFamilies, minTs)) {
            sendRow(topic, table, wm, rowEntry, sender);
            rowsSent++;
            cellsSent += rowEntry.getValue().size();
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Репликация: запись WAL обработана: таблица={}, топик={}, строк отправлено={}, ячеек отправлено={}, фильтр={}, ensure-включён={}",
                    table, topic, rowsSent, cellsSent, doFilter, topicEnsurer != null);
        }
    }

    /**
     * Возвращает только те строки, которые проходят фильтр по WAL-timestamp для целевого CF.
     * Если фильтр выключен, возвращается «живой» view {@code byRow.entrySet()} без копирования —
     * итерировать результат следует немедленно (в текущем потоке), не кэшировать и не передавать за пределы вызова.
     *
     * @implNote Возвращаемый «живой» view не должен кэшироваться и использоваться после возврата из метода.
     *
     * @param entry     запись WAL
     * @param doFilter  флаг включения фильтрации
     * @param cfFamilies массив целевых CF (в байтах) для фильтра
     * @param minTs     минимальный допустимый timestamp
     * @return итерируемая коллекция пар (rowkey → список ячеек)
     */
    private Iterable<Map.Entry<RowKeySlice, List<Cell>>> filteredRows(WAL.Entry entry,
                                                                      boolean doFilter,
                                                                      byte[][] cfFamilies,
                                                                      long minTs) {
        final Map<RowKeySlice, List<Cell>> byRow = groupByRow(entry);
        if (!doFilter) {
            // Фильтр выключен — возвращаем «живое» представление без копирования
            return byRow.entrySet();
        }
        // Ленивая аллокация результата — создаём список только если нашлись подходящие строки
        List<Map.Entry<RowKeySlice, List<Cell>>> out = null;
        for (Map.Entry<RowKeySlice, List<Cell>> e : byRow.entrySet()) {
            if (passWalTsFilter(e.getValue(), cfFamilies, minTs)) {
                if (out == null) {
                    // типичный порядок — небольшое число строк попадает под фильтр
                    out = new ArrayList<>(Math.min(byRow.size(), 16));
                }
                out.add(e);
            }
        }
        return (out != null) ? out : Collections.<Map.Entry<RowKeySlice, List<Cell>>>emptyList();
    }

    /**
     * Строит JSON‑payload для одной строки и отправляет сообщение в Kafka.
     * Предполагается, что фильтрация по WAL‑timestamp уже выполнена.
     *
     * @param topic   имя Kafka‑топика
     * @param table   имя таблицы HBase
     * @param wm      метаданные WAL
     * @param rowEntry пара (rowkey → список ячеек)
     * @param sender  батч‑отправитель
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
     * Читает {@code sequenceId} и {@code writeTime} из ключа {@link WAL.Entry}.
     * Для HBase 1.4.13:
     *  - {@code getSequenceId()} объявляет {@link java.io.IOException} — перехватываем и возвращаем {@code -1};
     *  - {@code getWriteTime()} не объявляет checked‑исключений — читаем напрямую без try/catch.
     *
     * @param entry запись WAL
     * @return контейнер с метаданными WAL
     */
    private static WalMeta readWalMeta(WAL.Entry entry) {
        long walSeq = -1L;
        try {
            walSeq = entry.getKey().getSequenceId();
        } catch (IOException e) {
            // Для некоторых сборок 1.4.13 метод объявляет IOException — считаем метаданные недоступными
            // и оставляем значение -1 без логирования (это не ошибка для функциональности).
        }
        final long walWriteTime = entry.getKey().getWriteTime();
        return new WalMeta(walSeq, walWriteTime);
    }

    /**
     * Быстрая проверка фильтра по WAL‑timestamp относительно целевых CF.
     *
     * @param cells     список ячеек строки
     * @param cfFamilies массив CF в байтах (или {@code null}, если фильтр выключен)
     * @param minTs     минимальный допустимый timestamp
     * @return {@code true}, если строка проходит фильтр
     */
    private static boolean passWalTsFilter(List<Cell> cells, byte[][] cfFamilies, long minTs) {
        // Фильтр выключен — пропускаем все строки
        if (cfFamilies == null || cfFamilies.length == 0) {
            return true;
        }
        final int n = cfFamilies.length;
        if (n == 1) {
            return passWalTsFilter1(cells, cfFamilies[0], minTs);
        } else if (n == 2) {
            return passWalTsFilter2(cells, cfFamilies[0], cfFamilies[1], minTs);
        }
        return passWalTsFilterN(cells, cfFamilies, minTs);
    }

    /**
     * Быстрый путь фильтрации по WAL-timestamp для одного CF.
     * Выполняет один линейный проход по списку ячеек и коротко замыкается при первом совпадении.
     *
     * @param cells  клетки строки (как правило, одной строки WAL)
     * @param cf0    искомое семейство колонок (в байтах)
     * @param minTs  минимально допустимый timestamp (включительно)
     * @return {@code true}, если найдена хотя бы одна ячейка из {@code cf0} с {@code ts >= minTs}
     */
    private static boolean passWalTsFilter1(List<Cell> cells,
                                            byte[] cf0,
                                            long minTs) {
        for (Cell c : cells) {
            if (c.getTimestamp() >= minTs &&
                CellUtil.matchingFamily(c, cf0)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Быстрый путь фильтрации для двух семейств CF без вложенных {@code if}.
     *
     * @param cells клетки строки
     * @param cf0   первое семейство колонок
     * @param cf1   второе семейство колонок
     * @param minTs минимально допустимый timestamp (включительно)
     * @return {@code true}, если найдена ячейка из {@code cf0} или {@code cf1} с {@code ts >= minTs}
     */
    private static boolean passWalTsFilter2(List<Cell> cells,
                                            byte[] cf0,
                                            byte[] cf1,
                                            long minTs) {
        for (Cell c : cells) {
            if (c.getTimestamp() >= minTs &&
                (CellUtil.matchingFamily(c, cf0) ||
                 CellUtil.matchingFamily(c, cf1))) {
                return true;
            }
        }
        return false;
    }

    /**
     * Общий случай фильтрации для {@code n >= 3} семейств CF.
     * Внешний цикл — по ячейкам (сравнение timestamp), внутренний — по списку CF (короткое замыкание при совпадении).
     *
     * @param cells       клетки строки
     * @param cfFamilies  массив целевых семейств колонок (байтовые имена)
     * @param minTs       минимально допустимый timestamp (включительно)
     * @return {@code true}, если хотя бы одна ячейка удовлетворяет условию
     */
    private static boolean passWalTsFilterN(List<Cell> cells,
                                            byte[][] cfFamilies,
                                            long minTs) {
        for (Cell c : cells) {
            final long ts = c.getTimestamp();
            if (ts >= minTs) {
                for (byte[] cf : cfFamilies) {
                    if (CellUtil.matchingFamily(c, cf)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Безопасно проверяет/создаёт топик в Kafka через {@link TopicEnsurer}.
     * Любые ошибки логируются и не прерывают репликацию. Для снижения накладных расходов
     * используется кэш множества уже успешно проверенных/созданных топиков ({@code ensuredTopics}).
     * Повторные вызовы для одного и того же топика пропускаются, пока предыдущая проверка считалась успешной.
     *
     * @param topic имя топика, который требуется гарантировать
     */
    private void ensureTopicSafely(String topic) {
        if (topicEnsurer == null) return;
        // Если этот топик уже успешно проверяли/создавали ранее — повторять не нужно
        if (!ensuredTopics.add(topic)) return;
        try {
            topicEnsurer.ensureTopic(topic);
        } catch (Exception e) {
            LOG.warn("Проверка/создание топика '{}' не выполнена: {}. Репликацию не прерываю", topic, e.toString());
            if (LOG.isDebugEnabled()) {
                LOG.debug("Трассировка ошибки ensureTopic()", e);
            }
            // В случае ошибки убираем из кеша, чтобы попытаться снова при следующем обращении
            ensuredTopics.remove(topic);
        }
    }

    /**
     * Небольшой иммутабельный контейнер метаданных записи WAL.
     * Поля могут принимать значение {@code -1}, когда соответствующая информация недоступна в текущей версии HBase.
     */
    private static final class WalMeta {
        static final WalMeta EMPTY = new WalMeta(-1L, -1L);
        final long seq;
        final long writeTime;
        WalMeta(long seq, long writeTime) { this.seq = seq; this.writeTime = writeTime; }
    }

    /**
     * Группирует клетки в пределах {@link WAL.Entry} по rowkey без лишних копий и с сохранением порядка вставки.
     *
     * @param entry запись WAL
     * @return мапа (rowkey → список ячеек), где ключи — {@link RowKeySlice} ссылаются на исходный массив байт
     * 
     * Порядок вставки сохраняется, что обеспечивает детерминированный порядок полей при сериализации в JSON (через LinkedHashMap).
     */
    private Map<RowKeySlice, List<Cell>> groupByRow(WAL.Entry entry) {
        final List<Cell> cells = entry.getEdit().getCells();
        if (cells == null || cells.isEmpty()) {
            // Страховка от «странных» оберток/тестов: возвращаем пустую мапу без NPE.
            return Collections.emptyMap();
        }
        final Map<RowKeySlice, List<Cell>> byRow = new LinkedHashMap<>(capacityFor(cells.size()));

        // Микро‑оптимизация: в WAL ячейки обычно идут батчами по одному rowkey.
        // Поэтому избегаем лишних аллокаций RowKeySlice и повторных hash‑поисков:
        // удерживаем "текущую" группу и добавляем клетки, пока rowkey не сменится.
        byte[] prevArr = null;
        int prevOff = -1;
        int prevLen = -1;
        List<Cell> currentList = null;

        for (Cell c : cells) {
            final byte[] arr = c.getRowArray();
            final int off = c.getRowOffset();
            final int len = c.getRowLength();

            if (currentList != null && arr == prevArr && off == prevOff && len == prevLen) {
                // Та же строка — просто добавляем без поиска в мапе и без новых объектов
                currentList.add(c);
            } else {
                // Новая строка — единожды создаём ключ‑срез и список
                RowKeySlice key = new RowKeySlice(arr, off, len);
                currentList = byRow.computeIfAbsent(key, k -> new ArrayList<>(8));
                currentList.add(c);

                // Обновляем маркеры «последней» строки
                prevArr = arr;
                prevOff = off;
                prevLen = len;
            }
        }
        return byRow;
    }

    /**
     * Сериализует объект в JSON (UTF‑8) и отправляет запись в Kafka.
     *
     * @param topic имя топика
     * @param key   ключ сообщения (байты rowkey)
     * @param obj   payload‑объект для сериализации
     * @return {@link Future} для последующего ожидания подтверждения
     */
    private Future<RecordMetadata> send(String topic, byte[] key, Map<String, Object> obj) {
        byte[] val = toJsonBytes(obj);
        return producer.send(new ProducerRecord<>(topic, key, val));
    }

    /**
     * Быстро сериализует объект в JSON-байты без промежуточной строки.
     * Использует переиспользуемые {@link #jsonOut} и {@link #jsonWriter}.
     * В случае непредвиденной IO-ошибки (теоретически невозможной для ByteArrayOutputStream)
     * выполняет безопасный фолбэк через строковое представление.
     */
    private byte[] toJsonBytes(Map<String, Object> obj) {
        jsonOut.reset();
        try {
            gson.toJson(obj, jsonWriter);
            jsonWriter.flush(); // Writer → BAOS
            return jsonOut.toByteArray();
        } catch (IOException ex) {
            // Редкая ситуация: ошибка при записи/flush в Writer. Не роняем поток, логируем и уходим в безопасный фолбэк.
            LOG.warn("Сериализация JSON через Writer не удалась ({}), переключаюсь на безопасный фолбэк toJson(obj): {}",
                    ex.getClass().getSimpleName(), ex.toString());
            if (LOG.isDebugEnabled()) {
                LOG.debug("Трассировка ошибки сериализации JSON через Writer:", ex);
            }
            return gson.toJson(obj).getBytes(StandardCharsets.UTF_8);
        }
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
         *  - pass-through: любые {@code h2k.producer.*} прокидываются как «родные» ключи продьюсера, если не заданы выше;
         *  - {@code client.id}: по умолчанию префикс + hostname, фолбэк — UUID.
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

            // client.id: уникальный per‑host (hostname), fallback — UUID
            try {
                String host = InetAddress.getLocalHost().getHostName();
                String fallback = (host == null || host.isEmpty())
                        ? (DEFAULT_CLIENT_ID + '-' + UUID.randomUUID())
                        : (DEFAULT_CLIENT_ID + '-' + host);
                props.put(ProducerConfig.CLIENT_ID_CONFIG, cfg.get("h2k.producer.client.id", fallback));
            } catch (UnknownHostException ignore) {
                String fallback = DEFAULT_CLIENT_ID + '-' + UUID.randomUUID();
                props.put(ProducerConfig.CLIENT_ID_CONFIG, cfg.get("h2k.producer.client.id", fallback));
            }

            // pass‑through: любые h2k.producer.* → «настоящие» ключи продьюсера, если не заданы выше
            final String prefix = Keys.PRODUCER_PREFIX;
            final int prefixLen = prefix.length();
            for (Map.Entry<String, String> e : cfg) {
                final String k = e.getKey();
                if (k.startsWith(prefix)) {
                    // Пропускаем алиас: h2k.producer.max.in.flight уже замаплен на
                    // max.in.flight.requests.per.connection, не нужно добавлять «неродной» ключ max.in.flight
                    if ("h2k.producer.max.in.flight".equals(k)) {
                        continue;
                    }
                    final String real = k.substring(prefixLen);
                    props.putIfAbsent(real, e.getValue());
                }
            }
            return props;
        }
    }
}