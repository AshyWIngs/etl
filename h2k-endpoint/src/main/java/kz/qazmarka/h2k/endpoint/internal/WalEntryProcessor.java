package kz.qazmarka.h2k.endpoint.internal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kz.qazmarka.h2k.kafka.producer.BatchSender;
import kz.qazmarka.h2k.payload.builder.PayloadBuilder;
import kz.qazmarka.h2k.util.RowKeySlice;

/**
 * Обрабатывает записи WAL: группирует по rowkey, фильтрует и отправляет события в Kafka.
 * Контракт использования:
 * - экземпляр создаётся и используется в одном потоке репликации HBase;
 * - горячий путь повторно использует {@link RowKeySlice.Mutable}, чтобы избегать лишних аллокаций;
 * - передаваемый {@link BatchSender} управляет жизненным циклом фьючерсов KafkaProducer и должен очищаться
 *   вызывающей стороной после каждого вызова;
 * - счётчики основаны на {@link java.util.concurrent.atomic.LongAdder} и потокобезопасны, их можно опрашивать
 *   из TopicManager или через JMX без дополнительной синхронизации.
 */
public final class WalEntryProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(WalEntryProcessor.class);
    private static final long THROUGHPUT_LOG_INTERVAL_NS = TimeUnit.SECONDS.toNanos(5);

    private final PayloadBuilder payloadBuilder;
    private final TopicManager topicManager;
    private final Producer<byte[], byte[]> producer;
    private final LongAdder entriesProcessed = new LongAdder();
    private final LongAdder rowsProcessed = new LongAdder();
    private final LongAdder cellsProcessed = new LongAdder();
    private final LongAdder rowsFiltered = new LongAdder();
    private final LongAdder entriesWindow = new LongAdder();
    private final LongAdder rowsWindow = new LongAdder();
    private final LongAdder cellsWindow = new LongAdder();
    private final LongAdder filteredRowsWindow = new LongAdder();
    private final AtomicLong throughputWindowStart = new AtomicLong(System.nanoTime());
    private final java.util.concurrent.atomic.AtomicReference<CfFilterCache> cfFilterCache =
            new java.util.concurrent.atomic.AtomicReference<>(CfFilterCache.EMPTY);

    public WalEntryProcessor(PayloadBuilder payloadBuilder,
                             TopicManager topicManager,
                             Producer<byte[], byte[]> producer) {
        this.payloadBuilder = payloadBuilder;
        this.topicManager = topicManager;
        this.producer = producer;
    }

    /**
     * Конвертирует одну запись WAL в набор Kafka-сообщений: группирует по rowkey, опционально фильтрует
     * по CF, собирает payload и добавляет futures в переданный {@link BatchSender}.
     */
    /**
     * Конвертирует одну запись WAL в набор Kafka-сообщений: группирует по rowkey, фильтрует и отправляет в Kafka.
     */
    public void process(WAL.Entry entry,
                        BatchSender sender,
                        boolean includeWalMeta,
                        boolean filterEnabled,
                        byte[][] cfFamilies) {
        final List<Cell> cells = entry.getEdit().getCells();
        if (cells == null || cells.isEmpty()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Репликация: запись WAL без ячеек — таблица={}, фильтр={}, ensure-включён={}",
                        entry.getKey().getTablename(), filterEnabled, topicManager.ensureEnabled());
            }
            return;
        }

        final TableName table = entry.getKey().getTablename();
        final String topic = topicManager.resolveTopic(table);
        topicManager.ensureTopicIfNeeded(topic);

        final WalMeta walMeta = includeWalMeta ? readWalMeta(entry) : WalMeta.EMPTY;

        ProcessingCounters counters = new ProcessingCounters();
        CfFilterCache cfCache = prepareCfCache(filterEnabled ? cfFamilies : null);

        RowProcessingContext context = new RowProcessingContext(
                filterEnabled && !cfCache.isEmpty(),
                cfCache,
                topic,
                table,
                walMeta,
                sender,
                counters);

        final List<Cell> rowBuffer = new ArrayList<>(8);
        RowKeySlice.Mutable currentSlice = null;
        byte[] prevArr = null;
        int prevOff = -1;
        int prevLen = -1;

        for (Cell cell : cells) {
            final byte[] arr = cell.getRowArray();
            final int off = cell.getRowOffset();
            final int len = cell.getRowLength();

            if (isSameRow(currentSlice, arr, off, len, prevArr, prevOff, prevLen)) {
                rowBuffer.add(cell);
                continue;
            }

            context.flushRow(rowBuffer, currentSlice);

            rowBuffer.add(cell);
            if (currentSlice == null) {
                currentSlice = new RowKeySlice.Mutable(arr, off, len);
            } else {
                currentSlice.reset(arr, off, len);
            }
            prevArr = arr;
            prevOff = off;
            prevLen = len;
        }

        context.flushRow(rowBuffer, currentSlice);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Репликация: запись WAL обработана: таблица={}, топик={}, строк отправлено={}, ячеек отправлено={}, фильтр={}, ensure-включён={}",
                    table, topic, counters.rowsSent, counters.cellsSent, filterEnabled, topicManager.ensureEnabled());
        }

        entriesProcessed.increment();
        rowsProcessed.add((long) counters.rowsSent + (long) counters.rowsFiltered);
        cellsProcessed.add(counters.cellsSeen);
        if (counters.rowsFiltered > 0) {
            rowsFiltered.add(counters.rowsFiltered);
        }
        logThroughput(counters);
    }

    /**
     * Быстрый тест: принадлежит ли текущая ячейка тому же rowkey, что и предыдущая.
     * Сравнение выполняется по ссылке и координатам, чтобы избежать побайтного сравнения на горячем пути.
     */
    private static boolean isSameRow(RowKeySlice currentSlice,
                                     byte[] arr,
                                     int off,
                                     int len,
                                     byte[] prevArr,
                                     int prevOff,
                                     int prevLen) {
        return currentSlice != null && arr == prevArr && off == prevOff && len == prevLen;
    }

    /**
     * Контекст обработки строки WAL: хранит настройки фильтра/топика/метаданных для передачи в helper-методы.
     */
    private final class RowProcessingContext {
        private final boolean filterEnabled;
        private final CfFilterCache cfCache;
        private final String topic;
        private final TableName table;
        private final WalMeta walMeta;
        private final BatchSender sender;
        private final ProcessingCounters counters;

        RowProcessingContext(boolean filterEnabled,
                             CfFilterCache cfCache,
                             String topic,
                             TableName table,
                             WalMeta walMeta,
                             BatchSender sender,
                             ProcessingCounters counters) {
            this.filterEnabled = filterEnabled;
            this.cfCache = cfCache;
            this.topic = topic;
            this.table = table;
            this.walMeta = walMeta;
            this.sender = sender;
            this.counters = counters;
        }

        /**
         * Отправляет накопленную по текущему rowkey партию ячеек, если она прошла фильтр CF.
         * Метод очищает буфер вне зависимости от результата, чтобы подготовиться к следующей строке.
         */
        void flushRow(List<Cell> rowBuffer, RowKeySlice currentSlice) {
            if (currentSlice == null || rowBuffer.isEmpty()) {
                return;
            }
            counters.cellsSeen += rowBuffer.size();
            if (filterEnabled && !passCfFilter(rowBuffer, cfCache.families, cfCache.hashes)) {
                rowBuffer.clear();
                counters.rowsFiltered++;
                return;
            }
            WalEntryProcessor.this.sendRow(topic, table, walMeta, currentSlice, rowBuffer, sender);
            counters.rowsSent++;
            counters.cellsSent += rowBuffer.size();
            rowBuffer.clear();
        }
    }

    private CfFilterCache prepareCfCache(byte[][] cfFamilies) {
        if (cfFamilies == null || cfFamilies.length == 0) {
            return CfFilterCache.EMPTY;
        }
        CfFilterCache current = cfFilterCache.get();
        if (current.matches(cfFamilies)) {
            return current;
        }
        CfFilterCache computed = CfFilterCache.build(cfFamilies);
        while (true) {
            if (cfFilterCache.compareAndSet(current, computed)) {
                return computed;
            }
            current = cfFilterCache.get();
            if (current.matches(cfFamilies)) {
                return current;
            }
        }
    }

    private static boolean passCfFilter(List<Cell> cells, byte[][] cfFamilies, int[] cfHashes) {
        if (cfFamilies == null || cfFamilies.length == 0) {
            return true; // фильтр выключен
        }
        final int n = cfFamilies.length;
        if (n == 1) {
            return passCfFilter1(cells, cfFamilies[0]);
        }
        if (n == 2) {
            return passCfFilter2(cells, cfFamilies[0], cfFamilies[1]);
        }
        return passCfFilterN(cells, cfFamilies, cfHashes);
    }

    /** Проверяет CF при одном допустимом семействе. */
    static boolean passCfFilter1(List<Cell> cells, byte[] cf0) {
        for (Cell c : cells) {
            if (CellUtil.matchingFamily(c, cf0)) {
                return true;
            }
        }
        return false;
    }

    /** Проверяет CF при двух допустимых семействах. */
    static boolean passCfFilter2(List<Cell> cells, byte[] cf0, byte[] cf1) {
        for (Cell c : cells) {
            if (CellUtil.matchingFamily(c, cf0)) {
                return true;
            }
            if (CellUtil.matchingFamily(c, cf1)) {
                return true;
            }
        }
        return false;
    }

    /** Проверяет CF при трех и более допустимых семействах. */
    static boolean passCfFilterN(List<Cell> cells, byte[][] cfFamilies, int[] cfHashes) {
        for (Cell c : cells) {
            int cellHash = Bytes.hashCode(c.getFamilyArray(), c.getFamilyOffset(), c.getFamilyLength());
            for (int i = 0; i < cfFamilies.length; i++) {
                if (cellHash == cfHashes[i]
                        && Bytes.equals(c.getFamilyArray(), c.getFamilyOffset(), c.getFamilyLength(),
                        cfFamilies[i], 0, cfFamilies[i].length)) {
                    return true;
                }
            }
        }
        return false;
    }

    /** Кеширует отфильтрованный список CF и их хеши для ускорения повторных проверок. */
    private static final class CfFilterCache {
        private static final CfFilterCache EMPTY = new CfFilterCache(null, null, null);

        final byte[][] sourceRef;
        final byte[][] families;
        final int[] hashes;

        private CfFilterCache(byte[][] sourceRef, byte[][] families, int[] hashes) {
            this.sourceRef = sourceRef;
            this.families = families;
            this.hashes = hashes;
        }

        boolean isEmpty() {
            return families == null || families.length == 0;
        }

        boolean matches(byte[][] candidate) {
            if (candidate == sourceRef) {
                return true;
            }
            return isEmpty() && (candidate == null || candidate.length == 0);
        }

        static CfFilterCache build(byte[][] source) {
            ArrayList<byte[]> sanitized = new ArrayList<>(source.length);
            for (byte[] cf : source) {
                if (cf == null || cf.length == 0) {
                    continue;
                }
                sanitized.add(cf);
            }
            if (sanitized.isEmpty()) {
                return EMPTY;
            }
            byte[][] copy = sanitized.toArray(new byte[0][]);
            Arrays.sort(copy, Bytes.BYTES_COMPARATOR);
            int uniqueCount = 1;
            for (int i = 1; i < copy.length; i++) {
                if (!Bytes.equals(copy[i], copy[i - 1])) {
                    uniqueCount++;
                }
            }
            byte[][] unique = new byte[uniqueCount][];
            int[] hashes = new int[uniqueCount];
            int idx = 0;
            unique[idx] = copy[0];
            hashes[idx] = Bytes.hashCode(copy[0], 0, copy[0].length);
            for (int i = 1; i < copy.length; i++) {
                if (!Bytes.equals(copy[i], copy[i - 1])) {
                    idx++;
                    unique[idx] = copy[i];
                    hashes[idx] = Bytes.hashCode(copy[i], 0, copy[i].length);
                }
            }
            return new CfFilterCache(source, unique, hashes);
        }
    }

    /**
     * Лёгкие счётчики по записи WAL — количество отправленных строк и ячеек.
     * Выделены в отдельный объект, чтобы передавать по ссылке и избегать возврата pair-структур.
     */
    
    /** Счётчики по текущей записи WAL (строки/ячейки/фильтр). */
    private static final class ProcessingCounters {
        int rowsSent;
        int cellsSent;
        int rowsFiltered;
        int cellsSeen;
    }

    /** Возвращает агрегированные счётчики обработанных записей WAL. */
    public WalMetrics metrics() {
        return new WalMetrics(
                entriesProcessed.sum(),
                rowsProcessed.sum(),
                cellsProcessed.sum(),
                rowsFiltered.sum());
    }

    /** @return суммарное количество обработанных WAL-записей. */
    public long entriesTotal() {
        return entriesProcessed.sum();
    }

    /** @return суммарное количество строк (включая отфильтрованные) с момента старта. */
    public long rowsTotal() {
        return rowsProcessed.sum();
    }

    /** @return суммарное количество ячеек, увиденных на горячем пути. */
    public long cellsTotal() {
        return cellsProcessed.sum();
    }

    /** @return суммарное количество строк, отфильтрованных по CF. */
    public long rowsFilteredTotal() {
        return rowsFiltered.sum();
    }

    /**
     * Агрегирует статистику за период и выводит DEBUG-лог с throughput (строки/ячейки/фильтрация).
     */
    private void logThroughput(ProcessingCounters counters) {
        entriesWindow.increment();
        if (counters.rowsSent > 0) {
            rowsWindow.add(counters.rowsSent);
        }
        if (counters.cellsSent > 0) {
            cellsWindow.add(counters.cellsSent);
        }
        if (counters.rowsFiltered > 0) {
            filteredRowsWindow.add(counters.rowsFiltered);
        }

        long now = System.nanoTime();
        long windowStart = throughputWindowStart.get();
        long elapsed = now - windowStart;
        if (elapsed < THROUGHPUT_LOG_INTERVAL_NS) {
            return;
        }
        if (!throughputWindowStart.compareAndSet(windowStart, now)) {
            return;
        }

        long rowsPreview = rowsWindow.sum();
        long cellsPreview = cellsWindow.sum();
        long filteredPreview = filteredRowsWindow.sum();
        if (rowsPreview == 0L && cellsPreview == 0L && filteredPreview == 0L) {
            return;
        }

        long entries = entriesWindow.sumThenReset();
        long rows = rowsWindow.sumThenReset();
        long cells = cellsWindow.sumThenReset();
        long filteredRows = filteredRowsWindow.sumThenReset();
        if (elapsed <= 0L) {
            return;
        }

        double intervalSeconds = elapsed / 1_000_000_000.0;
        double rowsPerSec = rows / intervalSeconds;
        double cellsPerSec = cells / intervalSeconds;

        if (LOG.isInfoEnabled()) {
            LOG.info(
                    "WAL throughput: entries={}, rows={}, rows/sec={}, cells={}, cells/sec={}, filteredRows={}, intervalMs={}",
                    entries,
                    rows,
                    formatDecimal(rowsPerSec),
                    cells,
                    formatDecimal(cellsPerSec),
                    filteredRows,
                    TimeUnit.NANOSECONDS.toMillis(elapsed));
        }
    }

    private static String formatDecimal(double value) {
        return String.format(Locale.ROOT, "%.1f", value);
    }

    public static final class WalMetrics {
        private final long entries;
        private final long rows;
        private final long cells;
        private final long filteredRows;

        WalMetrics(long entries, long rows, long cells, long filteredRows) {
            this.entries = entries;
            this.rows = rows;
            this.cells = cells;
            this.filteredRows = filteredRows;
        }

        public long entries() { return entries; }

        public long rows() { return rows; }

        public long cells() { return cells; }

        public long filteredRows() { return filteredRows; }
    }

    /**
     * Отправляет одну собранную строку в Kafka. Используется как горячим путём, так и в тестах
     * для проверки выбранного сериализатора без запуска полного цикла {@link #process}.
     */
    void sendRow(String topic,
                 TableName table,
                 WalMeta walMeta,
                 RowKeySlice rowKey,
                 List<Cell> cells,
                 BatchSender sender) {
        final byte[] keyBytes = rowKey.toByteArray();
        final byte[] valueBytes = payloadBuilder.buildRowPayloadBytes(table, cells, rowKey, walMeta.seq, walMeta.writeTime);
        sender.add(producer.send(new ProducerRecord<>(topic, keyBytes, valueBytes)));
    }

    /** Считывает sequenceId/writeTime из ключа WAL; ошибки приводят к -1. */
    private static WalMeta readWalMeta(WAL.Entry entry) {
        long walSeq = -1L;
        try {
            walSeq = entry.getKey().getSequenceId();
        } catch (IOException e) {
            // допускаем отсутствие sequenceId без логирования
        }
        final long walWriteTime = entry.getKey().getWriteTime();
        return new WalMeta(walSeq, walWriteTime);
    }

}
