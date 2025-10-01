package kz.qazmarka.h2k.endpoint.internal;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.endpoint.internal.WalEntryProcessor.WalMetrics;
import kz.qazmarka.h2k.kafka.ensure.TopicEnsurer;
import kz.qazmarka.h2k.kafka.producer.BatchSender;
import kz.qazmarka.h2k.payload.builder.PayloadBuilder;
import kz.qazmarka.h2k.schema.decoder.SimpleDecoder;

/**
 * Юнит‑тесты для внутренних помощников {@link WalEntryProcessor} (initialCapacity и фильтры WAL).
 */
class WalEntryProcessorTest {

    private static Cell cell(String row, String cf, long ts) {
        return new KeyValue(bytes(row), bytes(cf), bytes("q"), ts, bytes("v"));
    }

    private static byte[] bytes(String s) {
        return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private static int[] hashes(byte[][] families) {
        if (families == null) {
            return null;
        }
        int[] hashes = new int[families.length];
        for (int i = 0; i < families.length; i++) {
            byte[] cf = families[i];
            hashes[i] = cf == null ? 0 : Bytes.hashCode(cf, 0, cf.length);
        }
        return hashes;
    }

    @Test
    @DisplayName("passCfFilter1(): один CF — true, если встречается целевое семейство")
    void filter_oneCf() {
        List<Cell> cells = Arrays.asList(
                cell("r", "a", 50L),
                cell("r", "a", 100L)
        );
        byte[] cfA = bytes("a");
        assertTrue(WalEntryProcessor.passCfFilter1(cells, cfA));
        assertFalse(WalEntryProcessor.passCfFilter1(cells, bytes("b")));
        assertFalse(WalEntryProcessor.passCfFilter1(Collections.<Cell>emptyList(), cfA));
    }

    @Test
    @DisplayName("passCfFilter2(): true при совпадении с любым из двух семейств")
    void filter_twoCf() {
        List<Cell> cells = Arrays.asList(
                cell("r", "a", 100L),
                cell("r", "b", 90L),
                cell("r", "c", 120L)
        );
        assertTrue(WalEntryProcessor.passCfFilter2(cells, bytes("a"), bytes("b")), "совпадение по первому CF");
        assertTrue(WalEntryProcessor.passCfFilter2(cells, bytes("x"), bytes("b")), "совпадение по второму CF");
        assertFalse(WalEntryProcessor.passCfFilter2(cells, bytes("x"), bytes("y")), "нет совпадений");
    }

    @Test
    @DisplayName("passCfFilterN(): три и более CF")
    void filter_manyCf() {
        List<Cell> cells = Arrays.asList(
                cell("r", "a", 10L),
                cell("r", "b", 20L),
                cell("r", "c", 30L)
        );
        byte[][] fams1 = {bytes("a"), bytes("b"), bytes("c")};
        byte[][] fams2 = {bytes("z"), bytes("b"), bytes("y")};
        byte[][] fams3 = {bytes("x"), bytes("y"), bytes("z")};
        assertTrue(WalEntryProcessor.passCfFilterN(cells, fams1, hashes(fams1)));
        assertTrue(WalEntryProcessor.passCfFilterN(cells, fams2, hashes(fams2)));
        assertFalse(WalEntryProcessor.passCfFilterN(cells, fams3, hashes(fams3)));
        byte[][] famsEmpty = {bytes("a")};
        assertFalse(WalEntryProcessor.passCfFilterN(Collections.<Cell>emptyList(), famsEmpty, hashes(famsEmpty)));
    }

    @Test
    @DisplayName("WalMetrics агрегирует строки и фильтрацию")
    void metricsAccumulates() throws Exception {
        Configuration cfg = new Configuration(false);
        cfg.set("h2k.cf.list", "d");
        cfg.set("h2k.kafka.bootstrap.servers", "mock:9092");
        H2kConfig h2kConfig = H2kConfig.from(cfg, "mock:9092");

        PayloadBuilder builder = new PayloadBuilder(SimpleDecoder.INSTANCE, h2kConfig);
        TopicManager topicManager = new TopicManager(h2kConfig, TopicEnsurer.disabled());
        MockProducer<byte[], byte[]> producer = new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
        WalEntryProcessor processor = new WalEntryProcessor(builder, topicManager, producer);

        try (BatchSender sender = new BatchSender(10, 1000, false, false)) {
            processor.process(walEntry("row1", "d"), sender, false, false, null);
        }

        WalMetrics metrics = processor.metrics();
        assertEquals(1, metrics.entries());
        assertEquals(1, metrics.rows());
        assertEquals(1, metrics.cells());
        assertEquals(0, metrics.filteredRows());

        try (BatchSender sender = new BatchSender(10, 1000, false, false)) {
            processor.process(walEntry("row2", "d"), sender, false, true, new byte[][]{bytes("x")});
        }

        WalMetrics after = processor.metrics();
        assertEquals(2, after.entries());
        assertEquals(2, after.rows());
        assertEquals(2, after.cells());
        assertEquals(1, after.filteredRows());
    }

    private static WAL.Entry walEntry(String row, String cf) {
        byte[] rowBytes = bytes(row);
        byte[] cfBytes = bytes(cf);
        WALKey key = new WALKey(rowBytes, TableName.valueOf("t"), 1L);
        WALEdit edit = new WALEdit();
        edit.add(new KeyValue(rowBytes, cfBytes, bytes("q"), 1L, bytes("v")));
        return new Entry(key, edit);
    }
}
