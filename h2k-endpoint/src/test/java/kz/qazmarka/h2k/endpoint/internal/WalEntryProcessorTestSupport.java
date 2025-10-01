package kz.qazmarka.h2k.endpoint.internal;

import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;

import kz.qazmarka.h2k.kafka.producer.BatchSender;
import kz.qazmarka.h2k.util.RowKeySlice;

/**
 * Тестовый помощник для обращения к package-private методам {@link WalEntryProcessor} из внешних пакетов.
 */
public final class WalEntryProcessorTestSupport {
    private WalEntryProcessorTestSupport() { }

    public static void sendRow(WalEntryProcessor processor,
                               String topic,
                               TableName table,
                               WalMeta walMeta,
                               RowKeySlice slice,
                               List<Cell> cells,
                               BatchSender sender) {
        processor.sendRow(topic, table, walMeta, slice, cells, sender);
    }
}
