package kz.qazmarka.h2k.payload.builder;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.schema.decoder.Decoder;
import kz.qazmarka.h2k.util.Maps;
import kz.qazmarka.h2k.util.RowKeySlice;

/**
 * Собирает корневую карту payload для строки WAL, добавляя метаданные, PK и служебные поля.
 * Логику построения вынесено из {@link PayloadBuilder}, чтобы разгрузить горячий путь и облегчить тестирование.
 */
final class RowPayloadAssembler {

    private final Decoder decoder;
    private final H2kConfig cfg;
    private final QualifierCache qualifierCache = new QualifierCache();

    /**
     * @param decoder декодер Phoenix-значений, используемый для преобразования ячеек в Java-тип
     * @param cfg     неизменяемая конфигурация h2k с подсказками по соли, capacity и метаданным
     */
    RowPayloadAssembler(Decoder decoder, H2kConfig cfg) {
        this.decoder = decoder;
        this.cfg = cfg;
    }

    /**
     * Собирает основную карту payload для отдельной строки WAL с учётом всех включённых опций.
     *
     * @param table        таблица Phoenix (включая namespace)
     * @param cells        список ячеек строкового ключа (может быть пустым)
     * @param rowKey       срез байтов rowkey (может быть {@code null})
     * @param walSeq       номер записи WAL
     * @param walWriteTime «настенные» часы записи WAL (мс), либо отрицательное значение, если нет данных
     * @return карта payload, готовая к сериализации
     */
    Map<String, Object> assemble(TableName table,
                                 List<Cell> cells,
                                 RowKeySlice rowKey,
                                 long walSeq,
                                 long walWriteTime) {
        final boolean includeMeta = cfg.isIncludeMeta();
        final boolean includeWalMeta = includeMeta && cfg.isIncludeMetaWal();
        final boolean includeRowKey = cfg.isIncludeRowKey();
        final boolean rowkeyB64 = cfg.isRowkeyBase64();

        final int cellsCount = (cells == null ? 0 : cells.size());
        final boolean includeRowKeyPresent = includeRowKey && rowKey != null;
        int cap = 1 // _event_ts
                + cellsCount
                + (includeMeta ? 5 : 0)
                + (includeRowKeyPresent ? 1 : 0)
                + (includeWalMeta ? 2 : 0);

        final Map<String, Object> obj = newRootMap(table, cap);
        MetaWriter.addMetaIfEnabled(includeMeta, obj, table, cellsCount, cfg.getCfNamesCsv());
        decodePkFromRowKey(table, rowKey, obj);

        CellStats stats = decodeCells(table, cells, obj);
        MetaWriter.addCellsCfIfMeta(obj, includeMeta, stats.cfCells);
        if (stats.cfCells > 0) {
            obj.put(PayloadFields.EVENT_TS, stats.maxTs);
        }
        MetaWriter.addDeleteFlagIfNeeded(obj, stats.hasDelete);

        final int saltBytes = cfg.getSaltBytesFor(table);
        RowKeyWriter.addRowKeyIfPresent(includeRowKeyPresent, obj, rowKey, rowkeyB64, saltBytes);
        MetaWriter.addWalMeta(includeWalMeta, obj, walSeq, walWriteTime);
        return obj;
    }

    /**
     * Создаёт корневую {@link java.util.LinkedHashMap} с учётом конфигурационной подсказки ёмкости.
     *
     * @param table               таблица Phoenix (используется для подсказки ёмкости)
     * @param estimatedKeysCount  оценка числа ключей в payload до добавления метаданных
     * @return готовая LinkedHashMap требуемой начальной ёмкости
     */
    private Map<String, Object> newRootMap(TableName table, int estimatedKeysCount) {
        final int hint = cfg.getCapacityHintFor(table);
        final int target = Math.max(estimatedKeysCount, hint > 0 ? hint : 0);
        final int initialCapacity = Maps.initialCapacity(target);
        return new LinkedHashMap<>(initialCapacity, 0.75f);
    }

    /**
     * Распаковывает PK из rowkey и добавляет их в выходной payload.
     *
     * @param table таблица Phoenix
     * @param rk    срез rowkey; может быть {@code null}
     * @param out   карта payload, куда добавляются значения PK
     */
    private void decodePkFromRowKey(TableName table, RowKeySlice rk, Map<String, Object> out) {
        if (rk == null) {
            return;
        }
        final int saltBytes = cfg.getSaltBytesFor(table);
        decoder.decodeRowKey(table, rk, saltBytes, out);
    }

    /**
     * Раскодирует значения ячеек и заполняет карту payload, заодно собирая агрегаты для метаданных.
     *
     * @param table таблица Phoenix
     * @param cells список ячеек строки
     * @param obj   карта payload, куда помещаются значения
     * @return статистика по обработанным ячейкам
     */
    private CellStats decodeCells(TableName table, List<Cell> cells, Map<String, Object> obj) {
        CellStats s = new CellStats();
        if (cells == null || cells.isEmpty()) {
            return s;
        }

        final boolean serializeNulls = cfg.isJsonSerializeNulls();
        for (Cell cell : cells) {
            processCell(table, cell, obj, s, serializeNulls);
        }
        return s;
    }

    /**
     * Обрабатывает отдельную ячейку: применяет фильтр delete, декодирует значение и добавляет его в payload.
     *
     * @param table          таблица Phoenix
     * @param cell           ячейка WAL
     * @param obj            карта payload
     * @param s              статистика по строке (обновляется на месте)
     * @param serializeNulls разрешена ли сериализация {@code null}-значений
     */
    private void processCell(TableName table,
                             Cell cell,
                             Map<String, Object> obj,
                             CellStats s,
                             boolean serializeNulls) {
        s.cfCells++;
        long ts = cell.getTimestamp();
        if (ts > s.maxTs) s.maxTs = ts;
        if (CellUtil.isDelete(cell)) {
            s.hasDelete = true;
            return;
        }

        final byte[] qa = cell.getQualifierArray();
        final int qo = cell.getQualifierOffset();
        final int ql = cell.getQualifierLength();
        final String columnName = qualifierCache.intern(qa, qo, ql);

        final byte[] va = cell.getValueArray();
        final int vo = cell.getValueOffset();
        final int vl = cell.getValueLength();

        Object decoded = decoder.decode(table, qa, qo, ql, va, vo, vl);
        if (decoded != null) {
            obj.put(columnName, decoded);
            return;
        }

        if (va != null && vl > 0) {
            obj.put(columnName, new String(va, vo, vl, StandardCharsets.UTF_8));
            return;
        }

        if (serializeNulls && (!obj.containsKey(columnName) || obj.get(columnName) == null)) {
            obj.put(columnName, null);
        }
    }

    /** Минимальный потокобезопасный кэш qualifier → String, чтобы исключить повторные аллокации. */
    private static final class QualifierCache {
        private final ConcurrentHashMap<AbstractKey, String> cache = new ConcurrentHashMap<>(64);
        private final ThreadLocal<LookupKey> lookup = ThreadLocal.withInitial(LookupKey::new);

        /**
         * Возвращает интернированное строковое представление qualifier без лишних аллокаций.
         *
         * @param array  массив с qualifier в UTF-8
         * @param offset смещение qualifier
         * @param length длина qualifier
         * @return строка qualifier (общий экземпляр для повторных обращений)
         */
        String intern(byte[] array, int offset, int length) {
            if (length == 0) {
                return "";
            }
            LookupKey key = lookup.get();
            key.set(array, offset, length);
            try {
                String cached = cache.get(key);
                if (cached != null) {
                    return cached;
                }
                String created = new String(array, offset, length, StandardCharsets.UTF_8);
                OwnedKey stored = key.toOwned();
                String race = cache.putIfAbsent(stored, created);
                return race != null ? race : created;
            } finally {
                key.clear();
                lookup.remove();
                lookup.set(key);
            }
        }

        private abstract static class AbstractKey {
            byte[] bytes;
            int offset;
            int length;
            int hash;

            /** Сохраняет ссылку на массив и вычисляет хеш. */
            final void reset(byte[] array, int off, int len) {
                this.bytes = array;
                this.offset = off;
                this.length = len;
                this.hash = Bytes.hashCode(array, off, len);
            }

            /** Обнуляет ссылки, чтобы не удерживать исходный массив. */
            final void clear() {
                this.bytes = null;
                this.offset = 0;
                this.length = 0;
                this.hash = 0;
            }

            @Override
            public final int hashCode() {
                return hash;
            }

            @Override
            public final boolean equals(Object o) {
                if (this == o) return true;
                if (!(o instanceof AbstractKey)) return false;
                AbstractKey other = (AbstractKey) o;
                if (this.hash != other.hash || this.length != other.length) {
                    return false;
                }
                return Bytes.equals(this.bytes, this.offset, this.length,
                        other.bytes, other.offset, other.length);
            }
        }

        private static final class LookupKey extends AbstractKey {
            /** Подготовить ключ к поиску в кэше. */
            void set(byte[] array, int off, int len) {
                reset(array, off, len);
            }

            /** Создаёт владеющую копию qualifier для помещения в кэш. */
            OwnedKey toOwned() {
                return new OwnedKey(bytes, offset, length);
            }
        }

        private static final class OwnedKey extends AbstractKey {
            /**
             * @param source исходный массив qualifier (копируется, чтобы избежать совместного владения)
             * @param off    смещение
             * @param len    длина qualifier
             */
            OwnedKey(byte[] source, int off, int len) {
                byte[] copy = new byte[len];
                System.arraycopy(source, off, copy, 0, len);
                reset(copy, 0, len);
            }
        }
    }

    /** Утилиты для метаданных и служебных полей. */
    private static final class MetaWriter {
        private MetaWriter() { }

        /**
         * Добавляет стандартные метаполя (таблица, namespace, qualifier, список CF и счётчик ячеек).
         */
        static void addMetaIfEnabled(boolean includeMeta,
                                     Map<String, Object> obj,
                                     TableName table,
                                     int totalCells,
                                     String cfList) {
            if (!includeMeta) {
                return;
            }
            obj.put(PayloadFields.TABLE, table.getNameAsString());
            obj.put(PayloadFields.NAMESPACE, table.getNamespaceAsString());
            obj.put(PayloadFields.QUALIFIER, table.getQualifierAsString());
            obj.put(PayloadFields.CF, cfList);
            obj.put(PayloadFields.CELLS_TOTAL, totalCells);
        }

        /** Добавляет число ячеек после фильтра CF, если включены метаданные. */
        static void addCellsCfIfMeta(Map<String, Object> obj, boolean includeMeta, int cfCells) {
            if (includeMeta) {
                obj.put(PayloadFields.CELLS_CF, cfCells);
            }
        }

        /** Устанавливает флаг удаления, если в строке присутствовала delete-операция. */
        static void addDeleteFlagIfNeeded(Map<String, Object> obj, boolean hasDelete) {
            if (hasDelete) {
                obj.put(PayloadFields.DELETE, 1);
            }
        }

        /**
         * Дополняет payload полями WAL (_wal_seq, _wal_write_time), если они доступны и разрешены конфигурацией.
         */
        static void addWalMeta(boolean includeWalMeta,
                               Map<String, Object> obj,
                               long walSeq,
                               long walWriteTime) {
            if (!includeWalMeta) {
                return;
            }
            if (walSeq >= 0L) {
                obj.put(PayloadFields.WAL_SEQ, walSeq);
            }
            if (walWriteTime >= 0L) {
                obj.put(PayloadFields.WAL_WRITE_TIME, walWriteTime);
            }
        }
    }

    /** Утилита записи rowkey в payload. */
    private static final class RowKeyWriter {
        private RowKeyWriter() { }

        /**
         * Добавляет сериализованный rowkey (HEX/Base64) в payload, учитывая соль.
         */
        static void addRowKeyIfPresent(boolean includeRowKeyPresent,
                                       Map<String, Object> obj,
                                       RowKeySlice rk,
                                       boolean base64,
                                       int salt) {
            if (!includeRowKeyPresent || rk == null || rk.isEmpty()) {
                return;
            }
            final int len = rk.getLength();
            int off = rk.getOffset() + salt;
            int effLen = len - salt;
            if (effLen <= 0) {
                return;
            }
            if (base64) {
                obj.put(PayloadFields.ROWKEY, kz.qazmarka.h2k.util.Bytes.base64(rk.getArray(), off, effLen));
            } else {
                obj.put(PayloadFields.ROWKEY, kz.qazmarka.h2k.util.Bytes.toHex(rk.getArray(), off, effLen));
            }
        }
    }

    /** Аккумулирует статистику по ячейкам строки для последующего формирования метаданных. */
    private static final class CellStats {
        long maxTs;
        boolean hasDelete;
        int cfCells;
    }
}
