package kz.qazmarka.h2k.payload;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;

import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.schema.Decoder;
import kz.qazmarka.h2k.util.RowKeySlice;

/**
 * Собирает payload Map<String,Object> из клеток строки HBase.
 * Без побочных эффектов; удобно тестировать. Работает только с целевым CF из конфигурации.
 *
 * Ключи результата фиксированы константами в этом классе (никакого "хардкода" строк):
 *  _table, _namespace, _qualifier, _cf, _cells_total, _cells_cf,
 *  event_version (максимальная метка времени среди клеток CF), delete,
 *  rowkey_hex / rowkey_b64, _wal_seq, _wal_write_time.
 *
 * Производительность:
 *  - Используется LinkedHashMap с заранее рассчитанной ёмкостью (минимум рехешей).
 *  - Строка qualifier строится лениво: только если колонка реально попала в результат.
 *  - Декодер вызывается только для клеток целевого CF (минимум ветвлений).
 *
 * Логи и локализация: класс не пишет в лог; все сообщения в проекте — на русском.
 */
public final class PayloadBuilder {
    private static final char[] HEX = "0123456789abcdef".toCharArray();
    /** Общий Base64-энкодер для rowkey.
     *  Важно: стандартный java.util.Base64.Encoder кодирует весь массив и не поддерживает (offset,len),
     *  поэтому для среза rowkey делаем компактную копию нужного участка (аллокация O(len)).
     */
    private static final java.util.Base64.Encoder BASE64 = java.util.Base64.getEncoder();

    // Ключи payload (исключаем "магические строки" по всему коду)
    private static final String K_TABLE          = "_table";
    private static final String K_NAMESPACE      = "_namespace";
    private static final String K_QUALIFIER      = "_qualifier";
    private static final String K_CF             = "_cf";
    private static final String K_CELLS_TOTAL    = "_cells_total";
    private static final String K_CELLS_CF       = "_cells_cf";
    private static final String K_EVENT_VERSION  = "event_version";
    private static final String K_DELETE         = "delete";
    private static final String K_ROWKEY_HEX     = "rowkey_hex";
    private static final String K_ROWKEY_B64     = "rowkey_b64";
    private static final String K_WAL_SEQ        = "_wal_seq";
    private static final String K_WAL_WRITE_TIME = "_wal_write_time";

    /**
     * Подбирает начальную ёмкость LinkedHashMap под ожидаемое число пар (учитывая loadFactor 0.75).
     * Эквивалент ⌈expected/0.75⌉, но без операций с плавающей точкой (чуть быстрее и без FP‑округлений).
     */
    private static int capacityFor(int expectedEntries) {
        return expectedEntries <= 0 ? 1 : (expectedEntries * 4 / 3) + 1;
    }

    private final Decoder decoder;
    private final H2kConfig cfg;

    public PayloadBuilder(Decoder decoder, H2kConfig cfg) {
        this.decoder = Objects.requireNonNull(decoder, "decoder");
        this.cfg = Objects.requireNonNull(cfg, "cfg");
    }

    /**
     * Основной метод сборки payload (одно событие по rowkey).
     *
     * Поведение управляется флагами из {@link H2kConfig}:
     * includeMeta / includeMetaWal / includeRowKey / rowkeyEncoding и т.д.
     * Порядок ключей в результате стабильный благодаря LinkedHashMap. Строка qualifier строится лениво — только при фактическом добавлении поля.
     *
     * @param table        таблица HBase (для ключей _table/_namespace/_qualifier и для декодера)
     * @param cells        список ячеек этой строки (все CF); внутри будут выбраны и обработаны только ячейки целевого CF из конфигурации
     * @param rowKey       срез rowkey (может быть null, если источник не предоставляет ключ)
     * @param walSeq       sequenceId записи WAL (или < 0, если недоступен)
     * @param walWriteTime время записи в WAL в мс (или < 0, если недоступно)
     * @return LinkedHashMap с данными колонок и служебными полями
     */
    public Map<String, Object> buildRowPayload(TableName table,
                                               List<Cell> cells,
                                               RowKeySlice rowKey,
                                               long walSeq,
                                               long walWriteTime) {
        Objects.requireNonNull(table, "table");
        // Кэшируем флаги конфига (уменьшаем количество ветвлений в этом методе)
        final boolean includeMeta    = cfg.isIncludeMeta();
        final boolean includeWalMeta = includeMeta && cfg.isIncludeMetaWal();
        final boolean includeRowKey  = cfg.isIncludeRowKey();
        final boolean rowkeyB64      = cfg.isRowkeyBase64();

        // Оцениваем число ключей в результате и заранее задаём ёмкость мапы
        final int cellsCount = (cells == null ? 0 : cells.size());
        final boolean includeRowKeyPresent = includeRowKey && rowKey != null;
        int cap = 1 /*event_version*/
                + cellsCount
                + (includeMeta ? 5 : 0)       /*_table,_namespace,_qualifier,_cf,_cells_total*/
                + (includeRowKeyPresent ? 1 : 0) /*rowkey_hex|rowkey_b64*/
                + (includeWalMeta ? 2 : 0);   /*_wal_seq,_wal_write_time*/

        final Map<String, Object> obj = new LinkedHashMap<>(capacityFor(cap));

        // Метаданные таблицы (если включены)
        addMetaIfEnabled(includeMeta, obj, table, cellsCount);

        // Расшифровка ячеек: добавляет поля CF в obj и возвращает агрегаты
        CellStats stats = decodeCells(table, cells, obj);

        // Итоговые служебные поля
        addCellsCfIfMeta(obj, includeMeta, stats.cfCells);
        obj.put(K_EVENT_VERSION, stats.maxTs);
        addDeleteFlagIfNeeded(obj, stats.hasDelete);

        // RowKey (если включён и присутствует)
        addRowKeyIfPresent(includeRowKeyPresent, obj, rowKey, rowkeyB64);

        // Метаданные WAL (если включены)
        addWalMeta(includeWalMeta, obj, walSeq, walWriteTime);

        return obj;
    }

    /** Декодирует клетки целевого CF и заполняет obj; возвращает агрегаты. */
    private CellStats decodeCells(TableName table, List<Cell> cells, Map<String, Object> obj) {
        CellStats s = new CellStats();
        if (cells == null || cells.isEmpty()) {
            return s;
        }

        final byte[] cf = cfg.getCfBytes();
        final boolean serializeNulls = cfg.isJsonSerializeNulls();

        for (Cell cell : cells) {
            processCell(table, cell, obj, s, cf, serializeNulls);
        }
        return s;
    }

    /**
     * Обрабатывает одну ячейку: учитывает только целевой CF, обновляет агрегаты и при необходимости
     * декодирует значение и добавляет его в результирующую карту.
     */
    private void processCell(TableName table,
                              Cell cell,
                              Map<String, Object> obj,
                              CellStats s,
                              byte[] cf,
                              boolean serializeNulls) {
        if (!CellUtil.matchingFamily(cell, cf)) {
            return; // не наш CF
        }
        s.cfCells++;
        long ts = cell.getTimestamp();
        if (ts > s.maxTs) {
            s.maxTs = ts;
        }
        if (CellUtil.isDelete(cell)) {
            s.hasDelete = true; // удаление помечаем, но колонки не добавляем
            return;
        }
        final byte[] qa = cell.getQualifierArray();
        final int qo = cell.getQualifierOffset();
        final int ql = cell.getQualifierLength();
        final byte[] va = cell.getValueArray();
        final int vo = cell.getValueOffset();
        final int vl = cell.getValueLength();

        Object decoded = decoder.decode(
                table,
                qa, qo, ql,
                va, vo, vl
        );

        if (decoded != null || serializeNulls) {
            String q = new String(qa, qo, ql, StandardCharsets.UTF_8);
            obj.put(q, decoded);
        }
    }

    private void addMetaFields(Map<String, Object> obj, TableName table, int totalCells) {
          String ns = table.getNamespaceAsString();
          String qn = table.getQualifierAsString();
          obj.put(K_TABLE, table.getNameAsString());
          obj.put(K_NAMESPACE, ns);
          obj.put(K_QUALIFIER, qn);
          // Имя CF берём только из конфигурации: H2kConfig гарантирует non-null.
          obj.put(K_CF, cfg.getCfNameForPayload());
          obj.put(K_CELLS_TOTAL, totalCells);
    }

    /**
     * Добавляет базовые метаданные таблицы, если флаг включён.
     */
    private void addMetaIfEnabled(boolean includeMeta, Map<String, Object> obj, TableName table, int totalCells) {
        if (includeMeta) {
            addMetaFields(obj, table, totalCells);
        }
    }

    /**
     * Добавляет счётчик клеток по CF, только если включены метаданные.
     */
    private static void addCellsCfIfMeta(Map<String, Object> obj, boolean includeMeta, int cfCells) {
        if (includeMeta) {
            obj.put(K_CELLS_CF, cfCells);
        }
    }

    /**
     * Добавляет флаг удаления, если в партии встречалась операция удаления.
     */
    private static void addDeleteFlagIfNeeded(Map<String, Object> obj, boolean hasDelete) {
        if (hasDelete) {
            obj.put(K_DELETE, true);
        }
        }

    /**
     * Добавляет rowkey в hex или base64, если он присутствует.
     * Дополнительно проверяет на null для удовлетворения статического анализатора.
     */
    private static void addRowKeyIfPresent(boolean includeRowKeyPresent,
                                           Map<String, Object> obj,
                                           RowKeySlice rk,
                                           boolean base64) {
        if (!includeRowKeyPresent || rk == null) {
            return;
        }
        if (base64) {
            obj.put(K_ROWKEY_B64, encodeRowKeyBase64(rk));
        } else {
            obj.put(K_ROWKEY_HEX, toHex(rk.getArray(), rk.getOffset(), rk.getLength()));
        }
    }

    /**
     * Добавляет метаданные WAL (_wal_seq, _wal_write_time), если включены, и значения неотрицательны.
     */
    private static void addWalMeta(boolean includeWalMeta,
                                   Map<String, Object> obj,
                                   long walSeq,
                                   long walWriteTime) {
        if (!includeWalMeta) {
            return;
        }
        if (walSeq >= 0L) {
            obj.put(K_WAL_SEQ, walSeq);
        }
        if (walWriteTime >= 0L) {
            obj.put(K_WAL_WRITE_TIME, walWriteTime);
        }
    }

    private static String toHex(byte[] a, int off, int len) {
        if (a == null || len == 0) {
            return "";
        }
        char[] out = new char[len * 2];
        int p = 0;
        for (int i = 0; i < len; i++) {
            int v = a[off + i] & 0xFF;
            out[p++] = HEX[v >>> 4];
            out[p++] = HEX[v & 0x0F];
        }
        return new String(out);
    }

    private static String encodeRowKeyBase64(RowKeySlice s) {
        if (s == null) return null;
        if (s.getLength() == 0) return "";
        // Base64 работает только с полным массивом; копируем срез rowkey.
        byte[] copy = s.toByteArray();
        return BASE64.encodeToString(copy);
    }

    /** Агрегаты по CF. */
    private static final class CellStats {
        long maxTs;
        boolean hasDelete;
        int cfCells;
    }
}