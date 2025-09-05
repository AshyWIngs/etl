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
 * Собирает payload `Map<String,Object>` из клеток строки HBase.
 *
 * Назначение
 *  - Преобразовать набор ячеек (всех CF) одной строки в упорядоченную карту значений целевых CF и служебных полей.
 *  - Работает только с CF, указанными в {@link H2kConfig}; остальные ячейки пропускаются без накладных расходов.
 *
 * Производительность и потокобезопасность
 *  - Без побочных эффектов; удобно тестировать.
 *  - Используется {@link LinkedHashMap} с заранее рассчитанной ёмкостью (минимум рехешей), порядок ключей стабилен.
 *  - Строка qualifier строится «лениво» — только если колонка реально попала в результат.
 *  - Декодер вызывается только для клеток целевых CF (минимум ветвлений на горячем пути).
 *
 * Логи и локализация
 *  - Класс сам ничего не пишет в лог; все сообщения в проекте — на русском.
 *
 * Конфигурация
 *  - Поведение управляется флагами из {@link H2kConfig}: includeMeta / includeMetaWal / includeRowKey /
 *    rowkeyEncoding (hex|base64) и др.
 *  - Если для таблицы задана соль Phoenix, {@link H2kConfig#getSaltBytesFor(org.apache.hadoop.hbase.TableName)} позволяет
 *    «снять» первые N байт rowkey перед сериализацией (в payload попадает бизнес‑ключ без байта «корзины»).
 */
public final class PayloadBuilder {
    /**
     * Таблица символов для быстрого перевода байтов в hex без создания промежуточных объектов.
     * Используется только локальным методом {@link #toHex(byte[], int, int)}.
     */
    private static final char[] HEX = "0123456789abcdef".toCharArray();
    /**
     * Общий Base64-энкодер для rowkey.
     * JDK 8 предоставляет только array‑based API (без (offset,len)), поэтому кодируем из временного среза.
     */
    private static final java.util.Base64.Encoder BASE64 = java.util.Base64.getEncoder();
    /**
     * Порог предпросмотра для оценки числа целевых ячеек.
     * Если в строке ячеек больше порога — сканируем только первые N и экстраполируем.
     * Цель: точнее подобрать ёмкость LinkedHashMap без второго полного прохода.
     */
    private static final int PRESCAN_THRESHOLD = 128;
    /**
     * Ключи payload (исключаем "магические строки" по коду):
     *  - _table, _namespace, _qualifier, _cf — метаданные таблицы и CSV‑список целевых CF;
     *  - _cells_total, _cells_cf — общее количество ячеек в строке и число ячеек целевого CF;
     *  - event_version — максимальная метка времени среди ячеек CF;
     *  - delete — признак, что в партии встречалась операция удаления по строке;
     *  - _rowkey — представление rowkey (HEX или Base64 в зависимости от h2k.rowkey.encoding; включается по флагу includeRowKey);
     *  - _wal_seq, _wal_write_time — метаданные WAL (включаются по includeMetaWal).
     */
    private static final String K_TABLE          = "_table";
    private static final String K_NAMESPACE      = "_namespace";
    private static final String K_QUALIFIER      = "_qualifier";
    private static final String K_CF             = "_cf";
    private static final String K_CELLS_TOTAL    = "_cells_total";
    private static final String K_CELLS_CF       = "_cells_cf";
    private static final String K_EVENT_VERSION  = "event_version";
    private static final String K_DELETE         = "delete";
    /** Единое имя поля rowkey согласно требованиям проекта (HEX/BASE64 выбирается по конфигу). */
    private static final String K_ROWKEY         = "_rowkey";
    private static final String K_WAL_SEQ        = "_wal_seq";
    private static final String K_WAL_WRITE_TIME = "_wal_write_time";


    private final Decoder decoder;
    private final H2kConfig cfg;

    /**
     * @param decoder декодер значений клеток целевого CF (byte[] → Java‑типы)
     * @param cfg     снимок конфигурации h2k.* (флаги сериализации/метаданных)
     * @throws NullPointerException если любой из аргументов равен null
     */
    public PayloadBuilder(Decoder decoder, H2kConfig cfg) {
        this.decoder = Objects.requireNonNull(decoder, "decoder");
        this.cfg = Objects.requireNonNull(cfg, "cfg");
    }

    /**
     * Создаёт корневую LinkedHashMap с ёмкостью, подобранной по «подсказке» из H2kConfig (если задана)
     * и/или оценке фактического числа ключей (данные + метаданные). Расчёт ёмкости производится целочисленной формулой,
     * эквивалентной 1 + ceil(target / 0.75), без использования double/Math.ceil.
     *
     * @param table              таблица HBase для которой строится payload
     * @param estimatedKeysCount оценка общего числа ключей в payload (данные колонок + служебные поля)
     */
    private Map<String, Object> newRootMap(TableName table, int estimatedKeysCount) {
        final int hint = cfg.getCapacityHintFor(table); // 0, если подсказка не задана
        final int initialCapacity = computeInitialCapacity(estimatedKeysCount, hint);
        return new LinkedHashMap<>(initialCapacity, 0.75f);
    }

    /**
     * Вычисляет начальную ёмкость LinkedHashMap без использования FP‑арифметики:
     * формула 1 + ceil(target / 0.75) эквивалентна 1 + (4*target + 2) / 3 (целочисленно).
     * Это избавляет от Math.ceil/double‑деления на горячем пути и даёт те же результаты.
     */
    static int computeInitialCapacity(int estimatedKeysCount, int hint) {
        // Берём максимум из оценки и подсказки (если она задана)
        final int target = Math.max(estimatedKeysCount, hint > 0 ? hint : 0);
        if (target <= 0) {
            return 1;
        }
        // 1 + ceil(4*target / 3)  ==  1 + (4*target + 2) / 3  (целочисленное деление)
        final long n = ((long) target << 2) + 2; // 4*target + 2
        long cap = 1 + n / 3L;
        // HashMap/LinkedHashMap внутренняя таблица ограничена ~2^30
        final long MAX = 1L << 30;
        if (cap > MAX) cap = MAX;
        return (int) cap;
    }

    /**
     * Основной метод сборки payload (одно событие по rowkey).
     * Порядок ключей стабилен ({@link LinkedHashMap}). Null‑значения колонок включаются в JSON только
     * при {@code cfg.isJsonSerializeNulls()}.
     *
     * @param table        таблица HBase (для ключей _table/_namespace/_qualifier и для декодера)
     * @param cells        список ячеек этой строки (все CF); внутри будут обработаны только ячейки целевого CF
     * @param rowKey       срез rowkey (может быть null, если источник не предоставляет ключ)
     * @param walSeq       sequenceId записи WAL (или < 0, если недоступен)
     * @param walWriteTime время записи в WAL в мс (или < 0, если недоступно)
     * @return упорядоченная карта с данными колонок и служебными полями
     * @throws NullPointerException если {@code table} равен null
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

        // Точный/приближённый прогноз числа целевых ячеек.
        // Подсказка ёмкости учитывается на этапе newRootMap(...) как итоговое целевое число ключей.
        // Здесь для оценки числа колонок используем только прескан.
        final byte[][] cfFamilies = cfg.getCfFamiliesBytes();
        final int sampledCellsEstimate = estimateTargetCells(cells, cfFamilies);
        final int effectiveCellsEstimate = sampledCellsEstimate;

        int cap = 1 /*event_version*/
                + effectiveCellsEstimate
                + (includeMeta ? 5 : 0)         /*_table,_namespace,_qualifier,_cf,_cells_total*/
                + (includeRowKeyPresent ? 1 : 0)/*_rowkey*/
                + (includeWalMeta ? 2 : 0);     /*_wal_seq,_wal_write_time*/

        final Map<String, Object> obj = newRootMap(table, cap);

        // Метаданные таблицы (если включены)
        addMetaIfEnabled(includeMeta, obj, table, cellsCount);

        // Расшифровка ячеек: добавляет поля CF в obj и возвращает агрегаты
        CellStats stats = decodeCells(table, cells, obj, cfFamilies);

        // Итоговые служебные поля
        addCellsCfIfMeta(obj, includeMeta, stats.cfCells);
        obj.put(K_EVENT_VERSION, stats.maxTs);
        addDeleteFlagIfNeeded(obj, stats.hasDelete);

        // RowKey (если включён и присутствует). При необходимости снимаем префикс соли Phoenix.
        final int saltBytes = cfg.getSaltBytesFor(table);
        addRowKeyIfPresent(includeRowKeyPresent, obj, rowKey, rowkeyB64, saltBytes);

        // Метаданные WAL (если включены)
        addWalMeta(includeWalMeta, obj, walSeq, walWriteTime);

        return obj;
    }

    /**
     * Декодирует клетки целевых CF и заполняет {@code obj} декодированными значениями колонок.
     * Обновляет агрегаты (максимальную метку времени, число ячеек CF, флаг удаления).
     *
     * @param table таблица для декодера и построения имён
     * @param cells все ячейки строки (все CF)
     * @param obj   результирующая карта для добавления полей
     * @param cfFamilies список целевых семейств колонок (каждый — в виде байтов)
     * @return агрегаты по CF
     */
    private CellStats decodeCells(TableName table, List<Cell> cells, Map<String, Object> obj, byte[][] cfFamilies) {
        CellStats s = new CellStats();
        if (cells == null || cells.isEmpty()) {
            return s;
        }

        final boolean serializeNulls = cfg.isJsonSerializeNulls();
        final QualCache qcache = new QualCache();

        for (Cell cell : cells) {
            processCell(table, cell, obj, s, cfFamilies, serializeNulls, qcache);
        }
        return s;
    }

    /**
     * Обрабатывает одну ячейку:
     *  - игнорирует, если CF не входит в список целевых;
     *  - увеличивает счётчики и обновляет maxTs;
     *  - для операций удаления выставляет флаг и не добавляет колонку;
     *  - для обычных значений вызывает декодер и кладёт результат в {@code obj}.
     *
     * @param table           таблица (контекст декодера)
     * @param cell            текущая ячейка
     * @param obj             результирующая карта
     * @param s               агрегаты по CF
     * @param cfFamilies      список целевых семейств колонок (каждый — в виде байтов)
     * @param serializeNulls  добавлять ли колонку с null‑значением
     * @param qcache          пер-строковый кэш последнего qualifier (ссылка/смещение/длина) для снижения аллокаций String
     */
    private void processCell(TableName table,
                            Cell cell,
                            Map<String, Object> obj,
                            CellStats s,
                            byte[][] cfFamilies,
                            boolean serializeNulls,
                            QualCache qcache) {
        if (!matchesAnyFamily(cell, cfFamilies)) {
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
            String q;
            // Быстрый путь: та же ссылка массива/смещение/длина — значит тот же qualifier (обычно другая версия той же колонки)
            if (qcache.s != null && qcache.a == qa && qcache.o == qo && qcache.l == ql) {
                q = qcache.s;
            } else {
                q = new String(qa, qo, ql, StandardCharsets.UTF_8);
                qcache.a = qa;
                qcache.o = qo;
                qcache.l = ql;
                qcache.s = q;
            }
            obj.put(q, decoded);
        }
    }

    /**
     * Оценить число ячеек целевых CF в списке cells (дешёво).
     * Для коротких строк (≤ {@link #PRESCAN_THRESHOLD}) считает точно.
     * Для длинных — сканирует первые N и экстраполирует на общий размер.
     */
    private static int estimateTargetCells(List<Cell> cells, byte[][] cfFamilies) {
        if (cells == null || cells.isEmpty() || cfFamilies == null || cfFamilies.length == 0) {
            return 0;
        }
        final int total = cells.size();
        final int sample = Math.min(total, PRESCAN_THRESHOLD);

        int matches = 0;
        for (int i = 0; i < sample; i++) {
            if (matchesAnyFamily(cells.get(i), cfFamilies)) {
                matches++;
            }
        }

        if (sample == total) {
            return matches; // точное значение
        }
        // Экстраполируем долю совпадений на весь размер; защита от переполнений через long.
        int estimated = (int) ((long) matches * total / sample);
        if (estimated < 0) {
            return 0;
        }
        return Math.min(estimated, total);
    }

    /**
     * Проверяет, принадлежит ли ячейка одному из целевых семейства колонок.
     * Выполняется без дополнительной аллокации; прекращает поиск при первом совпадении.
     */
    private static boolean matchesAnyFamily(Cell cell, byte[][] families) {
        if (families == null || families.length == 0) return false;
        if (families.length == 1) {
            return CellUtil.matchingFamily(cell, families[0]);
        }
        for (byte[] f : families) {
            if (CellUtil.matchingFamily(cell, f)) return true;
        }
        return false;
    }

    /**
     * Добавляет базовые метаданные таблицы: _table, _namespace, _qualifier, _cf и _cells_total.
     */
    private void addMetaFields(Map<String, Object> obj, TableName table, int totalCells) {
          String ns = table.getNamespaceAsString();
          String qn = table.getQualifierAsString();
          obj.put(K_TABLE, table.getNameAsString());
          obj.put(K_NAMESPACE, ns);
          obj.put(K_QUALIFIER, qn);
          // CSV со списком целевых CF, см. H2kConfig
          obj.put(K_CF, cfg.getCfNamesCsv());
          obj.put(K_CELLS_TOTAL, totalCells);
    }

    /** Добавляет метаданные таблицы, если соответствующий флаг включён. */
    private void addMetaIfEnabled(boolean includeMeta, Map<String, Object> obj, TableName table, int totalCells) {
        if (includeMeta) {
            addMetaFields(obj, table, totalCells);
        }
    }

    /** Добавляет счётчик ячеек по целевому CF, только если включены метаданные. */
    private static void addCellsCfIfMeta(Map<String, Object> obj, boolean includeMeta, int cfCells) {
        if (includeMeta) {
            obj.put(K_CELLS_CF, cfCells);
        }
    }

    /** Устанавливает флаг удаления, если в партии встречалась операция удаления по строке. */
    private static void addDeleteFlagIfNeeded(Map<String, Object> obj, boolean hasDelete) {
        if (hasDelete) {
            obj.put(K_DELETE, true);
        }
    }

    /** Добавляет единое поле `_rowkey` (HEX или Base64 по конфигурации), если ключ присутствует и включён. */
    private static void addRowKeyIfPresent(boolean includeRowKeyPresent,
                                           Map<String, Object> obj,
                                           RowKeySlice rk,
                                           boolean base64,
                                           int saltBytes) {
        if (!includeRowKeyPresent || rk == null) {
            return;
        }
        // Безопасно «снимаем» соль Phoenix (если задана для таблицы): сдвигаем off/len, не создавая новых объектов.
        int len = rk.getLength();
        int salt = saltBytes > 0 ? Math.min(saltBytes, len) : 0;
        int off  = rk.getOffset() + salt;
        int effLen = len - salt;

        if (effLen <= 0) {
            // Нечего выводить — исходный ключ короче или равен длине соли; молча пропускаем, чтобы не шуметь.
            return;
        }
        if (base64) {
            obj.put(K_ROWKEY, encodeRowKeyBase64(rk.getArray(), off, effLen));
        } else {
            obj.put(K_ROWKEY, toHex(rk.getArray(), off, effLen));
        }
    }

    /** Добавляет метаданные WAL (_wal_seq, _wal_write_time), если включены и значения неотрицательны. */
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

    /**
     * Быстрая конвертация массива байт в hex‑строку без промежуточных объектов.
     * @param a   исходный массив
     * @param off смещение
     * @param len длина
     * @return строка из 2*len символов
     */
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

    // (encodeRowKeyBase64(RowKeySlice) method removed as unused)

    /**
     * Кодирует фрагмент массива байт в Base64 без промежуточных строк.
     * В JDK 8 нет API с offset/length, поэтому выполняются два шага:
     * 1) копируется ровно нужный срез в временный буфер;
     * 2) кодирование производится напрямую в предвычисленный выходной массив.
     *
     * @param a   исходный массив
     * @param off смещение начала фрагмента
     * @param len длина фрагмента
     * @return ASCII-строка Base64; пустая строка, если {@code len == 0}; {@code null}, если {@code a == null}
     */
    private static String encodeRowKeyBase64(byte[] a, int off, int len) {
        if (a == null) return null;
        if (len <= 0) return "";
        // Скопировать ровно нужный срез (JDK8 Base64 не принимает (offset,len))
        byte[] slice = new byte[len];
        System.arraycopy(a, off, slice, 0, len);
        // Предвычислить длину Base64: 4 * ceil(len/3) и закодировать напрямую в готовый буфер
        int outLen = ((len + 2) / 3) * 4;
        byte[] out = new byte[outLen];
        int n = BASE64.encode(slice, out); // n == outLen
        return new String(out, 0, n, StandardCharsets.US_ASCII);
    }

    /** Пер-строковый кэш последнего qualifier: снижает число аллокаций String при нескольких версиях колонки подряд. */
    private static final class QualCache {
        byte[] a;
        int o;
        int l;
        String s;
    }

    /** Агрегаты по CF. */
    private static final class CellStats {
        /** Максимальная метка времени среди обработанных ячеек целевого CF. */
        long maxTs;
        /** В партии встречалась операция удаления по строке. */
        boolean hasDelete;
        /** Сколько ячеек целевого CF было обработано. */
        int cfCells;
    }


}