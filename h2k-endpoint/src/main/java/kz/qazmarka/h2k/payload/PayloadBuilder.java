package kz.qazmarka.h2k.payload;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;

import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.schema.Decoder;
import kz.qazmarka.h2k.util.RowKeySlice;

/**
 * Сборщик JSON‑payload для отправки в Kafka в формате JSONEachRow.
 *
 * Назначение
 *  - На основе данных WAL формирует одну JSON-строку на одну строку HBase: ключи — квалификаторы колонок.
 *  - обрабатывает все ячейки строки; фильтрация по целевому CF предполагается на уровне источника/сканера.
 *
 * PK из rowkey
 *  - Перед сериализацией выполняется декодирование rowkey: {@link Decoder#decodeRowKey}
 *    обогащает карту значений расшифрованными компонентами PK (например, "c","t","opd"), чтобы PK были видны
 *    в Value как обычные колонки.
 *
 * Сериализация и метаданные
 *  - Нулевые значения колонок не сериализуются, если не включено cfg.isJsonSerializeNulls().
 *  - Колонки, не описанные в схеме, не отбрасываются: если декодер вернул {@code null}, но значение непустое —
 *    сохраняем как UTF‑8 строку.
 *  - Метаполя добавляются по флагам конфигурации (см. README и {@link H2kConfig}): h2k.include.meta.*,
 *    h2k.include.rowkey и др.
 *
 * Производительность и потокобезопасность
 *  - Без побочных эффектов; удобно тестировать.
 *  - Используется {@link LinkedHashMap} с заранее рассчитанной ёмкостью, порядок ключей стабилен.
 *  - Строка qualifier создаётся один раз на ячейку напрямую из буфера без промежуточного копирования.
 *  - Декодер вызывается для всех ячеек строки (фильтрация CF — на уровне источника/сканера).
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
    private static final Base64.Encoder BASE64 = Base64.getEncoder();
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
        this.decoder = Objects.requireNonNull(decoder, "декодер");
        this.cfg = Objects.requireNonNull(cfg, "конфигурация h2k");
    }

    /**
     * Создаёт корневую LinkedHashMap с ёмкостью, подобранной по «подсказке» из H2kConfig (если задана)
     * и/или оценке фактического числа ключей (данные + метаданные). Расчёт ёмкости производится целочисленной формулой,
     * эквивалентной 1 + ceil(target / 0.75), без использования double/Math.ceil.
     *
     * Подсказка берётся из {@code cfg.getCapacityHintFor(table)} (0 — подсказка отсутствует).
     * Итоговая ёмкость ограничивается внутренним максимумом HashMap/LinkedHashMap (~2^30).
     *
     * @param table              таблица HBase, для которой строится payload
     * @param estimatedKeysCount оценка общего числа ключей в payload (данные колонок + служебные поля)
     * @return новая упорядоченная мапа под корневой JSON‑объект
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
     *
     * @param estimatedKeysCount оценка общего числа ключей (данные + метаданные)
     * @param hint               «подсказка» из конфигурации (0 — нет подсказки)
     * @return рекомендуемая ёмкость для конструктора LinkedHashMap (с учётом верхнего ограничения ~2^30)
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
     * Ёмкость результирующей мапы рассчитывается заранее из числа ячеек и флагов включения метаданных/rowkey.
     *
     * @param table        таблица HBase (для ключей _table/_namespace/_qualifier и для декодера)
     * @param cells        список ячеек этой строки (обычно только целевой CF; фильтрация выполняется на источнике)
     * @param rowKey       срез rowkey (может быть {@code null}, если источник не предоставляет ключ)
     * @param walSeq       sequenceId записи WAL (или {@code < 0}, если недоступен)
     * @param walWriteTime время записи в WAL в мс (или {@code < 0}, если недоступно)
     * @return упорядоченная карта с данными колонок и служебными полями
     * @throws NullPointerException если {@code table} равен {@code null}
     */
    public Map<String, Object> buildRowPayload(TableName table,
                                               List<Cell> cells,
                                               RowKeySlice rowKey,
                                               long walSeq,
                                               long walWriteTime) {
        Objects.requireNonNull(table, "таблица");
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
                + (includeMeta ? 5 : 0)         /*_table,_namespace,_qualifier,_cf,_cells_total*/
                + (includeRowKeyPresent ? 1 : 0)/*_rowkey*/
                + (includeWalMeta ? 2 : 0);     /*_wal_seq,_wal_write_time*/

        final Map<String, Object> obj = newRootMap(table, cap);

        // Метаданные таблицы (если включены)
        addMetaIfEnabled(includeMeta, obj, table, cellsCount);

        // PK как отдельные поля значения (c/t/opd и т.п.) — извлекаем из rowkey по схеме Phoenix.
        // Включаем всегда (это бизнес-данные), если rowkey доступен и схема знает состав PK.
        decodePkFromRowKey(table, rowKey, obj);

        // Расшифровка ячеек: добавляет поля CF в obj и возвращает агрегаты
        CellStats stats = decodeCells(table, cells, obj);

        // Итоговые служебные поля
        addCellsCfIfMeta(obj, includeMeta, stats.cfCells);
        // event_version пишем только если были обработанные ячейки целевого CF
        if (stats.cfCells > 0) {
            obj.put(K_EVENT_VERSION, stats.maxTs);
        }
        addDeleteFlagIfNeeded(obj, stats.hasDelete);

        // RowKey (если включён и присутствует). При необходимости снимаем префикс соли Phoenix.
        final int saltBytes = cfg.getSaltBytesFor(table);
        addRowKeyIfPresent(includeRowKeyPresent, obj, rowKey, rowkeyB64, saltBytes);

        // Метаданные WAL (если включены)
        addWalMeta(includeWalMeta, obj, walSeq, walWriteTime);

        return obj;
    }

    /**
     * Декодирует составной PK из {@link RowKeySlice} в именованные поля значения.
     * Реализация декодирования и знание порядка/типов колонок PK — на стороне {@link Decoder}.
     * Если rowkey отсутствует/пустой или таблица не описана в схеме — метод ничего не делает.
     */
    private void decodePkFromRowKey(TableName table, RowKeySlice rk, Map<String, Object> out) {
        if (rk == null) {
            return;
        }
        // Если у таблицы есть соль Phoenix — декодер обязан корректно её учесть (сдвиг/обрезка).
        final int saltBytes = cfg.getSaltBytesFor(table);
        // Decoder API: decodeRowKey(table, rowKeySlice, saltBytes, outValues)
        // Контракт декодера: безопасно-небросающий; при отсутствии описания PK — no-op.
        decoder.decodeRowKey(table, rk, saltBytes, out);
    }

    /**
     * Декодирует клетки целевых CF и заполняет {@code obj} декодированными значениями колонок.
     * Обновляет агрегаты (максимальную метку времени, число ячеек CF, флаг удаления).
     *
     * Имплементационная заметка: обработка делегируется в {@link #processCell(TableName, Cell, Map, CellStats, boolean)},
     * где используется «быстрая» перегрузка декодера без копирования value; {@code CellUtil.cloneValue} не вызывается.
     *
     * @param table таблица для декодера и построения имён
     * @param cells все ячейки строки (все CF)
     * @param obj   результирующая карта для добавления полей
     * @return агрегаты по строке
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
     * Обрабатывает одну ячейку:
     *  - обрабатывает все ячейки (фильтрация CF выполняется на уровне источника/сканера);
     *  - увеличивает счётчики и обновляет {@code maxTs};
     *  - для операций удаления выставляет флаг и не добавляет колонку;
     *  - для обычных значений вызывает декодер и кладёт результат в {@code obj};
     *  - если декодер вернул {@code null}, но значение непустое — сохраняем как UTF‑8 строку.
     *
     * Имплементационная заметка: используется «быстрая» перегрузка декодера
     * {@link Decoder#decode(TableName, byte[], int, int, byte[], int, int)} (без копирования value).
     * Преобразование в строку выполняется только в фоллбэке, когда декодер вернул {@code null}.
     *
     * @param table           таблица (контекст декодера)
     * @param cell            текущая ячейка
     * @param obj             результирующая карта
     * @param s               агрегаты по CF
     * @param serializeNulls  добавлять ли колонку с null‑значением
     */
    private void processCell(TableName table,
                            Cell cell,
                            Map<String, Object> obj,
                            CellStats s,
                            boolean serializeNulls) {
        // Считаем все ячейки релевантными (фильтрация CF — на уровне источника/сканера)
        s.cfCells++;
        long ts = cell.getTimestamp();
        if (ts > s.maxTs) s.maxTs = ts;
        if (CellUtil.isDelete(cell)) {
            s.hasDelete = true;
            return;
        }

        final byte[] qa = cell.getQualifierArray();
        final int    qo = cell.getQualifierOffset();
        final int    ql = cell.getQualifierLength();

        final String q = new String(qa, qo, ql, StandardCharsets.UTF_8).toLowerCase(Locale.ROOT);

        final byte[] va = cell.getValueArray();
        final int    vo = cell.getValueOffset();
        final int    vl = cell.getValueLength();

        // Попытка декодировать известный тип: быстрая перегрузка без копирования value
        Object decoded = decoder.decode(table, qa, qo, ql, va, vo, vl);
        if (decoded != null) {
            obj.put(q, decoded);
            return;
        }

        // Фоллбэк: если байты непустые, кладём как UTF-8 строку (для пользовательских/неописанных колонок)
        if (va != null && vl > 0) {
            obj.put(q, new String(va, vo, vl, StandardCharsets.UTF_8));
            return;
        }

        // Иначе добавляем null только если это разрешено конфигурацией
        if (serializeNulls && (!obj.containsKey(q) || obj.get(q) == null)) {
            obj.put(q, null);
        }
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

    /**
     * Добавляет единое поле {@code _rowkey} (HEX или Base64 по конфигурации), если ключ присутствует и включён.
     * Учитывает соль Phoenix: первые {@code saltBytes} байт ключа пропускаются без аллокаций (смещение/длина).
     *
     * @param includeRowKeyPresent флаг «rowkey включён и присутствует»
     * @param obj                  результирующая карта
     * @param rk                   срез исходного rowkey
     * @param base64               {@code true} — кодировать Base64; {@code false} — как HEX
     * @param saltBytes            число байт соли Phoenix в начале ключа (0 — соли нет)
     */
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
     *
     * @param a   исходный массив (может быть {@code null})
     * @param off смещение
     * @param len длина
     * @return строка из {@code 2*len} символов; пустая строка, если массив {@code null} или {@code len == 0}
     * @throws IndexOutOfBoundsException если запрошен срез вне границ исходного массива (сообщение на русском)
     */
    private static String toHex(byte[] a, int off, int len) {
        if (a == null || len == 0) {
            return "";
        }
        if (off < 0 || len < 0 || off > a.length - len) {
            throw new IndexOutOfBoundsException("срез hex вне границ: off=" + off + " len=" + len + " cap=" + a.length);
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


    /**
     * Кодирует фрагмент массива байт в Base64 без промежуточных строк.
     * В JDK 8 нет API с (offset,len), поэтому выполняются два шага:
     * 1) копируется ровно нужный срез во временный буфер;
     * 2) кодирование производится напрямую в предвычисленный выходной массив.
     *
     * @param a   исходный массив
     * @param off смещение начала фрагмента
     * @param len длина фрагмента
     * @return ASCII‑строка Base64; пустая строка, если {@code len == 0}; {@code null}, если {@code a == null}
     */
    private static String encodeRowKeyBase64(byte[] a, int off, int len) {
        if (a == null) return null;
        if (len <= 0) return "";
        // Скопировать ровно нужный срез (JDK8 Base64 не принимает (offset,len))
        final byte[] slice = new byte[len];
        System.arraycopy(a, off, slice, 0, len);
        // Предвычислить длину Base64: 4 * ceil(len/3) и закодировать напрямую в готовый буфер
        int outLen = ((len + 2) / 3) * 4;
        byte[] out = new byte[outLen];
        int n = BASE64.encode(slice, out); // n == outLen
        return new String(out, 0, n, StandardCharsets.US_ASCII);
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