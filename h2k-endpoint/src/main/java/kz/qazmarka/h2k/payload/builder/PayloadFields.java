package kz.qazmarka.h2k.payload.builder;

/**
 * Единая точка для ключей служебных полей JSON-payload.
 * Централизация помогает избежать «магических строк» и рассинхронизации между классами.
 */
public final class PayloadFields {
    private PayloadFields() {}

    // Метаданные таблицы
    public static final String TABLE          = "_table";
    public static final String NAMESPACE      = "_namespace";
    public static final String QUALIFIER      = "_qualifier";
    public static final String CF             = "_cf";
    public static final String CELLS_TOTAL    = "_cells_total";
    public static final String CELLS_CF       = "_cells_cf";

    // Флаги / признаки
    public static final String DELETE         = "_delete";

    // RowKey
    public static final String ROWKEY         = "_rowkey";

    // WAL
    public static final String WAL_SEQ        = "_wal_seq";
    public static final String WAL_WRITE_TIME = "_wal_write_time";

    // Событийное время (максимальная метка времени среди ячеек CF).
    public static final String EVENT_TS = "_event_ts";
}
