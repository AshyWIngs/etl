package kz.qazmarka.h2k.endpoint.internal;

/**
 * Контейнер метаданных WAL: sequenceId и время записи. Используется при формировании payload.
 */
public final class WalMeta {
    public static final WalMeta EMPTY = new WalMeta(-1L, -1L);

    /** sequenceId записи WAL (или -1, если недоступен). */
    public final long seq;
    /** writeTime ключа WAL (или -1, если недоступен). */
    public final long writeTime;

    /** @param seq sequenceId, отрицательное значение означает «нет данных» */
    public WalMeta(long seq, long writeTime) {
        this.seq = seq;
        this.writeTime = writeTime;
    }
}
