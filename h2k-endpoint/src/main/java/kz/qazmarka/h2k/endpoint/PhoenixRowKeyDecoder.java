package kz.qazmarka.h2k.endpoint;

import java.nio.charset.StandardCharsets;

/**
 * Утилита для декодирования составного PK из бинарного rowkey Phoenix.
 * PK: ("c" VARCHAR, "t" UNSIGNED_TINYINT, "opd" TIMESTAMP)
 * TIMESTAMP в rowkey: 8 байт millis (BE) + 4 байта nanos (BE).
 * ВАЖНО: поддерживаются только ASC-колонки (без инверсии байтов для DESC).
 *
 * Если rk == null или пустой — возвращаются пустые значения: c="", t=0, opdMs=0.
 * Если salted=true, то saltBytes нормализуется в диапазон [0..rk.length].
 */
public final class PhoenixRowKeyDecoder {

    /** Контейнер с распарсенным PK. Иммутабелен. */
    public static final class Pk {
        /** VARCHAR c (уже распакованный из Phoenix-экодинга 0x00/0xFF), UTF-8. */
        public final String c;
        /** UNSIGNED_TINYINT t (0..255). */
        public final int t;
        /** Временная метка opd в миллисекундах (millis + nanos/1e6). */
        public final long opdMs;

        public Pk(String c, int t, long opdMs) {
            this.c = c;
            this.t = t;
            this.opdMs = opdMs;
        }
    }

    private PhoenixRowKeyDecoder() {}

    /** Результат сканирования VARCHAR-сегмента. */
    private static final class ScanResult {
        /** Индекс конца сегмента (позиция терминатора или len, если терминатор не найден). */
        final int end;
        /** Количество escape-пар 0x00 0xFF внутри сегмента. */
        final int escapePairs;
        /** Был ли найден терминатор (неэкранированный 0x00). */
        final boolean termFound;

        ScanResult(int end, int escapePairs, boolean termFound) {
            this.end = end;
            this.escapePairs = escapePairs;
            this.termFound = termFound;
        }
    }

    /**
     * Декодирует PK из всего массива rowkey.
     * Обёртка над перегрузкой со срезом: эквивалентна decodePk(rk, 0, rk.length, ...).
     */
    public static Pk decodePk(byte[] rk, boolean salted, int saltBytes) {
        return decodePk(rk, 0, rk == null ? 0 : rk.length, salted, saltBytes);
    }

    /**
     * Декодирует PK из среза rowkey: [off, off+len).
     * Важно: если salted=true, то {@code saltBytes} отсчитывается от начала среза (off),
     * а не от начала всего массива.
     * Без исключений на горячем пути: при нехватке байт возвращаются нули для t/ms/nanos.
     *
     * @param rk        исходный массив байт rowkey (может быть null)
     * @param off       смещение начала среза в массиве
     * @param len       длина среза (количество байт)
     * @param salted    наличие SALT в начале rowkey (в пределах среза)
     * @param saltBytes длина SALT в байтах (относительно off)
     * @return распакованный PK (строка c, t 0..255, opd в миллисекундах)
     */
    public static Pk decodePk(byte[] rk, int off, int len, boolean salted, int saltBytes) {
        if (rk == null || len <= 0) {
            return new Pk("", 0, 0L);
        }
        // Нормализуем границы среза и вычисляем индексы
        if (off < 0) off = 0;
        if (len < 0) len = 0;
        int end = off + len;
        if (end > rk.length) end = rk.length;
        if (off > end) off = end;

        // Смещение с учётом SALT в пределах среза
        int saltedSkip = salted ? Math.max(0, Math.min(saltBytes, end - off)) : 0;
        int start = off + saltedSkip;

        // c: VARCHAR с терминатором 0x00 и экранированием 0x00 0xFF
        ScanResult sr = scanVarchar(rk, start, end);
        final String c = decodeVarcharSegment(rk, start, sr.end, sr.escapePairs);
        int pos = sr.termFound ? (sr.end + 1) : sr.end; // позиция после строки (или конец, если терминатора нет)

        // t: UNSIGNED_TINYINT
        final int t = (pos < end) ? (rk[pos] & 0xFF) : 0;
        pos += 1;

        // opd: TIMESTAMP = 8 байт millis (BE) + 4 байта nanos (BE)
        final long ms = (pos + 8 <= end) ? be64(rk, pos) : 0L;
        pos += 8;
        final int nanos = (pos + 4 <= end) ? be32(rk, pos) : 0;

        final long opdMs = ms + (nanos / 1_000_000L);
        return new Pk(c, t, opdMs);
    }

    /**
     * Сканирует сегмент VARCHAR в rowkey Phoenix.
     * Идём от off до len, учитывая экранирование 0x00 0xFF (означающее «литеральный 0x00 внутри строки»).
     * Возвращаем позицию конца сегмента (терминатор или len), количество escape-пар и факт наличия терминатора.
     */
    private static ScanResult scanVarchar(byte[] rk, int off, int len) {
        int i = off;
        int escapePairs = 0;

        while (i < len) {
            byte b = rk[i];
            if (b == 0) {
                // 0x00 0xFF — экранированный нулевой байт
                if (i + 1 < len && rk[i + 1] == (byte) 0xFF) {
                    escapePairs++;
                    i += 2;
                    continue;
                }
                // неэкранированный 0x00 — это терминатор строки
                return new ScanResult(i, escapePairs, true);
            }
            i++;
        }
        // Терминатор не найден — считаем строку до конца массива
        return new ScanResult(len, escapePairs, false);
    }

    /**
     * Декодирует диапазон [off, end) с учётом escape-пар 0x00 0xFF.
     * Без экранирования создаёт String напрямую, без копии данных.
     */
    private static String decodeVarcharSegment(byte[] rk, int off, int end, int escapePairs) {
        int encodedLen = end - off;
        if (encodedLen <= 0) {
            return "";
        }
        if (escapePairs == 0) {
            // Быстрый путь: данные лежат сплошным блоком
            return new String(rk, off, encodedLen, StandardCharsets.UTF_8);
        }
        // Было экранирование — делаем единственную аллокацию точного размера и распаковываем.
        int decodedLen = encodedLen - escapePairs;
        byte[] out = new byte[decodedLen];
        int readIdx = off;     // Sonar S1659: объявляем переменные на отдельных строках
        int writeIdx = 0;

        while (readIdx < end) {
            byte bb = rk[readIdx];
            if (bb == 0 && readIdx + 1 < end && rk[readIdx + 1] == (byte) 0xFF) {
                out[writeIdx] = 0;
                writeIdx++;
                readIdx += 2;
            } else {
                out[writeIdx] = bb;
                writeIdx++;
                readIdx++;
            }
        }
        return new String(out, StandardCharsets.UTF_8);
    }

    /** Чтение 8-байтового BE long (millis since epoch). */
    private static long be64(byte[] b, int o) {
        return ((b[o]   & 0xFFL) << 56)
             | ((b[o+1] & 0xFFL) << 48)
             | ((b[o+2] & 0xFFL) << 40)
             | ((b[o+3] & 0xFFL) << 32)
             | ((b[o+4] & 0xFFL) << 24)
             | ((b[o+5] & 0xFFL) << 16)
             | ((b[o+6] & 0xFFL) << 8)
             |  (b[o+7] & 0xFFL);
    }

    /** Чтение 4-байтового BE int. */
    private static int be32(byte[] b, int o) {
        return ((b[o]   & 0xFF) << 24)
             | ((b[o+1] & 0xFF) << 16)
             | ((b[o+2] & 0xFF) << 8)
             |  (b[o+3] & 0xFF);
    }
}