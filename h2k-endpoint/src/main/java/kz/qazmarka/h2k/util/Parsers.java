package kz.qazmarka.h2k.util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;

/**
 * Набор утилит для чтения и нормализации конфигурации {@code h2k.*} и сопутствующих констант.
 * Вынесено из {@code H2kConfig} без изменения поведения.
 *
 * Класс не хранит состояния — все методы статические и thread‑safe при передаче неизменяемых аргументов.
 */
public final class Parsers {
    private Parsers() {}

    /**
     * Безопасно парсит целое из строки.
     *
     * @param s      исходная строка (может быть {@code null} или содержать пробелы)
     * @param defVal значение по умолчанию, если {@code s} пустая/некорректная
     * @return распарсенное целое либо {@code defVal} при ошибке
     */
    public static int parseIntSafe(String s, int defVal) {
        if (s == null) return defVal;
        try {
            return Integer.parseInt(s.trim());
        } catch (NumberFormatException ex) {
            return defVal;
        }
    }

    /**
     * Нормализует имя таблицы/ключа для поиска во внутренних картах.
     * Применяет {@code trim()} и переводит в {@link Locale#ROOT ROOT} верхний регистр.
     *
     * @param s исходная строка (может быть {@code null})
     * @return нормализованная строка; для {@code null} — пустая строка
     */
    public static String up(String s) {
        return (s == null) ? "" : s.trim().toUpperCase(Locale.ROOT);
    }

    /**
     * Считывает {@code int} из {@link Configuration} с дефолтом и нижней границей.
     *
     * @param cfg     конфигурация Hadoop
     * @param key     ключ
     * @param defVal  значение по умолчанию
     * @param minVal  минимально допустимое значение (включительно)
     * @return значение из конфигурации, но не меньше {@code minVal}
     */
    public static int readIntMin(Configuration cfg, String key, int defVal, int minVal) {
        int v = cfg.getInt(key, defVal);
        if (v < minVal) return minVal;
        return v;
    }

    /**
     * Считывает {@code long} из {@link Configuration} с мягкой деградацией.
     *
     * @param cfg    конфигурация Hadoop
     * @param key    ключ
     * @param defVal значение по умолчанию при {@code null}/некорректном вводе
     * @return валидное {@code long} значение либо {@code defVal}
     */
    public static long readLong(Configuration cfg, String key, long defVal) {
        String v = cfg.get(key);
        if (v == null) return defVal;
        try {
            return Long.parseLong(v.trim());
        } catch (NumberFormatException nfe) {
            return defVal;
        }
    }

    /**
     * Читает булево значение из {@link Configuration}, поддерживая распространённые алиасы.
     * Принимаются {@code true/false/1/0/yes/no/on/off} в любом регистре.
     *
     * @param cfg    конфигурация Hadoop
     * @param key    ключ
     * @param defVal значение по умолчанию, если ключ отсутствует или формат не распознан
     * @return boolean значение, либо {@code defVal}, если распознать не удалось
     */
    public static boolean readBoolean(Configuration cfg, String key, boolean defVal) {
        String v = cfg.getTrimmed(key);
        if (v == null) return defVal;
        String s = v.trim().toLowerCase(Locale.ROOT);
        if ("true".equals(s) || "1".equals(s) || "yes".equals(s) || "on".equals(s)) return true;
        if ("false".equals(s) || "0".equals(s) || "no".equals(s) || "off".equals(s)) return false;
        return defVal;
    }

    /**
     * Читает и нормализует шаблон имени топика: применяет {@code getTrimmed} и подставляет дефолт.
     *
     * @param cfg             конфигурация Hadoop
     * @param key             конфигурационный ключ
     * @param defaultPattern  шаблон по умолчанию, если значение пустое или отсутствует
     * @return непустой шаблон
     */
    public static String readTopicPattern(Configuration cfg, String key, String defaultPattern) {
        final String v = cfg.getTrimmed(key, defaultPattern);
        return (v == null || v.isEmpty()) ? defaultPattern : v;
    }

    /**
     * Читает список CF как CSV, обрезая пробелы и игнорируя пустые элементы.
     *
     * @param cfg       конфигурация Hadoop
     * @param key       ключ CSV
     * @param defaultCf имя CF по умолчанию, если список пуст
     * @return массив непустых имён CF; гарантирует хотя бы одно значение {@code defaultCf}
     */
    public static String[] readCfNames(Configuration cfg, String key, String defaultCf) {
        String raw = cfg.get(key, defaultCf);
        if (raw == null) return new String[]{ defaultCf };
        String[] parts = raw.split(",");
        ArrayList<String> out = new ArrayList<>(parts.length);
        for (String p : parts) {
            if (p == null) continue;
            String s = p.trim();
            if (!s.isEmpty()) out.add(s);
        }
        if (out.isEmpty()) out.add(defaultCf);
        return out.toArray(new String[0]);
    }

    /**
     * Быстро кодирует массив строк в UTF‑8 байтовые массивы.
     *
     * @param names массив строк
     * @return массив байтовых представлений в UTF‑8 той же длины
     */
    public static byte[][] toUtf8Bytes(String[] names) {
        byte[][] res = new byte[names.length][];
        for (int i = 0; i < names.length; i++) {
            res[i] = names[i].getBytes(StandardCharsets.UTF_8);
        }
        return res;
    }

    /**
     * Нормализует способ кодирования rowkey до двух допустимых значений: {@code "hex"} или {@code "base64"}.
     *
     * @param val исходное значение (может быть {@code null})
     * @return {@code "base64"}, если явно указано; иначе {@code "hex"}
     */
    public static String normalizeRowkeyEncoding(String val) {
        if (val == null) return "hex";
        String v = val.trim().toLowerCase(Locale.ROOT);
        return "base64".equals(v) ? "base64" : "hex";
    }

    /**
     * Параметры фильтрации WAL по минимальному timestamp.
     * Используется для выборки записей не старше заданного порога.
     */
    public static final class WalFilter {
        /** Признак включения фильтрации WAL. */
        public final boolean enabled;
        /** Минимально допустимый timestamp (включительно), {@link Long#MIN_VALUE} если не задан. */
        public final long minTs;
        /**
         * @param enabled включён ли фильтр
         * @param minTs   минимальный timestamp; при выключенном фильтре игнорируется
         */
        public WalFilter(boolean enabled, long minTs) { this.enabled = enabled; this.minTs = minTs; }
    }

    /**
     * Читает параметры фильтра WAL из конфигурации.
     *
     * @param cfg конфигурация Hadoop
     * @param key ключ, содержащий минимальный timestamp
     * @return {@link WalFilter} с флагом и значением минимального timestamp; при ошибке парсинга — выключенный фильтр
     */
    public static WalFilter readWalFilter(Configuration cfg, String key) {
        String walMinStr = cfg.get(key);
        if (walMinStr == null) return new WalFilter(false, Long.MIN_VALUE);
        try {
            return new WalFilter(true, Long.parseLong(walMinStr.trim()));
        } catch (NumberFormatException nfe) {
            return new WalFilter(false, Long.MIN_VALUE);
        }
    }

    /**
     * Извлекает topic‑level конфиги по заданному префиксу.
     * Пример: {@code h2k.topic.config.X=Y} → {@code {"X" → "Y"}}.
     *
     * @param cfg    конфигурация Hadoop
     * @param prefix строковый префикс ключей (например, {@code "h2k.topic.config."})
     * @return новая неизложная {@link Map} с нормализованными ключами без префикса
     */
    public static Map<String, String> readTopicConfigs(Configuration cfg, String prefix) {
        Map<String, String> out = new HashMap<>();
        for (Map.Entry<String, String> e : cfg) {
            String k = e.getKey();
            if (k != null && k.startsWith(prefix)) {
                String real = k.substring(prefix.length()).trim();
                if (!real.isEmpty()) {
                    String v = cfg.getTrimmed(k);
                    if (v != null && !v.isEmpty()) {
                        out.put(real, v);
                    }
                }
            }
        }
        return out;
    }

    /**
     * Читает число партиций топика с дефолтом и нижней границей.
     *
     * @param cfg    конфигурация Hadoop
     * @param key    ключ
     * @param defVal значение по умолчанию
     * @param minVal минимально допустимое значение
     * @return количество партиций не ниже {@code minVal}
     */
    public static int readTopicPartitions(Configuration cfg, String key, int defVal, int minVal) {
        return readIntMin(cfg, key, defVal, minVal);
    }

    /**
     * Читает replication factor как {@code short} с дефолтом и нижней границей.
     *
     * @param cfg    конфигурация Hadoop
     * @param key    ключ
     * @param defVal значение по умолчанию
     * @param minVal минимально допустимое значение (включительно)
     * @return валидный replication factor в диапазоне {@code [minVal, Short.MAX_VALUE]}
     */
    public static short readTopicReplication(Configuration cfg, String key, short defVal, short minVal) {
        String raw = cfg.getTrimmed(key);
        if (raw == null || raw.isEmpty()) return defVal;
        try {
            int parsed = Integer.parseInt(raw);
            if (parsed < minVal) return minVal;
            if (parsed > Short.MAX_VALUE) return Short.MAX_VALUE;
            return (short) parsed;
        } catch (NumberFormatException ignore) {
            return defVal;
        }
    }

    /**
     * Читает верхний лимит длины (например, имени/топика) с дефолтом и нижней границей.
     *
     * @param cfg    конфигурация Hadoop
     * @param key    ключ
     * @param defVal значение по умолчанию
     * @param minVal минимально допустимое значение
     * @return валидное значение не ниже {@code minVal}
     */
    public static long readTopicMaxLength(Configuration cfg, String key, long defVal, long minVal) {
        long v = readLong(cfg, key, defVal);
        if (v < minVal) return minVal;
        return v;
    }

    /**
     * Читает backoff (мс) для неизвестных ошибок с ограничениями диапазона.
     *
     * @param cfg    конфигурация Hadoop
     * @param key    ключ
     * @param defVal значение по умолчанию
     * @param minVal нижняя граница
     * @param maxVal верхняя граница
     * @return значение в диапазоне {@code [minVal, maxVal]}
     */
    public static long readUnknownBackoffMs(Configuration cfg, String key, long defVal, long minVal, long maxVal) {
        long v = readLong(cfg, key, defVal);
        if (v < minVal) return minVal;
        if (v > maxVal) return maxVal;
        return v;
    }

    /**
     * Парсит CSV‑карту солей в виде {@code TABLE[=BYTES]} или {@code NS:TABLE[:|=]BYTES}.
     * Пустые элементы игнорируются, значения клиппятся в диапазон {@code [0..8]}.
     *
     * @param cfg конфигурация Hadoop
     * @param key ключ CSV
     * @return карта { UPPERCASE имя таблицы → число байт соли }
     */
    public static Map<String, Integer> readSaltMap(Configuration cfg, String key) {
        String raw = cfg.get(key);
        if (raw == null || raw.trim().isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, Integer> out = new HashMap<>();
        String[] parts = raw.split(",");
        for (String part : parts) {
            if (part == null) continue;
            String s = part.trim();
            if (!s.isEmpty()) addSaltEntry(out, s);
        }
        return out;
    }

    /**
     * Разбирает один CSV‑токен карты соли и, при корректности, добавляет запись в {@code out}.
     * Форматы: {@code TABLE}, {@code TABLE=BYTES}, {@code NS:TABLE=BYTES}, {@code NS:TABLE:BYTES}.
     *
     * @param out   целевая карта
     * @param token токен CSV
     */
    private static void addSaltEntry(Map<String, Integer> out, String token) {
        int eq = token.lastIndexOf('=');
        int sep = (eq >= 0) ? eq : token.lastIndexOf(':'); // поддержка NS:TABLE=BYTES и NS:TABLE:BYTES
        String name = (sep > 0) ? token.substring(0, sep).trim() : token.trim();
        if (name.isEmpty()) return;

        // Нет явного значения байт — по умолчанию TABLE -> 1
        if (sep <= 0) { out.put(up(name), 1); return; }

        String num = token.substring(sep + 1).trim();
        if (num.isEmpty()) { out.put(up(name), 1); return; }

        int bytes = parseIntSafe(num, 1);
        if (bytes < 0) bytes = 0;
        else if (bytes > 8) bytes = 8;

        out.put(up(name), bytes);
    }

    /**
     * Собирает подсказки ёмкости (число ключей на таблицу) из CSV и индивидуальных ключей.
     *
     * @param cfg          конфигурация Hadoop
     * @param csvKey       ключ CSV (например, {@code h2k.capacity.hints})
     * @param singlePrefix префикс индивидуальных ключей (например, {@code h2k.capacity.hint.})
     * @return карта { UPPERCASE имя таблицы → положительное число ключей }
     */
    public static Map<String, Integer> readCapacityHints(Configuration cfg, String csvKey, String singlePrefix) {
        Map<String, Integer> out = new HashMap<>();

        // 1) CSV-вариант: h2k.capacity.hints=TABLE=keys[,NS:TABLE=keys2,...]
        String raw = cfg.get(csvKey);
        if (raw != null && !raw.trim().isEmpty()) {
            for (String t : raw.split(",")) {
                if (t != null) {
                    String s = t.trim();
                    if (!s.isEmpty()) {
                        addCapacityHintCsvToken(out, s);
                    }
                }
            }
        }
        // 2) Индивидуальные ключи с префиксом h2k.capacity.hint.TABLE = int
        for (Map.Entry<String, String> e : cfg) {
            addCapacityHint(out, e.getKey(), e.getValue(), singlePrefix);
        }
        return out;
    }

    /**
     * Добавляет подсказку ёмкости из индивидуального ключа, если он начинается с заданного префикса.
     *
     * @param out    целевая карта
     * @param key    полный ключ (например, {@code h2k.capacity.hint.TABLE})
     * @param val    строковое значение (может быть {@code null})
     * @param prefix ожидаемый префикс
     */
    private static void addCapacityHint(Map<String, Integer> out, String key, String val, String prefix) {
        if (key == null || !key.startsWith(prefix)) return;
        String table = key.substring(prefix.length()).trim();
        if (table.isEmpty()) return;
        if (val == null) return;
        int parsed = parseIntSafe(val, -1);
        if (parsed <= 0) return;
        out.put(up(table), parsed);
    }

    /**
     * Разбирает один CSV‑токен подсказки ёмкости формата {@code TABLE=keys} или {@code NS:TABLE=keys}.
     *
     * @param out   целевая карта
     * @param token токен CSV
     */
    private static void addCapacityHintCsvToken(Map<String, Integer> out, String token) {
        int eq = token.lastIndexOf('=');
        if (eq <= 0 || eq >= token.length() - 1) {
            return; // нет пары key=value
        }
        String name = token.substring(0, eq).trim();
        String val = token.substring(eq + 1).trim();
        if (name.isEmpty() || val.isEmpty()) return;
        int parsed = parseIntSafe(val, -1);
        if (parsed > 0) {
            out.put(up(name), parsed);
        }
    }

    /**
     * Формирует {@code client.id} для {@code AdminClient}.
     * Если значение явно задано — возвращает его; иначе пытается использовать имя хоста.
     *
     * @param cfg         конфигурация Hadoop
     * @param explicitKey ключ явного значения client.id
     * @param defaultId   базовый префикс на случай отсутствия явного значения/ошибки определения хоста
     * @return итоговый {@code client.id}
     */
    public static String buildAdminClientId(Configuration cfg, String explicitKey, String defaultId) {
        String adminClientId = cfg.getTrimmed(explicitKey);
        if (adminClientId != null && !adminClientId.isEmpty()) {
            return adminClientId;
        }
        try {
            return defaultId + "-" + InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return defaultId;
        }
    }

    /** Регулярное выражение для замены недопустимых символов Kafka на подчёркивание. */
    private static final Pattern TOPIC_SANITIZE = Pattern.compile("[^a-zA-Z0-9._-]");

    /**
     * Удаляет ведущие разделители {@code '.', '_', '-'} для корректной склейки плейсхолдеров.
     * Предполагается, что {@code s != null}.
     *
     * @param s исходная строка
     * @return строка без ведущих разделителей
     */
    public static String topicStripLeadingDelimiters(String s) {
        int i = 0;
        final int len = s.length();
        while (i < len) {
            char c = s.charAt(i);
            if (c == '.' || c == '_' || c == '-') {
                i++;
            } else {
                break;
            }
        }
        return (i > 0) ? s.substring(i) : s;
    }

    /**
     * Схлопывает повторы разделителей {@code '.', '_', '-'} до одного символа подряд.
     * Предполагается, что {@code s != null}.
     *
     * @param s исходная строка
     * @return строка с нормализованными разделителями
     */
    public static String topicCollapseRepeatedDelimiters(String s) {
        StringBuilder sb = new StringBuilder(s.length());
        char prev = 0;
        for (int j = 0; j < s.length(); j++) {
            char c = s.charAt(j);
            boolean isDelim = (c == '.' || c == '_' || c == '-');
            if (!(isDelim && c == prev)) {
                sb.append(c);
                prev = c;
            }
        }
        return sb.toString();
    }

    /**
     * Заменяет недопустимые для Kafka символы на подчёркивание.
     * Предполагается, что {@code s != null}.
     *
     * @param s исходная строка
     * @return строка с заменёнными недопустимыми символами
     */
    public static String topicSanitizeKafkaChars(String s) {
        return TOPIC_SANITIZE.matcher(s).replaceAll("_");
    }

    /**
     * Выполняет полную нормализацию произвольной строки до допустимого имени Kafka‑топика.
     * Шаги: {@code trim} → замена недопустимых символов → удаление ведущих разделителей →
     * схлопывание повторов разделителей.
     *
     * @param raw исходная строка (может быть {@code null})
     * @return нормализованное имя топика; для {@code null}/пустой строки — пустая строка
     */
    public static String sanitizeTopic(String raw) {
        if (raw == null) return "";
        String s = raw.trim();
        if (s.isEmpty()) return "";
        s = topicSanitizeKafkaChars(s);
        s = topicStripLeadingDelimiters(s);
        s = topicCollapseRepeatedDelimiters(s);
        return s;
    }
}