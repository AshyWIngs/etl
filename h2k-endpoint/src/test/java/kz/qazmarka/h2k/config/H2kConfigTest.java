package kz.qazmarka.h2k.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Набор юнит‑тестов для конфигурации {@link H2kConfig}.
 *
 * Что проверяем:
 * - парсинг карты соли Phoenix per‑table: {@code h2k.salt.map}. Поддерживаются форматы
 *   {@code NS:TABLE:BYTES}, {@code NS:TABLE=BYTES}, {@code TABLE:BYTES}, {@code TABLE=BYTES},
 *   а также одиночный {@code TABLE} со значением по умолчанию 1; значения клиппируются в диапазон 0..8;
 * - парсинг подсказок ёмкости JSON per‑table: {@code h2k.capacity.hint.*} с разрешением по полному
 *   имени ({@code NS:TABLE}) и только по квалифаеру ({@code TABLE});
 * - генерация имён Kafka‑топиков: плейсхолдеры ({@code ${namespace}}, {@code ${qualifier}}),
 *   санитайзинг недопустимых символов и жёсткое усечение по {@code h2k.topic.max.length};
 * - нормализация кодировки {@code rowkey} ({@code HEX} / {@code BASE64});
 * - разбор CSV‑списка колонко‑семейств: {@code h2k.cf.list} (пробелы, пустые элементы, порядок).
 *
 * Нефункциональные требования:
 * - используется только in‑memory {@link org.apache.hadoop.conf.Configuration}; без I/O;
 * - нет дополнительных фреймворков и рантайм‑зависимостей; всё выполняется быстро и без давления на GC.
 *
 * Как читать тесты:
 * - каждый метод оформлен в стиле GIVEN/WHEN/THEN и снабжён кратким описанием;
 * - негативные кейсы проверяют устойчивость к «грязному» вводу (нечисла, пустые токены и т.п.).
 */
class H2kConfigTest {

    /**
     * Хелпер для компактного создания {@link H2kConfig} в тестах.
     *
     * Зачем: изолировать тесты от внешних настроек Kafka; минимально необходимый
     * bootstrap указывается константой, остальное берётся из переданной in‑memory {@link Configuration}.
     *
     * @param cfg конфигурация Hadoop с ключами {@code h2k.*}, специфичными для теста
     * @return сконфигурированный {@link H2kConfig}
     */
    private static H2kConfig fromCfg(Configuration cfg) {
        return H2kConfig.from(cfg, "kafka1:9092");
    }

    /**
     * GIVEN: карта соли с разными вариантами записи токенов, включая отрицательные, нулевые,
     *        сверхлимитные и без числа.
     * WHEN:  парсим {@code h2k.salt.map}.
     * THEN:  значения нормализуются к диапазону 0..8; отсутствие числа трактуется как 1;
     *        поиск по {@link TableName} нечувствителен к регистру.
     */
    @Test
    @DisplayName("salt.map: поддержка ns:qualifier и TABLE:BYTES, клиппинг значений")
    void saltMap_parsing() {
        Configuration c = new Configuration(false);
        // BYTES: 1 по умолчанию, <0 → 0, >8 → 8
        c.set("h2k.salt.map", "DEFAULT:TBL:1,TBL2:0,DAILY_MOVES:-5,ANOTHER:99,QUALIFIER_ONLY");
        H2kConfig hc = fromCfg(c);

        assertEquals(1, hc.getSaltBytesFor(TableName.valueOf("DEFAULT", "TBL")));
        assertEquals(0, hc.getSaltBytesFor(TableName.valueOf("default", "tbl2"))); // нет регистра у ns в HBase, но оставим как есть
        assertEquals(0, hc.getSaltBytesFor(TableName.valueOf("default", "daily_moves")));
        assertEquals(8, hc.getSaltBytesFor(TableName.valueOf("default", "another")));
        assertEquals(1, hc.getSaltBytesFor(TableName.valueOf("default", "qualifier_only")));
    }

    /**
     * GIVEN: подсказки ёмкости заданы и для полного имени ({@code NS:TABLE}), и только для
     *        квалифаера ({@code TABLE}).
     * WHEN:  запрашиваем хинты для разных {@link TableName}.
     * THEN:  разрешение работает и по полному имени, и по квалифаеру; для отсутствующих ключей → 0.
     */
    @Test
    @DisplayName("capacity.hint.*: разрешение по полному имени и по квалифаеру")
    void capacityHints_resolve() {
        Configuration c = new Configuration(false);
        c.set("h2k.capacity.hint.DEFAULT:TBL", "36");
        c.set("h2k.capacity.hint.ONLYQUAL", "18");
        H2kConfig hc = fromCfg(c);

        assertEquals(36, hc.getCapacityHintFor(TableName.valueOf("DEFAULT", "TBL")));
        assertEquals(18, hc.getCapacityHintFor(TableName.valueOf("ANY", "ONLYQUAL")));
        assertEquals(0,  hc.getCapacityHintFor(TableName.valueOf("DEFAULT", "ABSENT")));
    }

    /**
     * GIVEN: шаблон топика со спецсимволами и маленький лимит длины.
     * WHEN:  генерируем имя топика.
     * THEN:  недопустимые символы заменяются на '_', строка усекётся до лимита,
     *        префикс соответствует ожидаемым плейсхолдерам.
     */
    @Test
    @DisplayName("topicFor: плейсхолдеры, sanitize и truncate по maxLength")
    void topicFor_sanitizeAndTruncate() {
        Configuration c = new Configuration(false);
        c.set("h2k.topic.pattern", "${namespace}.${qualifier}.raw?/bad");
        c.setInt("h2k.topic.max.length", 10); // намеренно маленький
        H2kConfig hc = fromCfg(c);

        String topic = hc.topicFor(TableName.valueOf("AGG", "INC_DOCS_ACT"));
        // После sanitize все недопустимые символы → '_', затем усечение до 10 символов
        assertEquals(10, topic.length());
        // Начало строки после подстановки и sanitize должно начинаться с "AGG.INC_"
        assertTrue(topic.startsWith("AGG.INC_") || topic.startsWith("AGG_INC_") || topic.startsWith("AGG.INC."));
    }

    /**
     * Проверка нормализации значения {@code h2k.rowkey.encoding}.
     * - {@code BASE64} (любой регистр/пробелы) приводит к {@link H2kConfig#isRowkeyBase64()} = true;
     * - {@code HEX} и незаданное значение приводят к false.
     */
    @Test
    @DisplayName("rowkey.encoding: нормализация значений (hex/base64)")
    void rowkeyEncoding_normalization() {
        Configuration c = new Configuration(false);
        c.set("h2k.rowkey.encoding", "BASE64");
        H2kConfig hc = fromCfg(c);
        assertTrue(hc.isRowkeyBase64());

        c.set("h2k.rowkey.encoding", " HeX ");
        hc = fromCfg(c);
        assertFalse(hc.isRowkeyBase64());

        c.unset("h2k.rowkey.encoding");
        hc = fromCfg(c); // дефолт HEX
        assertFalse(hc.isRowkeyBase64());
    }

    /**
     * GIVEN: CSV со смешанными пробелами и пустыми элементами.
     * WHEN:  парсим {@code h2k.cf.list}.
     * THEN:  пустые элементы отбрасываются, порядок сохраняется; байтовые имена CF выдаются для каждого.
     */
    @Test
    @DisplayName("cf.list: CSV с пропусками и пробелами")
    void cfList_csv() {
        Configuration c = new Configuration(false);
        c.set("h2k.cf.list", " d , ,b,0 ");
        H2kConfig hc = fromCfg(c);
        assertArrayEquals(new String[]{"d","b","0"}, hc.getCfNames());
        assertEquals("d,b,0", hc.getCfNamesCsv());
        assertEquals(3, hc.getCfFamiliesBytes().length);
    }

    /**
     * Пограничный кейс: минимальный лимит длины имени топика = 1.
     * Ожидаем: результат строго из одного допустимого символа после санитайза.
     */
    @Test
    @DisplayName("topic.max.length: минимальная граница (1 символ) и жёсткое усечение")
    void topicFor_minLength_oneChar() {
        Configuration c = new Configuration(false);
        c.set("h2k.topic.pattern", "${qualifier}");
        c.setInt("h2k.topic.max.length", 1); // минимально допустимый сценарий
        H2kConfig hc = fromCfg(c);

        String topic = hc.topicFor(TableName.valueOf("ANY", "ABC.DEF"));
        assertEquals(1, topic.length(), "Топик должен быть усечён до 1 символа");
        // любой допустимый символ — латиница/цифра/._-
        assertTrue(topic.matches("[a-zA-Z0-9._-]"), "Санитайзер обязан выдать валидный символ");
    }

    /**
     * Нормальный кейс: большой лимит длины.
     * Ожидаем: имя топика не усекётся, останется ≤ лимита и с валидным префиксом namespace.
     */
    @Test
    @DisplayName("topic.max.length: большой лимит — строка не должна усекаться (≤ лимита)")
    void topicFor_bigLimit_noTruncate() {
        Configuration c = new Configuration(false);
        c.set("h2k.topic.pattern", "${namespace}.${qualifier}.raw");
        c.setInt("h2k.topic.max.length", 255);
        H2kConfig hc = fromCfg(c);

        String topic = hc.topicFor(TableName.valueOf("AGG", "INC_DOCS_ACT"));
        assertTrue(topic.length() <= 255, "Длина не должна превышать лимит");
        assertTrue(topic.startsWith("AGG.") || topic.startsWith("AGG_"),
                "Ожидаем префикс с namespace после санитайза");
    }

    /**
     * Негативные значения подсказок ёмкости.
     * Ожидаем: нечисловые и отрицательные значения игнорируются, метод возвращает 0.
     */
    @Test
    @DisplayName("capacity.hint.*: нечисло/отрицательное — игнорируется (возврат 0)")
    void capacityHints_invalidValues() {
        Configuration c = new Configuration(false);
        c.set("h2k.capacity.hint.BADNUM", "abc");  // нечисло
        c.set("h2k.capacity.hint.NEG", "-5");      // отрицательное
        H2kConfig hc = fromCfg(c);

        assertEquals(0, hc.getCapacityHintFor(TableName.valueOf("DEFAULT", "BADNUM")));
        assertEquals(0, hc.getCapacityHintFor(TableName.valueOf("DEFAULT", "NEG")));
    }

    /**
     * Стойкость парсера карты соли к «мусорным» токенам.
     * Ожидаем: отсутствие падений; любые результаты в допустимом диапазоне 0..8,
     *           а для валидной пары — точное совпадение.
     */
    @Test
    @DisplayName("salt.map: «мусорные» токены не должны ронять парсер; значения в диапазоне 0..8")
    void saltMap_garbageTokensAreSafe() {
        Configuration c = new Configuration(false);
        // BAD: пропущенные части и нечисловые байты
        c.set("h2k.salt.map", "BADTOKEN,ONLYNS:,NS:QUAL:NaN,VALID:2");
        H2kConfig hc = fromCfg(c);

        int a = hc.getSaltBytesFor(TableName.valueOf("DEFAULT", "BADTOKEN"));
        int b = hc.getSaltBytesFor(TableName.valueOf("DEFAULT", "ONLYNS"));
        int d = hc.getSaltBytesFor(TableName.valueOf("NS", "QUAL"));
        int ok = hc.getSaltBytesFor(TableName.valueOf("DEFAULT", "VALID"));

        assertTrue(a >= 0 && a <= 8);
        assertTrue(b >= 0 && b <= 8);
        assertTrue(d >= 0 && d <= 8);
        assertEquals(2, ok);
    }

    /**
     * Дополнительная проверка CSV: пустые/пробельные элементы отбрасываются,
     * порядок элементов и представление CSV сохраняются.
     */
    @Test
    @DisplayName("cf.list: пустые/пробельные элементы отбрасываются; порядок сохраняется")
    void cfList_emptyItemsSkipped() {
        Configuration c = new Configuration(false);
        c.set("h2k.cf.list", " , d , , b ,,0, ");
        H2kConfig hc = fromCfg(c);

        assertArrayEquals(new String[]{"d","b","0"}, hc.getCfNames());
        assertEquals("d,b,0", hc.getCfNamesCsv());
    }

    /**
     * Неизвестное значение {@code h2k.rowkey.encoding}.
     * Ожидаем поведение по умолчанию: HEX (т.е. {@link H2kConfig#isRowkeyBase64()} = false).
     */
    @Test
    @DisplayName("rowkey.encoding: неизвестное значение трактуется как HEX (дефолт)")
    void rowkeyEncoding_unknownMeansHex() {
        Configuration c = new Configuration(false);
        c.set("h2k.rowkey.encoding", "unknown");
        H2kConfig hc = fromCfg(c);
        assertFalse(hc.isRowkeyBase64(), "Неизвестные значения должны приводить к HEX по умолчанию");
    }
}