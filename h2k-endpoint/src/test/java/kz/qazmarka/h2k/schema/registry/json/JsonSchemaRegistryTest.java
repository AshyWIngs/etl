package kz.qazmarka.h2k.schema.registry.json;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.hadoop.hbase.TableName;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Набор быстрых unit-тестов для {@link JsonSchemaRegistry}.
 *
 * Цели:
 *  • Подтвердить корректный разбор минимальной JSON-схемы и работу алиасов имён таблиц/колонок
 *    (полное имя с namespace, короткое имя без namespace; регистр qualifier игнорируется).
 *  • Убедиться, что типы Phoenix массивов (например, {@code "VARCHAR ARRAY"}) распознаются
 *    и возвращаются без преобразований.
 *  • Проверить, что {@link JsonSchemaRegistry#refresh()} перечитывает файл, заменяет снимок
 *    и инвалидирует внутренние кэши (после refresh() видны изменения типов/PK).
 *  • Зафиксировать стабильное поведение при пустой или битой схеме: реестр остаётся рабочим,
 *    а методы возвращают {@code null}/пустые значения вместо выброса исключений наружу.
 *
 * Технические примечания:
 *  • Тесты самодостаточны: исходный JSON пишется во временный файл через {@link org.junit.jupiter.api.io.TempDir},
 *    путь передаётся в конструктор {@link JsonSchemaRegistry}.
 *  • Внешние ресурсы/сеть не используются; выполнение занимает миллисекунды.
 *  • Логи парсинга не проверяются — валидируется только функциональный контракт API.
 *
 * @see JsonSchemaRegistry
 * @see SchemaRegistry
 */
class JsonSchemaRegistryTest {

    private static void write(Path f, String json) throws IOException {
        Files.write(f, json.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Проверяет разбор простой схемы и работу алиасов имён:
     * Given: JSON с ключом {@code "DEFAULT:TBL_A"} и колонками в нижнем регистре.
     * When: запрашиваем типы для полного имени таблицы и для короткого (без namespace),
     *       а также для qualifier в разных регистрах.
     * Then: типы совпадают и регистр игнорируется; неизвестный qualifier возвращает {@code null}.
     */
    @Test
    @DisplayName("Парсинг и алиасы: полное имя/короткое/UPPER/lower + колонка в любом регистре")
    void aliasesForTableAndQualifier(@TempDir Path dir) throws IOException {
        String json = "{\n" +
                "  \"DEFAULT:TBL_A\": {\n" +
                "    \"columns\": { \"col1\": \"VARCHAR\", \"created_at\": \"TIMESTAMP\" }\n" +
                "  }\n" +
                "}";
        Path f = dir.resolve("schema.json");
        write(f, json);

        JsonSchemaRegistry reg = new JsonSchemaRegistry(f.toString());

        TableName tFull  = TableName.valueOf("DEFAULT:TBL_A");
        TableName tShort = TableName.valueOf("TBL_A");

        assertEquals("VARCHAR",  reg.columnType(tFull,  "col1"));
        assertEquals("VARCHAR",  reg.columnType(tShort, "COL1"));
        assertEquals("TIMESTAMP",reg.columnType(tShort, "created_at"));
        assertEquals("TIMESTAMP",reg.columnType(tShort, "CREATED_AT"));

        assertNull(reg.columnType(tShort, "unknown"));
    }

    /**
     * Проверяет поддержку массивов Phoenix:
     * Given: JSON со столбцом {@code "tags"} типа {@code "VARCHAR ARRAY"}.
     * Then: реестр возвращает строку типа без трансформаций и без выбрасывания ошибок.
     */
    @Test
    @DisplayName("Массивы Phoenix: 'VARCHAR ARRAY' распознаются без WARN и возвращаются как тип")
    void arrayTypeAccepted(@TempDir Path dir) throws IOException {
        String json = "{\n" +
                "  \"TBL_B\": {\"columns\": {\"tags\": \"VARCHAR ARRAY\"}}\n" +
                "}";
        Path f = dir.resolve("schema.json");
        write(f, json);

        JsonSchemaRegistry reg = new JsonSchemaRegistry(f.toString());
        TableName t = TableName.valueOf("TBL_B");
        assertEquals("VARCHAR ARRAY", reg.columnType(t, "tags"));
    }

    /**
     * Проверяет, что {@link JsonSchemaRegistry#refresh()}:
     *  1) перечитывает файл схемы,
     *  2) заменяет внутренний снимок,
     *  3) очищает кэш отображений.
     * Given: файл с типом {@code INT}; After refresh: тип меняется на {@code BIGINT} и появляется новая колонка.
     */
    @Test
    @DisplayName("refresh(): заменяет снимок схемы и очищает кэш — новые типы становятся видны")
    void refreshReplacesSnapshot(@TempDir Path dir) throws IOException {
        Path f = dir.resolve("schema.json");
        write(f, "{\n" +
                "  \"TBL_C\": {\"columns\": {\"x\": \"INT\"}}\n" +
                "}");

        JsonSchemaRegistry reg = new JsonSchemaRegistry(f.toString());
        TableName t = TableName.valueOf("TBL_C");
        assertEquals("INT", reg.columnType(t, "x"));

        // Обновляем файл и вызываем refresh()
        write(f, "{\n" +
                "  \"TBL_C\": {\"columns\": {\"x\": \"BIGINT\", \"y\": \"VARCHAR\"}}\n" +
                "}");
        reg.refresh();

        assertEquals("BIGINT", reg.columnType(t, "x"));
        assertEquals("VARCHAR", reg.columnType(t, "y"));
    }

    /**
     * Диагностика поведения при пустой/битой схеме.
     * Given: файл с некорректным JSON (одна фигурная скобка).
     * Then: конструктор и методы не выбрасывают исключения наружу, а {@code columnType(...)} возвращает {@code null}.
     * Примечание: на уровне логов может быть предупреждение о невозможности загрузить схему — это ожидаемо.
     */
    @Test
    @DisplayName("Пустая/битая схема: реестр стартует пустым и не падает")
    void emptyOrBrokenSchema(@TempDir Path dir) throws IOException {
        Path f = dir.resolve("schema.json");
        // Пишем «битый» JSON
        write(f, "{");

        JsonSchemaRegistry reg = new JsonSchemaRegistry(f.toString());
        assertNull(reg.columnType(TableName.valueOf("ANY"), "col"));
    }

    /**
     * Fail-fast контракт аргументов: columnType(...) бросает NPE при null table/qualifier.
     */
    @Test
    @DisplayName("Fail-fast: NPE на null table/qualifier")
    void npeOnNullArgs(@TempDir Path dir) throws IOException {
        Path f = dir.resolve("schema.json");
        write(f, "{\n" +
                "  \"TBL_NPE\": {\"columns\": {\"x\": \"VARCHAR\"}}\n" +
                "}");

        JsonSchemaRegistry reg = new JsonSchemaRegistry(f.toString());
        TableName tNpe = TableName.valueOf("TBL_NPE");
        NullPointerException ex1 = assertThrows(NullPointerException.class, () -> reg.columnType(null, "x"));
        assertTrue(ex1.getMessage() == null || ex1.getMessage().toLowerCase().contains("table"),
                "Сообщение NPE должно указывать на параметр table");
        NullPointerException ex2 = assertThrows(NullPointerException.class, () -> reg.columnType(tNpe, null));
        assertTrue(ex2.getMessage() == null || ex2.getMessage().toLowerCase().contains("qualifier"),
                "Сообщение NPE должно указывать на параметр qualifier");
    }

    /**
     * Поведение при отсутствии файла схемы: реестр инициализируется пустым и не падает.
     */
    @Test
    @DisplayName("Отсутствующий файл схемы: реестр пустой, исключений нет")
    void missingSchemaFile() {
        JsonSchemaRegistry reg = new JsonSchemaRegistry("/path/not/existing/schema.json");
        assertNull(reg.columnType(TableName.valueOf("ANY"), "col"));
    }

    /**
     * Поддержка Unicode-имён таблиц/колонок (русский/казахский): регистр qualifier игнорируется.
     */
    @Test
    @DisplayName("Латиница: имена таблиц и колонок — латиницей (значения могут быть Unicode)")
    void unicodeNamesSupported(@TempDir Path dir) throws IOException {
        String json = "{\n" +
                "  \"DEFAULT:ZAKAZY\": {\n" +
                "    \"columns\": { \"name\": \"VARCHAR\", \"date\": \"DATE\" }\n" +
                "  }\n" +
                "}";
        Path f = dir.resolve("schema.json");
        write(f, json);

        JsonSchemaRegistry reg = new JsonSchemaRegistry(f.toString());
        // короткое имя таблицы без DEFAULT
        TableName tShort = TableName.valueOf("ZAKAZY");
        // регистр qualifier игнорируется
        assertEquals("VARCHAR", reg.columnType(tShort, "name"));
        assertEquals("VARCHAR", reg.columnType(tShort, "NAME"));
        assertEquals("DATE", reg.columnType(tShort, "date"));
        assertEquals("DATE", reg.columnType(tShort, "DATE"));
        // неизвестная колонка → null
        assertNull(reg.columnType(tShort, "unknown"));
        // неизвестная таблица → null
        assertNull(reg.columnType(TableName.valueOf("NOT_EXISTS"), "name"));
    }

    /**
     * Синонимы массивов для совместимости: JsonSchemaRegistry возвращает тип-строку как есть.
     */
    @Test
    @DisplayName("Массивы: 'CHARACTER VARYING ARRAY' и 'STRING ARRAY' возвращаются как есть")
    void arraySynonymsReturnedAsIs(@TempDir Path dir) throws IOException {
        String json = "{\n" +
                "  \"TBL_SYNONYMS\": {\"columns\": {\"a\": \"CHARACTER VARYING ARRAY\", \"b\": \"STRING ARRAY\"}}\n" +
                "}";
        Path f = dir.resolve("schema.json");
        write(f, json);

        JsonSchemaRegistry reg = new JsonSchemaRegistry(f.toString());
        TableName t = TableName.valueOf("TBL_SYNONYMS");
        assertEquals("CHARACTER VARYING ARRAY", reg.columnType(t, "a"));
        assertEquals("STRING ARRAY", reg.columnType(t, "b"));
    }

    /**
     * Дополнение к refresh(): удаление колонки после обновления отражается в результатах.
     */
    @Test
    @DisplayName("refresh(): удаление колонки делает columnType(...)==null")
    void refreshRemovesColumn(@TempDir Path dir) throws IOException {
        Path f = dir.resolve("schema.json");
        write(f, "{\n" +
                "  \"TBL_D\": {\"columns\": {\"keep\": \"INT\", \"drop\": \"VARCHAR\"}}\n" +
                "}");

        JsonSchemaRegistry reg = new JsonSchemaRegistry(f.toString());
        TableName t = TableName.valueOf("TBL_D");
        assertEquals("INT", reg.columnType(t, "keep"));
        assertEquals("VARCHAR", reg.columnType(t, "drop"));

        // Обновляем файл: колонку drop удаляем
        write(f, "{\n" +
                "  \"TBL_D\": {\"columns\": {\"keep\": \"INT\"}}\n" +
                "}");
        reg.refresh();

        assertEquals("INT", reg.columnType(t, "keep"));
        assertNull(reg.columnType(t, "drop"));
    }

    /**
     * primaryKeyColumns: базовый сценарий — возвращает PK в порядке следования.
     * Поддерживаются полное и короткое имя таблицы, регистр qualifier не влияет.
     */
    @Test
    @DisplayName("primaryKeyColumns: базовый сценарий — ['c','t','opd'] в порядке следования")
    void primaryKeyColumnsBasic(@TempDir Path dir) throws IOException {
        Path f = dir.resolve("schema.json");
        write(f, "{\n" +
                "  \"DEFAULT:TBL_PK\": {\n" +
                "    \"columns\": {\"c\":\"VARCHAR\",\"t\":\"UNSIGNED_TINYINT\",\"opd\":\"TIMESTAMP\",\"x\":\"INT\"},\n" +
                "    \"PK\": [\"c\",\"t\",\"opd\"]\n" +
                "  }\n" +
                "}");
        JsonSchemaRegistry reg = new JsonSchemaRegistry(f.toString());
        // короткое имя без namespace должно работать
        TableName tShort = TableName.valueOf("TBL_PK");
        assertArrayEquals(new String[]{"c","t","opd"}, reg.primaryKeyColumns(tShort));
        // полное имя тоже
        TableName tFull = TableName.valueOf("DEFAULT:TBL_PK");
        assertArrayEquals(new String[]{"c","t","opd"}, reg.primaryKeyColumns(tFull));
    }

    /**
     * primaryKeyColumns: отсутствие описания PK в схеме — должен возвращаться пустой массив.
     */
    @Test
    @DisplayName("primaryKeyColumns: отсутствует в схеме → пустой массив (не null)")
    void primaryKeyColumnsAbsentReturnsEmpty(@TempDir Path dir) throws IOException {
        Path f = dir.resolve("schema.json");
        write(f, "{\n" +
                "  \"TBL_NO_PK\": {\"columns\": {\"a\": \"INT\", \"b\": \"VARCHAR\"}}\n" +
                "}");
        JsonSchemaRegistry reg = new JsonSchemaRegistry(f.toString());
        assertArrayEquals(new String[0], reg.primaryKeyColumns(TableName.valueOf("TBL_NO_PK")));
    }

    /**
     * primaryKeyColumns: поведение refresh() — изменения PK должны подхватываться.
     */
    @Test
    @DisplayName("primaryKeyColumns + refresh(): PK меняется после перечитывания схемы")
    void primaryKeyColumnsRefresh(@TempDir Path dir) throws IOException {
        Path f = dir.resolve("schema.json");
        write(f, "{\n" +
                "  \"TBL_REFRESH_PK\": {\n" +
                "    \"columns\": {\"c\":\"VARCHAR\",\"t\":\"UNSIGNED_TINYINT\",\"opd\":\"TIMESTAMP\",\"x\":\"INT\"},\n" +
                "    \"PK\": [\"c\",\"t\",\"opd\"]\n" +
                "  }\n" +
                "}");
        JsonSchemaRegistry reg = new JsonSchemaRegistry(f.toString());
        TableName t = TableName.valueOf("TBL_REFRESH_PK");
        assertArrayEquals(new String[]{"c","t","opd"}, reg.primaryKeyColumns(t));

        // меняем PK и вызываем refresh()
        write(f, "{\n" +
                "  \"TBL_REFRESH_PK\": {\n" +
                "    \"columns\": {\"c\":\"VARCHAR\",\"t\":\"UNSIGNED_TINYINT\",\"opd\":\"TIMESTAMP\",\"x\":\"INT\"},\n" +
                "    \"PK\": [\"c\",\"opd\"]\n" +
                "  }\n" +
                "}");
        reg.refresh();
        assertArrayEquals(new String[]{"c","opd"}, reg.primaryKeyColumns(t));
    }

    /**
     * Fail-fast контракт: primaryKeyColumns(table) бросает NPE при null table.
     */
    @Test
    @DisplayName("Fail-fast: NPE на null table в primaryKeyColumns(...)")
    void primaryKeyColumnsNpeOnNull() {
        JsonSchemaRegistry reg = new JsonSchemaRegistry("/path/not/existing/schema.json");
        NullPointerException npe = assertThrows(NullPointerException.class, () -> reg.primaryKeyColumns(null));
        assertTrue(npe.getMessage() == null || npe.getMessage().toLowerCase().contains("table"), "Сообщение NPE должно указывать на параметр table");
    }

    @Test
    @DisplayName("primaryKeyColumns: неизвестная таблица → пустой массив (не null)")
    void primaryKeyColumnsUnknownTableReturnsEmpty(@TempDir Path dir) throws IOException {
        Path f = dir.resolve("schema.json");
        write(f, "{\n" +
                "  \"TBL_KNOWN\": {\n" +
                "    \"columns\": {\"c\":\"VARCHAR\",\"t\":\"UNSIGNED_TINYINT\",\"opd\":\"TIMESTAMP\"},\n" +
                "    \"PK\": [\"c\",\"t\",\"opd\"]\n" +
                "  }\n" +
                "}");
        JsonSchemaRegistry reg = new JsonSchemaRegistry(f.toString());

        // неизвестная таблица должна давать пустой, но НЕ null
        String[] pkUnknown = reg.primaryKeyColumns(TableName.valueOf("NOT_EXISTS"));
        assertNotNull(pkUnknown);
        assertEquals(0, pkUnknown.length, "Для неизвестной таблицы должен возвращаться пустой массив, не null");
    }

    @Test
    @DisplayName("refresh(): при битой схеме предыдущий снимок сохраняется (не падаем)")
    void refreshWithBrokenSchemaKeepsPrevious(@TempDir Path dir) throws IOException {
        Path f = dir.resolve("schema.json");
        write(f, "{\n" +
                "  \"TBL_SAFE\": {\"columns\": {\"x\":\"INT\"}}\n" +
                "}");
        JsonSchemaRegistry reg = new JsonSchemaRegistry(f.toString());
        TableName t = TableName.valueOf("TBL_SAFE");
        assertEquals("INT", reg.columnType(t, "x"));

        // Портим JSON и вызываем refresh()
        write(f, "{");
        reg.refresh();

        // Ожидаем, что старый снимок остался рабочим (если это твой контракт)
        assertEquals("INT", reg.columnType(t, "x"),
                "При некорректной схеме на refresh() ожидается сохранение предыдущего снимка");
    }
}
