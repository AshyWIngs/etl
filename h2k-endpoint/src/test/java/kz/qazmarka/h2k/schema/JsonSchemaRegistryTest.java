package kz.qazmarka.h2k.schema;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.hadoop.hbase.TableName;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Набор быстрых unit‑тестов для {@link JsonSchemaRegistry}.
 *
 * Цели:
 *  • Подтвердить корректный разбор minimal JSON‑схемы и работу алиасов имён таблиц/колонок
 *    (полное имя с namespace, короткое имя без namespace, регистронезависимость qualifier).
 *  • Убедиться, что типы Phoenix массивов (например, {@code "VARCHAR ARRAY"}) распознаются без предупреждений
 *    и возвращаются как есть.
 *  • Проверить, что {@link JsonSchemaRegistry#refresh()} заменяет снимок схемы и инвалидирует внутренний кэш.
 *  • Зафиксировать стабильное поведение при пустой/битой схеме (реестр остаётся пустым, ошибок на уровне теста нет).
 *
 * Технические детали:
 *  • Все тесты самодостаточные: исходный JSON пишется в файл во временной директории через {@link TempDir},
 *    путь передаётся в конструктор {@link JsonSchemaRegistry}.
 *  • Никаких внешних ресурсов и сети; выполнение занимает миллисекунды, GC‑нагрузка минимальна.
 *  • Логи парсинга в тестах не проверяются: функционально нас интересуют возвращаемые типы/nullable‑поведение.
 */
class JsonSchemaRegistryTest {

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
        Files.write(f, json.getBytes(StandardCharsets.UTF_8));

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
        Files.write(f, json.getBytes(StandardCharsets.UTF_8));

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
        Files.write(f, ("{\n" +
                "  \"TBL_C\": {\"columns\": {\"x\": \"INT\"}}\n" +
                "}").getBytes(StandardCharsets.UTF_8));

        JsonSchemaRegistry reg = new JsonSchemaRegistry(f.toString());
        TableName t = TableName.valueOf("TBL_C");
        assertEquals("INT", reg.columnType(t, "x"));

        // Обновляем файл и вызываем refresh()
        Files.write(f, ("{\n" +
                "  \"TBL_C\": {\"columns\": {\"x\": \"BIGINT\", \"y\": \"VARCHAR\"}}\n" +
                "}").getBytes(StandardCharsets.UTF_8));
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
        Files.write(f, "{".getBytes(StandardCharsets.UTF_8));

        JsonSchemaRegistry reg = new JsonSchemaRegistry(f.toString());
        assertNull(reg.columnType(TableName.valueOf("ANY"), "col"));
    }
}