package kz.qazmarka.h2k.schema;

import org.apache.hadoop.hbase.TableName;

/**
 * SchemaRegistry — тонкая абстракция реестра типов колонок Phoenix.
 *
 * По паре (имя таблицы, имя колонки/qualifier) возвращает строковое
 * имя *phoenix-типа* из официального набора (например: "VARCHAR",
 * "CHAR", "DECIMAL", "TIMESTAMP", "UNSIGNED_TINYINT", "BINARY" и т.д.).
 *
 * Контракт:
 * - Возвращает null, если тип неизвестен для данной колонки.
 * - Вызов может быть дорогим (IO/парсинг), поэтому вызывающая сторона
 *   должна кэшировать ответы (см. ValueCodecPhoenix).
 * - Реализация обязана учитывать правила регистра Phoenix:
 *   некавыченные идентификаторы — UPPERCASE; кавычные — регистр сохраняется.
 *   Для удобства в вызывающем коде есть default-метод columnTypeRelaxed(...).
 * - Параметры table и qualifier не должны быть null при вызове методов с default-реализацией.
 * - При передаче null параметров в default-методы будет выброшен NullPointerException.
 *
 * Потокобезопасность:
 * - Реализации должны быть потокобезопасными или неизменяемыми,
 *   т.к. вызываются из параллельных потоков RegionServer.
 * Ограничения и предварительные условия:
 * - В default-методах этого интерфейса параметры table/qualifier/defaultType
 *   не допускают null — при нарушении будет выброшен NullPointerException
 *   с понятным сообщением (fail-fast; упрощает диагностику).
 */
@FunctionalInterface
public interface SchemaRegistry {
    /**
     * Возвращает имя phoenix-типа колонки либо null, если тип неизвестен.
     * Реализация сама решает источник (JSON, SYSTEM.CATALOG и т.д.) и
     * политику нормализации идентификаторов.
     *
     * @param table     имя таблицы (HBase TableName с namespace)
     * @param qualifier имя колонки (Phoenix qualifier)
     * @return точное строковое имя phoenix-типа или null
     */
    String columnType(TableName table, String qualifier);

    /**
     * Удобная обёртка: вернуть тип или значение по умолчанию, если тип не найден.
     * Не влияет на контракт интерфейса и не требует от реализаций дополнительных
     * методов (default-метод поддерживается начиная с Java 8).
     *
     * @param table       имя таблицы
     * @param qualifier   имя колонки
     * @param defaultType тип по умолчанию (например, "VARCHAR")
     * @return тип из реестра либо defaultType, если тип не найден или реестр пуст
     * Примечание: параметры table/qualifier/defaultType не допускают null (fail-fast).
     */
    default String columnTypeOrDefault(TableName table, String qualifier, String defaultType) {
        java.util.Objects.requireNonNull(table, "table");
        java.util.Objects.requireNonNull(qualifier, "qualifier");
        java.util.Objects.requireNonNull(defaultType, "defaultType");
        String t = columnType(table, qualifier);
        return (t != null) ? t : defaultType;
        }

    /**
     * Ищет тип с постепенной нормализацией регистра: exact → UPPER → lower.
     * Реализации могут переопределить этот метод, если нужна более хитрая
     * логика (quoted identifiers, экранирование и т.п.).
     *
     * Это облегчает жизнь вызывающему коду, когда регистр qualifier'а может
     * варьироваться между WAL/DDL/JSON.
     *
     * @param table     имя таблицы
     * @param qualifier имя колонки (как пришло из источника)
     * @return строковое имя phoenix-типа или null
     */
    default String columnTypeRelaxed(TableName table, String qualifier) {
        java.util.Objects.requireNonNull(table, "table");
        java.util.Objects.requireNonNull(qualifier, "qualifier");

        String t = columnType(table, qualifier);
        if (t != null) return t;

        String upper = qualifier.toUpperCase();
        if (!upper.equals(qualifier)) {
            t = columnType(table, upper);
            if (t != null) return t;
        }

        String lower = qualifier.toLowerCase();
        if (!lower.equals(qualifier)) {
            t = columnType(table, lower);
        }
        return t;
    }

    /**
     * Проверка наличия колонки в реестре с релаксированной нормализацией регистра.
     * Эквивалентна columnTypeRelaxed(...) != null.
     */
    default boolean hasColumnRelaxed(TableName table, String qualifier) {
        return columnTypeRelaxed(table, qualifier) != null;
    }

    /**
     * Возвращает тип с релаксированной нормализацией регистра (exact → UPPER → lower)
     * либо значение по умолчанию, если тип не найден.
     *
     * Примечание: параметры table/qualifier/defaultType не допускают null (fail-fast).
     */
    default String columnTypeOrDefaultRelaxed(TableName table, String qualifier, String defaultType) {
        java.util.Objects.requireNonNull(table, "table");
        java.util.Objects.requireNonNull(qualifier, "qualifier");
        java.util.Objects.requireNonNull(defaultType, "defaultType");
        String t = columnTypeRelaxed(table, qualifier);
        return (t != null) ? t : defaultType;
    }

    /**
     * Дешёвая проверка наличия колонки в реестре.
     *
     * @param table     имя таблицы
     * @param qualifier имя колонки
     * @return true, если тип найден; иначе false
     */
    default boolean hasColumn(TableName table, String qualifier) {
        return columnType(table, qualifier) != null;
    }

    /**
     * Явный хук на обновление источника схем (по умолчанию — no-op).
     * Реализации на JSON или SYSTEM.CATALOG могут переопределить этот метод.
     */
    default void refresh() {
        // no-op
    }

    /**
     * Фабрика "пустого" реестра: всегда возвращает null.
     * Удобно для тестов/стабов и микробенчмарков.
     */
    static SchemaRegistry noop() {
        return (table, qualifier) -> null;
    }
}