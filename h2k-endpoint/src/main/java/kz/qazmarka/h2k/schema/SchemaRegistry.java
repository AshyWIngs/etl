package kz.qazmarka.h2k.schema;

import org.apache.hadoop.hbase.TableName;
import java.util.Locale;

/**
 * SchemaRegistry — тонкая абстракция реестра типов колонок Phoenix.
 *
 * Назначение
 *  - по паре (имя таблицы, имя колонки/qualifier) возвращает строковое имя *phoenix‑типа*
 *    из официального набора (например: "VARCHAR", "CHAR", "DECIMAL", "TIMESTAMP",
 *    "UNSIGNED_TINYINT", "BINARY" и т.д.);
 *  - скрывает источник данных (JSON‑файл, SYSTEM.CATALOG и т.п.) и политику нормализации.
 *
 * Контракт
 *  - Возвращает {@code null}, если тип неизвестен для данной колонки.
 *  - Вызов может быть дорогим (IO/парсинг), вызывающая сторона должна кэшировать ответы
 *    (см. реализацию ValueCodecPhoenix и его кэш колонок).
 *  - Реализация обязана учитывать правила регистра Phoenix: некавыченные идентификаторы — UPPERCASE;
 *    кавычные — регистр сохраняется. Для удобства есть {@link #columnTypeRelaxed(TableName, String)}.
 *  - Default‑методы этого интерфейса выполняют проверку аргументов (fail‑fast) и бросают
 *    {@link NullPointerException} при {@code null} параметрах — это упрощает диагностику.
 *  - Реестр не выполняет глобальных эвристик — только поиск по паре (table, qualifier) с нормализацией регистра (exact → UPPER → lower при необходимости).
 *
 * Потокобезопасность
 *  - Реализации должны быть потокобезопасными или неизменяемыми, т.к. вызываются из параллельных потоков RegionServer.
 */
@FunctionalInterface
public interface SchemaRegistry {
    /**
     * Константа: «пустой» реестр — всегда возвращает {@code null}.
     * Полезно в тестах и микробенчмарках, исключает лишние аллокации при каждом вызове {@link #empty()}.
     */
    SchemaRegistry NOOP = (table, qualifier) -> null;

    /**
     * Возвращает имя phoenix‑типа колонки либо {@code null}, если тип неизвестен.
     * Реализация сама выбирает источник (JSON, SYSTEM.CATALOG и т.д.) и политику нормализации.
     *
     * @param table     имя таблицы (HBase TableName с namespace), не {@code null}
     * @param qualifier имя колонки (Phoenix qualifier), не {@code null}
     * @return точное строковое имя phoenix‑типа или {@code null}
     */
    String columnType(TableName table, String qualifier);

    /**
     * Возвращает тип из реестра или значение по умолчанию, если тип не найден.
     *
     * @param table       имя таблицы, не {@code null}
     * @param qualifier   имя колонки, не {@code null}
     * @param defaultType тип по умолчанию (например, "VARCHAR"), не {@code null}
     * @return тип из реестра либо {@code defaultType}
     * @throws NullPointerException если любой из параметров равен {@code null}
     */
    default String columnTypeOrDefault(TableName table, String qualifier, String defaultType) {
        java.util.Objects.requireNonNull(table, "table");
        java.util.Objects.requireNonNull(qualifier, "qualifier");
        java.util.Objects.requireNonNull(defaultType, "defaultType");
        String t = columnType(table, qualifier);
        return (t != null) ? t : defaultType;
    }

    /**
     * Ищет тип с постепенной нормализацией регистра: exact → UPPER → lower (через {@link Locale#ROOT}),
     * избегая лишних выделений строк (toUpperCase/toLowerCase вызываются только при необходимости).
     * Реализации могут переопределить метод для поддержки кавычных идентификаторов и спец. правил.
     *
     * @param table     имя таблицы, не {@code null}
     * @param qualifier имя колонки (как пришло из источника), не {@code null}
     * @return строковое имя phoenix‑типа или {@code null}
     * @throws NullPointerException если любой из параметров равен {@code null}
     */
    default String columnTypeRelaxed(TableName table, String qualifier) {
        java.util.Objects.requireNonNull(table, "table");
        java.util.Objects.requireNonNull(qualifier, "qualifier");

        // 1) Прямая попытка — самая частая и без аллокаций
        String t = columnType(table, qualifier);
        if (t != null) return t;

        // 2) Если есть хотя бы одна строчная буква — пробуем UPPER без создания строки в очевидных случаях
        boolean hasLower = false;
        for (int i = 0, n = qualifier.length(); i < n; i++) {
            char ch = qualifier.charAt(i);
            if (Character.isLowerCase(ch)) { hasLower = true; break; }
        }
        if (hasLower) {
            String upper = qualifier.toUpperCase(Locale.ROOT);
            t = columnType(table, upper);
            if (t != null) return t;
        }

        // 3) Если есть хотя бы одна прописная буква — пробуем lower
        boolean hasUpper = false;
        for (int i = 0, n = qualifier.length(); i < n; i++) {
            char ch = qualifier.charAt(i);
            if (Character.isUpperCase(ch)) { hasUpper = true; break; }
        }
        if (hasUpper) {
            String lower = qualifier.toLowerCase(Locale.ROOT);
            t = columnType(table, lower);
        }

        return t;
    }

    /**
     * Проверяет наличие колонки в реестре с релаксированной нормализацией регистра.
     * Эквивалентно {@code columnTypeRelaxed(table, qualifier) != null}.
     *
     * @param table     имя таблицы, не {@code null}
     * @param qualifier имя колонки, не {@code null}
     * @return {@code true}, если тип найден; иначе {@code false}
     * @throws NullPointerException если любой из параметров равен {@code null}
     */
    default boolean hasColumnRelaxed(TableName table, String qualifier) {
        return columnTypeRelaxed(table, qualifier) != null;
    }

    /**
     * Возвращает тип с релаксированной нормализацией (exact → UPPER → lower) либо значение по умолчанию.
     *
     * @param table        имя таблицы, не {@code null}
     * @param qualifier    имя колонки, не {@code null}
     * @param defaultType  тип по умолчанию, не {@code null}
     * @return тип из реестра либо {@code defaultType}
     * @throws NullPointerException если любой из параметров равен {@code null}
     */
    default String columnTypeOrDefaultRelaxed(TableName table, String qualifier, String defaultType) {
        java.util.Objects.requireNonNull(table, "table");
        java.util.Objects.requireNonNull(qualifier, "qualifier");
        java.util.Objects.requireNonNull(defaultType, "defaultType");
        String t = columnTypeRelaxed(table, qualifier);
        return (t != null) ? t : defaultType;
    }

    /**
     * Быстрая проверка наличия определения колонки в реестре (без релаксированного поиска).
     * Выполняет fail‑fast валидацию аргументов согласно контракту интерфейса.
     *
     * @param table     имя таблицы, не {@code null}
     * @param qualifier имя колонки, не {@code null}
     * @return {@code true}, если тип найден; иначе {@code false}
     * @throws NullPointerException если любой из параметров равен {@code null}
     */
    default boolean hasColumn(TableName table, String qualifier) {
        java.util.Objects.requireNonNull(table, "table");
        java.util.Objects.requireNonNull(qualifier, "qualifier");
        return columnType(table, qualifier) != null;
    }

    /**
     * Явный хук на обновление/перечтение источника схем (по умолчанию — no‑op).
     * Реализации на JSON или SYSTEM.CATALOG могут переопределить и выполнить загрузку.
     */
    default void refresh() {
        // no-op
    }

    /**
     * Фабрика «пустого» реестра: всегда возвращает {@code null} для любого запроса.
     * Удобно для тестов/стабов и микробенчмарков.
     *
     * @return реализация SchemaRegistry, возвращающая всегда {@code null}
     */
    static SchemaRegistry empty() {
        return NOOP;
    }
}