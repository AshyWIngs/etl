package kz.qazmarka.h2k.schema;

import java.util.Locale;
import java.util.Objects;

import org.apache.hadoop.hbase.TableName;

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
 * Полезно в тестах и микробенчмарках, исключает лишние аллокации при каждом вызове {@link #emptyRegistry()}.
     */
    SchemaRegistry NOOP = (table, qualifier) -> null;

    /** Пустой неизменяемый массив строк (общая константа без аллокаций). */
    String[] EMPTY = new String[0];

    /**
     * Возвращает имя phoenix‑типа колонки либо {@code null}, если тип неизвестен.
     * Реализация сама выбирает источник (JSON, SYSTEM.CATALOG и т.д.) и политику нормализации.
     *
     * @param table     имя таблицы ({@link TableName} с namespace), не {@code null}
     * @param qualifier имя колонки (Phoenix qualifier), не {@code null}
     * @return точное строковое имя phoenix‑типа или {@code null}
     */
    String columnType(TableName table, String qualifier);

    /**
     * Возвращает тип из реестра или значение по умолчанию, если тип не найден.
     * @implSpec Выполняет fail‑fast валидацию аргументов через {@link Objects#requireNonNull(Object, String)}.
     *
     * @param table       имя таблицы, не {@code null}
     * @param qualifier   имя колонки, не {@code null}
     * @param defaultType тип по умолчанию (например, "VARCHAR"), не {@code null}
     * @return тип из реестра либо {@code defaultType}
     * @throws NullPointerException если любой из параметров равен {@code null}
     */
    default String columnTypeOrDefault(TableName table, String qualifier, String defaultType) {
        Objects.requireNonNull(table, "table");
        Objects.requireNonNull(qualifier, "qualifier");
        Objects.requireNonNull(defaultType, "defaultType");
        String t = columnType(table, qualifier);
        return (t != null) ? t : defaultType;
    }

    /**
     * Ищет тип с постепенной нормализацией регистра: exact → UPPER → lower (через {@link Locale#ROOT}),
     * избегая лишних выделений строк (toUpperCase/toLowerCase вызываются только при необходимости).
     * Реализации могут переопределить метод для поддержки кавычных идентификаторов и спец. правил.
     * @implSpec Делает до трёх обращений к {@link #columnType(TableName, String)}; аллокации строк (upper/lower) выполняются только при наличии соответствующих символов регистра.
     *
     * @param table     имя таблицы, не {@code null}
     * @param qualifier имя колонки (как пришло из источника), не {@code null}
     * @return строковое имя phoenix‑типа или {@code null}
     * @throws NullPointerException если любой из параметров равен {@code null}
     */
    default String columnTypeRelaxed(TableName table, String qualifier) {
        Objects.requireNonNull(table, "table");
        Objects.requireNonNull(qualifier, "qualifier");

        // 1) Прямая попытка — самая частая и без аллокаций
        String t = columnType(table, qualifier);
        if (t != null) return t;

        // 2) Единый проход по символам: определяем наличие и нижних, и верхних букв
        boolean hasLower = false;
        boolean hasUpper = false;
        for (int i = 0, n = qualifier.length(); i < n && !(hasLower && hasUpper); i++) {
            char ch = qualifier.charAt(i);
            if (!hasLower && Character.isLowerCase(ch)) {
                hasLower = true;
            } else if (!hasUpper && Character.isUpperCase(ch)) {
                hasUpper = true;
            }
        }
        if (hasLower) {
            String upper = qualifier.toUpperCase(Locale.ROOT);
            t = columnType(table, upper);
            if (t != null) return t;
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
     * @implSpec Эквивалент вызову {@code columnTypeRelaxed(table, qualifier) != null}.
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
     * @implSpec Выполняет те же шаги, что и {@link #columnTypeRelaxed(TableName, String)}, и затем подставляет {@code defaultType} при отсутствии результата; также выполняет fail‑fast валидацию аргументов.
     *
     * @param table        имя таблицы, не {@code null}
     * @param qualifier    имя колонки, не {@code null}
     * @param defaultType  тип по умолчанию, не {@code null}
     * @return тип из реестра либо {@code defaultType}
     * @throws NullPointerException если любой из параметров равен {@code null}
     */
    default String columnTypeOrDefaultRelaxed(TableName table, String qualifier, String defaultType) {
        Objects.requireNonNull(table, "table");
        Objects.requireNonNull(qualifier, "qualifier");
        Objects.requireNonNull(defaultType, "defaultType");
        String t = columnTypeRelaxed(table, qualifier);
        return (t != null) ? t : defaultType;
    }

    /**
     * Быстрая проверка наличия определения колонки в реестре (без релаксированного поиска).
     * Выполняет fail‑fast валидацию аргументов согласно контракту интерфейса.
     * @implSpec Делает один вызов {@link #columnType(TableName, String)} после fail‑fast проверки аргументов.
     *
     * @param table     имя таблицы, не {@code null}
     * @param qualifier имя колонки, не {@code null}
     * @return {@code true}, если тип найден; иначе {@code false}
     * @throws NullPointerException если любой из параметров равен {@code null}
     */
    default boolean hasColumn(TableName table, String qualifier) {
        Objects.requireNonNull(table, "table");
        Objects.requireNonNull(qualifier, "qualifier");
        return columnType(table, qualifier) != null;
    }

    /**
     * Имена PK-колонок таблицы в порядке их размещения в rowkey (как в Phoenix).
     *
     * Назначение
     *  - Позволяет вызывающей стороне восстановить значения PK из rowkey, когда PK не пишутся в клетки.
     *
     * Контракт
     *  - Никогда не возвращает {@code null}; при отсутствии информации возвращает пустой массив.
     *  - Не бросает исключений при отсутствии определения; fail‑fast только на {@code null} аргументах.
     *  - Реализация может кэшировать результат и/или подставлять алиасы имён таблиц.
     *
     * Потокобезопасность
     *  - Реализации должны быть потокобезопасными или неизменяемыми.
     * @implSpec Реализация по умолчанию возвращает {@link #EMPTY} и никогда не возвращает {@code null}.
     *
     * @param table имя таблицы ({@link TableName} с namespace), не {@code null}
     * @return массив имён PK‑колонок (возможно пустой, но не {@code null})
     * @throws NullPointerException если {@code table == null}
     */
    default String[] primaryKeyColumns(TableName table) {
        Objects.requireNonNull(table, "table");
        return EMPTY;
    }

    /**
     * Явный хук на обновление/перечтение источника схем (по умолчанию — no‑op).
     * Реализации на JSON или SYSTEM.CATALOG могут переопределить и выполнить загрузку.
     * @implSpec По умолчанию метод — no‑op; переопределите в реализациях, читающих внешние источники.
     */
    default void refresh() {
        // no-op
    }

    /**
     * Фабрика «пустого» реестра: всегда возвращает {@code null} для любого запроса.
     * Удобно для тестов/стабов и микробенчмарков.
     * @implNote Возвращает константу {@link #NOOP}.
     *
     * @return реализация SchemaRegistry, возвращающая всегда {@code null}
     */
    static SchemaRegistry emptyRegistry() {
        return NOOP;
    }
}