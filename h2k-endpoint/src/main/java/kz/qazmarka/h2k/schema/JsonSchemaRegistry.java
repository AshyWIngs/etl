package kz.qazmarka.h2k.schema;

import java.io.Reader;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.hbase.TableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

/**
 * JsonSchemaRegistry — реестр типов колонок, загружаемый из JSON‑файла.
 *
 * Назначение
 *  - сопоставляет имя таблицы и квалификатор колонки с строковым типом Phoenix (например, "VARCHAR", "UNSIGNED_TINYINT");
 *  - терпимо относится к регистру: публикует алиасы имён таблиц/колонок (исходное/UPPER/lower);
 *  - обеспечивает быстрый доступ на «горячем пути» через кэш {TableName -> карта колонок}.
 *
 * Формат файла (пример):
 * {
 *   "TBL_NAME": {
 *     "columns": {
 *       "col1": "VARCHAR",
 *       "col2": "UNSIGNED_TINYINT",
 *       "created_at": "TIMESTAMP"
 *     }
 *   },
 *   "OTHER_TBL": { "columns": { ... } }
 * }
 *
 * Особенности реализации
 *  - для имён таблиц и колонок публикуются алиасы: исходное, верхний и нижний регистры;
 *  - строковые значения типов канонизируются в верхний регистр (Locale.ROOT);
 *  - после загрузки структура иммутабельна (unmodifiable) и атомарно обновляется методом {@link #refresh()};
 *  - чтение потокобезопасно без синхронизации: используется {@link java.util.concurrent.atomic.AtomicReference}
 *    для публикации корня и локальный {@link java.util.concurrent.ConcurrentMap}‑кэш по таблицам.
 *  - типы массивов Phoenix (например, "VARCHAR ARRAY") считаются корректными и не генерируют предупреждений;
 *  - default-namespace можно опускать: записи вида "DEFAULT:TBL" и "TBL" будут доступны при поиске.
 *  - контракт аргументов: методы API требуют не-null параметров table/qualifier; при null выбрасывается {@link NullPointerException}.
 *  - реестр не выполняет глобальных эвристик — только поиск по паре (table, qualifier) с нормализацией регистра и поддержкой короткого имени (после ':').
 */
public final class JsonSchemaRegistry implements SchemaRegistry {

    /** Логгер реестра; все сообщения — на русском языке. */
    private static final Logger LOG = LoggerFactory.getLogger(JsonSchemaRegistry.class);
    /** Ключ JSON-секции с определениями колонок. */
    private static final String KEY_COLUMNS = "columns";

    /**
     * Шаблон предупреждения о дубликате определения таблицы. Используется, когда под одним алиасом
     * публикуется отличающаяся карта колонок — фиксируем и продолжаем с последним определением.
     */
    private static final String DUP_TBL_LOG =
        "Обнаружен дубликат таблицы '{}' (ключ '{}') в файле схемы '{}' — существующее определение будет перезаписано";

    /**
     * Общий экземпляр Gson для парсинга JSON. Держим статически, чтобы избежать лишних аллокаций.
     */
    private static final Gson GSON = new Gson();
    /** Тип корня JSON: Map<table, Map<"columns", Map<qualifier, type>>>. */
    private static final Type ROOT_TYPE =
        new TypeToken<Map<String, Map<String, Map<String, String>>>>(){}.getType();

    /**
     * Допустимые имена типов (канонизированные в UPPER/Locale.ROOT).
     * Это мягкая валидация: при неизвестном типе пишем WARN и продолжаем.
     */
    private static final Set<String> ALLOWED_TYPES;
    static {
        Set<String> s = new HashSet<>(64);
        // Базовые числовые
        s.add("TINYINT"); s.add("UNSIGNED_TINYINT");
        s.add("SMALLINT"); s.add("UNSIGNED_SMALLINT");
        s.add("INTEGER"); s.add("INT"); s.add("UNSIGNED_INT");
        s.add("BIGINT"); s.add("UNSIGNED_LONG");
        s.add("FLOAT"); s.add("UNSIGNED_FLOAT");
        s.add("DOUBLE"); s.add("UNSIGNED_DOUBLE");
        s.add("DECIMAL");
        // Строки/байты
        s.add("CHAR"); s.add("VARCHAR");
        s.add("BINARY"); s.add("VARBINARY");
        // Даты/время
        s.add("DATE"); s.add("TIME"); s.add("TIMESTAMP");
        // Прочее
        s.add("BOOLEAN");
        // Разрешим и массивы, если в схеме появятся
        s.add("ARRAY");
        ALLOWED_TYPES = Collections.unmodifiableSet(s);
    }

    /** Множество уже залогированных «неизвестных» типов, чтобы не засорять WARN-ами. */
    private static final Set<String> UNKNOWN_TYPES_LOGGED = ConcurrentHashMap.newKeySet();

    /**
     * Возвращает true, если тип допустим:
     *  - входит в белый список скалярных типов;
     *  - либо является массивом Phoenix (например, "VARCHAR ARRAY").
     */
    private static boolean isAllowedType(String tname) {
        if (ALLOWED_TYPES.contains(tname)) {
            return true;
        }
        // Разрешаем массивы: "VARCHAR ARRAY", "INTEGER ARRAY", ...
        return tname.endsWith(" ARRAY");
    }

    /**
     * Абсолютный или относительный путь к JSON‑файлу схемы. Используется при {@link #refresh()}.
     */
    private final String sourcePath;

    /**
     * Основная структура:
     * tableName -> (qualifier -> PHOENIX_TYPE_NAME)
     *
     * Иммутабельная карта; ссылка на неё хранится в AtomicReference и
     * переустанавливается атомарно при обновлении (см. {@link #refresh()}).
     * Это даёт корректную видимость между потоками без синхронизации на чтении.
     */
    private final AtomicReference<Map<String, Map<String, String>>> schemaRef;

    /**
     * Локальный кэш {TableName -> карта колонок}. Обнуляется при {@link #refresh()}.
     * Устраняет повторную конкатенацию строк имён таблиц на горячем пути.
     */
    private final ConcurrentMap<TableName, Map<String, String>> tableCache = new ConcurrentHashMap<>(64);

    /**
     * Загружает схему из указанного JSON‑файла и публикует её для чтения.
     *
     * @param path путь к JSON‑файлу (UTF‑8)
     */
    public JsonSchemaRegistry(String path) {
        this.sourcePath = path;
        this.schemaRef = new AtomicReference<>(loadFromFile(path));
    }

    /**
     * Переинициализировать реестр из исходного файла.
     * Атомарно заменяет ссылку на корневую карту и очищает локальный кэш {@code tableCache}.
     */
    @Override
    public void refresh() {
        this.schemaRef.set(loadFromFile(this.sourcePath));
        this.tableCache.clear();
    }

    /**
     * Читает файл и строит иммутабельную карту схемы. В случае любой ошибки пишет предупреждение
     * с исключением и возвращает пустую карту — это позволяет системе стартовать с пустой схемой.
     *
     * @param path путь к файлу схемы
     * @return иммутабельная карта {tableAlias -> {qualifierAlias -> TYPE}}, либо пустая карта при ошибке
     */
    private Map<String, Map<String, String>> loadFromFile(String path) {
        try (Reader r = Files.newBufferedReader(Paths.get(path), StandardCharsets.UTF_8)) {
            Map<String, Map<String, Map<String, String>>> root = parseRoot(r);
            return buildSchemaFromRoot(root);
        } catch (Exception ex) {
            if (LOG.isDebugEnabled()) {
                LOG.warn("Не удалось загрузить файл схемы '{}'; будет использована пустая схема", path, ex);
            } else {
                LOG.warn("Не удалось загрузить файл схемы '{}'; будет использована пустая схема ({}: {})",
                         path, ex.getClass().getSimpleName(), ex.getMessage());
            }
            return Collections.emptyMap();
        }
    }

    /**
     * Парсит JSON в корневую структуру вида { table -> { "columns" : { qualifier -> typeName } } }.
     * Если формат неожиданный или данные пусты, возвращает пустую карту с предупреждением в логах.
     *
     * @param r поток чтения JSON
     * @return разобранная корневая структура или пустая карта
     */
    private Map<String, Map<String, Map<String, String>>> parseRoot(Reader r) {
        Map<String, Map<String, Map<String, String>>> root = GSON.fromJson(r, ROOT_TYPE);
        if (root == null || root.isEmpty()) {
            LOG.warn("Схема пуста или имеет неожиданный формат — реестр останется пустым");
            return Collections.emptyMap();
        }
        return root;
    }

    /**
     * Строит иммутабельную карту схемы из корневой JSON-структуры.
     *
     * Для каждой таблицы из JSON выполняется:
     * - извлечение раздела {@code "columns"};
     * - нормализация имён колонок и типов;
     * - публикация одной и той же карты под несколькими алиасами имени таблицы
     *   (исходное/UPPER/lower и, при наличии, короткое имя после двоеточия).
     * Если определение таблицы пустое или некорректное — оно пропускается.
     *
     * @param root корневая карта {table -> {"columns" -> {qualifier -> type}}}
     * @return иммутабельная карта {tableAlias -> {qualifierAlias -> TYPE}}
     */
    private Map<String, Map<String, String>> buildSchemaFromRoot(
            Map<String, Map<String, Map<String, String>>> root) {
        if (root == null || root.isEmpty()) {
            return Collections.emptyMap();
        }
        // Приблизительная вместимость: по 1 записи на таблицу, но с учётом алиасов.
        int expected = Math.max(8, root.size() * 4);
        int cap = (expected * 4 / 3) + 1; // эквивалент 1/0.75 без float
        Map<String, Map<String, String>> result = new HashMap<>(cap);

        for (Map.Entry<String, Map<String, Map<String, String>>> e : root.entrySet()) {
            final String table = e.getKey();
            final Map<String, Map<String, String>> sect = e.getValue();

            final Map<String, String> normalizedCols = prepareColumns(table, sect);
            if (!normalizedCols.isEmpty()) {
                putTableAliases(result, table, normalizedCols);
            }
        }
        return Collections.unmodifiableMap(result);
    }

    /**
     * Добавляет алиас таблицы в результирующую карту. Если под алиасом уже есть другое определение — пишет WARN.
     *
     * @param result          результирующая карта {tableAlias -> columnsMap}
     * @param alias           публикуемый алиас имени таблицы
     * @param normalizedCols  карта колонок (иммутабельная)
     * @param table           исходное имя таблицы (для сообщений)
     */
    private void addAlias(Map<String, Map<String, String>> result,
                          String alias,
                          Map<String, String> normalizedCols,
                          String table) {
        Map<String, String> prev = result.put(alias, normalizedCols);
        if (prev != null && prev != normalizedCols) {
            LOG.warn(DUP_TBL_LOG, table, alias, sourcePath);
        }
    }

    /**
     * Публикует алиас, если он ещё не добавлялся в рамках обработки текущей таблицы.
     *
     * @param seen            набор уже добавленных алиасов
     * @param result          результирующая карта
     * @param alias           алиас имени таблицы
     * @param normalizedCols  карта колонок
     * @param table           исходное имя таблицы (для сообщений)
     */
    private void addDistinctAlias(Set<String> seen,
                                  Map<String, Map<String, String>> result,
                                  String alias,
                                  Map<String, String> normalizedCols,
                                  String table) {
        if (alias == null) return;
        if (seen.add(alias)) {
            addAlias(result, alias, normalizedCols, table);
        }
    }

    /**
     * Возвращает «короткое» имя таблицы (часть после двоеточия), либо {@code null}, если двоеточия нет/после него пусто.
     * @param table полное имя таблицы (namespace:table)
     * @return короткое имя или {@code null}
     */
    private static String getShortName(String table) {
        int idx = table.indexOf(':');
        if (idx >= 0 && idx + 1 < table.length()) {
            String sn = table.substring(idx + 1);
            return sn.isEmpty() ? null : sn;
        }
        return null;
    }

    /**
     * Публикует три варианта имени (исходное, UPPER, lower) как алиасы.
     *
     * @param seen           набор уже добавленных алиасов для текущей таблицы
     * @param result         результирующая карта
     * @param name           имя (полное или короткое)
     * @param normalizedCols карта колонок
     * @param table          исходное полное имя (для сообщений)
     */
    private void addAliasVariants(Set<String> seen,
                                  Map<String, Map<String, String>> result,
                                  String name,
                                  Map<String, String> normalizedCols,
                                  String table) {
        if (name == null || name.isEmpty()) return;
        final String up  = name.toUpperCase(Locale.ROOT);
        final String low = name.toLowerCase(Locale.ROOT);
        addDistinctAlias(seen, result, name, normalizedCols, table);
        if (!up.equals(name))  addDistinctAlias(seen, result, up,  normalizedCols, table);
        if (!low.equals(name) && !low.equals(up)) addDistinctAlias(seen, result, low, normalizedCols, table);
    }

    /**
     * Публикует карту колонок под несколькими алиасами имени таблицы: исходное/UPPER/lower и, при наличии, короткое имя.
     * При конфликте определений пишет предупреждение и оставляет последнее добавленное.
     *
     * @param result         результирующая карта
     * @param table          полное имя таблицы
     * @param normalizedCols иммутабельная карта колонок
     */
    private void putTableAliases(Map<String, Map<String, String>> result,
                                 String table,
                                 Map<String, String> normalizedCols) {
        Set<String> seen = new HashSet<>(6);
        addAliasVariants(seen, result, table, normalizedCols, table);
        String shortName = getShortName(table);
        if (shortName != null) {
            addAliasVariants(seen, result, shortName, normalizedCols, table);
        }
    }

    /**
     * Проверяет наличие секции `columns` и делегирует нормализацию. При отсутствии — пишет WARN и возвращает пустую карту.
     *
     * @param table полное имя таблицы (для сообщений)
     * @param sect  секция таблицы из JSON
     * @return иммутабельная карта колонок или пустая карта
     */
    private Map<String, String> prepareColumns(String table, Map<String, Map<String, String>> sect) {
        if (sect == null) {
            LOG.warn("Для таблицы '{}' отсутствует секция (ожидался объект с ключом 'columns') в файле схемы '{}'", table, sourcePath);
            return Collections.emptyMap();
        }
        Map<String, String> cols = sect.get(KEY_COLUMNS);
        if (cols == null || cols.isEmpty()) {
            LOG.warn("Для таблицы '{}' отсутствует раздел 'columns' в файле схемы '{}'", table, sourcePath);
            return Collections.emptyMap();
        }
        return normalizeColumns(cols, table);
    }

    /**
     * Нормализует имена колонок и типы: публикует orig/UPPER/lower для каждой колонки, тип приводит к UPPER.
     * Некорректные записи пропускает с понятными предупреждениями. Возвращает иммутабельную карту.
     *
     * @param cols  исходная карта {qualifier -> type}
     * @param table имя таблицы (для сообщений)
     * @return иммутабельная карта {qualifierAlias -> TYPE}
     */
    private Map<String, String> normalizeColumns(Map<String, String> cols, String table) {
        int expected = Math.max(8, cols.size() * 3);
        int cap = (expected * 4 / 3) + 1;
        Map<String, String> normalized = new HashMap<>(cap);
        for (Map.Entry<String, String> e : cols.entrySet()) {
            processColumn(normalized, e.getKey(), e.getValue(), table);
        }
        return Collections.unmodifiableMap(normalized);
    }

    /**
     * Обрабатывает пару (qualifier, type): валидирует, нормализует и публикует все формы ключей в выходную карту.
     *
     * @param out     результирующая карта
     * @param rawQ    исходное имя колонки
     * @param rawType исходный тип
     * @param table   имя таблицы (для сообщений)
     */
    private void processColumn(Map<String, String> out,
                               String rawQ,
                               String rawType,
                               String table) {
        if (rawQ == null) {
            LOG.warn("Пропуск колонки с пустым именем у таблицы '{}' (файл '{}')", table, sourcePath);
            return;
        }
        final String qTrim = rawQ.trim();
        if (qTrim.isEmpty()) {
            LOG.warn("Пропуск колонки с пустым именем у таблицы '{}' (файл '{}')", table, sourcePath);
            return;
        }
        if (rawType == null) {
            LOG.warn("Пропуск колонки '{}' с пустым типом у таблицы '{}' (файл '{}')", qTrim, table, sourcePath);
            return;
        }
        final String typeTrim = rawType.trim();
        if (typeTrim.isEmpty()) {
            LOG.warn("Пропуск колонки '{}' с пустым типом у таблицы '{}' (файл '{}')", qTrim, table, sourcePath);
            return;
        }
        final String tname = typeTrim.toUpperCase(Locale.ROOT);
        if (!isAllowedType(tname)) {
            if (UNKNOWN_TYPES_LOGGED.add(tname)) {
                LOG.warn("Неизвестный тип '{}' для колонки '{}' в таблице '{}' (файл '{}') — будет использован как есть",
                        tname, qTrim, table, sourcePath);
            } else {
                LOG.debug("Повтор неизвестного типа '{}' для колонки '{}' в таблице '{}'",
                        tname, qTrim, table);
            }
        }
        putAllForms(out, qTrim, tname);
    }

    /**
     * Кладёт три формы ключа (исходная/UPPER/lower), не перезаписывая уже существующие значения.
     *
     * @param map       целевая карта
     * @param qualifier имя колонки
     * @param type      канонизированный тип (UPPER)
     */
    private static void putAllForms(Map<String, String> map, String qualifier, String type) {
        map.putIfAbsent(qualifier, type);
        String up = qualifier.toUpperCase(Locale.ROOT);
        if (!up.equals(qualifier)) {
            map.putIfAbsent(up, type);
        }
        String low = qualifier.toLowerCase(Locale.ROOT);
        if (!low.equals(qualifier) && !low.equals(up)) {
            map.putIfAbsent(low, type);
        }
    }

    /**
     * Возвращает иммутабельную карту колонок для таблицы: пробует полное имя и короткое (после ':').
     *
     * @param t объект имени таблицы
     * @return карта колонок или пустая карта
     */
    private Map<String, String> resolveColumnsForTable(TableName t) {
        Map<String, Map<String, String>> local = schemaRef.get();
        String raw = t.getNameAsString();
        Map<String, String> found = local.get(raw);
        if (found == null) {
            int idx = raw.indexOf(':');
            if (idx >= 0 && idx + 1 < raw.length()) {
                found = local.get(raw.substring(idx + 1));
            }
        }
        return (found != null) ? found : Collections.emptyMap();
    }

    /**
     * Возвращает строковый тип колонки (PHOENIX_TYPE_NAME) по таблице и имени колонки.
     * Поиск регистронезависимый: пробует исходный ключ, затем UPPER и lower.
     *
     * @param table     имя таблицы, не {@code null}
     * @param qualifier имя колонки (любой регистр), не {@code null}
     * @return тип или {@code null}, если не найдено
     * @throws NullPointerException если любой из параметров равен {@code null}
     */
    @Override
    public String columnType(TableName table, String qualifier) {
        java.util.Objects.requireNonNull(table, "table");
        java.util.Objects.requireNonNull(qualifier, "qualifier");

        final Map<String, String> cols = tableCache.computeIfAbsent(table, this::resolveColumnsForTable);
        if (cols.isEmpty()) return null;

        String v = cols.get(qualifier);
        if (v != null) return v;

        String up = qualifier.toUpperCase(Locale.ROOT);
        if (!up.equals(qualifier)) {
            v = cols.get(up);
            if (v != null) return v;
        }
        String low = qualifier.toLowerCase(Locale.ROOT);
        if (!low.equals(qualifier) && !low.equals(up)) {
            return cols.get(low);
        }
        return null;
    }

    /**
     * Краткая диагностика состояния реестра: число таблиц, размер кэша и путь к источнику.
     * @return строка для DEBUG‑логов
     */
    @Override
    public String toString() {
        Map<String, Map<String, String>> schema = schemaRef.get();
        return new StringBuilder(96)
                .append("JsonSchemaRegistry{")
                .append("tables=").append(schema.size())
                .append(", cache=").append(tableCache.size())
                .append(", source='").append(sourcePath).append('\'')
                .append('}')
                .toString();
    }
}