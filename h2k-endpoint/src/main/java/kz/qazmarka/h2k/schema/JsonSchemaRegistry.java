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
 * JsonSchemaRegistry
 * Реестр типов колонок, загружаемый из JSON-файла.
 *
 * Формат файла (пример):
 *
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
 * Имя таблицы берётся как {@code TableName.getNameAsString()}.
 * Чтобы избежать регистрозависимости и аллокаций в «горячем» пути,
 * при загрузке мы публикуем один и тот же набор колонок по трём ключам
 * полного имени таблицы (orig/UPPER/lower) и дублируем алиасы для
 * «короткого» имени без namespace (часть после двоеточия) — это делает
 * схему более «толерантной» к формату.
 *
 * Для имён колонок делаем то же самое (orig/UPPER/lower).
 * Значения типов канонизируем в UPPER/Locale.ROOT, чтобы
 * сопоставление с Phoenix/HBase типами было детерминированным.
 *
 * На горячем пути используем небольшой кэш TableName->columns, который
 * устраняет повторную конкатенацию строк имени таблицы.
 *
 * После загрузки структура иммутабельна (unmodifiable) и может быть
 * атомарно обновлена методом {@link #refresh()}.
 */
public final class JsonSchemaRegistry implements SchemaRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(JsonSchemaRegistry.class);
    /** Ключ JSON-секции с определениями колонок. */
    private static final String KEY_COLUMNS = "columns";

    private static final String DUP_TBL_LOG =
        "Обнаружен дубликат таблицы '{}' (ключ '{}') в файле схемы '{}' — существующее определение будет перезаписано";

    /** Статический Gson и тип корня во избежание повторных аллокаций. */
    private static final Gson GSON = new Gson();
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

    /** Путь к источнику для возможного refresh(). */
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

    /** Небольшой кэш: TableName -> карта колонок. Сбрасывается при refresh(). */
    private final ConcurrentMap<TableName, Map<String, String>> tableCache = new ConcurrentHashMap<>(64);

    /** Загружает схему из указанного JSON-файла. */
    public JsonSchemaRegistry(String path) {
        this.sourcePath = path;
        this.schemaRef = new AtomicReference<>(loadFromFile(path));
    }

    /**
     * Переинициализировать реестр из исходного файла.
     * Атомарно заменяет ссылку на корневую карту.
     */
    @Override
    public void refresh() {
        this.schemaRef.set(loadFromFile(this.sourcePath));
        this.tableCache.clear();
    }

    /**
     * Парсит и строит иммутабельную карту из файла.
     * В случае ошибки — лог с полным стектрейсом и пустая карта.
     */
    private Map<String, Map<String, String>> loadFromFile(String path) {
        try (Reader r = Files.newBufferedReader(Paths.get(path), StandardCharsets.UTF_8)) {
            Map<String, Map<String, Map<String, String>>> root = parseRoot(r);
            return buildSchemaFromRoot(root);
        } catch (Exception ex) {
            LOG.warn("Не удалось загрузить файл схемы '{}'; будет использована пустая схема", path, ex);
            return Collections.emptyMap();
        }
    }

    /**
     * Парсит JSON в корневую структуру:
     * { table -> { "columns" : { qualifier -> typeName } } }.
     * Возвращает пустую map, если формат неожиданный.
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

    /** Добавляет алиас в result и пишет WARN, если перезаписали другое определение. */
    private void addAlias(Map<String, Map<String, String>> result,
                          String alias,
                          Map<String, String> normalizedCols,
                          String table) {
        Map<String, String> prev = result.put(alias, normalizedCols);
        if (prev != null && prev != normalizedCols) {
            LOG.warn(DUP_TBL_LOG, table, alias, sourcePath);
        }
    }

    /** Добавляет алиас, если ещё не добавляли в рамках текущей таблицы. */
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

    /** Возвращает короткое имя таблицы (часть после ':'), либо null если отсутствует. */
    private static String getShortName(String table) {
        int idx = table.indexOf(':');
        if (idx >= 0 && idx + 1 < table.length()) {
            String sn = table.substring(idx + 1);
            return sn.isEmpty() ? null : sn;
        }
        return null;
    }

    /** Добавляет исходное имя и его UPPER/lower-варианты как алиасы. */
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
     * Публикует карту колонок под несколькими алиасами имени таблицы:
     * исходное/UPPER/lower и, при наличии, «короткое» имя после двоеточия.
     * Если под алиасом уже была другая карта — пишем предупреждение.
     */
    private void putTableAliases(Map<String, Map<String, String>> result,
                                 String table,
                                 Map<String, String> normalizedCols) {
        // Используем набор уже добавленных алиасов, чтобы не повторяться и упростить логику
        Set<String> seen = new HashSet<>(6);
        // Полное имя: orig/UPPER/lower
        addAliasVariants(seen, result, table, normalizedCols, table);
        // Короткое имя (после двоеточия), если есть
        String shortName = getShortName(table);
        if (shortName != null) {
            addAliasVariants(seen, result, shortName, normalizedCols, table);
        }
    }

    /** Подготавливает (и нормализует) карту колонок; возвращает пустую map, если секция пуста/некорректна. */
    private Map<String, String> prepareColumns(String table, Map<String, Map<String, String>> sect) {
        if (sect == null) {
            LOG.warn("Для таблицы '{}' отсутствует раздел в файле схемы '{}'", table, sourcePath);
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
     * Нормализует имена колонок и типы: для каждого квалайфера публикует
     * 3 ключа (orig/UPPER/lower), тип приводится к UPPER (Locale.ROOT).
     * Некорректные записи аккуратно пропускаются с понятными WARN.
     */
    private Map<String, String> normalizeColumns(Map<String, String> cols, String table) {
        int expected = Math.max(8, cols.size() * 3);
        int cap = (expected * 4 / 3) + 1; // эквивалент 1/0.75 без float
        Map<String, String> normalized = new HashMap<>(cap);

        for (Map.Entry<String, String> e : cols.entrySet()) {
            processColumn(normalized, e.getKey(), e.getValue(), table);
        }

        return Collections.unmodifiableMap(normalized);
    }

    /**
     * Обрабатывает одну пару (qualifier, type): валидирует, логирует и
     * публикует все формы ключа в результирующую карту.
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
        if (!ALLOWED_TYPES.contains(tname)) {
            LOG.warn("Неизвестный тип '{}' для колонки '{}' в таблице '{}' (файл '{}') — будет использован как есть",
                    tname, qTrim, table, sourcePath);
        }

        putAllForms(out, qTrim, tname);
    }

    /**
     * Кладёт в map три формы ключа: исходную, UPPER и lower — без перезаписи
     * уже существующих значений.
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

    /** Возвращает карту колонок для указанной таблицы, учитывая короткое имя после ':'. */
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
     * Возвращает тип колонки (как канонизированную строку PHOENIX_TYPE_NAME)
     * по имени таблицы и имени квалайфера.
     * 
     * Поиск выполняется с использованием небольшого кэша {TableName -> columns}.
     * Если таблица не найдена в реестре — возвращает {@code null}.
     *
     * @param table     объект {@link TableName}
     * @param qualifier имя колонки (в любом регистре)
     * @return строковое имя типа или {@code null}, если не найдено
     */
    @Override
    public String columnType(TableName table, String qualifier) {
        if (qualifier == null || table == null) return null;

        final Map<String, String> cols = tableCache.computeIfAbsent(table, this::resolveColumnsForTable);

        if (cols.isEmpty()) return null;
        return cols.get(qualifier);
    }

    /**
     * Краткая диагностика: размеры схемы и кэша. Используйте в DEBUG-логах.
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