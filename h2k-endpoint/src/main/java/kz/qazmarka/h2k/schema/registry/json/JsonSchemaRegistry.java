package kz.qazmarka.h2k.schema.registry.json;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.hbase.TableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import kz.qazmarka.h2k.schema.registry.SchemaRegistry;

/**
 * JsonSchemaRegistry — реестр типов колонок, загружаемый из JSON-файла.
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
 *  - чтение потокобезопасно без синхронизации: используется {@link AtomicReference}
 *    для публикации корня и локальный {@link java.util.concurrent.ConcurrentMap}‑кэш по таблицам.
 *  - типы массивов Phoenix (например, "VARCHAR ARRAY") считаются корректными и не генерируют предупреждений;
 *  - default-namespace можно опускать: записи вида "DEFAULT:TBL" и "TBL" будут доступны при поиске.
 *  - секция {@code "pk": [...] } (необязательно) может содержать имена колонок PK; метод
 *    {@link #primaryKeyColumns(org.apache.hadoop.hbase.TableName)} вернёт их в порядке следования;
 *    при отсутствии секции возвращается пустой массив.
 *  - имена ключей PK не переименовываются реестром — публикуются ровно такие, как указаны в схеме (с учётом нормализации регистра имён, описанной выше);
 *  - нормализация временных типов Phoenix (TIMESTAMP/DATE/TIME) — обязанность реализации декодера значений, а не реестра типов; в проекте это делает {@code ValueCodecPhoenix}.
 *  - контракт аргументов: методы API требуют не-null параметров table/qualifier; при null выбрасывается {@link NullPointerException}.
 *  - реестр не выполняет глобальных эвристик — только поиск по паре (table, qualifier) с нормализацией регистра и поддержкой короткого имени (после ':').
 *
 * Совместимость
 *  - Требуется Java 8.
 */
public final class JsonSchemaRegistry implements SchemaRegistry {

    /** Логгер реестра; все сообщения — на русском языке. */
    private static final Logger LOG = LoggerFactory.getLogger(JsonSchemaRegistry.class);
    /** Ключ JSON-секции с определениями колонок. */
    private static final String KEY_COLUMNS = "columns";
    /** Ключ JSON-секции с определением PK (массив строк); поддерживаются варианты регистра "pk"/"PK". */
    private static final String KEY_PK = "pk";


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
        // Массивы валидируем отдельно по правилу "<SCALAR> ARRAY" (см. isAllowedType)
        ALLOWED_TYPES = Collections.unmodifiableSet(s);
    }

    /** Суффикс обозначения массивов Phoenix (используется в {@link #isAllowedType(String)}); всегда в верхнем регистре. */
    private static final String ARRAY_SUFFIX = " ARRAY";

    /**
     * Множество уже залогированных «неизвестных» типов, чтобы не засорять WARN-ами.
     * Используется только для подавления повторных предупреждений и не влияет на функциональность.
     */
    private static final Set<String> UNKNOWN_TYPES_LOGGED = ConcurrentHashMap.newKeySet();

    /**
     * Флаг «уже логировали» для версии Phoenix.
     * Логируем версию Phoenix ровно один раз за процесс, чтобы не дублировать сообщения в логах.
     */
    private static final AtomicBoolean PHOENIX_LOGGED =
            new AtomicBoolean(false);

    /**
     * Проверяет, является ли указанное строковое представление типа допустимым.
     *
     * Допускаются:
     *  - скалярные типы из белого списка ({@link #ALLOWED_TYPES});
     *  - массивы Phoenix в форме {@literal <SCALAR> ARRAY} — регистр не учитывается.
     *
     * @param tname исходное имя типа (может быть {@code null} или с лишними пробелами)
     * @return {@code true}, если тип допустим; иначе {@code false}
     */
    private static boolean isAllowedType(String tname) {
        if (tname == null) {
            return false;
        }
        // Нормализуем регистр один раз для регистронезависимой обработки массивов
        String upper = tname.trim().toUpperCase(Locale.ROOT);
        if (upper.isEmpty()) {
            return false;
        }
        // Массивы: допускаем форму "<SCALAR> ARRAY" (регистр суффикса игнорируется за счёт нормализации)
        if (upper.endsWith(ARRAY_SUFFIX)) {
            String base = upper.substring(0, upper.length() - ARRAY_SUFFIX.length()).trim();
            return ALLOWED_TYPES.contains(base);
        }
        return ALLOWED_TYPES.contains(upper);
    }

    /**
     * Абсолютный или относительный путь к JSON-файлу схемы — источник для {@link #refresh()}.
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

    /** Публикация алиасов таблицы → массив имён PK (иммутабельно). */
    private final AtomicReference<Map<String, String[]>> pkRef = new AtomicReference<>();

    /** Версия опубликованной схемы; инкрементируется при каждом refresh(). */
    private final AtomicLong schemaVersion =
            new AtomicLong(0L);

    /**
     * Запись для кэша колонок с привязкой к версии опубликованной схемы.
     *
     * Используется для того, чтобы отличать актуальные снэпшоты от устаревших при конкурентных
     * вызовах {@link #refresh()}: если версия в кэше отличается от текущей, запись считается
     * устаревшей и переинициализируется.
     */
    private static final class VersionedCols {
        /** Версия схемы на момент помещения записи в кэш. */
        final long ver;
        /** Неизменяемая карта «квалификатор → тип Phoenix» для конкретной таблицы. */
        final Map<String, String> cols;
        /**
         * Создаёт новую версионированную запись кэша.
         *
         * @param ver  версия опубликованной схемы
         * @param cols неизменяемая карта колонок (квалификатор → тип)
         */
        VersionedCols(long ver, Map<String, String> cols) {
            this.ver = ver;
            this.cols = cols;
        }
    }
    /**
     * Кэш колонок с версионированием, чтобы отсекать устаревшие записи при конкурентном refresh().
     */
    private final ConcurrentMap<String, VersionedCols> tableCache = new ConcurrentHashMap<>(64);

    /**
     * Создаёт реестр и сразу выполняет {@link #refresh()}.
     * @param path путь к JSON-файлу (UTF-8), не {@code null}
     */
    public JsonSchemaRegistry(String path) {
        if (path == null) {
            throw new NullPointerException("Аргумент 'schemaPath' (путь к JSON‑схеме) не может быть null");
        }
        this.sourcePath = path;
        this.schemaRef = new AtomicReference<>(Collections.emptyMap());
        this.pkRef.set(Collections.emptyMap());
        refresh();
    }

    /**
     * Переинициализирует реестр из {@link #sourcePath}.
     * Атомарно публикует новые иммутабельные структуры и очищает кэш таблиц.
     * При любой ошибке остаётся предыдущий снимок.
     */
    @Override
    public void refresh() {
        final Map<String, Map<String, String>> oldCols = schemaRef.get();
        final Map<String, String[]> oldPk = pkRef.get();
        logPhoenixRuntimeIfAvailable();
        try {
            ParsedSchema parsed = parseFromDisk(sourcePath);
            UNKNOWN_TYPES_LOGGED.clear();
            schemaRef.set(parsed.columnsByTable);
            pkRef.set(parsed.pkByTable);
            // Обновляем версию и очищаем кэш. Предзагрузку не выполняем:
            // кэш наполняется лениво при первых обращениях (без кеширования промахов).
            schemaVersion.incrementAndGet();
            tableCache.clear();
        } catch (IOException | RuntimeException e) {
            if (LOG.isDebugEnabled()) {
                LOG.warn("Не удалось загрузить файл схемы '{}'; сохранён предыдущий снимок", sourcePath, e);
            } else {
                LOG.warn("Не удалось загрузить файл схемы '{}'; сохранён предыдущий снимок ({}: {})",
                         sourcePath, e.getClass().getSimpleName(), e.getMessage());
            }
            schemaRef.set(oldCols);
            pkRef.set(oldPk);
        }
    }

    /**
     * Внутренний снимок разобранной схемы.
     * Содержит иммутабельные представления колонок и PK, публикуемые атомарно.
     */
    private static final class ParsedSchema {
        final Map<String, Map<String, String>> columnsByTable;
        final Map<String, String[]> pkByTable;
        /**
         * Создаёт неизменяемый снимок разобранной схемы.
         *
         * @param c иммутабельная карта «алиас таблицы → карта колонок»
         * @param p иммутабельная карта «алиас таблицы → массив имён PK»
         */
        ParsedSchema(Map<String, Map<String, String>> c, Map<String, String[]> p) {
            this.columnsByTable = c; this.pkByTable = p;
        }
    }

    /**
     * Читает JSON-файл и возвращает корневой объект.
     * @param path путь к файлу (UTF-8)
     * @return корневой {@code JsonObject}
     * @throws IOException при ошибке чтения или парсинга
     */
    private JsonObject readRootJson(String path) throws IOException {
        final byte[] bytes = Files.readAllBytes(Paths.get(path));
        String json = new String(bytes, StandardCharsets.UTF_8);
        json = stripUtf8Bom(json);

        final JsonElement el = JsonParser.parseString(json);
        if (!el.isJsonObject()) {
            throw new IOException("Корень JSON схемы должен быть объектом, но получено: " + el.getClass().getSimpleName());
        }
        return el.getAsJsonObject();
    }

    /**
     * Удаляет префикс BOM (U+FEFF) из начала строки в кодировке UTF‑8, если он присутствует.
     *
     * @param s исходная строка (может быть {@code null})
     * @return строка без BOM в начале; исходная строка без изменений, если BOM отсутствует; {@code null}, если вход {@code null}
     */
    private static String stripUtf8Bom(String s) {
        if (s != null && !s.isEmpty() && s.charAt(0) == '\uFEFF') {
            return s.substring(1);
        }
        return s;
    }

    /** Логирует версию Phoenix (если драйвер доступен на classpath). Выполняется один раз. */
    private static void logPhoenixRuntimeIfAvailable() {
        if (PHOENIX_LOGGED.getAndSet(true)) {
            return; // уже логировали
        }
        try {
            Class<?> drv = Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            Package p = drv.getPackage();
            String impl = (p != null ? p.getImplementationVersion() : "unknown");
            LOG.info("Версия Phoenix во время выполнения: {}", impl);
        } catch (ClassNotFoundException e) { // драйвер отсутствует (scope=provided) — это штатно
            if (LOG.isDebugEnabled()) {
                LOG.debug("Версия Phoenix недоступна: {}", e.toString());
            }
        }
    }

    /**
     * Достаёт значение поля без учёта регистра из набора возможных имён (возвращает первое совпадение).
     *
     * @param obj   объект JSON, из которого выполняется чтение (не {@code null})
     * @param names список возможных имён поля; проверяются в указанном порядке
     * @return найденный {@link JsonElement} или {@code null}, если ни одно имя не присутствует
     */
    private static JsonElement ciGet(JsonObject obj, String... names) {
        for (String n : names) {
            if (obj.has(n)) return obj.get(n);
        }
        return null;
    }

    /**
     * Безопасно приводит значение JSON к объекту.
     *
     * @param el элемент JSON (может быть {@code null})
     * @return {@link JsonObject}, если элемент является объектом; иначе {@code null}
     */
    private static JsonObject asObject(JsonElement el) {
        return (el != null && el.isJsonObject()) ? el.getAsJsonObject() : null;
    }

    /**
     * Извлекает строковое значение из {@link JsonElement}.
     *
     * @param v           элемент JSON
     * @param onTypeError необязательное действие, вызываемое при ошибке приведения типа
     * @return строка или {@code null}, если значение отсутствует/не является строкой
     */
    private static String extractString(JsonElement v, Runnable onTypeError) {
        if (v == null || v.isJsonNull()) return null;
        if (v.isJsonPrimitive()) {
            try {
                return v.getAsString();
            } catch (RuntimeException ex) {
                if (onTypeError != null) onTypeError.run();
                return null;
            }
        }
        if (onTypeError != null) onTypeError.run();
        return null;
    }

    /**
     * Нормализует строку типа к верхнему регистру (Locale.ROOT).
     *
     * @param raw исходная строка типа (может быть {@code null})
     * @return канонизированная строка или {@code null} для пустых/пробельных значений
     */
    private static String normalizeType(String raw) {
        if (raw == null) return null;
        String t = raw.trim().toUpperCase(Locale.ROOT);
        return t.isEmpty() ? null : t;
    }

    /**
     * Безопасно приводит строку к нижнему регистру (Locale.ROOT).
     *
     * @param s исходная строка (может быть {@code null})
     * @return строка в нижнем регистре или {@code null}, если вход равен {@code null}
     */
    private static String lower(String s) {
        return (s == null) ? null : s.toLowerCase(Locale.ROOT);
    }

    /**
     * Добавляет одно имя PK-колонки в аккумулятор после {@code trim()} и приведения к lower-case.
     *
     * @param v   исходное значение
     * @param out список-аккумулятор (не {@code null})
     */
    private static void addPkValue(String v, ArrayList<String> out) {
        if (v == null) return;
        String q = v.trim();
        if (!q.isEmpty()) out.add(q.toLowerCase(Locale.ROOT));
    }

    /**
     * Разбирает секцию {@code "columns"}: нормализует квалификаторы в lower-case и типы в UPPER.
     * Неизвестные типы допускаются, но логируются как предупреждение один раз на тип.
     *
     * @param tableObj объект таблицы из корневого JSON
     * @param tableKey исходный ключ таблицы (для диагностики)
     * @param path     путь к файлу схемы (для диагностики)
     * @return изменяемая карта «квалификатор(lower-case) → тип(UPPER или {@code null})»
     */
    private Map<String, String> parseColumns(JsonObject tableObj, String tableKey, String path) {
        Map<String, String> cols = new HashMap<>();
        JsonObject colsObj = asObject(ciGet(tableObj, KEY_COLUMNS, "COLUMNS", "Columns"));
        if (colsObj == null) {
            return cols;
        }
        for (Map.Entry<String, JsonElement> c : colsObj.entrySet()) {
            final String qLower = lower(c.getKey());
            final String rawType = extractString(c.getValue(), () ->
                    LOG.warn("Тип для колонки '{}' в таблице '{}' (файл '{}') должен быть строкой — пропускается",
                            c.getKey(), tableKey, path)
            );
            final String tUpper = normalizeType(rawType);
            if (tUpper != null
                    && !isAllowedType(tUpper)
                    && UNKNOWN_TYPES_LOGGED.add(tUpper)) {
                LOG.warn("Неизвестный тип '{}' для колонки '{}' в таблице '{}' (файл '{}') — будет использован как есть",
                        tUpper, c.getKey(), tableKey, path);
            }
            cols.put(qLower, tUpper);
        }
        return cols;
    }

    /**
     * Разбирает секцию {@code "pk"/"PK"} и возвращает массив имён PK.
     * Поддерживает массив строк или одиночное строковое поле.
     *
     * @param tableObj объект таблицы
     * @return массив имён PK в нижнем регистре; пустой массив (не {@code null}) при отсутствии секции
     */
    private String[] parsePk(JsonObject tableObj) {
        JsonElement pkEl = ciGet(tableObj, KEY_PK, "PK", "Pk", "pK");
        if (pkEl == null || pkEl.isJsonNull()) {
            return new String[0];
        }

        ArrayList<String> tmp = new ArrayList<>();
        if (pkEl.isJsonArray()) {
            JsonArray a = pkEl.getAsJsonArray();
            for (int i = 0; i < a.size(); i++) {
                addPkValue(extractString(a.get(i), null), tmp);
            }
        } else {
            addPkValue(extractString(pkEl, null), tmp);
        }
        return tmp.toArray(new String[0]);
    }

    /**
     * Парсит JSON‑файл на диске и строит иммутабельные карты:
     *  • {@code tableAlias -> (qualifierAlias -> TYPE)}
     *  • {@code tableAlias -> String[] PK}
     * Табличные алиасы: исходное имя, UPPER, lower, а также «короткое» имя (после двоеточия), если есть.
     * Имена квалификаторов нормализуются до lower-case — один раз на парсинге.
     */
    private ParsedSchema parseFromDisk(String path) throws IOException {
        JsonObject root = readRootJson(path);
        return buildParsedSchema(root, path);
    }

    /**
     * Строит {@code ParsedSchema} из уже прочитанного корня JSON.
     * Выделен отдельно для снижения когнитивной сложности (S1199).
     * @param root корневой объект
     * @param path исходный путь (для диагностики)
     * @return иммутабельный снимок разобранной схемы
     */
    private ParsedSchema buildParsedSchema(JsonObject root, String path) {
        Map<String, Map<String, String>> colsTmp = new HashMap<>();
        Map<String, String[]> pkTmp = new HashMap<>();

        for (Map.Entry<String, JsonElement> e : root.entrySet()) {
            if (!e.getValue().isJsonObject()) {
                continue;
            }
            String tableKey = e.getKey();
            JsonObject tableObj = e.getValue().getAsJsonObject();

            /**
             * Обрабатывает одну запись таблицы из корня JSON: разбирает колонки и PK, затем публикует алиасы.
             *
             * @param colsOut  результирующая карта «алиас таблицы → карта колонок»
             * @param pkOut    результирующая карта «алиас таблицы → массив PK»
             * @param tableKey исходное имя таблицы в корне JSON
             * @param tableObj объект JSON с данными таблицы
             * @param path     путь к файлу (для диагностики)
             */
            processTableEntry(colsTmp, pkTmp, tableKey, tableObj, path);
        }
        if (colsTmp.isEmpty() && pkTmp.isEmpty()) {
            LOG.info("Файл схемы '{}' не содержит таблиц (пустая схема)", path);
        }
        return new ParsedSchema(Collections.unmodifiableMap(colsTmp),
                                Collections.unmodifiableMap(pkTmp));
    }

    /** Обрабатывает одну запись таблицы из корня JSON: парсит колонки/PK и публикует алиасы. */
    private void processTableEntry(Map<String, Map<String, String>> colsOut,
                                   Map<String, String[]> pkOut,
                                   String tableKey,
                                   JsonObject tableObj,
                                   String path) {
        Map<String, String> colsView = Collections.unmodifiableMap(parseColumns(tableObj, tableKey, path));
        String[] pk = parsePk(tableObj);
        // Строгая проверка согласованности: PK должны присутствовать среди columns
        if (pk.length > 0 && !colsView.isEmpty()) {
            for (String pkCol : pk) {
                if (!colsView.containsKey(pkCol)) {
                    LOG.warn("PK-колонка '{}' не найдена среди columns для таблицы '{}' (файл '{}')",
                             pkCol, tableKey, path);
                }
            }
        }
        publishTableAliases(colsOut, pkOut, tableKey, colsView, pk);
    }

    /**
     * Публикует алиасы таблицы: исходное/UPPER/lower и «короткое» имя (после двоеточия).
     *
     * @param colsOut  карта для публикации колонок
     * @param pkOut    карта для публикации PK
     * @param tableKey исходное имя таблицы (как в JSON)
     * @param colsView неизменяемое представление колонок
     * @param pk       массив имён PK
     */
    private static void publishTableAliases(Map<String, Map<String, String>> colsOut,
                                            Map<String, String[]> pkOut,
                                            String tableKey,
                                            Map<String, String> colsView,
                                            String[] pk) {
        /**
         * Публикует три варианта имени таблицы: исходное, UPPER и lower.
         *
         * @param colsOut  карта для публикации колонок
         * @param pkOut    карта для публикации PK
         * @param name     имя таблицы
         * @param colsView неизменяемое представление колонок
         * @param pk       массив имён PK
         */
        addAliases(colsOut, pkOut, tableKey, colsView, pk);
        int idx = tableKey.indexOf(':');
        if (idx > 0 && idx + 1 < tableKey.length()) {
            addAliases(colsOut, pkOut, tableKey.substring(idx + 1), colsView, pk);
        }
    }

    /**
     * Публикует три варианта имени таблицы: исходное, UPPER и lower.
     *
     * @param colsOut  карта для публикации соответствий «алиас таблицы → карта колонок»
     * @param pkOut    карта для публикации соответствий «алиас таблицы → массив PK»
     * @param name     имя таблицы (источник для генерации алиасов); пустое/{@code null} игнорируется
     * @param colsView неизменяемое представление колонок для публикации под всеми алиасами
     * @param pk       неизменяемый массив имён PK для публикации под всеми алиасами
     */
    private static void addAliases(Map<String, Map<String, String>> colsOut,
                                   Map<String, String[]> pkOut,
                                   String name,
                                   Map<String, String> colsView,
                                   String[] pk) {
        if (name == null || name.isEmpty()) return;
        putAlias(colsOut, pkOut, name, colsView, pk);
        String up = name.toUpperCase(Locale.ROOT);
        if (!up.equals(name)) putAlias(colsOut, pkOut, up, colsView, pk);
        String low = name.toLowerCase(Locale.ROOT);
        if (!low.equals(name) && !low.equals(up)) putAlias(colsOut, pkOut, low, colsView, pk);
    }

    /**
     * Регистрирует один алиас таблицы одновременно в обеих структурах.
     *
     * Примечание: в обе карты записываются одни и те же неизменяемые ссылки без копирования:
     * alias → {@code colsView} и alias → {@code pk}. Значения не клонируются.
     *
     * @param colsOut карта-приёмник: алиас таблицы → неизменяемая карта колонок (квалификатор → тип)
     * @param pkOut   карта-приёмник: алиас таблицы → неизменяемый массив имён PK
     * @param alias   публикуемый алиас таблицы (как есть)
     * @param colsView неизменяемое представление колонок, привязываемое к алиасу
     * @param pk      неизменяемый массив PK, привязываемый к алиасу
     */
    private static void putAlias(Map<String, Map<String, String>> colsOut,
                                 Map<String, String[]> pkOut,
                                 String alias,
                                 Map<String, String> colsView,
                                 String[] pk) {
        Map<String, String> prevCols = colsOut.put(alias, colsView);
        String[] prevPk = pkOut.put(alias, pk);
        if ((prevCols != null && prevCols != colsView) || (prevPk != null && prevPk != pk)) {
            LOG.warn("Переопределение алиаса таблицы '{}': ранее в схеме уже была другая запись", alias);
        }
    }

    /**
     * Возвращает массив имён PK‑колонок (может быть пустым, но не {@code null}).
     * Поиск выполняется по полному и «короткому» имени таблицы (часть после ':').
     * Возвращается копия массива, чтобы защитить неизменяемость снимка.
     *
     * @param table имя таблицы (не {@code null})
     * @return массив имён PK в порядке следования в схеме; пустой массив (не {@code null}), если PK не задан
     * @throws NullPointerException если {@code table} равен {@code null}
     */
    @Override
    public String[] primaryKeyColumns(TableName table) {
        if (table == null) {
            throw new NullPointerException("Аргумент 'table' (имя таблицы) не может быть null");
        }
        Map<String, String[]> local = pkRef.get();
        String raw = table.getNameAsString();
        String[] found = local.get(raw);
        if (found == null) {
            int idx = raw.indexOf(':');
            if (idx >= 0 && idx + 1 < raw.length()) {
                found = local.get(raw.substring(idx + 1));
            }
        }
        return (found != null) ? found.clone() : new String[0];
    }


    /**
     * Возвращает иммутабельную карту колонок для таблицы: сперва по полному имени,
     * затем — по «короткому» имени (часть после двоеточия).
     *
     * @param raw полное строковое имя таблицы (как возвращает {@link org.apache.hadoop.hbase.TableName#getNameAsString()})
     * @return неизменяемая карта <qualifier(lower-case), PHOENIX_TYPE_NAME> или {@link Collections#emptyMap() emptyMap()}, если таблица не найдена
     */
    private Map<String, String> resolveColumnsForTable(String raw) {
        Map<String, Map<String, String>> local = schemaRef.get();
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
     * Возвращает карту колонок для таблицы, используя кэш с версионированием.
     * Если в кэше нет актуальной записи, резолвит из текущего снимка и аккуратно размещает её в кэше.
     * Промахи (пустые карты) в кэш не записываются.
     *
     * @param raw        полное строковое имя таблицы
     * @param currentVer текущая версия опубликованной схемы
     * @param vc         текущее значение из кэша (может быть {@code null})
     * @return неизменяемая карта колонок; {@link Collections#emptyMap() emptyMap()}, если таблица не найдена
     */
    private Map<String, String> getColumnsWithCache(String raw, long currentVer, VersionedCols vc) {
        // 1) Попытка хита в кэше
        if (vc != null && vc.ver == currentVer) {
            return vc.cols;
        }

        // 2) Резолвим из опубликованного снимка
        Map<String, String> resolved = resolveColumnsForTable(raw);
        if (resolved.isEmpty()) {
            // Промахи не кэшируем — после refresh() таблица может появиться
            return Collections.emptyMap();
        }

        VersionedCols fresh = new VersionedCols(currentVer, resolved);

        // 3) Публикуем в кэш (без лишних аллокаций/копий)
        if (vc == null) {
            VersionedCols prev = tableCache.putIfAbsent(raw, fresh);
            if (prev == null) {
                return fresh.cols;
            }
            if (prev.ver == currentVer) {
                return prev.cols;
            }
            return fresh.cols;
        } else {
            boolean replaced = tableCache.replace(raw, vc, fresh);
            if (replaced) {
                return fresh.cols;
            }
            // Кто-то другой уже заменил запись: используем актуальную, если она той же версии
            VersionedCols after = tableCache.get(raw);
            if (after != null && after.ver == currentVer) {
                return after.cols;
            }
            // В сомнительных случаях возвращаем свежеразрешённую карту (она точно соответствует currentVer)
            return fresh.cols;
        }
    }

    /**
     * Возвращает строковый тип колонки (PHOENIX_TYPE_NAME) по таблице и имени колонки.
     * Поиск по lower-case ключу (квалификатор нормализуется к lower-case).
     *
     * @return тип как строка или {@code null}, если не найдено
     * @throws NullPointerException если любой из параметров равен {@code null}
     */
    @Override
    public String columnType(TableName table, String qualifier) {
        if (table == null) {
            throw new NullPointerException("Аргумент 'table' (имя таблицы) не может быть null");
        }
        if (qualifier == null) {
            throw new NullPointerException("Аргумент 'qualifier' (имя колонки) не может быть null");
        }

        final String raw = table.getNameAsString();
        final long currentVer = schemaVersion.get();

        // Пытаемся получить из кэша, при необходимости — резолвим и публикуем
        VersionedCols vc = tableCache.get(raw);
        Map<String, String> cols = getColumnsWithCache(raw, currentVer, vc);

        if (cols.isEmpty()) {
            return null;
        }
        return cols.get(qualifier.toLowerCase(Locale.ROOT));
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
