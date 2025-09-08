package kz.qazmarka.h2k.schema;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.hbase.TableName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Юнит-тесты для интерфейса {@link SchemaRegistry} (дефолт-методы).
 *
 * Назначение:
 *  • Зафиксировать поведение «релаксированного» поиска типов колонок: exact → UPPER → lower.
 *  • Проверить контракты orDefault/has-вариантов и фабрики {@link SchemaRegistry#empty()}.
 *  • Проверить fail-fast валидацию аргументов (NPE) в default-методах.
 *
 * Технические детали:
 *  • Используется лёгкая in-memory реализация реестра на основе HashMap без IO.
 *  • JUnit 5 (Jupiter), совместим с Java 8 (без List.of/var и т.п.).
 */
class SchemaRegistryTest {

    /** Условная таблица (namespace:table) для тестов. */
    private static final TableName TBL = TableName.valueOf("DEFAULT:TBL_JTI_TRACE_CIS_HISTORY");

    @BeforeAll
    static void quietLogging() {
        // На некоторых стендах log4j может пытаться писать в корень — подстрахуемся.
        System.setProperty("h2k.log.dir", "target");
    }

    @Test
    @DisplayName("empty(): всегда null/false")
    void emptyRegistry() {
        SchemaRegistry r = SchemaRegistry.empty();
        assertNull(r.columnType(TBL, "any"));
        assertNull(r.columnTypeRelaxed(TBL, "any"));
        assertFalse(r.hasColumn(TBL, "any"));
        assertFalse(r.hasColumnRelaxed(TBL, "any"));
        assertEquals("VARCHAR", r.columnTypeOrDefault(TBL, "x", "VARCHAR"));
        assertEquals("VARCHAR", r.columnTypeOrDefaultRelaxed(TBL, "x", "VARCHAR"));

        // refresh — no-op
        r.refresh();
    }

    @Test
    @DisplayName("columnTypeOrDefault: возвращает default при отсутствии типа")
    void orDefault() {
        SchemaRegistry r = mapRegistry()
                .put("DEFAULT:TBL_JTI_TRACE_CIS_HISTORY", "id", "VARCHAR")
                .build();

        assertEquals("VARCHAR", r.columnTypeOrDefault(TBL, "id", "CHAR"));
        assertEquals("CHAR", r.columnTypeOrDefault(TBL, "missing", "CHAR"));
    }

    @Test
    @DisplayName("columnTypeRelaxed: exact/UPPER/lower поиск без лишних аллокаций")
    void relaxedLookup() {
        SchemaRegistry r = mapRegistry()
                // В карте лежат типы с разным регистром ключей:
                .put("DEFAULT:TBL_JTI_TRACE_CIS_HISTORY", "MiXeD", "TIMESTAMP") // exact
                .put("DEFAULT:TBL_JTI_TRACE_CIS_HISTORY", "FOO",   "INT")       // UPPER
                .put("DEFAULT:TBL_JTI_TRACE_CIS_HISTORY", "bar",   "BIGINT")    // lower
                .build();

        // exact
        assertEquals("TIMESTAMP", r.columnTypeRelaxed(TBL, "MiXeD"));

        // приход "foo" → пробует UPPER("FOO")
        assertEquals("INT", r.columnTypeRelaxed(TBL, "foo"));

        // приход "BAR" → пробует lower("bar")
        assertEquals("BIGINT", r.columnTypeRelaxed(TBL, "BAR"));

        // не найдено ни в одном регистре
        assertNull(r.columnTypeRelaxed(TBL, "absent"));
    }

    @Test
    @DisplayName("hasColumn / hasColumnRelaxed: true/false в ожидаемых случаях")
    void hasChecks() {
        SchemaRegistry r = mapRegistry()
                .put("DEFAULT:TBL_JTI_TRACE_CIS_HISTORY", "X", "VARCHAR")
                .build();

        // exact-версия — только точное совпадение
        assertTrue(r.hasColumn(TBL, "X"));
        assertFalse(r.hasColumn(TBL, "x"));

        // relaxed — найдёт через UPPER
        assertTrue(r.hasColumnRelaxed(TBL, "x"));
        assertFalse(r.hasColumnRelaxed(TBL, "missing"));
    }

    @Test
    @DisplayName("columnTypeOrDefaultRelaxed: default при отсутствии")
    void orDefaultRelaxed() {
        SchemaRegistry r = mapRegistry().build();
        assertEquals("VARCHAR", r.columnTypeOrDefaultRelaxed(TBL, "nope", "VARCHAR"));
    }

    @Test
    @DisplayName("NPE: все default-методы валидируют аргументы (fail-fast)")
    void nullChecks() {
        SchemaRegistry r = SchemaRegistry.empty();

        NullPointerException e1 = assertThrows(NullPointerException.class,
                () -> r.columnTypeOrDefault(null, "q", "V"));
        assertNotNull(e1.getMessage());

        NullPointerException e2 = assertThrows(NullPointerException.class,
                () -> r.columnTypeOrDefault(TBL, null, "V"));
        assertNotNull(e2.getMessage());

        NullPointerException e3 = assertThrows(NullPointerException.class,
                () -> r.columnTypeOrDefault(TBL, "q", null));
        assertNotNull(e3.getMessage());

        NullPointerException e4 = assertThrows(NullPointerException.class,
                () -> r.columnTypeOrDefaultRelaxed(null, "q", "V"));
        assertNotNull(e4.getMessage());

        NullPointerException e5 = assertThrows(NullPointerException.class,
                () -> r.columnTypeOrDefaultRelaxed(TBL, null, "V"));
        assertNotNull(e5.getMessage());

        NullPointerException e6 = assertThrows(NullPointerException.class,
                () -> r.columnTypeOrDefaultRelaxed(TBL, "q", null));
        assertNotNull(e6.getMessage());

        NullPointerException e7 = assertThrows(NullPointerException.class,
                () -> r.columnTypeRelaxed(null, "q"));
        assertNotNull(e7.getMessage());

        NullPointerException e8 = assertThrows(NullPointerException.class,
                () -> r.columnTypeRelaxed(TBL, null));
        assertNotNull(e8.getMessage());

        NullPointerException e9 = assertThrows(NullPointerException.class,
                () -> r.hasColumnRelaxed(null, "q"));
        assertNotNull(e9.getMessage());

        NullPointerException e10 = assertThrows(NullPointerException.class,
                () -> r.hasColumnRelaxed(TBL, null));
        assertNotNull(e10.getMessage());
    }

    /* ======================== вспомогательная in-memory реализация ======================== */

    /**
     * Простейший map-бэкенд: ищет тип только по exact-ключу (без нормализации регистра).
     * Дефолт-методы интерфейса поверх него проверяют relaxed-логику.
     */
    private static class MapBackedRegistry implements SchemaRegistry {
        private final Map<String, String> map;

        MapBackedRegistry(Map<String, String> map) {
            this.map = map;
        }

        @Override
        public String columnType(TableName table, String qualifier) {
            return map.get(key(table, qualifier));
        }

        static String key(TableName t, String q) {
            // у TableName.getNameAsString() уже содержит namespace:table
            return t.getNameAsString() + "|" + q;
        }
    }

    /** builder для удобного наполнения тестового реестра. */
    private static class MapRegistryBuilder {
        private final Map<String, String> map = new HashMap<>();

        MapRegistryBuilder put(String table, String qualifier, String type) {
            map.put(table + "|" + qualifier, type);
            return this;
        }

        SchemaRegistry build() {
            return new MapBackedRegistry(map);
        }
    }

    private static MapRegistryBuilder mapRegistry() {
        return new MapRegistryBuilder();
    }

    @Test
    @DisplayName("Relaxed-приоритет: exact > UPPER > lower при одновременном наличии")
    void relaxedPriorityOrder() {
        SchemaRegistry r = mapRegistry()
                .put("DEFAULT:TBL_JTI_TRACE_CIS_HISTORY", "baz", "LOW")     // lower
                .put("DEFAULT:TBL_JTI_TRACE_CIS_HISTORY", "BAZ", "UP")      // UPPER
                .put("DEFAULT:TBL_JTI_TRACE_CIS_HISTORY", "BaZ", "EXACT")   // exact
                .build();

        // exact должен побеждать
        assertEquals("EXACT", r.columnTypeRelaxed(TBL, "BaZ"));

        // Здесь exact есть для "baz" в lower → по правилу exact > UPPER > lower берём его (LOW)
        SchemaRegistry r2 = mapRegistry()
                .put("DEFAULT:TBL_JTI_TRACE_CIS_HISTORY", "baz", "LOW")   // lower (exact для входа "baz")
                .put("DEFAULT:TBL_JTI_TRACE_CIS_HISTORY", "BAZ", "UP")    // UPPER
                .build();
        assertEquals("LOW", r2.columnTypeRelaxed(TBL, "baz"));
    }

    @Test
    @DisplayName("hasColumn: NPE на null table/qualifier (fail-fast)")
    void hasExactNullChecks() {
        SchemaRegistry r = SchemaRegistry.empty();
        NullPointerException he1 = assertThrows(NullPointerException.class, () -> r.hasColumn(null, "q"));
        assertNotNull(he1.getMessage());
        NullPointerException he2 = assertThrows(NullPointerException.class, () -> r.hasColumn(TBL, null));
        assertNotNull(he2.getMessage());
    }

    @Test
    @DisplayName("columnTypeOrDefaultRelaxed: при наличии exact (lower) возвращает его (а не default)")
    void orDefaultRelaxedPrefersUpper() {
        SchemaRegistry r = mapRegistry()
                .put("DEFAULT:TBL_JTI_TRACE_CIS_HISTORY", "FOO", "INT")
                .put("DEFAULT:TBL_JTI_TRACE_CIS_HISTORY", "foo", "BIGINT")
                .build();

        // В реестре есть exact (lower) для "foo" → вернуть его, а не default
        assertEquals("BIGINT", r.columnTypeOrDefaultRelaxed(TBL, "foo", "DECIMAL"));
    }

    @Test
    @DisplayName("Параллельный relaxed‑поиск: без исключений и с ожидаемыми результатами")
    void relaxedLookupConcurrencySmoke() throws InterruptedException {
        // Подготовим реестр с множеством кейсов по регистру
        MapRegistryBuilder b = mapRegistry();
        for (int i = 0; i < 100; i++) {
            String base = "K" + i;
            b.put(TBL.getNameAsString(), base, "UP_" + i);          // UPPER
            b.put(TBL.getNameAsString(), base.toLowerCase(), "LO_" + i); // lower
        }
        final SchemaRegistry r = b.build();

        final int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);
        AtomicReference<Throwable> firstError = new AtomicReference<>();

        for (int t = 0; t < threads; t++) {
            final int offset = t;
            pool.execute(() -> {
                try {
                    for (int j = 0; j < 500; j++) {
                        int i = (j + offset) % 100;
                        String qLower = ("K" + i).toLowerCase();
                        String qUpper = ("K" + i);

                        // relaxed для нижнего регистра → должен выбрать lower-вариант
                        assertEquals("LO_" + i, r.columnTypeRelaxed(TBL, qLower));
                        assertTrue(r.hasColumnRelaxed(TBL, qLower));

                        // relaxed для верхнего регистра → exact сразу
                        assertEquals("UP_" + i, r.columnTypeRelaxed(TBL, qUpper));
                        assertTrue(r.hasColumnRelaxed(TBL, qUpper));
                    }
                } catch (Throwable e) {
                    firstError.compareAndSet(null, e);
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        pool.shutdownNow();

        if (firstError.get() != null) {
            // Перебрасываем первую ошибку, чтобы тест корректно упал с причиной
            if (firstError.get() instanceof AssertionError) {
                throw (AssertionError) firstError.get();
            }
            fail(firstError.get());
        }
    }
    @Test
    @DisplayName("Smoke: quietLogging() is referenced explicitly (harmless)")
    void _quietLogging_isReferenced_smoke() {
        // Повторный вызов безвреден: метод идемпотентен (устанавливает системное свойство).
        quietLogging();
        // Минимальная проверка: системное свойство действительно установлено ожидаемым образом
        assertEquals("target", System.getProperty("h2k.log.dir"));
    }
}