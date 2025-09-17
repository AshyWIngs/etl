package kz.qazmarka.h2k.schema;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.TableName;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PTime;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.schema.types.PVarbinary;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

/**
 * Консолидированный набор юнит-тестов для {@link ValueCodecPhoenix}.
 *
 * Цели:
 *  1) Позитивные сценарии: базовые типы и null-path.
 *  2) Негативные сценарии: строгие проверки длины для фиксированных Phoenix-типов; NPE на обязательных аргументах.
 *  3) Вспомогательные алгоритмы: нормализация имён типов и конвертация массивов в List.
 *
 * Ограничения:
 *  - Не кодируем бинарный формат Phoenix для TIMESTAMP/INT/etc. (это задача интеграционных тестов Phoenix).
 *    Для позитивных сценариев используем безопасные типы (VARCHAR) и null-path.
 *  - Для проверки массивов используем прямой вызов приватной утилиты через рефлексию (минимум аллокаций).
 */
class ValueCodecPhoenixTest {

    private static final TableName TBL = TableName.valueOf("DEFAULT:UT");

    /** Простой фейковый реестр типов: отдаёт типы по имени колонки, null — если неизвестно. */
    static final class FakeRegistry implements SchemaRegistry {
        private final java.util.Map<String, String> map = new java.util.HashMap<>();

        FakeRegistry with(String qualifier, String type) {
            map.put(qualifier, type);
            return this;
        }

        @Override
        public String columnType(TableName table, String qualifier) {
            return map.get(qualifier);
        }
    }

    // --- Позитивные сценарии

    @Nested
    @DisplayName("Позитивные сценарии")
    class Positive {

        @Test
        @DisplayName("decode: VARCHAR → String, а value==null → null")
        void decode_varchar_and_null_ok() {
            ValueCodecPhoenix vc = new ValueCodecPhoenix(new FakeRegistry().with("A", "VARCHAR"));
            byte[] bytes = "hello".getBytes(StandardCharsets.UTF_8);

            Object s1 = vc.decode(TBL, "A", bytes);
            assertEquals("hello", s1);

            Object s2 = vc.decode(TBL, "A", null);
            assertNull(s2, "null-значение должно возвращать null без попытки декодирования");
        }

        @Test
        @DisplayName("unknown type → fallback к VARCHAR и декодирование строки")
        void unknown_type_fallback_varchar() {
            ValueCodecPhoenix vc = new ValueCodecPhoenix(new FakeRegistry()); // нет записи для A → unknown → VARCHAR
            byte[] bytes = "абв".getBytes(StandardCharsets.UTF_8);

            Object s = vc.decode(TBL, "A", bytes);
            assertEquals("абв", s);
        }
    }

    // --- Негативные сценарии

    @Nested
    @DisplayName("Негативные сценарии")
    class Negative {

        @Test
        @DisplayName("NPE при null table/qualifier (контракт)")
        void npe_on_null_args() {
            ValueCodecPhoenix vc = new ValueCodecPhoenix(new FakeRegistry().with("A", "VARCHAR"));
            byte[] bytes = "x".getBytes(StandardCharsets.UTF_8);

            NullPointerException e1 = assertThrows(NullPointerException.class, () -> vc.decode(null, "A", bytes));
            NullPointerException e2 = assertThrows(NullPointerException.class, () -> ((Decoder) vc).decode(TBL, (String) null, bytes));
            assertNotNull(e1);
            assertNotNull(e2);
        }

        @Test
        @DisplayName("UNSIGNED_INT: несоответствие длины байтов (ожидается 4)")
        void fixed_unsigned_int_wrong_length() {
            ValueCodecPhoenix vc = new ValueCodecPhoenix(new FakeRegistry().with("N", "UNSIGNED_INT"));
            byte[] three = new byte[] {1,2,3}; // 3 байта вместо 4

            IllegalStateException ex = assertThrows(IllegalStateException.class, () -> vc.decode(TBL, "N", three));
            String msg = ex.getMessage();
            assertTrue(msg.contains("ожидает 4"), "Диагностика должна указывать ожидаемый размер: " + msg);
        }

        @Test
        @DisplayName("UNSIGNED_LONG: несоответствие длины байтов (ожидается 8)")
        void fixed_unsigned_long_wrong_length() {
            ValueCodecPhoenix vc = new ValueCodecPhoenix(new FakeRegistry().with("N8", "UNSIGNED_LONG"));
            byte[] wrong = new byte[] {1,2,3,4,5,6,7}; // 7 байт

            IllegalStateException ex = assertThrows(IllegalStateException.class, () -> vc.decode(TBL, "N8", wrong));
            assertTrue(ex.getMessage().contains("ожидает 8"));
        }
    }

    // --- Вспомогательные алгоритмы (частные методы через рефлексию)

    @Nested
    @DisplayName("Нормализация имён типов (normalizeTypeName)")
    class NormalizeTypeName {

        @Test
        @DisplayName("VARCHAR(100) / DECIMAL(10,2) / ARRAY<T> / T[] / подчёркивания / пробелы")
        void normalize_various_forms() throws Exception {
            Method m = ValueCodecPhoenix.class.getDeclaredMethod("normalizeTypeName", String.class);
            m.setAccessible(true);

            assertEquals("VARCHAR", invokeNorm(m, "VARCHAR(100)"));
            assertEquals("DECIMAL", invokeNorm(m, "DECIMAL(10,2)"));
            assertEquals("VARCHAR ARRAY", invokeNorm(m, "ARRAY<VARCHAR>"));
            assertEquals("INTEGER ARRAY", invokeNorm(m, "INTEGER[]"));
            assertEquals("UNSIGNED INT", invokeNorm(m, "UNSIGNED_INT"));
            assertEquals("UNSIGNED INT", invokeNorm(m, "  unsigned   int  "));
            assertEquals("VARCHAR", invokeNorm(m, null));
            assertEquals("VARCHAR", invokeNorm(m, ""));
        }

        private String invokeNorm(Method m, String in) throws InvocationTargetException, IllegalAccessException {
            return (String) m.invoke(null, in);
        }
    }

    @Nested
    @DisplayName("Конвертация массивов в List (toListFromRawArray)")
    class ArrayConversion {

        @Test
        @DisplayName("Object[] → List без копий")
        void object_array_to_list() throws Exception {
            Method m = ValueCodecPhoenix.class.getDeclaredMethod("toListFromRawArray", Object.class);
            m.setAccessible(true);

            Object[] src = new Object[] {"a", "b", "c"};
            @SuppressWarnings("unchecked")
            List<Object> out = (List<Object>) m.invoke(null, (Object) src);

            assertEquals(Arrays.asList("a","b","c"), out);
            assertEquals(3, out.size());
        }

        @Test
        @DisplayName("Примитивные массивы → List с боксингом")
        void primitive_arrays_boxed() throws Exception {
            Method m = ValueCodecPhoenix.class.getDeclaredMethod("toListFromRawArray", Object.class);
            m.setAccessible(true);

            List<Object> ints = invokeList(m, new int[] {1,2,3});
            List<Object> longs = invokeList(m, new long[] {4L,5L});
            List<Object> bools = invokeList(m, new boolean[] {true,false});
            List<Object> empty = invokeList(m, new byte[0]);

            assertEquals(Arrays.asList(1,2,3), ints);
            assertEquals(Arrays.asList(4L,5L), longs);
            assertEquals(Arrays.asList(true,false), bools);
            assertTrue(empty.isEmpty());
        }

        @SuppressWarnings("unchecked")
        private List<Object> invokeList(Method m, Object array) throws InvocationTargetException, IllegalAccessException {
            return (List<Object>) m.invoke(null, array);
        }
    }

    // --- Дополнительные позитивные и негативные сценарии ---

    @Nested
    @DisplayName("Позитив: временные типы (TIMESTAMP/DATE/TIME) → epoch millis")
    class TemporalPositive {

        @Test
        @DisplayName("TIMESTAMP → epoch millis")
        void timestamp_ok() {
            long ts = 1_725_000_000_000L; // произвольная дата
            Timestamp val = new Timestamp(ts);
            byte[] raw = PTimestamp.INSTANCE.toBytes(val);

            ValueCodecPhoenix vc = new ValueCodecPhoenix(new FakeRegistry().with("TS", "TIMESTAMP"));
            Object out = vc.decode(TBL, "TS", raw);
            assertTrue(out instanceof Long, "TIMESTAMP должен декодироваться в Long epochMillis");
            assertEquals(ts, ((Long) out).longValue());
        }

        @Test
        @DisplayName("DATE → epoch millis (UTC‑полночь)")
        void date_ok() {
            long midnight = 1_725_000_000_000L - (1_725_000_000_000L % 86_400_000L);
            Date val = new Date(midnight);
            byte[] raw = PDate.INSTANCE.toBytes(val);

            ValueCodecPhoenix vc = new ValueCodecPhoenix(new FakeRegistry().with("D", "DATE"));
            Object out = vc.decode(TBL, "D", raw);
            assertTrue(out instanceof Long, "DATE должен декодироваться в Long epochMillis");
            assertEquals(midnight, ((Long) out).longValue());
        }

        @Test
        @DisplayName("TIME → epoch millis (доля суток)")
        void time_ok() {
            long time = (12 * 60 * 60 + 34 * 60 + 56) * 1000L + 789; // 12:34:56.789
            Time val = new Time(time);
            byte[] raw = PTime.INSTANCE.toBytes(val);

            ValueCodecPhoenix vc = new ValueCodecPhoenix(new FakeRegistry().with("T", "TIME"));
            Object out = vc.decode(TBL, "T", raw);
            assertTrue(out instanceof Long, "TIME должен декодироваться в Long epochMillis");
            assertEquals(time, ((Long) out).longValue());
        }
    }

    @Nested
    @DisplayName("Позитив: бинарные типы (VARBINARY)")
    class BinaryPositive {

        @Test
        @DisplayName("VARBINARY → byte[] (содержательное равенство)")
        void varbinary_ok() {
            byte[] bytes = new byte[] {1,2,3,4,5};
            byte[] raw = PVarbinary.INSTANCE.toBytes(bytes);

            ValueCodecPhoenix vc = new ValueCodecPhoenix(new FakeRegistry().with("VB", "VARBINARY"));
            Object out = vc.decode(TBL, "VB", raw);
            assertTrue(out instanceof byte[], "VARBINARY должен декодироваться как byte[]");
            assertArrayEquals(bytes, (byte[]) out);
        }
    }

    @Nested
    @DisplayName("Негатив: повреждённые байты для DECIMAL → IllegalStateException")
    class DecimalNegative {
        @Test
        @DisplayName("DECIMAL: битые байты приводят к IllegalStateException с контекстом")
        void decimal_broken_bytes() {
            ValueCodecPhoenix vc = new ValueCodecPhoenix(new FakeRegistry().with("D", "DECIMAL"));
            byte[] broken = new byte[] {0x01}; // заведомо некорректно

            IllegalStateException ex = assertThrows(IllegalStateException.class, () -> vc.decode(TBL, "D", broken));
            String msg = ex.getMessage();
            assertTrue(msg.contains("DECIMAL") || msg.contains("Decimal"), "Диагностика должна содержать тип: " + msg);
            assertTrue(msg.contains("D"), "Диагностика должна содержать имя колонки: " + msg);
        }
    }

    /** Простой in-memory appender для подсчёта WARN‑сообщений. */
    static final class CountingAppender extends AppenderSkeleton {
        final AtomicInteger warns = new AtomicInteger(0);
        @Override protected void append(LoggingEvent event) {
            if (event.getLevel().isGreaterOrEqual(Level.WARN)) {
                warns.incrementAndGet();
            }
        }
        @Override public void close() { /* no-op */ }
        @Override public boolean requiresLayout() { return false; }
    }

    @Nested
    @DisplayName("Логирование: unknown‑type WARN срабатывает один раз (warn‑once)")
    class WarnOnce {

        @Test
        @DisplayName("Повторные decode с неизвестным типом → не более одного WARN")
        void unknown_type_warn_once() {
            Logger log = Logger.getLogger(ValueCodecPhoenix.class);
            CountingAppender app = new CountingAppender();
            log.addAppender(app);
            try {
                ValueCodecPhoenix vc = new ValueCodecPhoenix(new FakeRegistry()); // неизвестный тип
                byte[] bytes = "x".getBytes(StandardCharsets.UTF_8);

                // несколько вызовов подряд
                vc.decode(TBL, "U", bytes);
                vc.decode(TBL, "U", bytes);
                vc.decode(TBL, "U2", bytes);
            } finally {
                log.removeAppender(app);
            }
            assertTrue(app.warns.get() <= 1, "Должен быть не более одного WARN при неизвестных типах");
        }
    }

    @Nested
    @DisplayName("Параллельность: многопоточный decode без гонок; WARN остаётся единичным")
    class Concurrency {

        @Test
        @DisplayName("8 потоков × 100 итераций: без исключений; WARN<=1")
        void parallel_decode_ok_warn_once() throws Exception {
            Logger log = Logger.getLogger(ValueCodecPhoenix.class);
            CountingAppender app = new CountingAppender();
            log.addAppender(app);
            try {
                final ValueCodecPhoenix vc = new ValueCodecPhoenix(
                        new FakeRegistry().with("A", "VARCHAR")); // известный тип для части задач

                ExecutorService pool = Executors.newFixedThreadPool(8);
                try {
                    List<Callable<Boolean>> tasks = new ArrayList<>();
                    for (int i = 0; i < 8; i++) {
                        tasks.add(() -> {
                            for (int j = 0; j < 100; j++) {
                                // известный тип
                                Object s = vc.decode(TBL, "A", "ok".getBytes(StandardCharsets.UTF_8));
                                if (!"ok".equals(s)) return false;
                                // неизвестный тип (должен логировать не более 1 WARN на весь процесс)
                                vc.decode(TBL, "U" + j, "x".getBytes(StandardCharsets.UTF_8));
                            }
                            return true;
                        });
                    }
                    List<Future<Boolean>> res = pool.invokeAll(tasks);
                    for (Future<Boolean> f : res) {
                        assertTrue(f.get(5, TimeUnit.SECONDS), "Подзадача вернула false");
                    }
                } finally {
                    pool.shutdownNow();
                }
            } finally {
                log.removeAppender(app);
            }
            assertTrue(app.warns.get() <= 1, "В многопоточном сценарии WARN также должен сработать не более одного раза");
        }
    }
    @Nested
    @DisplayName("Инварианты: унификация возвращаемых типов")
    class Invariants {

        @Test
        @DisplayName("TIMESTAMP/DATE/TIME → Long epochMillis")
        void temporal_return_long_epoch_millis() {
            long ts = 1_725_000_000_000L;
            byte[] tsRaw = PTimestamp.INSTANCE.toBytes(new Timestamp(ts));

            long midnight = ts - (ts % 86_400_000L);
            byte[] dRaw = PDate.INSTANCE.toBytes(new Date(midnight));

            long tOfDay = (12 * 60 * 60 + 34 * 60 + 56) * 1000L + 789;
            byte[] timeRaw = PTime.INSTANCE.toBytes(new Time(tOfDay));

            ValueCodecPhoenix vc = new ValueCodecPhoenix(new FakeRegistry()
                    .with("TS", "TIMESTAMP")
                    .with("D", "DATE")
                    .with("T", "TIME"));

            Object tsOut = vc.decode(TBL, "TS", tsRaw);
            Object dOut = vc.decode(TBL, "D", dRaw);
            Object tOut = vc.decode(TBL, "T", timeRaw);

            assertAll(
                    () -> { assertTrue(tsOut instanceof Long, "TIMESTAMP должен возвращать Long"); assertEquals(ts, ((Long) tsOut).longValue()); },
                    () -> { assertTrue(dOut instanceof Long, "DATE должен возвращать Long"); assertEquals(midnight, ((Long) dOut).longValue()); },
                    () -> { assertTrue(tOut instanceof Long, "TIME должен возвращать Long"); assertEquals(tOfDay, ((Long) tOut).longValue()); }
            );
        }

        @Test
        @DisplayName("ARRAY → List<Object> для разных входов")
        void arrays_become_list() {
            try {
                Method m = ValueCodecPhoenix.class.getDeclaredMethod("toListFromRawArray", Object.class);
                m.setAccessible(true);

                assertAll(
                        () -> assertEquals(Arrays.asList("a", "b"), invokeList(m, new Object[]{"a", "b"})),
                        () -> assertEquals(Arrays.asList(1, 2), invokeList(m, new int[]{1, 2})),
                        () -> assertEquals(Arrays.asList((short) 1, (short) 2), invokeList(m, new short[]{1, 2})),
                        () -> assertEquals(Arrays.asList('x', 'y'), invokeList(m, new char[]{'x', 'y'})),
                        () -> assertEquals(Arrays.asList(1L, 2L, 3L), invokeList(m, new long[]{1, 2, 3})),
                        () -> assertEquals(Arrays.asList(1.0, 2.5), invokeList(m, new double[]{1.0, 2.5})),
                        () -> assertEquals(Arrays.asList(true, false), invokeList(m, new boolean[]{true, false}))
                );
            } catch (NoSuchMethodException e) {
                throw new AssertionError("Ошибка при тестировании arrays_become_list", e);
            }
        }

        @SuppressWarnings("unchecked")
        private List<Object> invokeList(Method m, Object array) {
            try {
                return (List<Object>) m.invoke(null, array);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new AssertionError("Ошибка вызова toListFromRawArray через рефлексию", e);
            }
        }

        @Test
        @DisplayName("Фиксированные размеры: строгая проверка длины (1/2/4/8)")
        void fixed_width_strict_lengths() {
            java.util.Map<String, Integer> types = new java.util.LinkedHashMap<>();
            types.put("UNSIGNED_TINYINT", 1);
            types.put("UNSIGNED_SMALLINT", 2);
            types.put("UNSIGNED_INT", 4);
            types.put("UNSIGNED_LONG", 8);

            assertAll(types.entrySet().stream()
                    .map(e -> (Executable) () -> {
                        String q = "Q" + e.getValue();
                        byte[] wrong = new byte[e.getValue() + 1];
                        ValueCodecPhoenix vc = new ValueCodecPhoenix(new FakeRegistry().with(q, e.getKey()));
                        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> vc.decode(TBL, q, wrong));
                        assertTrue(ex.getMessage().contains(String.valueOf(e.getValue())),
                                "Диагностика должна содержать ожидаемую длину: " + ex.getMessage());
                    })
                    .toArray(Executable[]::new));
        }
    }

    @Test
    @DisplayName("Anchors to mark nested test classes as used")
    void anchors() {
        Class<?>[] refs = {
                Positive.class, Negative.class, NormalizeTypeName.class, ArrayConversion.class,
                TemporalPositive.class, BinaryPositive.class, DecimalNegative.class, WarnOnce.class, Concurrency.class,
                Invariants.class
        };
        assertNotNull(refs);
    }
}