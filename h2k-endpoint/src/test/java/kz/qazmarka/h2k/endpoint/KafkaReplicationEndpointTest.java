package kz.qazmarka.h2k.endpoint;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Юнит‑тесты для «горячих» приватных помощников {@code KafkaReplicationEndpoint}.
 *
 * Задачи:
 *  • Проверить корректность формулы расчёта начальной ёмкости {@code capacityFor(int)} без использования чисел с плавающей точкой.
 *  • Проверить работу быстрых фильтров по WAL‑timestamp для 1/2/N CF: {@code passWalTsFilter1/2/N}.
 *  • Накрыть позитивные и негативные сценарии (границы, отсутствие совпадений, пустые входы).
 *  • Проверить монотонность и кэп: отрицательные/нулевые значения, большие входы, верхняя граница.
 *  • Проверить чувствительность к регистру qualifier CF и корректность обработки пустых/дублирующихся массивов CF.
 *
 * Подход:
 *  • Доступ к приватным методам — через рефлексию (без изменения публичного API).
 *  • Создание {@link org.apache.hadoop.hbase.Cell} — через {@link org.apache.hadoop.hbase.KeyValue} (минимальная зависимость от HBase).
 *  • Без поднятия Kafka/HBase окружения: тесты быстрые, не шумят в логах и не влияют на GC.
 */
class KafkaReplicationEndpointTest {

    private static java.lang.reflect.Method privateMethod(String name, Class<?>... types) throws Exception {
        final Method m = KafkaReplicationEndpoint.class.getDeclaredMethod(name, types);
        m.setAccessible(true);
        return m;
    }
    
    private static Cell cell(String row, String cf, long ts) {
        return new KeyValue(bytes(row), bytes(cf), bytes("q"), ts, bytes("v"));
    }
    
    private static byte[] bytes(String s) {
        return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    /**
     * Эталонная формула для capacityFor(n):
     *  • если n <= 0 → 16 (дефолт);
     *  • иначе initial = 1 + ceil(n / 0.75) = 1 + (4*n + 2)/3;
     *  • верхний кэп = 1<<30.
     */
    private static int expectedCapacity(int n) {
        if (n <= 0) return 16;
        long t = 1L + (4L * n + 2L) / 3L; // 1 + ceil(4*n/3) без FP
        return (t > (1L << 30)) ? (1 << 30) : (int) t;
    }

    @Test
    @DisplayName("capacityFor(): граничные и типовые значения")
    void capacityFor_basic() throws Exception {
        Method m = privateMethod("capacityFor", int.class);
        assertEquals(expectedCapacity(0),  (Integer) m.invoke(null, 0));
        assertEquals(expectedCapacity(1),  (Integer) m.invoke(null, 1));
        assertEquals(expectedCapacity(3),  (Integer) m.invoke(null, 3));
        assertEquals(expectedCapacity(100),(Integer) m.invoke(null, 100));
    }

    @Test
    @DisplayName("capacityFor(): отрицательные/нулевые и экстремальные значения (кэп, минимум)")
    void capacityFor_edges() throws Exception {
        Method m = privateMethod("capacityFor", int.class);
        // минимум: n<=0 → дефолт 16
        assertEquals(16, (Integer) m.invoke(null, 0));
        assertEquals(16, (Integer) m.invoke(null, -1));
        assertEquals(16, (Integer) m.invoke(null, Integer.MIN_VALUE));
        // верхняя граница: кэп 1<<30
        assertEquals(1 << 30, (Integer) m.invoke(null, Integer.MAX_VALUE));
    }

    @Test
    @DisplayName("passWalTsFilter1(): один CF — true/false по порогу и семейству")
    void filter_oneCf() throws Exception {
        List<Cell> cells = Arrays.asList(
                cell("r", "a", 50L),
                cell("r", "a", 100L)
        );
        byte[] cfA = bytes("a");
        Method m = privateMethod("passWalTsFilter1", List.class, byte[].class, long.class);
        assertTrue((Boolean) m.invoke(null, cells, cfA, 90L));   // есть 100 >= 90
        assertTrue((Boolean) m.invoke(null, cells, cfA, 100L));  // ровно на границе
        assertFalse((Boolean) m.invoke(null, cells, cfA, 110L)); // нет >= 110
        assertFalse((Boolean) m.invoke(null, cells, bytes("b"), 10L)); // другое CF
        // порог ниже всех — должно сработать
        assertTrue((Boolean) m.invoke(null, cells, cfA, 0L));
        // регистрозависимость CF: 'A' не равен 'a'
        assertFalse((Boolean) m.invoke(null, cells, bytes("A"), 0L));
    }

    @Test
    @DisplayName("passWalTsFilter2(): пустой список клеток ➜ false")
    void filter_twoCf_empty_returnsFalse() throws Exception {
        Method m = privateMethod("passWalTsFilter2", List.class, byte[].class, byte[].class, long.class);
        assertFalse((Boolean) m.invoke(null, Collections.<Cell>emptyList(), bytes("a"), bytes("b"), 1L));
    }

    @Test
    @DisplayName("passWalTsFilter2(): два CF — срабатывает по любому")
    void filter_twoCf() throws Exception {
        List<Cell> cells = Arrays.asList(
                cell("r", "a", 80L),
                cell("r", "b", 120L)
        );
        byte[] cfA = bytes("a");
        byte[] cfB = bytes("b");
        Method m = privateMethod("passWalTsFilter2", List.class, byte[].class, byte[].class, long.class);
        assertTrue((Boolean) m.invoke(null, cells, cfA, cfB, 100L)); // попали по cfB:120
        assertFalse((Boolean) m.invoke(null, cells, cfA, cfB, 130L));
        // ровно на границе по cfB
        assertTrue((Boolean) m.invoke(null, cells, cfA, cfB, 120L));
        // если ни один CF не совпадает по имени — false даже при больших ts
        assertFalse((Boolean) m.invoke(null, cells, bytes("x"), bytes("y"), 0L));
    }

    @Test
    @DisplayName("passWalTsFilterN(): n>=3 CF — общий случай")
    void filter_manyCf() throws Exception {
        List<Cell> cells = Arrays.asList(
                cell("r", "a", 10L),
                cell("r", "b", 20L),
                cell("r", "c", 30L)
        );
        byte[] cfA = bytes("a");
        byte[] cfB = bytes("b");
        byte[] cfC = bytes("c");
        Method m = privateMethod("passWalTsFilterN", List.class, byte[][].class, long.class);
        assertTrue((Boolean) m.invoke(null, cells, new byte[][]{cfA, cfB, cfC}, 25L)); // cfC:30
        assertFalse((Boolean) m.invoke(null, cells, new byte[][]{cfA, cfB, cfC}, 31L));
        // дубликаты CF не мешают найти совпадение
        assertTrue((Boolean) m.invoke(null, cells, new byte[][]{cfA, cfB, cfC, cfB}, 25L));
        // пустой список CF → false
        assertFalse((Boolean) m.invoke(null, cells, new byte[][]{}, 0L));
    }

    @Test
    @DisplayName("passWalTsFilter1(): пустой список клеток ➜ false")
    void filter_oneCf_empty_returnsFalse() throws Exception {
        Method m = privateMethod("passWalTsFilter1", List.class, byte[].class, long.class);
        List<Cell> cells = java.util.Collections.emptyList();
        assertFalse((Boolean) m.invoke(null, cells, bytes("a"), 1L));
    }

    @Test
    @DisplayName("passWalTsFilterN(): CF заданы, но ни одна ячейка не принадлежит им ➜ false")
    void filter_manyCf_noneMatch() throws Exception {
        Method m = privateMethod("passWalTsFilterN", List.class, byte[][].class, long.class);
        List<Cell> cells = Arrays.asList(
            cell("r", "x", 100L),
            cell("r", "y", 200L)
        );
        byte[][] cfs = new byte[][] { bytes("a"), bytes("b"), bytes("c") };
        assertFalse((Boolean) m.invoke(null, cells, cfs, 50L));
    }

    @Test
    @DisplayName("passWalTsFilterN(): чувствительность к регистру и отсутствие совпадений при любых ts")
    void filter_manyCf_caseSensitive_and_highTs_noMatch() throws Exception {
        Method m = privateMethod("passWalTsFilterN", List.class, byte[][].class, long.class);
        List<Cell> cells = Arrays.asList(
            cell("r", "a", 1_000_000L),
            cell("r", "b", 1_000_000L)
        );
        // верхний регистр не совпадает с нижним
        byte[][] cfs = new byte[][] { bytes("A"), bytes("B"), bytes("C") };
        assertFalse((Boolean) m.invoke(null, cells, cfs, 1L));
    }

    /**
     * Быстрая регрессия: ёмкость не убывает при росте оценочного числа ключей на малых значениях.
     * Это фиксирует формулу и защищает от случайной замены на FP‑арифметику.
     */
    @Test
    @DisplayName("capacityFor(): монотонность на малых n (1..10)")
    void capacityFor_monotonic_smallRange() throws Exception {
        Method m = privateMethod("capacityFor", int.class);
        int prev = (Integer) m.invoke(null, 1);
        for (int n = 2; n <= 10; n++) {
            int cur = (Integer) m.invoke(null, n);
            assertTrue(cur >= prev, "capacityFor must be non-decreasing");
            prev = cur;
        }
    }

    @Test
    @DisplayName("capacityFor(): сетка малых значений совпадает с целочисленной формулой")
    void capacityFor_grid_small() throws Exception {
        Method m = privateMethod("capacityFor", int.class);
        for (int n = -3; n <= 30; n++) {
            int exp = expectedCapacity(n);
            int act = (Integer) m.invoke(null, n);
            assertEquals(exp, act, "n=" + n);
        }
    }
}