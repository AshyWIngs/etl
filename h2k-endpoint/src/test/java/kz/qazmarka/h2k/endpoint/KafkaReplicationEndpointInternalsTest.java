package kz.qazmarka.h2k.endpoint;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Юнит‑тесты для «горячих» приватных помощников {@code KafkaReplicationEndpoint}.
 *
 * Задачи:
 *  • Проверить корректность формулы расчёта начальной ёмкости {@code capacityFor(int)} без использования чисел с плавающей точкой.
 *  • Проверить работу быстрых фильтров по WAL‑timestamp для 1/2/N CF: {@code passWalTsFilter1/2/N}.
 *  • Накрыть позитивные и негативные сценарии (границы, отсутствие совпадений, пустые входы).
 *
 * Подход:
 *  • Доступ к приватным методам — через рефлексию (без изменения публичного API).
 *  • Создание {@link org.apache.hadoop.hbase.Cell} — через {@link org.apache.hadoop.hbase.KeyValue} (минимальная зависимость от HBase).
 *  • Без поднятия Kafka/HBase окружения: тесты быстрые, не шумят в логах и не влияют на GC.
 */
class KafkaReplicationEndpointInternalsTest {

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

    @Test
    @DisplayName("capacityFor(): граничные и типовые значения")
    void capacityFor_basic() throws Exception {
        Method m = privateMethod("capacityFor", int.class);
        assertEquals(16, m.invoke(null, 0));     // дефолт
        assertEquals(2,  m.invoke(null, 1));     // ceil(1/0.75)=2
        assertEquals(5,  m.invoke(null, 3));     // ceil(3/0.75)=5
        assertEquals(134, m.invoke(null, 100));  // ceil(100/0.75)=134
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
}