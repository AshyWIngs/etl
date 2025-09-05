package kz.qazmarka.h2k.payload;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Юнит‑тесты для расчёта начальной ёмкости корневой LinkedHashMap
 * в PayloadBuilder.computeInitialCapacity(...).
 *
 * Цель:
 *  • Зафиксировать контракт без использования дробной арифметики.
 *  • Проверить граничные случаи (отрицательные, нули, большие значения).
 *  • Проверить базовые "монотонные" свойства (неубывание по estimate/hint).
 *
 * Формула ожидания в тестах:
 *   target = max(estimated, hint&gt;0 ? hint : 0)
 *   initial = 1 + ceil(target / 0.75)
 * Эквивалент целочисленного варианта без double:
 *   initial = 1 + (4*target + 2)/3
 * Ограничение сверху: initial ≤ (1&lt;&lt;30).
 */
class PayloadBuilderInitialCapacityTest {

    /**
     * Эталонный расчёт ёмкости без плавающей точки и без переполнений int.
     */
    private static int expectedCapacity(int estimated, int hint) {
        final int target = Math.max(estimated, hint > 0 ? hint : 0);
        if (target <= 0) return 1;
        final long n = ((long) target << 2) + 2; // 4*target + 2
        long cap = 1 + n / 3L;
        final long MAX = 1L << 30;
        if (cap > MAX) cap = MAX;
        return (int) cap;
    }

    @Test
    @DisplayName("Если подсказки нет, используется оценка")
    void usesEstimateWhenNoHint() {
        int est = 20;
        int hint = 0;
        int expected = expectedCapacity(est, hint);
        assertEquals(expected, PayloadBuilder.computeInitialCapacity(est, hint));
    }

    @Test
    @DisplayName("Если подсказка больше — доминирует над оценкой")
    void respectsLargerHint() {
        int est = 20;
        int hint = 40;
        int expected = expectedCapacity(est, hint);
        assertEquals(expected, PayloadBuilder.computeInitialCapacity(est, hint));
    }

    @ParameterizedTest(name = "estimated={0}, hint={1}")
    @CsvSource({
            // базовые точки
            "1, 0",
            "15, 0",
            "50, 100",
            // равные значения
            "24, 24",
            // граничные/нулевые
            "0, 0",
            "-5, 0",
            "0, -7",
            "-3, -9",
    })
    @DisplayName("Корректное округление и обработка нулей/отрицательных")
    void roundsUpProperly(int estimated, int hint) {
        assertEquals(expectedCapacity(estimated, hint),
                PayloadBuilder.computeInitialCapacity(estimated, hint));
    }

    @Test
    @DisplayName("Отрицательные значения не занижают результат (мин=1)")
    void ignoresNegativeValues() {
        assertEquals(expectedCapacity(-1, -1), PayloadBuilder.computeInitialCapacity(-1, -1));
        assertEquals(expectedCapacity(-1, 10), PayloadBuilder.computeInitialCapacity(-1, 10));
        assertEquals(expectedCapacity(10, -1), PayloadBuilder.computeInitialCapacity(10, -1));
    }

    @Test
    @DisplayName("Кэп по максимуму HashMap (1<<30)")
    void capsAtMax() {
        int est = Integer.MAX_VALUE / 2;
        int hint = Integer.MAX_VALUE;
        assertEquals(expectedCapacity(est, hint),
                PayloadBuilder.computeInitialCapacity(est, hint));
    }

    @Test
    @DisplayName("Монотонность по estimated: неубывающее значение")
    void monotonicInEstimated() {
        final int hint = 32;
        int prev = PayloadBuilder.computeInitialCapacity(0, hint);
        for (int est = 1; est <= 200; est++) {
            int cur = PayloadBuilder.computeInitialCapacity(est, hint);
            assertTrue(cur >= prev, "capacity должна быть неубывающей по estimated");
            prev = cur;
        }
    }

    @Test
    @DisplayName("Монотонность по hint: неубывающее значение")
    void monotonicInHint() {
        final int est = 24;
        int prev = PayloadBuilder.computeInitialCapacity(est, 0);
        for (int h = 1; h <= 200; h++) {
            int cur = PayloadBuilder.computeInitialCapacity(est, h);
            assertTrue(cur >= prev, "capacity должна быть неубывающей по hint");
            prev = cur;
        }
    }
}