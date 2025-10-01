package kz.qazmarka.h2k.util;

/** Внутренние утилиты для коллекций. */
public final class Maps {
    private Maps() {}

    /**
     * Вычисляет начальную ёмкость LinkedHashMap без использования FP‑арифметики:
     * формула 1 + ceil(target / 0.75) эквивалентна 1 + (4*target + 2) / 3 (целочисленно).
     * Это избавляет от Math.ceil/double‑деления на горячем пути и даёт те же результаты.
     *
     * @param estimatedKeysCount оценка общего числа ключей (данные + метаданные)
     * @param hint               «подсказка» из конфигурации (0 — нет подсказки)
     * @return рекомендуемая ёмкость для конструктора LinkedHashMap (с учётом верхнего ограничения ~2^30)
     */
    public static int computeInitialCapacity(int estimatedKeysCount, int hint) {
        // Берём максимум из оценки и подсказки (если она задана)
        final int target = Math.max(estimatedKeysCount, hint > 0 ? hint : 0);
        if (target <= 0) {
            return 1;
        }
        // 1 + ceil(4*target / 3)  ==  1 + (4*target + 2) / 3  (целочисленное деление)
        final long n = ((long) target << 2) + 2; // 4*target + 2
        long cap = 1 + n / 3L;
        // HashMap/LinkedHashMap внутренняя таблица ограничена ~2^30
        final long MAX = 1L << 30;
        if (cap > MAX) cap = MAX;
        return (int) cap;
    }

    /**
     * Упрощённая версия для случаев, когда нет отдельной подсказки (hint).
     * Совместима с прежними вызовами {@code Maps.initialCapacity(n)}.
     *
     * @param estimatedKeysCount оценка общего числа ключей
     * @return рекомендуемая ёмкость для конструктора LinkedHashMap
     */
    public static int initialCapacity(int estimatedKeysCount) {
        return computeInitialCapacity(estimatedKeysCount, 0);
    }

    /**
     * Совместимая перегрузка: проксирует к {@link #computeInitialCapacity(int, int)}.
     *
     * @param estimatedKeysCount оценка общего числа ключей (данные + метаданные)
     * @param hint               «подсказка» из конфигурации (0 — нет подсказки)
     * @return рекомендуемая ёмкость для конструктора LinkedHashMap
     */
    public static int initialCapacity(int estimatedKeysCount, int hint) {
        return computeInitialCapacity(estimatedKeysCount, hint);
    }
}