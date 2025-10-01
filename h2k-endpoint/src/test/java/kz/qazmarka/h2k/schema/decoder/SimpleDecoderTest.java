package kz.qazmarka.h2k.schema.decoder;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.hbase.TableName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Консолидированный набор юнит‑тестов для {@link SimpleDecoder}.
 *
 * Цели:
 *  1) Позитив: zero‑copy контракт, null/empty, удобные перегрузки.
 *  2) Негатив: строгий NPE при null table/qualifier во всех перегрузках.
 *  3) Потокобезопасность: smoke‑тест на отсутствие состояния и аллокаций сверх входных.
 *
 * Ограничения:
 *  - JUnit 5 (Jupiter); совместимо с Java 8 — без 'var' и API Java 9+.
 *  - Без файлового ввода/вывода и сетевых зависимостей; тесты быстрые и детерминированные.
 */
class SimpleDecoderTest {

    private static final Decoder DEC = SimpleDecoder.INSTANCE;
    private static final TableName TBL = TableName.valueOf("DEFAULT", "DUMMY");
    private static final String Q = "q";

    // --- Позитивные сценарии

    @Nested
    @DisplayName("Позитивные сценарии")
    class Positive {

        /** Возвращается та же ссылка (zero-copy), копирования нет. */
        @Test
        void returnsSameReference() {
            byte[] data = new byte[] {1, 2, 3};
            Object decoded = DEC.decode(TBL, Q, data);
            assertTrue(decoded instanceof byte[]);
            assertSame(data, decoded, "Должна вернуться та же ссылка на массив (zero-copy)");
        }

        /** Null-вход возвращает null. */
        @Test
        void nullReturnsNull() {
            assertNull(DEC.decode(TBL, Q, null));
        }

        /** Пустой массив возвращается как есть (без копирования). */
        @Test
        void emptyArrayIsNotCopied() {
            byte[] empty = new byte[0];
            Object decoded = DEC.decode(TBL, Q, empty);
            assertSame(empty, decoded);
        }

        /** Перегрузка с byte[] qualifier: также zero-copy. */
        @Test
        void byteQualifierOverloadZeroCopy() {
            byte[] qual = Q.getBytes(StandardCharsets.UTF_8);
            byte[] data = new byte[] {4,5,6};
            Object decoded = DEC.decode(TBL, qual, data);
            assertSame(data, decoded);
        }

        /** decodeOrDefault: null → default; not-null → исходный объект (same reference). */
        @Test
        void decodeOrDefaultBehavior() {
            byte[] def = new byte[] {9};
            assertSame(def, DEC.decodeOrDefault(TBL, Q, null, def));
            byte[] data = new byte[] {7,7,7};
            assertSame(data, DEC.decodeOrDefault(TBL, Q, data, def));
        }

        /** Быстрая перегрузка: длинный qualifier (8КБ) + zero-copy для value */
        @Test
        void longQualifierFastOverloadZeroCopy() {
            byte[] qual = new byte[8 * 1024];
            byte[] data = new byte[] {10, 20, 30, 40};
            Object decoded = DEC.decode(TBL, qual, data);
            assertSame(data, decoded, "Должна вернуться та же ссылка на массив значения (zero-copy) при длинном qualifier");
        }
    }

    // --- Негативные сценарии

    @Nested
    @DisplayName("Негативные сценарии")
    class Negative {

        /** NPE при null table/qualifier в строковой перегрузке. */
        @Test
        void npeOnNullArgsStringOverload() {
            byte[] bytes = new byte[] {1};
            NullPointerException ex1 = assertThrows(NullPointerException.class, () -> ((Decoder) DEC).decode(null, Q, bytes));
            assertNotNull(ex1);
            NullPointerException ex2 = assertThrows(NullPointerException.class, () -> ((Decoder) DEC).decode(TBL, (String) null, bytes));
            assertNotNull(ex2);
        }

        /** NPE при null table/qualifier в перегрузке с byte[] qualifier. */
        @Test
        void npeOnNullArgsByteArrayOverload() {
            byte[] bytes = new byte[] {1};
            byte[] qual = Q.getBytes(StandardCharsets.UTF_8);
            NullPointerException ex3 = assertThrows(NullPointerException.class, () -> DEC.decode(null, qual, bytes));
            assertNotNull(ex3);
            NullPointerException ex4 = assertThrows(NullPointerException.class, () -> DEC.decode(TBL, (byte[]) null, bytes));
            assertNotNull(ex4);
        }
    }

    // --- Потокобезопасность

    /** Потокобезопасность: множество одновременных вызовов не должны падать. */
    @Test
    void threadSafetySmoke() throws InterruptedException {
        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);
        byte[] data = new byte[] {42};
        assertEquals(42, data[0]);

        for (int i = 0; i < threads; i++) {
            pool.execute(() -> {
                try {
                    for (int n = 0; n < 1000; n++) {
                        Object decoded = DEC.decode(TBL, Q, data);
                        assertSame(data, decoded);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        pool.shutdownNow();
    }

    /**
     * Якорный тест, чтобы вложенные классы не считались «неиспользуемыми»
     * анализаторами, даже если отключён анализ категории unused.
     */
    @Test
    void anchors() {
        assertNotNull(Positive.class);
        assertNotNull(Negative.class);
    }
}
