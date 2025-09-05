package kz.qazmarka.h2k.schema;

import org.apache.hadoop.hbase.TableName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Юнит‑тесты для {@link SimpleDecoder}.
 *
 * Назначение:
 *  • Зафиксировать zero‑copy контракт: метод возвращает ту же ссылку на входной byte[].
 *  • Проверить поведение для null и пустых массивов.
 *  • Smoke‑тест потокобезопасности (отсутствие состояния).
 *
 * Технические детали:
 *  • JUnit 5 (Jupiter); совместимо с Java 8 — без использования 'var' и API Java 9+.
 *  • Без файлового ввода/вывода и сетевых зависимостей; тесты быстрые и детерминированные.
 */
class SimpleDecoderTest {

    private static final Decoder DEC = SimpleDecoder.INSTANCE;
    private static final TableName TBL = TableName.valueOf("DEFAULT", "DUMMY");
    private static final String Q = "q";

    /**
     * Возвращается та же ссылка (zero-copy), копирования нет.
     */
    @Test
    void returnsSameReference() {
        byte[] data = new byte[] {1, 2, 3};
        Object decoded = DEC.decode(TBL, Q, data);
        assertTrue(decoded instanceof byte[]);
        assertSame(data, decoded, "Должна вернуться та же ссылка на массив (zero-copy)");
    }

    /**
     * Null-вход возвращает null.
     */
    @Test
    void nullReturnsNull() {
        assertNull(DEC.decode(TBL, Q, null));
    }

    /**
     * Пустой массив возвращается как есть (без копирования).
     */
    @Test
    void emptyArrayIsNotCopied() {
        byte[] empty = new byte[0];
        Object decoded = DEC.decode(TBL, Q, empty);
        assertSame(empty, decoded);
    }

    /**
     * Потокобезопасность: множество одновременных вызовов не должны падать.
     * (Никаких аллокаций сверх входных, состояние не хранится.)
     */
    @Test
    void threadSafetySmoke() throws InterruptedException {
        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);
        byte[] data = new byte[] {42};

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
        pool.shutdown();
    }
}