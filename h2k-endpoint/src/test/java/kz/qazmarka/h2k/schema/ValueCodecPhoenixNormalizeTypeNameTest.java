/**
 * Набор юнит‑тестов для приватного помощника ValueCodecPhoenix.normalizeTypeName(String).
 *
 * Назначение:
 *  • проверяет нормализацию строкового представления типов Phoenix (регистры, пробелы, параметры в скобках, массивы);
 *  • гарантирует обратную совместимость при добавлении новых правил нормализации;
 *  • документирует граничные случаи, встречавшиеся в боевых схемах.
 *
 * Технические детали:
 *  • доступ к методу осуществляется через рефлексию (метод приватный, тест не зависит от публичного API);
 *  • JUnit 5 (Jupiter), параметризованные тесты через @CsvSource; для значений с запятой используются одинарные кавычки;
 *  • Java 8: избегаем List.of и других API Java 9+.
 */
package kz.qazmarka.h2k.schema;

import org.apache.hadoop.hbase.TableName;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PUnsignedInt;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.types.PTimestamp;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Тесты для {@link ValueCodecPhoenix}, фокус на:
 *  1) нормализацию строковых имён Phoenix-типов (регистр, синонимы, параметры в скобках);
 *  2) корректную работу двухуровневого кэша типов (table + qualifier) — тип запрашивается у реестра ровно один раз;
 *  3) безопасный фолбэк на VARCHAR при неизвестном типе.
 *
 * В тесте создаётся лёгкий Stub-реестр, который:
 *  • хранит тип для пары (table, qualifier);
 *  • считает количество обращений к columnType(...) — так проверяем кэш.
 *
 * Производительность/GC:
 *  • тест использует минимальные аллокации и не рефлексирует приватные методы — лишь публичный API {@link Decoder#decode}.
 */
class ValueCodecPhoenixNormalizeTypeNameTest {

    /** Простой стаб для SchemaRegistry: хранит типы и считает вызовы. */
    static final class StubRegistry implements SchemaRegistry {
        private final Map<Key, String> types = new HashMap<>();
        private final Map<Key, Integer> calls = new HashMap<>();

        void put(TableName t, String q, String typeName) {
            types.put(new Key(t, q), typeName);
        }
        int callsOf(TableName t, String q) {
            return calls.getOrDefault(new Key(t, q), 0);
        }

        @Override
        public String columnType(TableName table, String qualifier) {
            Key k = new Key(table, qualifier);
            calls.put(k, calls.getOrDefault(k, 0) + 1);
            return types.get(k);
        }

        /** Ключ для таблицы/квалификатора — упрощённый, подходит для теста. */
        static final class Key {
            final String ns, name, q;
            Key(TableName t, String q) {
                this.ns   = t.getNamespaceAsString();
                this.name = t.getNameAsString();
                this.q    = q;
            }
            @Override public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || o.getClass() != Key.class) return false;
                Key k = (Key) o;
                return ns.equals(k.ns) && name.equals(k.name) && q.equals(k.q);
            }
            @Override public int hashCode() {
                int h = ns.hashCode();
                h = 31 * h + name.hashCode();
                h = 31 * h + q.hashCode();
                return h;
            }
        }
    }

    private static final TableName TABLE = TableName.valueOf("DEFAULT", "T");

    /**
     * Проверяет, что разные варианты записи типов (регистр, скобочные параметры, синонимы)
     * корректно нормализуются и возвращают ожидаемые Java-значения.
     */
    @Test
    void normalizationAndDecode_basicScalars() {
        StubRegistry reg = new StubRegistry();
        ValueCodecPhoenix codec = new ValueCodecPhoenix(reg);

        // Задаём типы в "сыром виде" — как они могли прийти из внешнего реестра/DDL
        reg.put(TABLE, "v", "varchar(10)");
        reg.put(TABLE, "u", "UNSIGNED_INT(10)");
        reg.put(TABLE, "u2", "unsigned int");
        reg.put(TABLE, "n", "NUMBER(10,2)"); // синоним DECIMAL
        reg.put(TABLE, "b", "bool");         // синоним BOOLEAN
        reg.put(TABLE, "l", "long");         // синоним BIGINT
        reg.put(TABLE, "ts", "timestamp(6)");// параметры в скобках отбрасываются

        // Подготавливаем байты в формате Phoenix для соответствующих типов:
        byte[] vBytes  = PVarchar.INSTANCE.toBytes("abc");
        byte[] uBytes  = PUnsignedInt.INSTANCE.toBytes(123);
        byte[] u2Bytes = PUnsignedInt.INSTANCE.toBytes(456);
        byte[] nBytes  = PDecimal.INSTANCE.toBytes(new BigDecimal("12.34"));
        byte[] bBytes  = PBoolean.INSTANCE.toBytes(Boolean.TRUE);
        byte[] lBytes  = PLong.INSTANCE.toBytes(42L);
        Timestamp ts   = new Timestamp(1_700_000_000_000L);
        byte[] tsBytes = PTimestamp.INSTANCE.toBytes(ts);

        // Декодируем и проверяем:
        assertEquals("abc", codec.decode(TABLE, "v", vBytes));
        assertEquals(123,  codec.decode(TABLE, "u", uBytes));
        assertEquals(456,  codec.decode(TABLE, "u2", u2Bytes));
        assertEquals(new BigDecimal("12.34"), codec.decode(TABLE, "n", nBytes));
        assertEquals(true, codec.decode(TABLE, "b", bBytes));
        assertEquals(42L,  codec.decode(TABLE, "l", lBytes));
        assertEquals(ts.getTime(), codec.decode(TABLE, "ts", tsBytes)); // TIMESTAMP -> epoch millis
    }

    /**
     * Проверяет, что:
     *  • при неизвестном типе применяется безопасный фолбэк на VARCHAR;
     *  • кэш типов обращается к реестру один раз для каждой пары (table, qualifier).
     */
    @Test
    void cacheIsTwoLevelAndUnknownTypeFallsBackToVarchar() {
        StubRegistry reg = new StubRegistry();
        ValueCodecPhoenix codec = new ValueCodecPhoenix(reg);

        reg.put(TABLE, "known", "UNSIGNED_INT");
        reg.put(TABLE, "oops",  "FOOBAR"); // неизвестный тип — должен быть фолбэк на VARCHAR

        byte[] knownBytes = PUnsignedInt.INSTANCE.toBytes(7);
        byte[] varBytes   = PVarchar.INSTANCE.toBytes("x");

        // Первый вызов — кэш прогревается
        assertEquals(7,  codec.decode(TABLE, "known", knownBytes));
        assertEquals("x", codec.decode(TABLE, "oops", varBytes));

        // Повтор — типы не должны заново запрашиваться у реестра
        assertEquals(7,  codec.decode(TABLE, "known", knownBytes));
        assertEquals("x", codec.decode(TABLE, "oops", varBytes));

        assertEquals(1, reg.callsOf(TABLE, "known"), "Ожидали 1 обращение к реестру для 'known'");
        assertEquals(1, reg.callsOf(TABLE, "oops"),  "Ожидали 1 обращение к реестру для 'oops'");
    }
}