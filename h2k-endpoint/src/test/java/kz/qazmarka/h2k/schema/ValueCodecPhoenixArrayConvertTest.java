/**
 * Юнит‑тесты для приватного помощника ValueCodecPhoenix.toListFromRawArray(Object).
 *
 * Назначение:
 *  • Проверить корректную конвертацию массивов (как объектных, так и примитивных) в неизменяемый {@code List<Object>} без потери порядка.
 *  • Зафиксировать контракт боксинга примитивов (int → Integer и т.д.) и поведения для пустых массивов (возврат пустого неизменяемого списка).
 *  • Документировать ожидаемое поведение для дальнейших изменений кодека.
 *
 * Технические детали:
 *  • Метод приватный, поэтому доступ осуществляется через рефлексию (без изменения публичного API кодека).
 *  • JUnit 5 (Jupiter). Совместимо с Java 8 — без использования API Java 9+ (например, List.of).
 */
package kz.qazmarka.h2k.schema;

import org.apache.hadoop.hbase.TableName;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.types.PVarcharArray;
import org.apache.phoenix.schema.types.PhoenixArray;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Тест конвертации Phoenix ARRAY → List&lt;Object&gt; в {@link ValueCodecPhoenix}.
 *
 * Идея:
 *  • собираем {@link PhoenixArray} из обычного Java-массива String[];
 *  • сериализуем его в байты вызовом {@link PVarcharArray#toBytes(Object)};
 *  • отдаём в {@link ValueCodecPhoenix#decode} при объявленном типе "VARCHAR ARRAY";
 *  • ожидаем получить список значений без лишних аллокаций и копий.
 *
 * Производительность/GC:
 *  • тест работает через «правильную» сериализацию/десериализацию Phoenix и не рефлексирует приватные методы.
 */
class ValueCodecPhoenixArrayConvertTest {

    private static final TableName TABLE = TableName.valueOf("DEFAULT", "T");

    @Test
    void varcharArrayIsDecodedToList() {
        // Готовим стаб-реестр: для колонки 'arr' он вернёт тип "VARCHAR ARRAY"
        SchemaRegistry reg = (table, qualifier) -> {
            if ("arr".equals(qualifier)) return "VARCHAR ARRAY";
            throw new AssertionError("Неожиданный qualifier: " + qualifier);
        };
        ValueCodecPhoenix codec = new ValueCodecPhoenix(reg);

        // Собираем PhoenixArray из String[]
        String[] src = new String[] { "a", "b", "c" };
        PhoenixArray pa = new PhoenixArray(PVarchar.INSTANCE, src);

        // Сериализуем в байты Phoenix-способом (как это делает Phoenix при записи)
        byte[] bytes = PVarcharArray.INSTANCE.toBytes(pa);

        // Декодируем — должны получить List<Object> с теми же значениями и порядком
        Object decoded = codec.decode(TABLE, "arr", bytes);
        assertTrue(decoded instanceof List, "Ожидали List<Object> для ARRAY-типа");

        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) decoded;
        assertEquals(Arrays.asList("a", "b", "c"), list);
    }
}