package kz.qazmarka.h2k.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class JsonWriterTest {

    @Test
    @DisplayName("writeMap(): экранирование спецсимволов и порядок ключей")
    void writeMap_withEscaping() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("plain", "value");
        map.put("escaped", "line\nbreak");
        map.put("control", '\u0001');
        map.put("number", new BigDecimal("123.45"));
        map.put("array", Arrays.asList(1, 2));

        StringBuilder sb = new StringBuilder();
        JsonWriter.writeMap(sb, map);

        assertEquals("{\"plain\":\"value\",\"escaped\":\"line\\nbreak\",\"control\":\"\\u0001\",\"number\":123.45,\"array\":[1,2]}", sb.toString());
    }

    @Test
    @DisplayName("writeAny(): массивы примитивов и NaN → null")
    void writeAny_arraysAndNaN() {
        double[] doubles = {1.0, Double.NaN};
        boolean[] bools = {true, false};

        StringBuilder sb = new StringBuilder();
        JsonWriter.writeAny(sb, doubles);
        sb.append('|');
        JsonWriter.writeAny(sb, bools);

        assertEquals("[1.0,null]|[true,false]", sb.toString());
    }
}
