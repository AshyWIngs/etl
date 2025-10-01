package kz.qazmarka.h2k.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Base64;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class BytesTest {

    @Test
    @DisplayName("base64(): кодирует произвольный срез без дополнительных копий")
    void base64_slice() {
        byte[] src = "__payload-123__".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        int off = 2;
        int len = 11; // "payload-123"
        String expected = Base64.getEncoder().encodeToString(java.util.Arrays.copyOfRange(src, off, off + len));
        assertEquals(expected, Bytes.base64(src, off, len));
    }

    @Test
    @DisplayName("toHex(): выбрасывает IndexOutOfBounds при некорректном срезе")
    void toHex_outOfBounds() {
        byte[] data = {0x00, 0x01};
        IndexOutOfBoundsException error = assertThrows(IndexOutOfBoundsException.class, () -> Bytes.toHex(data, 1, 5));
        assertEquals(IndexOutOfBoundsException.class, error.getClass());
    }
}
