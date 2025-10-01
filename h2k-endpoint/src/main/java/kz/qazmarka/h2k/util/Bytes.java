package kz.qazmarka.h2k.util;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Быстрые побитовые/байтовые утилиты без сторонних зависимостей.
 * Содержит только «горячие» методы, востребованные при построении payload.
 */
public final class Bytes {
    private Bytes() { /* no instances */ }

    // Общая таблица для быстрого вывода HEX
    private static final char[] HEX = "0123456789abcdef".toCharArray();
    // Общий Base64-энкодер (JDK8: только array-based API)
    private static final Base64.Encoder BASE64 = Base64.getEncoder();

    /**
     * Конвертирует срез байтов в HEX-строку без промежуточных объектов.
     *
     * @throws IndexOutOfBoundsException если (off,len) выходят за границы массива
     */
    public static String toHex(byte[] a, int off, int len) {
        if (a == null || len == 0) return "";
        if (off < 0 || len < 0 || off > a.length - len) {
            throw new IndexOutOfBoundsException("срез hex вне границ: off=" + off + " len=" + len + " cap=" + a.length);
        }
        char[] out = new char[len * 2];
        int p = 0;
        int iEnd = off + len;
        for (int i = off; i < iEnd; i++) {
            int v = a[i] & 0xFF;
            out[p++] = HEX[v >>> 4];
            out[p++] = HEX[v & 0x0F];
        }
        return new String(out);
    }

    /**
     * Кодирует срез байтов в Base64 без промежуточного копирования.
     */
    public static String base64(byte[] a, int off, int len) {
        if (a == null) return null;
        if (len <= 0) return "";
        ByteBuffer src = ByteBuffer.wrap(a, off, len);
        ByteBuffer encoded = BASE64.encode(src); // создаёт новый heap-буфер
        if (encoded.hasArray()) {
            return new String(encoded.array(), encoded.arrayOffset(), encoded.remaining(), StandardCharsets.US_ASCII);
        }
        byte[] out = new byte[encoded.remaining()];
        encoded.get(out);
        return new String(out, StandardCharsets.US_ASCII);
    }
}
