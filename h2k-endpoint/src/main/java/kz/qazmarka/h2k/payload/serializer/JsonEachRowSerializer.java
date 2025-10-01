package kz.qazmarka.h2k.payload.serializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import kz.qazmarka.h2k.util.JsonWriter;

/**
 * Сериализатор формата JSONEachRow.
 *
 * Тонкая обёртка над {@link JsonWriter}: преобразует переданную карту в одну JSON-строку
 * и добавляет завершающий перевод строки ('\n'), как требует формат JSONEachRow.
 *
 * Характеристики:
 * - Stateless/потокобезопасен: не хранит состояния, допускается переиспользование в нескольких потоках.
 * - Минимум аллокаций: использует один StringBuilder и отдаёт байты UTF-8.
 * - Совместимость: вывод — валидный JSON, пригодный для ClickHouse (Kafka Engine, JSONEachRow) и любых потребителей JSON.
 *
 * Все детали экранирования, поддерживаемых типов и поведения чисел делегируются в {@link JsonWriter}.
 */
public final class JsonEachRowSerializer implements PayloadSerializer {

    /** MIME-тип сериализованного вывода. Совместим с Kafka/HTTP‑клиентами. */
    public static final String CONTENT_TYPE = "application/json";

    /** Короткое человекочитаемое имя формата (для логов/метрик). */
    public static final String NAME = "json-each-row";

    /**
     * Сериализует карту полей в одну строку JSONEachRow.
     * Итерация полей соответствует порядку {@link Map#entrySet()} исходной карты.
     *
     * @param obj карта полей и значений; не должна быть {@code null}
     * @return байтовый массив (UTF‑8) одной JSON‑строки + перевод строки
     * @throws NullPointerException если {@code obj} равен {@code null}
     */
    @Override
    public byte[] serialize(Map<String, ?> obj) {
        if (obj == null) throw new NullPointerException("obj");
        // Предварительная оценка ёмкости: ~40 символов на поле + небольшой базовый запас
        StringBuilder sb = new StringBuilder(64 + obj.size() * 40);
        JsonWriter.writeMap(sb, obj);
        sb.append('\n'); // JSONEachRow: одна запись = одна строка
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    /** Короткое имя формата (для логов/метрик). */
    @Override
    public String format() {
        return NAME;
    }

    /** MIME-тип сериализованного представления. */
    @Override
    public String contentType() {
        return CONTENT_TYPE;
    }
}
