package kz.qazmarka.h2k.kafka.ensure.util;

/**
 * Утилита проверки имён Kafka‑топиков. Выделена из TopicEnsureService, чтобы упростить будущий
 * рефактор и повторное использование правил в тестах/других компонентах.
 */
public final class TopicNameValidator {

    private TopicNameValidator() {
    }

    /**
     * Быстрая проверка имени топика: допускаются латиница/цифры/._-, без "."/".." и не длиннее лимита.
     */
    public static boolean isValid(String topic, int maxLength) {
        if (topic == null) {
            return false;
        }
        int len = topic.length();
        if (len == 0 || len > maxLength) {
            return false;
        }
        if (".".equals(topic) || "..".equals(topic)) {
            return false;
        }
        for (int i = 0; i < len; i++) {
            if (!isAllowedTopicChar(topic.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean isAllowedTopicChar(char ch) {
        return (ch >= 'a' && ch <= 'z')
                || (ch >= 'A' && ch <= 'Z')
                || (ch >= '0' && ch <= '9')
                || ch == '.'
                || ch == '-'
                || ch == '_';
    }
}
