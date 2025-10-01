package kz.qazmarka.h2k.kafka.ensure;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import kz.qazmarka.h2k.kafka.ensure.util.TopicNameValidator;

class TopicNameValidatorTest {

    @Test
    @DisplayName("Корректные имена проходят проверку")
    void validNames() {
        assertTrue(TopicNameValidator.isValid("abc-123", 249));
        assertTrue(TopicNameValidator.isValid("A._-Z", 249));
    }

    @Test
    @DisplayName("Некорректные имена отклоняются")
    void invalidNames() {
        assertFalse(TopicNameValidator.isValid(null, 249));
        assertFalse(TopicNameValidator.isValid("", 249));
        assertFalse(TopicNameValidator.isValid(".", 249));
        assertFalse(TopicNameValidator.isValid("..", 249));
        assertFalse(TopicNameValidator.isValid("bad*topic", 249));
    }

    @Test
    @DisplayName("Длина превышающая лимит отклоняется")
    void lengthLimit() {
        String tooLong = new String(new char[5]).replace('\0', 'a');
        assertFalse(TopicNameValidator.isValid(tooLong, 4));
    }
}
