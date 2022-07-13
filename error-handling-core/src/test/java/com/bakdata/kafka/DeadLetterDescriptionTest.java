package com.bakdata.kafka;

import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatCode;

import com.bakdata.kafka.DeadLetterDescription.Cause;
import org.junit.jupiter.api.Test;

class DeadLetterDescriptionTest {

    private static final Cause CAUSE = Cause.builder().build();

    @Test
    void shouldNotAllowNullDescription() {
        assertThatNullPointerException()
                .isThrownBy(() -> DeadLetterDescription.builder().description(null).cause(CAUSE).build());
    }

    @Test
    void shouldNotAllowNullCause() {
        assertThatNullPointerException()
                .isThrownBy(() -> DeadLetterDescription.builder().description("foo").cause(null).build());
    }

    @Test
    void shouldAllowOtherFieldsNull() {
        assertThatCode(() -> DeadLetterDescription.builder()
                .description("foo")
                .cause(CAUSE)
                .inputValue(null)
                .topic(null)
                .partition(null)
                .offset(null).build()).doesNotThrowAnyException();
    }

    @Test
    void shouldAllowCauseFieldsNull() {
        assertThatCode(() -> Cause.builder()
                .message(null)
                .errorClass(null)
                .stackTrace(null)
                .build()).doesNotThrowAnyException();
    }

}
