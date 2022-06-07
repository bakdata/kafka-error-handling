import com.bakdata.kafka.DeadLetterDescription;
import com.bakdata.kafka.proto.v1.Deadletter.DeadLetter;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
class ProtoDeadLetterConverterTest {

    static final ProtoDeadLetterConverter converter = new ProtoDeadLetterConverter();

    @Test
    void shouldConvertDeadletterDescriptionWithOptionalFields(final SoftAssertions softly) {
        final DeadLetterDescription deadLetterDescription = DeadLetterDescription.builder()
                .inputValue("inputValue")
                .cause(DeadLetterDescription.Cause.builder()
                        .message("message")
                        .stackTrace("stackTrace")
                        .errorClass("errorClass")
                        .build())
                .description("description")
                .topic("topic")
                .partition(1)
                .offset(1L)
                .build();

        final DeadLetter deadLetter = converter.convert(deadLetterDescription);
        softly.assertThat(deadLetter.getInputValue().getValue()).isEqualTo("inputValue");
        softly.assertThat(deadLetter.getCause().getMessage().getValue()).isEqualTo("message");
        softly.assertThat(deadLetter.getCause().getStackTrace().getValue()).isEqualTo("stackTrace");
        softly.assertThat(deadLetter.getCause().getErrorClass().getValue()).isEqualTo("errorClass");
        softly.assertThat(deadLetter.getDescription()).isEqualTo("description");
        softly.assertThat(deadLetter.getPartition().getValue()).isEqualTo(1);
        softly.assertThat(deadLetter.getOffset().getValue()).isEqualTo(1L);
    }

    @Test
    void shouldConvertDeadletterDescriptionWithoutOptionalFields(final SoftAssertions softly) {
        final DeadLetterDescription onlyRequiredFieldsDeadLetterDescription = DeadLetterDescription.builder()
                .description("description")
                .cause(DeadLetterDescription.Cause.builder().build())
                .build();
        final DeadLetter deadLetter = converter.convert(onlyRequiredFieldsDeadLetterDescription);
        softly.assertThat(deadLetter.hasInputValue()).isFalse();
        softly.assertThat(deadLetter.getCause().hasMessage()).isFalse();
        softly.assertThat(deadLetter.getCause().hasStackTrace()).isFalse();
        softly.assertThat(deadLetter.getCause().hasErrorClass()).isFalse();
        softly.assertThat(deadLetter.getDescription()).isEqualTo("description");
        softly.assertThat(deadLetter.hasPartition()).isFalse();
        softly.assertThat(deadLetter.hasOffset()).isFalse();
    }

}
