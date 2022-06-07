import com.bakdata.kafka.AvroDeadLetterConverter;
import com.bakdata.kafka.DeadLetter;
import com.bakdata.kafka.DeadLetterDescription;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
class AvroDeadLetterConverterTest {

    static final AvroDeadLetterConverter converter = new AvroDeadLetterConverter();

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
        softly.assertThat(deadLetter.getInputValue()).isPresent();
        softly.assertThat(deadLetter.getInputValue().get()).isEqualTo("inputValue");
        softly.assertThat(deadLetter.getCause().getMessage()).isPresent();
        softly.assertThat(deadLetter.getCause().getMessage().get()).isEqualTo("message");
        softly.assertThat(deadLetter.getCause().getStackTrace()).isPresent();
        softly.assertThat(deadLetter.getCause().getStackTrace().get()).isEqualTo("stackTrace");
        softly.assertThat(deadLetter.getCause().getErrorClass()).isPresent();
        softly.assertThat(deadLetter.getCause().getErrorClass().get()).isEqualTo("errorClass");
        softly.assertThat(deadLetter.getDescription()).isEqualTo("description");
        softly.assertThat(deadLetter.getPartition()).isPresent();
        softly.assertThat(deadLetter.getPartition().get()).isEqualTo(1);
        softly.assertThat(deadLetter.getOffset()).isPresent();
        softly.assertThat(deadLetter.getOffset().get()).isEqualTo(1L);
    }

    @Test
    void shouldConvertDeadletterDescriptionWithoutOptionalFields(final SoftAssertions softly) {
        final DeadLetterDescription onlyRequiredFieldsDeadLetterDescription = DeadLetterDescription.builder()
                .description("description")
                .cause(DeadLetterDescription.Cause.builder().build())
                .build();
        final DeadLetter deadLetter = converter.convert(onlyRequiredFieldsDeadLetterDescription);
        softly.assertThat(deadLetter.getInputValue()).isNotPresent();
        softly.assertThat(deadLetter.getCause().getMessage()).isNotPresent();
        softly.assertThat(deadLetter.getCause().getStackTrace()).isNotPresent();
        softly.assertThat(deadLetter.getCause().getErrorClass()).isNotPresent();
        softly.assertThat(deadLetter.getDescription()).isEqualTo("description");
        softly.assertThat(deadLetter.getPartition()).isNotPresent();
        softly.assertThat(deadLetter.getOffset()).isNotPresent();
    }

}
