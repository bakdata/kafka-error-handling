import com.bakdata.kafka.DeadLetterConverter;
import com.bakdata.kafka.DeadLetterDescription;
import com.bakdata.kafka.proto.v1.Deadletter.DeadLetter;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import com.google.protobuf.StringValue;

public class ProtoDeadLetterConverter implements DeadLetterConverter<DeadLetter> {

    @Override
    public DeadLetter convert(final DeadLetterDescription deadLetterDescription) {
        final DeadLetter.Builder builder = DeadLetter.newBuilder();
        final DeadLetterDescription.Cause cause = deadLetterDescription.getCause();
        final DeadLetter.Cause.Builder causeBuilder = builder.getCauseBuilder();
        // Everything is optional with fix defaults in proto3, so use wrappers
        if (cause.getMessage() != null) {
            causeBuilder.setMessage(StringValue.of(cause.getMessage()));
        }
        if (cause.getStackTrace() != null) {
            causeBuilder.setStackTrace(StringValue.of(cause.getStackTrace()));
        }
        if (cause.getErrorClass() != null) {
            causeBuilder.setErrorClass(StringValue.of(cause.getErrorClass()));
        }
        builder.setDescription(deadLetterDescription.getDescription());
        if (deadLetterDescription.getInputValue() != null) {
            builder.setInputValue(StringValue.of(deadLetterDescription.getInputValue()));
        }
        if (deadLetterDescription.getTopic() != null) {
            builder.setTopic(StringValue.of(deadLetterDescription.getTopic()));
        }
        if (deadLetterDescription.getPartition() != null) {
            builder.setPartition(Int32Value.of(deadLetterDescription.getPartition()));
        }
        if (deadLetterDescription.getOffset() != null) {
            builder.setOffset(Int64Value.of(deadLetterDescription.getOffset()));
        }
        return builder.build();
    }
}
