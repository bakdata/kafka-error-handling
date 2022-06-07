import com.bakdata.kafka.DeadLetterTransformer;
import com.bakdata.kafka.ProcessingError;
import com.bakdata.kafka.proto.v1.Deadletter.DeadLetter;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import lombok.NonNull;
public final class ProtoDeadLetterTransformer<V> extends DeadLetterTransformer<V, DeadLetter> {

    private ProtoDeadLetterTransformer(final @NonNull String description) {
        super(description, new ProtoDeadLetterConverter());
    }

    public static <V> ValueTransformerSupplier<ProcessingError<V>, DeadLetter> create(final String description) {
        return () -> new ProtoDeadLetterTransformer<>(description);
    }
}
