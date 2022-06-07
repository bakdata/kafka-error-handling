package com.bakdata.kafka;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

@Builder
@Value
public class DeadLetterDescription {

    @Builder
    @Value
    public static class Cause {
        String message;
        String stackTrace;
        String errorClass;
    }

    @NonNull String description;
    Cause cause;
    String inputValue;
    String topic;
    Integer partition;
    Long offset;
}
