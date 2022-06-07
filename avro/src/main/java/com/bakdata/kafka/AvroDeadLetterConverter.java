package com.bakdata.kafka;

public final class AvroDeadLetterConverter implements DeadLetterConverter<DeadLetter> {

    @Override
    public DeadLetter convert(final DeadLetterDescription deadLetterDescription) {
        return DeadLetter.newBuilder()
                .setInputValue(deadLetterDescription.getInputValue())
                .setCause(ErrorDescription.newBuilder()
                        .setMessage(deadLetterDescription.getCause().getMessage())
                        .setStackTrace(deadLetterDescription.getCause().getStackTrace())
                        .setErrorClass(deadLetterDescription.getCause().getErrorClass())
                        .build())
                .setDescription(deadLetterDescription.getDescription())
                .setTopic(deadLetterDescription.getTopic())
                .setPartition(deadLetterDescription.getPartition())
                .setOffset(deadLetterDescription.getOffset())
                .build();
    }

}
