/*
 * MIT License
 *
 * Copyright (c) 2022 bakdata
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.bakdata.kafka;

import com.bakdata.kafka.proto.v1.Deadletter.DeadLetter;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
class ProtoDeadLetterConverterTest {

    @InjectSoftAssertions
    private SoftAssertions softly;

    static final ProtoDeadLetterConverter converter = new ProtoDeadLetterConverter();

    @Test
    void shouldConvertDeadletterDescriptionWithOptionalFields() {
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
        this.softly.assertThat(deadLetter.getInputValue().getValue()).isEqualTo("inputValue");
        this.softly.assertThat(deadLetter.getCause().getMessage().getValue()).isEqualTo("message");
        this.softly.assertThat(deadLetter.getCause().getStackTrace().getValue()).isEqualTo("stackTrace");
        this.softly.assertThat(deadLetter.getCause().getErrorClass().getValue()).isEqualTo("errorClass");
        this.softly.assertThat(deadLetter.getDescription()).isEqualTo("description");
        this.softly.assertThat(deadLetter.getPartition().getValue()).isEqualTo(1);
        this.softly.assertThat(deadLetter.getOffset().getValue()).isEqualTo(1L);
    }

    @Test
    void shouldConvertDeadletterDescriptionWithoutOptionalFields() {
        final DeadLetterDescription onlyRequiredFieldsDeadLetterDescription = DeadLetterDescription.builder()
                .description("description")
                .cause(DeadLetterDescription.Cause.builder().build())
                .build();
        final DeadLetter deadLetter = converter.convert(onlyRequiredFieldsDeadLetterDescription);
        this.softly.assertThat(deadLetter.hasInputValue()).isFalse();
        this.softly.assertThat(deadLetter.getCause().hasMessage()).isFalse();
        this.softly.assertThat(deadLetter.getCause().hasStackTrace()).isFalse();
        this.softly.assertThat(deadLetter.getCause().hasErrorClass()).isFalse();
        this.softly.assertThat(deadLetter.getDescription()).isEqualTo("description");
        this.softly.assertThat(deadLetter.hasPartition()).isFalse();
        this.softly.assertThat(deadLetter.hasOffset()).isFalse();
    }

}
