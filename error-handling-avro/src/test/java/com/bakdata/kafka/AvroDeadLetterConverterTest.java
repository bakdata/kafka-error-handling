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

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.StandardSoftAssertionsProvider;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
class AvroDeadLetterConverterTest {

    static final AvroDeadLetterConverter converter = new AvroDeadLetterConverter();

    @Test
    void shouldConvertDeadletterDescriptionWithOptionalFields() {
        final StandardSoftAssertionsProvider softly = new SoftAssertions();
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

        final AvroDeadLetter deadLetter = converter.convert(deadLetterDescription);
        softly.assertThat(deadLetter.getInputValue()).hasValue("inputValue");
        softly.assertThat(deadLetter.getCause().getMessage()).hasValue("message");
        softly.assertThat(deadLetter.getCause().getStackTrace()).hasValue("stackTrace");
        softly.assertThat(deadLetter.getCause().getErrorClass()).hasValue("errorClass");
        softly.assertThat(deadLetter.getDescription()).isEqualTo("description");
        softly.assertThat(deadLetter.getPartition()).hasValue(1);
        softly.assertThat(deadLetter.getOffset()).hasValue(1L);
    }

    @Test
    void shouldConvertDeadletterDescriptionWithoutOptionalFields() {
        final StandardSoftAssertionsProvider softly = new SoftAssertions();
        final DeadLetterDescription onlyRequiredFieldsDeadLetterDescription = DeadLetterDescription.builder()
                .description("description")
                .cause(DeadLetterDescription.Cause.builder().build())
                .build();
        final AvroDeadLetter deadLetter = converter.convert(onlyRequiredFieldsDeadLetterDescription);
        softly.assertThat(deadLetter.getInputValue()).isNotPresent();
        softly.assertThat(deadLetter.getCause().getMessage()).isNotPresent();
        softly.assertThat(deadLetter.getCause().getStackTrace()).isNotPresent();
        softly.assertThat(deadLetter.getCause().getErrorClass()).isNotPresent();
        softly.assertThat(deadLetter.getDescription()).isEqualTo("description");
        softly.assertThat(deadLetter.getPartition()).isNotPresent();
        softly.assertThat(deadLetter.getOffset()).isNotPresent();
    }

}
