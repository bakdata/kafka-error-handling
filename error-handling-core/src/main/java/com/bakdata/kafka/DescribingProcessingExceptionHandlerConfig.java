/*
 * MIT License
 *
 * Copyright (c) 2026 bakdata
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

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

/**
 * Configuration for {@link DescribingProcessingExceptionHandler}. It offers configuration of the following properties:
 * <ul>
 *     <li>{@link #FILTER_CONFIG}</li>
 * </ul>
 */
public class DescribingProcessingExceptionHandlerConfig extends AbstractConfig {
    public static final String PREFIX = "error.handling.";
    public static final String FILTER_CONFIG = PREFIX + "filter";
    private static final ConfigDef config = baseConfigDef();
    public static final String FILTER_DOC =
            "Class implementing a filter for errors which should be thrown and not captured";

    /**
     * Create a new configuration from the given properties
     *
     * @param originals properties for configuring this config
     */
    public DescribingProcessingExceptionHandlerConfig(final Map<?, ?> originals) {
        super(config, originals);
    }

    private static ConfigDef baseConfigDef() {
        return new ConfigDef()
                .define(FILTER_CONFIG, Type.CLASS, NothingIsRecoverableErrorFilter.class, Importance.MEDIUM,
                        FILTER_DOC);
    }

    public ErrorFilter getErrorFilter() {
        return this.getConfiguredInstance(FILTER_CONFIG, ErrorFilter.class);
    }
}
