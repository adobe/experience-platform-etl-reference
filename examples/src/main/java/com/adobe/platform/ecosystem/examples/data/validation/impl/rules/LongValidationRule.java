/*
 * ADOBE CONFIDENTIAL
 * __________________
 * Copyright 2018 Adobe Systems Incorporated
 * All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Adobe Systems Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to Adobe Systems Incorporated and its
 * suppliers and are protected by all applicable intellectual property laws,
 * including trade secret and copyright laws.
 *
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Adobe Systems Incorporated.
 *
 *
 */
package com.adobe.platform.ecosystem.examples.data.validation.impl.rules;

import com.adobe.platform.ecosystem.examples.data.validation.exception.ValidationException;
import com.adobe.platform.ecosystem.examples.data.validation.exception.ValidationExceptionBuilder;

import java.util.Optional;

/**
 * Concrete {@link SchemaValidationRule} for
 * type {@link java.lang.Long}.
 *
 * @author vedhera on 11/12/2018.
 */
public class LongValidationRule extends SchemaValidationRule<Long> {

    private Optional<Long> minimum;

    private Optional<Long> maximum;

    private LongValidationRule() {

    }

    /**
     * Implementation for
     * {@link java.lang.Long} input types.
     */
    @Override
    public void apply(Long value) throws ValidationException {
        if (minimum.isPresent()) {
            if (value < minimum.get()) {
                throw ValidationExceptionBuilder
                    .<Long>numericRuleExceptionBuilder()
                    .failingLowerBound()
                    .withValue(value)
                    .withBound(minimum.get())
                    .build();
            }
        }

        if (maximum.isPresent()) {
            if (value > maximum.get()) {
                throw ValidationExceptionBuilder
                    .<Long>numericRuleExceptionBuilder()
                    .failingUpperBound()
                    .withValue(value)
                    .withBound(maximum.get())
                    .build();
            }
        }
        return;
    }

    public static LongValidationRule.Builder longValidationRuleBuilder() {
        return new LongValidationRule.Builder();
    }

    /**
     * Builder for type
     * {@link LongValidationRule}
     */
    public static class Builder {

        private LongValidationRule validationRule = new LongValidationRule();

        protected Builder() {
            this.validationRule.minimum = Optional.empty();
            this.validationRule.maximum = Optional.empty();
        }

        public LongValidationRule.Builder withMinimum(long minimum) {
            this.validationRule.minimum = Optional.of(minimum);
            return this;
        }

        public LongValidationRule.Builder withMaximum(long maximum) {
            this.validationRule.maximum = Optional.of(maximum);
            return this;
        }

        public SchemaValidationRule<Long> build() {
            return this.validationRule;
        }

    }

    public Optional<Long> getMinimum() {
        return minimum;
    }

    public Optional<Long> getMaximum() {
        return maximum;
    }
}
