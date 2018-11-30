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
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Optional;

/**
 * Concrete {@link SchemaValidationRule} for
 * type {@link String}.
 *
 * @author vedhera on 11/12/2018.
 */
public class StringValidationRule extends SchemaValidationRule<String> {
    private Optional<Integer> minLength;

    private Optional<Integer> maxLength;

    private Optional<String> format;

    private Optional<String> pattern;

    private Optional<List<String>> enumList;

    private StringValidationRule() {
    }

    /**
     * Implementation for
     * {@link String} input types.
     */
    @Override
    public void apply(String value) throws ValidationException {
        if (this.minLength.isPresent()) {
            if (value.length() < this.minLength.get()) {
                throw ValidationExceptionBuilder
                    .stringRuleExceptionBuilder()
                    .failingLowerBound()
                    .withValue(value)
                    .withBound(minLength.get().toString())
                    .build();
            }
        }

        if (this.maxLength.isPresent()) {
            if (value.length() > this.maxLength.get()) {
                throw ValidationExceptionBuilder
                    .stringRuleExceptionBuilder()
                    .failingUpperBound()
                    .withValue(value)
                    .withBound(maxLength.get().toString())
                    .build();
            }
        }

        if (this.enumList.isPresent()) {
            final boolean match = this.enumList
                .get()
                .stream()
                .anyMatch(e -> e.equals(value));
            if (!match)
                throw ValidationExceptionBuilder
                    .stringRuleExceptionBuilder()
                    .failingRangeBound()
                    .withValue(value)
                    .withBound(buildList(enumList.get()))
                    .build();
        }

        // TODO: Add validations using format and pattern.
        return;
    }

    private String buildList(List<String> allowedValues) {
        return StringUtils.join(allowedValues, '.');
    }

    public static StringValidationRule.Builder stringValidationRuleBuilder() {
        return new StringValidationRule.Builder();
    }

    /**
     * Builder for type
     * {@link StringValidationRule}
     */
    public static class Builder {
        private StringValidationRule validationRule = new StringValidationRule();

        protected Builder() {
            this.validationRule.minLength = Optional.empty();
            this.validationRule.maxLength = Optional.empty();
            this.validationRule.format = Optional.empty();
            this.validationRule.pattern = Optional.empty();
            this.validationRule.enumList = Optional.empty();
        }

        public Builder withMinLength(int minLength) {
            this.validationRule.minLength = Optional.of(minLength);
            return this;
        }

        public Builder withMaxLength(int maxLength) {
            this.validationRule.maxLength = Optional.of(maxLength);
            return this;
        }

        public Builder withEnumList(List<String> enumList) {
            this.validationRule.enumList = Optional.of(enumList);
            return this;
        }

        public Builder withFormat(String format) {
            this.validationRule.format = Optional.of(format);
            return this;
        }

        public Builder withPattern(String pattern) {
            this.validationRule.pattern = Optional.of(pattern);
            return this;
        }

        public SchemaValidationRule<String> build() {
            return this.validationRule;
        }
    }

    public Optional<Integer> getMinLength() {
        return minLength;
    }

    public Optional<Integer> getMaxLength() {
        return maxLength;
    }

    public Optional<List<String>> getEnumList() {
        return enumList;
    }

    public Optional<String> getFormat() {
        return format;
    }

    public Optional<String> getPattern() {
        return pattern;
    }
}
