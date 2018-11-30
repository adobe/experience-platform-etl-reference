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
package com.adobe.platform.ecosystem.examples.data.validation.exception;

/**
 * @author vedhera on 11/28/2018.
 */
public class ValidationExceptionBuilder {
    private static final String LOWER_BOUND_ERROR = "Value: [%s] not in bounds of property [minimum:%s]";

    private static final String UPPER_BOUND_ERROR = "Value: [%s] not in bounds of property [maximum:%s]";

    private static final String STRING_LOWER_BOUND_ERROR = "Value: [%s] not in bounds of property [minLength:%s]";

    private static final String STRING_UPPER_BOUND_ERROR = "Value: [%s] not in bounds of property [maxLength:%s]";

    private static final String STRING_ENUM_BOUND_ERROR = "Value: [%s] not in bounds of property [enum:(%s)]";

    /**
     * Entry point for building a
     * {@link ValidationException} for
     * numeric based rules.
     */
    public static <T extends Number> InvalidBound<T> numericRuleExceptionBuilder() {
        return new NumericRuleExceptionBuilder<>();
    }

    /**
     * Entry point for building a
     * {@link ValidationException} for
     * string based rules.
     */
    public static InvalidRangeBound<String> stringRuleExceptionBuilder() {
        return new StringRuleExceptionBuilder();
    }

    /**
     * Builder to provide implementation
     * for building a String based rule
     * validation exceptions.
     */
    private static class StringRuleExceptionBuilder implements InvalidRangeBound<String>, CurrentValue<String>, BoundValue<String>, Builder {
        private String errorMsg;
        private String value;
        private String bound;

        @Override
        public CurrentValue<String> failingLowerBound() {
            this.errorMsg = STRING_LOWER_BOUND_ERROR;
            return this;
        }

        @Override
        public CurrentValue<String> failingUpperBound() {
            this.errorMsg = STRING_UPPER_BOUND_ERROR;
            return this;
        }

        @Override
        public CurrentValue<String> failingRangeBound() {
            this.errorMsg = STRING_ENUM_BOUND_ERROR;
            return this;
        }

        @Override
        public BoundValue<String> withValue(String data) {
            this.value = data;
            return this;
        }

        @Override
        public Builder withBound(String bound) {
            this.bound = bound;
            return this;
        }

        @Override
        public ValidationException build() {
            final String errorMessage = String.format(errorMsg, value, bound);
            return new ValidationException(errorMessage);
        }
    }


    /**
     * Builder to provide implementation
     * for building a Numeric based rule
     * validation exceptions.
     */
    private static class NumericRuleExceptionBuilder<T extends Number> implements InvalidBound<T>, CurrentValue<T>, BoundValue<T>, Builder {
        private String errorMsg;
        private T value;
        private T bound;

        @Override
        public CurrentValue<T> failingLowerBound() {
            this.errorMsg = LOWER_BOUND_ERROR;
            return this;
        }

        @Override
        public CurrentValue<T> failingUpperBound() {
            this.errorMsg = UPPER_BOUND_ERROR;
            return this;
        }

        @Override
        public BoundValue<T> withValue(T data) {
            this.value = data;
            return this;
        }

        @Override
        public Builder withBound(T bound) {
            this.bound = bound;
            return this;
        }

        @Override
        public ValidationException build() {
            final String errorMessage = String.format(errorMsg, value, bound);
            return new ValidationException(errorMessage);
        }
    }

    /**
     * @param <T>
     */
    public interface InvalidRangeBound<T> extends InvalidBound<T> {
        CurrentValue<T> failingRangeBound();
    }

    /**
     * Interface for providing
     * APIs to set correct error
     * message based on the type
     * of failure.
     *
     * Use {@link InvalidBound#failingLowerBound()} if
     * the value is less than the {@code minimum} value
     * allowed.
     *
     * Use {@link InvalidBound#failingUpperBound()} ()} if
     * the value is more than the {@code maximum} value
     * allowed.
     */
    public interface InvalidBound<T> {
        /**
         * @return Builder for providing input of type
         *         {@code T}.
         */
        CurrentValue<T> failingLowerBound();

        /**
         * @return Builder for providing input of type
         *         {@code T}.
         */
        CurrentValue<T> failingUpperBound();
    }

    /**
     * Interface to provide
     * the value which causes
     * a validation failure.
     * @param <T> Generic type {@code T}
     */
    @FunctionalInterface
    public interface CurrentValue<T> {
        BoundValue<T> withValue(T data);
    }

    /**
     * Interface to provide
     * the bound value which
     * is not satisfied by the
     * input value.
     * @param <T> Generic type {@code T}
     */
    @FunctionalInterface
    public interface BoundValue<T> {
        Builder withBound(T bound);
    }

    /**
     * Interface for
     * finalizing and returning
     * the created
     * {@link ValidationException}.
     */
    @FunctionalInterface
    public interface Builder {
        ValidationException build();
    }
}
