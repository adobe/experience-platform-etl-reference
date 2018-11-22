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

import com.adobe.platform.ecosystem.examples.data.validation.api.Rule;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Interface to define a validation
 * rule.
 *
 * @param <T> The type parameter.
 * @author vedhera on 11/18/2018.
 */
public abstract class SchemaValidationRule<T> implements Rule<T> {

    /**
     * Applies the rule and returns
     * <code>code</code>if the input passes
     * all the constraints of the defined rule
     * else returns <code>false</code>.
     *
     * @param value
     * @return true/false
     */
    public abstract boolean apply(T value);


    /**
     * Helper method to instantiate
     * builders for {@link SchemaValidationRule}
     *
     * @return
     */
    public static BuilderData<JSONObject> fromJsonBuilder() {
        return new JsonBuilder();
    }

    /**
     * Builder for building rules from
     * {@link JSONObject} data types.
     */
    private static class JsonBuilder implements BuilderData<JSONObject>, Builder {
        private JSONObject data;

        /**
         * Builder to accept JSON which
         * contains metadata information
         * as key-value pairs for a given
         * field.
         *
         * @param data JSONObject
         * @return {@link Builder} instance
         */
        @Override
        public Builder with(JSONObject data) {
            this.data = data;
            return this;
        }

        @Override
        public SchemaValidationRule<? extends Object> build() {
            final String type = (String) data.get("type");
            switch (type.toLowerCase()) {
                case "string":
                    return buildStringRule();
                case "integer":
                    final String metaXdmType = data.containsKey("meta:xdmType") ? (String) data.get("meta:xdmType") : "";
                    if (metaXdmType.equalsIgnoreCase("long")) {
                        return buildLongValidationRule();
                    } else {
                        return buildIntegerRule();
                    }

                default:
                    return null;
            }
        }

        private SchemaValidationRule<Integer> buildIntegerRule() {
            IntegerValidationRule.Builder runningBuilder = IntegerValidationRule.integerValidationRuleBuilder();

            if (data.containsKey("maximum")) {
                runningBuilder.withMaximum(Integer.valueOf(data.get("maximum").toString()));
            }

            if (data.containsKey("minimum")) {
                runningBuilder.withMinimum(Integer.valueOf(data.get("minimum").toString()));
            }

            return runningBuilder.build();
        }

        private SchemaValidationRule<Long> buildLongValidationRule() {
            LongValidationRule.Builder runningBuilder = LongValidationRule.longValidationRuleBuilder();

            if (data.containsKey("maximum")) {
                runningBuilder.withMaximum((Long) data.get("maximum"));
            }

            if (data.containsKey("minimum")) {
                runningBuilder.withMinimum((Long) data.get("minimum"));
            }

            return runningBuilder.build();
        }

        private SchemaValidationRule<String> buildStringRule() {
            StringValidationRule.Builder runningBuilder = StringValidationRule.stringValidationRuleBuilder();

            if (data.containsKey("maxLength")) {
                runningBuilder.withMaxLength(Integer.valueOf(data.get("maxLength").toString()));
            }

            if (data.containsKey("minLength")) {
                runningBuilder.withMinLength(Integer.valueOf(data.get("maxLength").toString()));
            }

            if (data.containsKey("format")) {
                runningBuilder.withFormat((String) data.get("format"));
            }

            if (data.containsKey("pattern")) {
                runningBuilder.withPattern((String) data.get("pattern"));
            }

            if (data.containsKey("enum")) {
                JSONArray jsonArray = (JSONArray) data.get("enum");
                final List<String> enums = Arrays.stream(jsonArray.toArray())
                    .map(key -> (String) key)
                    .collect(Collectors.toList());
                runningBuilder.withEnumList(enums);
            }

            return runningBuilder.build();
        }
    }


    /**
     * Starting point for building
     * validation rule.
     *
     * @param <K> type parameter for input type.
     */
    @FunctionalInterface
    public interface BuilderData<K> {
        Builder with(K data);
    }


    /**
     * Interface for finalizing
     * and building the validation
     * rule.
     */
    @FunctionalInterface
    public interface Builder {
        SchemaValidationRule build();
    }

}
