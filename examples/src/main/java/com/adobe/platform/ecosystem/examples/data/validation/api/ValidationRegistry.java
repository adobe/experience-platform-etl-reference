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
package com.adobe.platform.ecosystem.examples.data.validation.api;

import com.adobe.platform.ecosystem.examples.data.validation.impl.TraversablePath;

import java.util.List;

/**
 * Registry to obtain validation rules
 * for input schema field path.
 * For getting rule for schema {@code firstName}
 * below, a {@code List} of (person, firstname)
 * should be passed as the input.
 *
 * <pre>
 *     "person" : {
 *         "type" : "object",
 *         "properties : {
 *             "firstName" : {
 *                 "type" : "string"
 *             }
 *         }
 *     }
 * </pre>
 *
 * @author vedhera on 11/12/2018.
 */
public interface ValidationRegistry {

    /**
     * Given a list of traversable schema
     * path for a field, returns a {@code List<Rule<String>>}
     * rules.
     *
     * @param path root to leaf path.
     * @return rules for type {@link java.lang.String}
     */
    List<Rule<String>> getStringValidationRule(TraversablePath path);

    /**
     * Given a list of traversable schema
     * path for a field, returns a {@code List<Rule<Integer>>}
     * rules.
     *
     * @param path root to leaf path.
     * @return rules for type {@link java.lang.Integer}
     */
    List<Rule<Integer>> getIntegerValidationRule(TraversablePath path);

    /**
     * Given a list of traversable schema
     * path for a field, returns a {@code List<Rule<Long>>}
     * rules.
     *
     * @param path root to leaf path.
     * @return rules for type {@link java.lang.Long}
     */
    List<Rule<Long>> getLongValidationRule(TraversablePath path);
}
