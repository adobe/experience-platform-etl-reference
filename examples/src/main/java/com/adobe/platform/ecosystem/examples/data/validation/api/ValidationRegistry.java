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

    import java.util.List;

/**
 * Registry to obtain validation rules
 * for input schema field path.
 *
 * @author vedhera on 11/12/2018.
 */
public interface ValidationRegistry<T> {

    /**
     * Given a list of traversable schema
     * path for a field, returns a {@code List}
     * of Rules
     *
     * @param path
     * @return
     */
    List<Rule<T>> getValidationRule(List<String> path);
}
