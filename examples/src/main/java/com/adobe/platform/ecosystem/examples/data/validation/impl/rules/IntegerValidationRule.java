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

/**
 * Concrete {@link SchemaValidationRule} for
 * following types
 * <pre>
 *     1. {@link Integer}
 *     2. {@link Short}
 *     3. {@link Byte}
 * </pre>
 *
 * @author vedhera on 11/12/2018.
 */
public class IntegerValidationRule extends SchemaValidationRule<Integer> {

    @Override
    public boolean apply(Integer value) {
        return false;
    }
}
