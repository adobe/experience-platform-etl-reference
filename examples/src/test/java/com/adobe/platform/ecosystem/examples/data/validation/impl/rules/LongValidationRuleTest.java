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
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author vedhera on 11/26/2018.
 */
public class LongValidationRuleTest {
    private SchemaValidationRule<Long> longValidationRule;

    @Before
    public void setup() {
        longValidationRule = LongValidationRule.longValidationRuleBuilder()
            .withMinimum(4)
            .withMaximum(8)
            .build();
    }

    @Test
    public void testSetup() {
        LongValidationRule rule = (LongValidationRule) longValidationRule;
        assertEquals(rule.getMinimum().get(), new Long(4));
        assertEquals(rule.getMaximum().get(), new Long(8));
    }

    @Test(expected = ValidationException.class)
    public void testMinLength() throws ValidationException {
        longValidationRule.apply(0l);
    }

    @Test(expected = ValidationException.class)
    public void testMaxLength() throws ValidationException {
        longValidationRule.apply(10l);
    }

    @Test
    public void testAllRulesPass() throws ValidationException {
        longValidationRule.apply(6l);
    }
}
