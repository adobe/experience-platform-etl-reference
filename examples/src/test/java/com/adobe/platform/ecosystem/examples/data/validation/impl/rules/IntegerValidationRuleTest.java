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
public class IntegerValidationRuleTest {
    private SchemaValidationRule<Integer> integerValidationRule;

    @Before
    public void setup() {
        integerValidationRule = IntegerValidationRule.integerValidationRuleBuilder()
            .withMinimum(4)
            .withMaximum(8)
            .build();
    }

    @Test
    public void testSetup() {
        IntegerValidationRule rule = (IntegerValidationRule) integerValidationRule;
        assertEquals(rule.getMinimum().get(), new Integer(4));
        assertEquals(rule.getMaximum().get(), new Integer(8));
    }

    @Test(expected = ValidationException.class)
    public void testMinLength() throws ValidationException {
        integerValidationRule.apply(0);
    }

    @Test(expected = ValidationException.class)
    public void testMaxLength() throws ValidationException {
        integerValidationRule.apply(10);
    }

    @Test
    public void testAllRulesPass() throws ValidationException {
        integerValidationRule.apply(6);
    }
}
