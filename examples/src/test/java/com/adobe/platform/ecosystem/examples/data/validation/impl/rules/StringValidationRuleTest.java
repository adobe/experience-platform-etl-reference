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

import java.util.Arrays;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

/**
 * @author vedhera on 11/26/2018.
 */
public class StringValidationRuleTest {

    private SchemaValidationRule<String> stringValidationRule;

    @Before
    public void setup() {
        stringValidationRule = StringValidationRule.stringValidationRuleBuilder()
            .withMinLength(4)
            .withMaxLength(8)
            .withEnumList(Arrays.asList("value1", "value2"))
            .build();
    }

    @Test
    public void testSetup() {
        StringValidationRule rule = (StringValidationRule) stringValidationRule;
        assertEquals(rule.getMinLength().get(), new Integer(4));
        assertEquals(rule.getMaxLength().get(), new Integer(8));
        assertEquals(rule.getEnumList().get().size(), 2);
        assertEquals(rule.getFormat(), Optional.empty());
        assertEquals(rule.getPattern(), Optional.empty());
    }

    @Test(expected = ValidationException.class)
    public void testMinLength() throws ValidationException {
        stringValidationRule.apply("ab");
    }

    @Test(expected = ValidationException.class)
    public void testMaxLength() throws ValidationException {
        stringValidationRule.apply("ababababa");
    }

    @Test(expected = ValidationException.class)
    public void testEnumValidation() throws ValidationException {
        stringValidationRule.apply("value3");
    }

    @Test
    public void testAllRulesPass() throws ValidationException {
        stringValidationRule.apply("value1");
    }
}
