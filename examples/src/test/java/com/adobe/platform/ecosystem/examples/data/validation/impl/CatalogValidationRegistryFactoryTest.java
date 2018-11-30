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
package com.adobe.platform.ecosystem.examples.data.validation.impl;

import com.adobe.platform.ecosystem.examples.catalog.model.DataType;
import com.adobe.platform.ecosystem.examples.catalog.model.SchemaField;
import com.adobe.platform.ecosystem.examples.data.validation.api.ValidationRegistry;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.ArrayList;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

/**
 * @author vedhera on 11/26/2018
 */
public class CatalogValidationRegistryFactoryTest {

    @Mock
    SchemaField schemaField;

    private CatalogValidationRegistryFactory factory;

    @Before
    public void setup() {
        initMocks(this);

        when(schemaField.getType()).thenReturn(DataType.StringType);
        when(schemaField.getName()).thenReturn("primitiveField");
        when(schemaField.getRules()).thenReturn(new ArrayList<>());

        factory = new CatalogValidationRegistryFactory();
    }

    @Test
    public void testRegistryFactory() {
        ValidationRegistry validationRegistry = factory.get(schemaField);
        assertNotNull(validationRegistry);
        assertTrue(validationRegistry instanceof CatalogValidationRegistry);
    }
}
