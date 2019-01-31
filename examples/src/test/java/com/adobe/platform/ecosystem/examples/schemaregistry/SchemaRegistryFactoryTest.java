/*
 * ADOBE CONFIDENTIAL
 * __________________
 * Copyright 2019 Adobe Systems Incorporated
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
package com.adobe.platform.ecosystem.examples.schemaregistry;

import com.adobe.platform.ecosystem.examples.schemaregistry.api.SchemaRegistryService;
import com.adobe.platform.ecosystem.ut.BaseTest;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class SchemaRegistryFactoryTest extends BaseTest {

    @Test
    public void testSchemaRegistryFactory() throws Exception {
        setUp();
        assertTrue(SchemaRegistryFactory.getSchemaRegistryService() != null);
        SchemaRegistryService schemaRegistryService = SchemaRegistryFactory.getSchemaRegistryService(null);
        assertTrue(schemaRegistryService != null);
    }

}
