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

import com.adobe.platform.ecosystem.examples.catalog.model.SchemaField;
import com.adobe.platform.ecosystem.examples.catalog.model.SchemaRef;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKUtil;
import com.adobe.platform.ecosystem.examples.util.ResourceName;
import com.adobe.platform.ecosystem.ut.BaseTest;
import org.json.simple.JSONObject;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SchemaRegistryServiceTest extends BaseTest {
    SchemaRegistryServiceImpl schemaRegistryService;

    @Before
    public void setupTest() throws Exception {
        setUp();
        schemaRegistryService = new SchemaRegistryServiceImpl(ConnectorSDKUtil.getInstance().getEndPoint(ResourceName.SCHEMA_REGISTRY), httpClient);
    }

    @Test
    public void testConstructor() throws Exception {
        assertTrue(schemaRegistryService != null);

        schemaRegistryService = new SchemaRegistryServiceImpl(null);
        assertTrue(schemaRegistryService != null);

        schemaRegistryService = new SchemaRegistryServiceImpl(ConnectorSDKUtil.getInstance().getEndPoint(ResourceName.SCHEMA_REGISTRY));
        assertTrue(schemaRegistryService != null);
    }

    @Test
    public void testGetSchemaFields() throws IOException, ConnectorSDKException {
        setupTestForHttpOutput(schemaSample);
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("id" , "https://ns.adobe.com/test/Id");
        jsonObject.put("contentType" , "application/vnd.adobe.xed+json");
        SchemaRef schemaRef = new SchemaRef(jsonObject);
        List<SchemaField> schemaFields = schemaRegistryService.getSchemaFields("testOrg", "testToken", schemaRef, true);
        assertTrue(schemaFields !=  null);
        assertEquals(schemaFields.size(),4);
    }
}
