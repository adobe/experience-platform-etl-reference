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

import com.adobe.platform.ecosystem.examples.catalog.model.Schema;
import com.adobe.platform.ecosystem.examples.catalog.model.SchemaField;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.apache.commons.io.IOUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * @author vedhera on 11/25/2018
 */
@RunWith(DataProviderRunner.class)
public class CatalogValidationRegistryTest {

    private CatalogValidationRegistry catalogValidationRegistry;

    private static String primitiveFieldJson;

    private static String primitiveObjectJson;

    private static String primitiveObjectWithRootJson;

    private static String primitiveArrayJson;

    private static String structArrayJson;

    static {
        try {
            primitiveFieldJson = IOUtils.toString(
                ClassLoader.getSystemResource("primitive.json"),
                "UTF-8"
            );

            primitiveObjectJson = IOUtils.toString(
                ClassLoader.getSystemResource("objectXDM.json"),
                "UTF-8"
            );

            primitiveObjectWithRootJson = IOUtils.toString(
                ClassLoader.getSystemResource("objectWithRootNode.json"),
                "UTF-8"
            );

            primitiveArrayJson = IOUtils.toString(
                ClassLoader.getSystemResource("primitiveXdmArray.json"),
                "UTF-8"
            );

            structArrayJson = IOUtils.toString(
                ClassLoader.getSystemResource("structXdmArray.json"),
                "UTF-8"
            );
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Before
    public void setup() throws IOException {
        // stubbing.
    }

    @DataProvider
    public static Object[][] provideSchemaFieldAndExpectedRulesLength() throws ParseException {
        return new Object[][]{
            {getPrimitiveSchemaField(), 1},
            {getObjectSchemaField(), 16},
            {getArraySchemaField(), 1},
            {getStructArraySchemaField(), 12}
        };
    }

    @DataProvider
    public static Object[][] provideDataTypeRules() throws ParseException {
        return new Object[][]{
            {getObjectSchemaField(), 16},
            {getObjectWithRootSchemaField(), 16}
        };
    }

    private static SchemaField getPrimitiveSchemaField() throws ParseException {
        JSONParser parser = new JSONParser();
        JSONObject jsonObject = (JSONObject) parser.parse(primitiveFieldJson);
        return new Schema(jsonObject).getSchemaFields(false).get(0);
    }

    private static SchemaField getObjectSchemaField() throws ParseException {
        JSONParser parser = new JSONParser();
        JSONObject jsonObject = (JSONObject) parser.parse(primitiveObjectJson);
        return new Schema(jsonObject).getSchemaFields(false).get(0);
    }

    private static SchemaField getObjectWithRootSchemaField() throws ParseException {
        JSONParser parser = new JSONParser();
        JSONObject jsonObject = (JSONObject) parser.parse(primitiveObjectWithRootJson);
        return new Schema(jsonObject).getSchemaFields(false).get(0);
    }

    private static SchemaField getArraySchemaField() throws ParseException {
        JSONParser parser = new JSONParser();
        JSONObject jsonObject = (JSONObject) parser.parse(primitiveArrayJson);
        return new Schema(jsonObject).getSchemaFields(false).get(0);
    }

    private static Object getStructArraySchemaField() throws ParseException {
        JSONParser parser = new JSONParser();
        JSONObject jsonObject = (JSONObject) parser.parse(structArrayJson);
        return new Schema(jsonObject).getSchemaFields(false).get(0);
    }


    @Test
    @UseDataProvider("provideSchemaFieldAndExpectedRulesLength")
    public void testValidationRegistry(SchemaField schemaField, int size) {
        catalogValidationRegistry = (CatalogValidationRegistry) new CatalogValidationRegistry(schemaField).build();
        assertEquals(catalogValidationRegistry.getValidationRulesMap().size(), size);
    }

    @Test
    @UseDataProvider("provideDataTypeRules")
    public void testGetValidationRules(SchemaField schemaField, int size) {
        catalogValidationRegistry = (CatalogValidationRegistry) new CatalogValidationRegistry(schemaField).build();
        assertEquals(catalogValidationRegistry.getStringValidationRule(TraversablePath.path()).size(), 0);
        assertEquals(catalogValidationRegistry.getIntegerValidationRule(TraversablePath.path()).size(), 0);
        assertEquals(catalogValidationRegistry.getLongValidationRule(TraversablePath.path()).size(), 0);
        assertEquals(catalogValidationRegistry.getStringValidationRule(TraversablePath.path().withNode("person").withNode( "name").withNode("firstName")).size(), 1);
        assertEquals(catalogValidationRegistry.getStringValidationRule(TraversablePath.path().withNode("root").withNode("person").withNode( "name").withNode("firstName")).size(), 1);
        assertEquals(catalogValidationRegistry.getIntegerValidationRule(TraversablePath.path().withNode("person").withNode( "birthDay")).size(), 1);
        assertEquals(catalogValidationRegistry.getIntegerValidationRule(TraversablePath.path().withNode("root").withNode("person").withNode( "birthDay")).size(), 1);
        assertEquals(catalogValidationRegistry.getLongValidationRule(TraversablePath.path().withNode("person").withNode( "timestamp")).size(), 1);
        assertEquals(catalogValidationRegistry.getLongValidationRule(TraversablePath.path().withNode("root").withNode("person").withNode( "timestamp")).size(), 1);
    }
}
