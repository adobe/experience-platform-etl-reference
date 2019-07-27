
/*
 *  Copyright 2019-2020 Adobe.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.adobe.platform.ecosystem.examples.catalog.model;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class SchemaTest {

    JSONParser parser;

    @Before
    public void setup() {
        parser = new JSONParser();
    }

    @Test
    public void testGetSchemaFields() throws ParseException {
        String properties = "{\n" +
                "  \"meta:extends\": [\n" +
                "        \"https://ns.adobe.com/xdm/context/profile\",\n" +
                "        \"https://ns.adobe.com/xdm/data/record\",\n" +
                "        \"https://ns.adobe.com/xdm/context/identitymap\",\n" +
                "        \"https://ns.adobe.com/xdm/common/extensible\",\n" +
                "        \"https://ns.adobe.com/xdm/common/auditable\",\n" +
                "        \"https://ns.adobe.com/workshop/mixins/b3b69de9bc499ecd2b808b7a63edd5ca\"\n" +
                "    ],\n" +
                "  \"properties\": {\n" +
                "    \"_namespace\": {\n" +
                "      \"type\": \"object\",\n" +
                "      \"meta:xdmType\": \"object\",\n" +
                "      \"properties\": {\n" +
                "        \"field1\": {\n" +
                "          \"title\": \"field1\",\n" +
                "          \"type\": \"string\",\n" +
                "          \"meta:xdmType\": \"string\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";

        JSONObject propertiesObj = (JSONObject) parser.parse(properties);
        Schema schema = new Schema(propertiesObj);
        List<SchemaField> schemaFields = schema.getSchemaFields(false);

        Assert.assertNotNull(schemaFields);
        Assert.assertEquals(schemaFields.size(), 1);

        Assert.assertEquals(schemaFields.get(0).getName(), "_namespace");
        Assert.assertEquals(schemaFields.get(0).getType(), DataType.Field_ObjectType);
        Assert.assertEquals(schemaFields.get(0).getSubFields().size(), 1);
        Assert.assertEquals(schemaFields.get(0).getSubFields().get(0).getName(), "field1");
        Assert.assertEquals(schemaFields.get(0).getSubFields().get(0).getType(), DataType.StringType);
    }

    @Test
    public void testGetSchemaFieldsForAdhocSchema() throws ParseException {
        String adhocProperties = "{\n" +
                "  \"meta:extends\": [\n" +
                "    \"https://ns.adobe.com/workshop/classes/b2aa1f08111638d1b5899842c9c4cbd9\",\n" +
                "    \"https://ns.adobe.com/xdm/data/adhoc\"\n" +
                "  ],\n" +
                "  \"meta:datasetNamespace\": \"_b2aa1f08111638d1b5899842c9c4cbd9\",\n" +
                "  \"properties\": {\n" +
                "    \"_b2aa1f08111638d1b5899842c9c4cbd9\": {\n" +
                "      \"title\": \"testAdhocSchema\",\n" +
                "      \"type\": \"object\",\n" +
                "      \"meta:xdmType\": \"object\",\n" +
                "      \"properties\": {\n" +
                "        \"MapField1\": {\n" +
                "          \"type\": \"object\",\n" +
                "          \"meta:xdmType\": \"map\",\n" +
                "          \"additionalProperties\": {\n" +
                "            \"type\": \"array\",\n" +
                "            \"meta:xdmType\": \"array\",\n" +
                "            \"items\": {\n" +
                "              \"$id\": \"context/identityitem\",\n" +
                "              \"type\": \"object\",\n" +
                "              \"meta:xdmType\": \"object\",\n" +
                "              \"properties\": {\n" +
                "                \"id\": {\n" +
                "                  \"title\": \"Identifier\",\n" +
                "                  \"type\": \"string\",\n" +
                "                  \"meta:xdmType\": \"string\"\n" +
                "                },\n" +
                "                \"primary\": {\n" +
                "                  \"title\": \"Primary\",\n" +
                "                  \"type\": \"boolean\",\n" +
                "                  \"default\": false,\n" +
                "                  \"meta:xdmType\": \"boolean\"\n" +
                "                }\n" +
                "              }\n" +
                "            }\n" +
                "          }\n" +
                "        },\n" +
                "        \"productListItems\": {\n" +
                "          \"title\": \"Product List Items\",\n" +
                "          \"type\": \"array\",\n" +
                "          \"meta:xdmType\": \"array\",\n" +
                "          \"items\": {\n" +
                "            \"type\": \"object\",\n" +
                "            \"required\": [\n" +
                "              \"SKU\"\n" +
                "            ],\n" +
                "            \"meta:xdmType\": \"object\",\n" +
                "            \"properties\": {\n" +
                "              \"SKU\": {\n" +
                "                \"title\": \"SKU\",\n" +
                "                \"type\": \"string\",\n" +
                "                \"meta:xdmType\": \"string\"\n" +
                "              },\n" +
                "              \"_id\": {\n" +
                "                \"title\": \"Line Item ID.\",\n" +
                "                \"type\": \"string\",\n" +
                "                \"format\": \"uri-reference\",\n" +
                "                \"meta:xdmType\": \"string\"\n" +
                "              }\n" +
                "            }\n" +
                "          }\n" +
                "        },\n" +
                "        \"_repo\": {\n" +
                "          \"type\": \"object\",\n" +
                "          \"meta:xdmType\": \"object\",\n" +
                "          \"properties\": {\n" +
                "            \"createDate\": {\n" +
                "              \"type\": \"string\",\n" +
                "              \"format\": \"date-time\",\n" +
                "              \"meta:immutable\": true,\n" +
                "              \"meta:usereditable\": false,\n" +
                "              \"examples\": [\n" +
                "                \"2004-10-23T12:00:00-06:00\"\n" +
                "              ],\n" +
                "              \"meta:xdmType\": \"date-time\"\n" +
                "            },\n" +
                "            \"lastModifiedDate\": {\n" +
                "              \"type\": \"string\",\n" +
                "              \"format\": \"date-time\",\n" +
                "              \"meta:usereditable\": false,\n" +
                "              \"examples\": [\n" +
                "                \"2004-10-23T12:00:00-06:00\"\n" +
                "              ],\n" +
                "              \"meta:xdmType\": \"date-time\"\n" +
                "            }\n" +
                "          }\n" +
                "        },\n" +
                "        \"_id\": {\n" +
                "          \"title\": \"Identifier\",\n" +
                "          \"type\": \"string\",\n" +
                "          \"format\": \"uri-reference\",\n" +
                "          \"meta:xdmType\": \"string\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";

        JSONObject adhocPropertiesObj = (JSONObject) parser.parse(adhocProperties);
        Schema schema = new Schema(adhocPropertiesObj);
        List<SchemaField> schemaFields = schema.getSchemaFields(false);

        Assert.assertNotNull(schemaFields);
        Assert.assertEquals(schemaFields.size(), 4);

        Assert.assertEquals(schemaFields.get(0).getName(), "_repo");
        Assert.assertEquals(schemaFields.get(0).getType(), DataType.Field_ObjectType);
        Assert.assertEquals(schemaFields.get(0).getSubFields().size(), 2);
        Assert.assertEquals(schemaFields.get(0).getSubFields().get(0).getName(), "lastModifiedDate");
        Assert.assertEquals(schemaFields.get(0).getSubFields().get(0).getType(), DataType.DateTimeType);
        Assert.assertEquals(schemaFields.get(0).getSubFields().get(1).getName(), "createDate");
        Assert.assertEquals(schemaFields.get(0).getSubFields().get(1).getType(), DataType.DateTimeType);

        Assert.assertEquals(schemaFields.get(1).getName(), "MapField1");
        Assert.assertEquals(schemaFields.get(1).getType(), DataType.Field_MapType);
        Assert.assertEquals(schemaFields.get(1).getSubFields().size(), 2);
        Assert.assertEquals(schemaFields.get(1).getSubFields().get(0).getName(), "key");
        Assert.assertEquals(schemaFields.get(1).getSubFields().get(0).getType(), DataType.StringType);
        Assert.assertEquals(schemaFields.get(1).getSubFields().get(1).getName(), "value");
        Assert.assertEquals(schemaFields.get(1).getSubFields().get(1).getType(), DataType.Field_ArrayType);
        Assert.assertEquals(schemaFields.get(1).getSubFields().get(1).getArraySubType(), DataType.Field_ObjectType);
        Assert.assertEquals(schemaFields.get(1).getSubFields().get(1).getSubFields().size(), 2);
        Assert.assertEquals(schemaFields.get(1).getSubFields().get(1).getSubFields().get(0).getName(), "id");
        Assert.assertEquals(schemaFields.get(1).getSubFields().get(1).getSubFields().get(0).getType(), DataType.StringType);
        Assert.assertEquals(schemaFields.get(1).getSubFields().get(1).getSubFields().get(1).getName(), "primary");
        Assert.assertEquals(schemaFields.get(1).getSubFields().get(1).getSubFields().get(1).getType(), DataType.BooleanType);

        Assert.assertEquals(schemaFields.get(2).getName(), "_id");
        Assert.assertEquals(schemaFields.get(2).getType(), DataType.StringType);

        Assert.assertEquals(schemaFields.get(3).getName(), "productListItems");
        Assert.assertEquals(schemaFields.get(3).getType(), DataType.Field_ArrayType);
        Assert.assertEquals(schemaFields.get(3).getArraySubType(), DataType.Field_ObjectType);
        Assert.assertEquals(schemaFields.get(3).getSubFields().size(), 2);
        Assert.assertEquals(schemaFields.get(3).getSubFields().get(0).getName(), "_id");
        Assert.assertEquals(schemaFields.get(3).getSubFields().get(0).getType(), DataType.StringType);
        Assert.assertEquals(schemaFields.get(3).getSubFields().get(1).getName(), "SKU");
        Assert.assertEquals(schemaFields.get(3).getSubFields().get(0).getType(), DataType.StringType);
    }


}
