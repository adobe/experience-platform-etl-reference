
/*
 *  Copyright 2017-2018 Adobe.
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

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.adobe.platform.ecosystem.examples.catalog.api.CatalogService;
import com.adobe.platform.ecosystem.examples.catalog.impl.CatalogFactory;
import com.adobe.platform.ecosystem.examples.schemaregistry.api.SchemaRegistryService;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.Before;
import org.junit.Test;

import com.adobe.platform.ecosystem.ut.BaseTest;
import com.adobe.platform.ecosystem.examples.catalog.model.DataSet.FieldsFrom;

public class DataSetTest extends BaseTest{

    DataSet ds = null;
    DataSet ds_Sample1 = null;
    DataSet ds_Sample2 = null;
    DataSet ds_Sample3 = null;

    String datasetInnerSample1 = "{\"name\":\"dsvName\","
            + "\"viewId\":\"dsvId\",\"connectionId\":\"conId\",\"dule\":"
            + "{\"loginState\":[\"Identified\"],\"specialTypes\":[\"S1\",\"S2\"]},"
            + "\"fields\":[{\"name\":\"id1\",\"type\":\"string\"},"
            + "{\"name\":\"createtime\",\"type\":\"long\"},"
            + "{\"name\":\"_date\",\"type\":\"date\"},"
            + "{\"name\":\"abc_xyz\",\"type\":\"string\"},"
            + "{\"name\":\"Name\",\"type\":\"object\","
            + "\"subFields\":["
            + "{\"name\":\"firstName\",\"type\":\"string\"},"
            + "{\"name\":\"_yahoo\",\"type\":\"string\"},"
            + "{\"name\":\"abc_\",\"type\":\"object\","
            + "\"subFields\":["
            + "{\"name\":\"superb\",\"type\":\"string\"}"
            + "]}"
            + "]}],"
            + "\"basePath\":\"adl://datalake/datalake\","
            + "\"fileDescription\":{\"persisted\":false,\"format\":\"csv\",\"delimiters\":[\",\"]}}";

    String datasetInnerSample2 ="{"
            + " \"version\": \"1.0.1\","
            + " \"imsOrg\": \"EDCE5A655A5E73FF0A494113@AdobeOrg\","
            + " \"name\": \"Data set for Testing\","
            + " \"created\": 1527085987308,"
            + " \"updated\": 1527085992694,"
            + " \"createdClient\": \"acp_core_identity_data\","
            + " \"createdUser\": \"acp_core_identity_data@AdobeID\","
            + " \"updatedUser\": \"acp_foundation_dataTracker@AdobeID\","
            + " \"namespace\": \"ACP\","
            + " \"tags\": {"
            + "  \"unifiedprofile\": ["
            + "   \"enabled:true\","
            + "   \"identityField:identities.id\","
            + "   \"relatedModels:aaaprofile\""
            + "  ]"
            + " },"
            + " \"dule\": {},"
            + " \"statsCache\": {},"
            + " \"lastBatchId\": \"ee702a5a792143b9b1437d0be598265b\","
            + " \"lastBatchStatus\": \"success\","
            + " \"lastSuccessfulBatch\": \"ee702a5a792143b9b1437d0be598265b\","
            + " \"viewId\": \"5b057ba38fd95a01decdb923\","
            + " \"aspect\": \"production\","
            + " \"status\": \"enabled\","
            + " \"fileDescription\": {"
            + "  \"persisted\": true,"
            + "  \"containerFormat\": \"parquet\","
            + "  \"format\": \"parquet\""
            + " },"
            + " \"transforms\": \"@/dataSets/5b057ba38fd95a01decdb922/views/5b057ba38fd95a01decdb923/transforms\","
            + " \"files\": \"@/dataSets/5b057ba38fd95a01decdb922/views/5b057ba38fd95a01decdb923/files\","
            + " \"schema\": \"@/xdms/model/Profile\","
            + " \"observableSchema\": {"
            + "  \"type\": \"object\","
            + "  \"meta:xdmType\": \"object\","
            + "  \"properties\": {"
            + "   \"homeAddress\": {"
            + "    \"type\": \"object\","
            + "    \"meta:xdmType\": \"object\","
            + "    \"properties\": {"
            + "     \"city\": {"
            + "      \"type\": \"string\","
            + "      \"meta:xdmType\": \"string\","
            + "      \"title\": \"City\","
            + "      \"description\": \"The town, city, village or other metropolitan identity of the address.\""
            + "     },"
            + "     \"postalCode\": {"
            + "      \"type\": \"string\","
            + "      \"meta:xdmType\": \"string\","
            + "      \"title\": \"Postal Code\","
            + "      \"description\": \"The postal code, zip code of other postal ordering for the address. Note, if zip codes are used either the base zip or zip 4 format can be used.\""
            + "     },"
            + "     \"stateProvince\": {"
            + "      \"type\": \"string\","
            + "      \"meta:xdmType\": \"string\","
            + "      \"title\": \"State or Province\","
            + "      \"description\": \"The state, province, region, territory portion of the address.\""
            + "     },"
            + "     \"street1\": {"
            + "      \"type\": \"string\","
            + "      \"meta:xdmType\": \"string\","
            + "      \"title\": \"Street 1\","
            + "      \"description\": \"Primary Street level information, apartment number, street number and street name.\""
            + "     }"
            + "    }"
            + "   },"
            + "   \"homePhone\": {"
            + "    \"type\": \"object\","
            + "    \"meta:xdmType\": \"object\","
            + "    \"properties\": {"
            + "     \"number\": {"
            + "      \"type\": \"string\","
            + "      \"meta:xdmType\": \"string\","
            + "      \"title\": \"Number\","
            + "      \"description\": \"The phone number. Note the phone number is a string and may include meaningful characters such as brackets (), hyphens - or characters to indicate sub dialing identifiers like extensions x. E.g 1-353(0)18391111 or 613 9403600x1234.\""
            + "     }"
            + "    }"
            + "   },"
            + "   \"person\": {"
            + "    \"type\": \"object\","
            + "    \"meta:xdmType\": \"object\","
            + "    \"properties\": {"
            + "     \"firstName\": {"
            + "      \"type\": \"string\","
            + "      \"meta:xdmType\": \"string\","
            + "      \"title\": \"First Name\","
            + "      \"description\": \"The personal, or given name.\""
            + "     },"
            + "     \"gender\": {"
            + "      \"type\": \"string\","
            + "      \"meta:xdmType\": \"string\","
            + "      \"title\": \"Gender\","
            + "      \"description\": \"Gender identity of the person.\n\""
            + "     },"
            + "     \"lastName\": {"
            + "      \"type\": \"string\","
            + "      \"meta:xdmType\": \"string\","
            + "      \"title\": \"Last Name\","
            + "      \"description\": \"The inherited family name, surname, patronymic, or matronymic name.\""
            + "     }"
            + "    }"
            + "   },"
            + "   \"personalEmail\": {"
            + "    \"type\": \"object\","
            + "    \"meta:xdmType\": \"object\","
            + "    \"properties\": {"
            + "     \"address\": {"
            + "      \"type\": \"string\","
            + "      \"meta:xdmType\": \"string\","
            + "      \"title\": \"Address\","
            + "      \"description\": \"The technical address, e.g 'name@domain.com' as commonly defined in RFC2822 and subsequent standards.\""
            + "     }"
            + "    }"
            + "   },"
            + "   \"numbers\": {"
            + "    \"type\": \"array\","
            + "    \"items\": {"
            + "     \"type\": \"string\""
            + "     }"
            + "   },"
            + "   \"identities\": {"
            + "    \"type\": \"array\","
            + "    \"meta:xdmType\": \"array\","
            + "    \"items\": {"
            + "     \"type\": \"object\","
            + "     \"meta:xdmType\": \"object\","
            + "     \"properties\": {"
            + "      \"id\": {"
            + "       \"type\": \"string\","
            + "       \"meta:xdmType\": \"string\","
            + "       \"title\": \"Identifier\","
            + "       \"description\": \"Identity of the consumer in the related namespace.\""
            + "      },"
            + "      \"namespace\": {"
            + "       \"type\": \"object\","
            + "       \"meta:xdmType\": \"object\","
            + "       \"properties\": {"
            + "        \"code\": {"
            + "         \"type\": \"string\","
            + "         \"meta:xdmType\": \"string\","
            + "         \"title\": \"Code\","
            + "         \"description\": \"The string based name associated associated with the id attribute. Sometimes refered to as the data source integration code.\""
            + "        },"
            + "        \"id\": {"
            + "         \"type\": \"integer\","
            + "         \"meta:xdmType\": \"integer\","
            + "         \"title\": \"Identifier\","
            + "         \"description\": \"The numeric ID associated with this Datasource. This would be provided by the individual or system that created the Datasource.\n\""
            + "        }"
            + "       }"
            + "      },"
            + "      \"xid\": {"
            + "       \"type\": \"string\","
            + "       \"meta:xdmType\": \"string\","
            + "       \"title\": \"Adobe Experience Identifier\","
            + "       \"description\": \"The optional, universally unique Adobe identity of the consumer. Generated using the Identity and the related namespace id.\""
            + "      }"
            + "     }"
            + "    }"
            + "   }"
            + "  }"
            + " }"
            + "}";

    String datasetInnerSample3 ="{"
            + " \"version\": \"1.0.1\","
            + " \"imsOrg\": \"EDCE5A655A5E73FF0A494113@AdobeOrg\","
            + " \"name\": \"Data set for Testing\","
            + " \"created\": 1527085987308,"
            + " \"updated\": 1527085992694,"
            + " \"createdClient\": \"acp_core_identity_data\","
            + " \"createdUser\": \"acp_core_identity_data@AdobeID\","
            + " \"updatedUser\": \"acp_foundation_dataTracker@AdobeID\","
            + " \"namespace\": \"ACP\","
            + " \"tags\": {"
            + "  \"unifiedprofile\": ["
            + "   \"enabled:true\","
            + "   \"identityField:identities.id\","
            + "   \"relatedModels:aaaprofile\""
            + "  ]"
            + " },"
            + " \"dule\": {},"
            + " \"statsCache\": {},"
            + " \"lastBatchId\": \"ee702a5a792143b9b1437d0be598265b\","
            + " \"lastBatchStatus\": \"success\","
            + " \"lastSuccessfulBatch\": \"ee702a5a792143b9b1437d0be598265b\","
            + " \"viewId\": \"5b057ba38fd95a01decdb923\","
            + " \"aspect\": \"production\","
            + " \"status\": \"enabled\","
            + " \"fileDescription\": {"
            + "  \"persisted\": true,"
            + "  \"containerFormat\": \"parquet\","
            + "  \"format\": \"parquet\""
            + " },"
            + " \"transforms\": \"@/dataSets/5b057ba38fd95a01decdb922/views/5b057ba38fd95a01decdb923/transforms\","
            + " \"files\": \"@/dataSets/5b057ba38fd95a01decdb922/views/5b057ba38fd95a01decdb923/files\","
            + " \"schemaRef\": {"
            +       "\"id\": \"https://ns.adobe.com/model/Profile\","
            +       "\"contentType\": \"application/vnd.adobe.xed+json\""
            + " }"
            + "}";

    @Before
    public void setupDataSet() throws Exception {
        setUp();
        setUpHttpForJwtResponse();
        ds = getDataSetFromString(datasetInnerSample);
        ds_Sample1 = getDataSetFromString(datasetInnerSample1);
        ds_Sample2 = getDataSetFromString(datasetInnerSample2);
        ds_Sample3 = getDataSetFromString(datasetInnerSample3);
    }

    @Test
    public void testGetConnectionId() {
        assertTrue(ds.getConnectionId().equalsIgnoreCase("conId"));
    }

    @Test
    public void testGetFields() {
        assertTrue(ds.getFields(false).size() == 4);
    }

    @Test
    public void testGetFieldsFromObservableSchema() {
        assertTrue(ds_Sample2.getFields(false, FieldsFrom.OBSERVABLE_SCHEMA).size() == 6);
    }

    @Test
    public void testGetFlattenedFieldsFromObservableSchema() {
        assertTrue(ds_Sample2.getFlattenedSchemaFields(FieldsFrom.OBSERVABLE_SCHEMA).size() == 14);
    }

    @Test
    public void testGetFlattenedSchemaFields() {
        assertTrue(ds.getFlattenedSchemaFields().size() == 4);
    }

    @Test
    public void testMatchFlattenedSchemaFields() {
        Map<String,String> sdkFields = new HashMap<>();
        sdkFields.put("Name_firstName", "");
        sdkFields.put("_date", "");
        sdkFields.put("Name_abc__superb", "");
        sdkFields.put("abc_xyz", "");
        ds_Sample1.matchFlattenedSchemaFields(sdkFields);
        assertTrue(sdkFields.get("_date").equals("_date"));
        assertTrue(sdkFields.get("Name_firstName").equals("Name.firstName"));
        assertTrue(sdkFields.get("Name_abc__superb").equals("Name.abc_.superb"));
        assertTrue(sdkFields.get("abc_xyz").equals("abc_xyz"));
    }

    @Test
    public void testGetDataSetViewId() {
        assertTrue("dsvId".equals(ds.getViewId()));
    }

    @Test
    public void testGetDataSetViewFieldName() {
        assertTrue("id1".equals(ds.getFlattenedSchemaFields().get(0).getName()));
    }

    @Test
    public void testGetDataSetViewFieldType() {
        assertTrue(ds.getFlattenedSchemaFields().get(0).getType() == DataType.StringType);
    }

    @Test
    public void testGetIsDuleEnabled() {
        assertTrue(ds.getIsDuleEnabled() == true);
    }

    @Test
    public void testGetFieldDescription() {
        assertNotNull(ds.getFileDescription());
        assertNotNull(ds.getFileDescription().getDelimiter());
        assertNotNull (ds.getFileDescription().getFormat());
        assertNotNull (ds.getFileDescription().isPersisted());
        assert (ds.getFileDescription().getDelimiter() == ',');
    }

    @Test
    public void testGetSchemaRef(){
        assertNotNull(ds.getSchemaRef());
        assertEquals(ds.getSchemaRef().getId(), "testId");
        assertEquals(ds.getSchemaRef().getContentType(), "testContentType");
    }

    @Test
    public void testGetSchemaFieldsFromCatalog() throws ParseException, ConnectorSDKException {
        System.setProperty("com.adobe.platform.xdm.blacklist.filter", "model/Profile.person");

        JSONParser jsonParser = new JSONParser();
        JSONObject jsonObject = (JSONObject)(jsonParser.parse(datasetInnerSample2));

        when(catService.getSchemaFields(
                anyString(),
                any(),
                anyString(),
                eq("/xdms/model/Profile"),
                eq(false)
        )).thenReturn(ds_Sample2.getFields(DataSet.FieldsFrom.OBSERVABLE_SCHEMA));

        DataSet dataSet = new DataSetExtension(jsonObject);

        final List<SchemaField> fieldList = dataSet.getFields(false, DataSet.FieldsFrom.SCHEMA);
        assertEquals(fieldList.size(), 5);

    }

    @Test
    public void testGetSchemaFieldsFromSchemaRegistry() throws ParseException, ConnectorSDKException {
        JSONParser jsonParser = new JSONParser();
        JSONObject jsonObject = (JSONObject)(jsonParser.parse(datasetInnerSample3));

        when(schemaRegistryService.getSchemaFields(
                anyString(),
                any(),
                anyString(),
                anyObject(),
                eq(false)
        )).thenReturn(ds_Sample2.getFields(DataSet.FieldsFrom.OBSERVABLE_SCHEMA));

        DataSet dataSet = new DataSetExtension(jsonObject);

        final List<SchemaField> fieldList = dataSet.getFields(false, DataSet.FieldsFrom.SCHEMA);
        //No blacklisting currently
        assertEquals(fieldList.size(), 6);
    }

    class DataSetExtension extends DataSet {
        public DataSetExtension(JSONObject jsonObject) {
            super(jsonObject);
        }

        @Override
        protected CatalogService getCatalogService() throws ConnectorSDKException {
            return catService;
        }

        @Override
        protected SchemaRegistryService getSchemaRegistryService() throws ConnectorSDKException {
            return schemaRegistryService;
        }
    }
}