
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
package com.adobe.platform.ecosystem.examples.data.write.field.converter.parquet;

import com.adobe.platform.ecosystem.examples.catalog.api.CatalogService;
import com.adobe.platform.ecosystem.examples.catalog.model.DataType;
import com.adobe.platform.ecosystem.examples.catalog.model.SchemaField;
import com.adobe.platform.ecosystem.examples.parquet.model.ParquetIOField;
import com.adobe.platform.ecosystem.examples.catalog.model.DataSet;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;
import com.adobe.platform.ecosystem.ut.BaseTest;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static com.adobe.platform.ecosystem.examples.constants.SDKConstants.CATALOG_MAP_KEY;
import static com.adobe.platform.ecosystem.examples.constants.SDKConstants.CATALOG_MAP_VALUE;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

/**
 * @author vedhera on 2/20/2018
 */
public class JSONParquetFieldConverterTest extends BaseTest {
    @Mock
    CatalogService catalogService;

    private ParquetFieldConverter fieldConverter;

    private final String dataSetString = "{\"id\":\"mockDataSetId\",\"version\":\"1.0.11\",\"imsOrg\":\"dummy@AdobeOrg\",\"name\":\"Test_DataSet_Parquet_6\",\"namespace\":\"ACP\",\"dule\":{},\"statsCache\":{},\"aspect\":\"production\",\"status\":\"enabled\",\"fields\":[{\"name\":\"Person\",\"type\":\"object\",\"subFields\":[{\"name\":\"firstName\",\"type\":\"string\"},{\"name\":\"lastName\",\"type\":\"string\"},{\"name\":\"middleName\",\"type\":\"string\"},{\"name\":\"gender\",\"type\":\"string\"},{\"name\":\"nicknames\",\"type\":\"array\",\"subType\":{\"type\":\"string\"}},{\"name\":\"birthArray\",\"type\":\"array\",\"subType\":{\"type\":\"object\",\"subFields\":[{\"name\":\"birthDay\",\"type\":\"byte\"},{\"name\":\"birthMonth\",\"type\":\"byte\"},{\"name\":\"birthYear\",\"type\":\"short\"}]}},{\"name\":\"id\",\"type\":\"integer\"},{\"name\":\"courtesyTitle\",\"type\":\"string\"}]},{\"name\":\"Address\",\"type\":\"object\",\"subFields\":[{\"name\":\"state\",\"type\":\"string\"},{\"name\":\"postalCode\",\"type\":\"long\"},{\"name\":\"addObject\",\"type\":\"object\",\"subFields\":[{\"name\":\"key1\",\"type\":\"string\"},{\"name\":\"key2\",\"type\":\"string\"}]}]},{\"name\":\"Audit\",\"type\":\"object\",\"subFields\":[{\"name\":\"lastModifiedDate\",\"type\":\"string\",\"format\":\"date\"},{\"name\":\"lastModifiedDateTime\",\"type\":\"string\",\"format\":\"date-time\"},{\"name\":\"creationDate\",\"type\":\"date\"},{\"name\":\"creationDateTime\",\"type\":\"date-time\"}]}],\"fileDescription\":{\"persisted\":false,\"format\":\"parquet\"},\"schema\":\"@/xdms/context/profile\"}";

    private final String testData = "{\"Person\":{\"id\":\"dummyId\",\"firstName\":\"dummyFirstName\",\"lastName\":\"dummyLastName\",\"middleName\":\"dummyMiddleName\",\"nicknames\":[\"dummyNickName1, dummyNickName2\"],\"gender\":\"male\",\"birthArray\":[{\"birthDay\":\"6\",\"birthMonth\":\"September\",\"birthYear\":\"1991\"}]}, \"mapData\" : { \"key1\" : \"value1\", \"key2\" : \"value2\"}}";

    @Before
    public void setup() throws Exception {
        initMocks(this);

        // Setting up JWT mocks.
        setUp();
        setUpHttpForJwtResponse();

        when(catalogService.getSchemaFields(
            anyString(),
            any(),
            anyString(),
            eq("/xdms/context/profile"),
            anyBoolean()
        )).thenReturn(getMockSchemaFields());

        JSONParser parser = new JSONParser();
        JSONObject dataSetObj = (JSONObject) parser.parse(dataSetString);
        TestDataSet dataSet = new TestDataSet(dataSetObj);

        fieldConverter = new JSONParquetFieldConverter(dataSet.getFields(false, DataSet.FieldsFrom.SCHEMA));
    }

    private List<SchemaField> getMockSchemaFields() {
        SchemaField idField = new SchemaField("id", DataType.StringType, null);
        SchemaField firstNameField = new SchemaField("firstName", DataType.StringType, null);
        SchemaField lastNameField = new SchemaField("lastName", DataType.StringType, null);
        SchemaField middleNameField = new SchemaField("middleName", DataType.StringType, null);
        SchemaField nicknameArrayField = new SchemaField("nicknames", DataType.Field_ArrayType, null, DataType.StringType);
        SchemaField genderField = new SchemaField("gender", DataType.StringType, null);

        SchemaField birthDay = new SchemaField("birthDay", DataType.StringType, null);
        SchemaField birthMonth = new SchemaField("birthMonth", DataType.StringType, null);
        SchemaField birthYear = new SchemaField("birthYear", DataType.StringType, null);
        SchemaField birthArray = new SchemaField(
            "birthArray",
            DataType.Field_ArrayType,
            Arrays.asList(birthDay, birthMonth, birthYear),
            DataType.Field_ObjectType
        );

        SchemaField personField = new SchemaField(
            "Person",
            DataType.Field_ObjectType,
            Arrays.asList(idField, firstNameField, lastNameField, middleNameField, nicknameArrayField, genderField, birthArray)
        );

        SchemaField mapKey = new SchemaField(CATALOG_MAP_KEY, DataType.StringType, null);
        SchemaField mapValue = new SchemaField(CATALOG_MAP_VALUE, DataType.StringType, null);
        SchemaField mapField = new SchemaField("mapData", DataType.Field_MapType, Arrays.asList(mapKey, mapValue));

        return Arrays.asList(personField, mapField);
    }

    @Test
    public void TestJsonDataToParquetIOFieldConversion() throws ParseException, ConnectorSDKException {
        JSONParser parser = new JSONParser();
        JSONObject data = (JSONObject) parser.parse(testData);
        List<ParquetIOField> parquetFields = fieldConverter.convert(data);
        assert (parquetFields.size() == 2);
    }

    @Test(expected = ConnectorSDKException.class)
    public void TestNonExistingCatalogField() throws ParseException, ConnectorSDKException {
        JSONParser parser = new JSONParser();
        JSONObject data = (JSONObject) parser.parse("{\"badField\":{}}");
        fieldConverter.convert(data);
    }

    private class TestDataSet extends DataSet {
        public TestDataSet(JSONObject dataSetObj) {
            super(dataSetObj);
        }

        @Override
        protected CatalogService getCatalogService() throws ConnectorSDKException {
            return catalogService;
        }
    }
}