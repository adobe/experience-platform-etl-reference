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

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.http.client.HttpClient;
import org.apache.parquet.schema.MessageType;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import com.adobe.platform.ecosystem.examples.authentication.AccessTokenProcessor;
import com.adobe.platform.ecosystem.examples.catalog.api.CatalogService;
import com.adobe.platform.ecosystem.examples.catalog.model.DataSet;
import com.adobe.platform.ecosystem.examples.catalog.model.DataSet.FieldsFrom;
import com.adobe.platform.ecosystem.examples.catalog.model.SchemaField;
import com.adobe.platform.ecosystem.examples.data.validation.api.ValidationRegistry;
import com.adobe.platform.ecosystem.examples.data.write.writer.extractor.JsonObjectsExtractor;
import com.adobe.platform.ecosystem.examples.data.write.writer.formatter.ParquetDataFormatter;
import com.adobe.platform.ecosystem.examples.parquet.model.ParquetIOField;
import com.adobe.platform.ecosystem.examples.parquet.write.ParquetIOWriter;
import com.adobe.platform.ecosystem.examples.parquet.write.ParquetIOWriterImpl;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKUtil;

/**
 * @author vedhera on 2/20/2018
 */
public class JSONParquetFieldConverterTest extends com.adobe.platform.ecosystem.ut.BaseTest {
    private ParquetFieldConverter fieldConverter;

    private final String dataSetString = "{\"id\":\"5a7d70983b8ec401d8ceab9b\",\"version\":\"1.0.11\",\"imsOrg\":\"dummy@AdobeOrg\",\"name\":\"Test_DataSet_Parquet_6\",\"namespace\":\"ACP\",\"dule\":{},\"statsCache\":{},\"aspect\":\"production\",\"status\":\"enabled\",\"fields\":[{\"name\":\"Person\",\"type\":\"object\",\"subFields\":[{\"name\":\"firstName\",\"type\":\"string\"},{\"name\":\"lastName\",\"type\":\"string\"},{\"name\":\"middleName\",\"type\":\"string\"},{\"name\":\"gender\",\"type\":\"string\"},{\"name\":\"nicknames\",\"type\":\"array\",\"subType\":{\"type\":\"string\"}},{\"name\":\"birthArray\",\"type\":\"array\",\"subType\":{\"type\":\"object\",\"subFields\":[{\"name\":\"birthDay\",\"type\":\"byte\"},{\"name\":\"birthMonth\",\"type\":\"byte\"},{\"name\":\"birthYear\",\"type\":\"short\"}]}},{\"name\":\"id\",\"type\":\"integer\"},{\"name\":\"courtesyTitle\",\"type\":\"string\"}]},{\"name\":\"Address\",\"type\":\"object\",\"subFields\":[{\"name\":\"state\",\"type\":\"string\"},{\"name\":\"postalCode\",\"type\":\"long\"} ,{\"name\":\"addObject\",\"type\":\"object\",\"subFields\":[{\"name\":\"key1\",\"type\":\"string\"},{\"name\":\"key2\",\"type\":\"string\"}]}]}, {\"name\":\"Audit\",\"type\":\"object\",\"subFields\":[ {\"name\":\"lastModifiedDate\",\"type\":\"string\",\"format\":\"date\"},{\"name\":\"lastModifiedDateTime\",\"type\":\"string\",\"format\":\"date-time\"},{\"name\":\"creationDate\",\"type\":\"date\"},{\"name\":\"creationDateTime\",\"type\":\"date-time\"}]}],\"fileDescription\":{\"persisted\":false,\"format\":\"parquet\"}}";
    private final String testData = "{\"Person\":{\"id\":\"dummyId\",\"firstName\":\"dummyFirstName\",\"lastName\":\"dummyLastName\",\"middleName\":\"dummyMiddleName\",\"nicknames\":[\"dummyNickName1, dummyNickName2\"],\"gender\":\"male\",\"birthArray\":[{\"birthDay\":\"6\",\"birthMonth\":\"September\",\"birthYear\":\"1991\"}]},\"Address\":{\"state\":\"Delhi\",\"postalCode\":1234}, \"Audit\": {\"creationDate\":\"10000\", \"creationDateTime\":\"1527848884000\",\"lastModifiedDateTime\":\"1527848884000\",\"lastModifiedDate\":\"10000\" }}";
    private static String mapFieldDataset;
    private static String mapTestData;

    @Mock
    ValidationRegistry validationRegistry;

    static {
        try {
            mapFieldDataset = IOUtils.toString(
                ClassLoader.getSystemResource("mapFieldDataset.json"),
                "UTF-8"
            );
            mapTestData = IOUtils.toString(
                    ClassLoader.getSystemResource("mapTestData.json"),
                    "UTF-8"
                );

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Before
    public void setup() throws Exception {
        JSONParser parser = new JSONParser();
        JSONObject dataSetObj = (JSONObject) parser.parse(mapFieldDataset);
        DataSet dataSet = new DataSetExtension(dataSetObj);
        setUp();
        //setUpHttpForJwtResponse();
        //setupTestForHttpOutput("{\"signals\": {\"auxiliaryProperties\": [{\"key\":\"Key1\",\"value\":\"Value1\"},{\"key\":\"Key2\",\"value\":\"Value2\"},{\"key\":\"Key3\",\"value\":\"Value3\"}]}}");
        List<SchemaField> schemaFields = dataSet.getFields(DataSet.FieldsFrom.SCHEMA);
        when(catService.getSchemaFields(
                anyString(),
                eq(dummyAccessToken),
                eq("/xdms/_customer/default/eemap3"),
                eq(false)
        )).thenReturn(schemaFields);
        ConnectorSDKUtil sdkUtil = ConnectorSDKUtil.getInstance();
        //when(AccessTokenProcessor.generateAccessToken(anyString(), anyString(), anyString(),httpClient)).thenReturn(dummyAccessToken);
        //when(sdkUtil.getAccessToken()).thenReturn(dummyAccessToken);
        fieldConverter = new JSONParquetFieldConverter(schemaFields);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testJsonDataToParquetIOFieldConversion() throws ParseException, ConnectorSDKException, IOException {
        JSONParser parser = new JSONParser();
        JSONObject data = (JSONObject) parser.parse(mapTestData);
        List<ParquetIOField> parquetFields = fieldConverter.convert(data);
        ParquetIOWriter writer = new ParquetIOWriterImpl();
        MessageType mt = writer.getSchema(parquetFields);
        System.out.println(mt.toString());
        ParquetDataFormatter parquetDataFormatter = new ParquetDataFormatter(writer,param,fieldConverter, new JsonObjectsExtractor(), validationRegistry);
        List<JSONObject> dataTable = new ArrayList<>();
        dataTable.add(data);
        byte[] parquetBuffer = parquetDataFormatter.getBuffer(dataTable);
        System.out.println(parquetBuffer.length);
    }

    @Test(expected = NullPointerException.class)
    public void testNonExistingCatalogField() throws ParseException {
        JSONParser parser = new JSONParser();
        JSONObject data = (JSONObject) parser.parse("{\"badField\":{}}");
        fieldConverter.convert(data);
    }

    class DataSetExtension extends DataSet {
        public DataSetExtension(JSONObject jsonObject) {
            super(jsonObject);
        }

        @Override
        protected CatalogService getCatalogService() throws ConnectorSDKException {
            return catService;
        }
    }
}