
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

import com.adobe.platform.ecosystem.examples.parquet.model.ParquetIOField;
import com.adobe.platform.ecosystem.examples.catalog.model.DataSet;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * @author vedhera on 2/20/2018
 */
public class JSONParquetFieldConverterTest {
    private ParquetFieldConverter fieldConverter;

    private final String dataSetString = "{\"id\":\"5a7d70983b8ec401d8ceab9b\",\"version\":\"1.0.11\",\"imsOrg\":\"dummy@AdobeOrg\",\"name\":\"Test_DataSet_Parquet_6\",\"namespace\":\"ACP\",\"dule\":{},\"statsCache\":{},\"aspect\":\"production\",\"status\":\"enabled\",\"fields\":[{\"name\":\"Person\",\"type\":\"object\",\"subFields\":[{\"name\":\"firstName\",\"type\":\"string\"},{\"name\":\"lastName\",\"type\":\"string\"},{\"name\":\"middleName\",\"type\":\"string\"},{\"name\":\"gender\",\"type\":\"string\"},{\"name\":\"nicknames\",\"type\":\"array\",\"subType\":{\"type\":\"string\"}},{\"name\":\"birthArray\",\"type\":\"array\",\"subType\":{\"type\":\"object\",\"subFields\":[{\"name\":\"birthDay\",\"type\":\"byte\"},{\"name\":\"birthMonth\",\"type\":\"byte\"},{\"name\":\"birthYear\",\"type\":\"short\"}]}},{\"name\":\"id\",\"type\":\"integer\"},{\"name\":\"courtesyTitle\",\"type\":\"string\"}]},{\"name\":\"Address\",\"type\":\"object\",\"subFields\":[{\"name\":\"state\",\"type\":\"string\"},{\"name\":\"postalCode\",\"type\":\"long\"} ,{\"name\":\"addObject\",\"type\":\"object\",\"subFields\":[{\"name\":\"key1\",\"type\":\"string\"},{\"name\":\"key2\",\"type\":\"string\"}]}]}, {\"name\":\"Audit\",\"type\":\"object\",\"subFields\":[ {\"name\":\"lastModifiedDate\",\"type\":\"string\",\"format\":\"date\"},{\"name\":\"lastModifiedDateTime\",\"type\":\"string\",\"format\":\"date-time\"},{\"name\":\"creationDate\",\"type\":\"date\"},{\"name\":\"creationDateTime\",\"type\":\"date-time\"}]}],\"fileDescription\":{\"persisted\":false,\"format\":\"parquet\"}}";

    private final String testData = "{\"Person\":{\"id\":\"dummyId\",\"firstName\":\"dummyFirstName\",\"lastName\":\"dummyLastName\",\"middleName\":\"dummyMiddleName\",\"nicknames\":[\"dummyNickName1, dummyNickName2\"],\"gender\":\"male\",\"birthArray\":[{\"birthDay\":\"6\",\"birthMonth\":\"September\",\"birthYear\":\"1991\"}]},\"Address\":{\"state\":\"Delhi\",\"postalCode\":1234}, \"Audit\": {\"creationDate\":\"10000\", \"creationDateTime\":\"1527848884000\",\"lastModifiedDateTime\":\"1527848884000\",\"lastModifiedDate\":\"10000\" }}";

    @Before
    public void setup() throws ParseException {
        JSONParser parser = new JSONParser();
        JSONObject dataSetObj = (JSONObject) parser.parse(dataSetString);
        DataSet dataSet = new DataSet(dataSetObj);
        fieldConverter = new JSONParquetFieldConverter(dataSet.getFields(false));
    }

    @Test
    public void TestJsonDataToParquetIOFieldConversion() throws ParseException {
        JSONParser parser = new JSONParser();
        JSONObject data = (JSONObject) parser.parse(testData);
        List<ParquetIOField> parquetFields = fieldConverter.convert(data);
        assert (parquetFields.size() == 3);
    }

    @Test(expected = NullPointerException.class)
    public void TestNonExistingCatalogField() throws ParseException {
        JSONParser parser = new JSONParser();
        JSONObject data = (JSONObject) parser.parse("{\"badField\":{}}");
        fieldConverter.convert(data);
    }
}