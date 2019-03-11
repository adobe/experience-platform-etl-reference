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
package com.adobe.platform.ecosystem.examples.data.write.field.converter.parquet.catalog;


import com.adobe.platform.ecosystem.examples.catalog.model.DataType;
import com.adobe.platform.ecosystem.examples.catalog.model.SchemaField;
import com.adobe.platform.ecosystem.examples.data.write.field.converter.parquet.ParquetFieldConverter;
import com.adobe.platform.ecosystem.examples.parquet.model.ParquetIODataType;
import com.adobe.platform.ecosystem.examples.parquet.model.ParquetIOField;
import com.adobe.platform.ecosystem.examples.parquet.model.ParquetIORepetitionType;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;
import com.adobe.platform.ecosystem.ut.BaseTest;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.adobe.platform.ecosystem.examples.constants.SDKConstants.CATALOG_MAP_KEY;
import static com.adobe.platform.ecosystem.examples.constants.SDKConstants.CATALOG_MAP_VALUE;
import static org.junit.Assert.assertEquals;
import static org.mockito.MockitoAnnotations.initMocks;

public class CatalogSchemaParquetFieldConverterTest extends BaseTest {

    private ParquetFieldConverter<List<SchemaField>> fieldConverter;

    @Before
    public void setup() throws ConnectorSDKException, IOException {
        initMocks(this);

        // Setting up JWT mocks.
        setUp();
        setUpHttpForJwtResponse();

        fieldConverter = new CatalogSchemaParquetFieldConverter();
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
    public void TestSchemaFieldToParquetIOFieldConversion() throws ConnectorSDKException {
        List<ParquetIOField> parquetFields = fieldConverter.convert(getMockSchemaFields());
        assert (parquetFields.size() == 2);
    }

    @Test
    public void TestCreateArrayField() throws ConnectorSDKException {
        List<SchemaField> subFields = new ArrayList<>();
        SchemaField subField1 = new SchemaField("subField1", DataType.StringType, null);
        SchemaField subField2 = new SchemaField("subField2", DataType.StringType, null);
        SchemaField subField3 = new SchemaField("subField3", DataType.StringType, null);
        subFields.add(subField1);
        subFields.add(subField2);
        subFields.add(subField3);
        SchemaField arrayField = new SchemaField("arrayField", DataType.Field_ArrayType, subFields, DataType.StringType);

        List<SchemaField> data = new ArrayList<>();
        data.add(arrayField);
        List<ParquetIOField> parquetIOFields = fieldConverter.convert(data);

        assertEquals(parquetIOFields.get(0).getSubFields().size(), 1);
        assertEquals(parquetIOFields.get(0).getSubFields().get(0).getType(), ParquetIODataType.STRING);
    }

    @Test
    public void TestCreateStructField() throws ConnectorSDKException {
        List<SchemaField> subFields = new ArrayList<>();
        SchemaField subField1 = new SchemaField("subField1", DataType.StringType, null);
        SchemaField subField2 = new SchemaField("subField2", DataType.StringType, null);
        SchemaField subField3 = new SchemaField("subField3", DataType.StringType, null);
        subFields.add(subField1);
        subFields.add(subField2);
        subFields.add(subField3);
        SchemaField objectField = new SchemaField("objectField", DataType.Field_ObjectType, subFields);

        List<SchemaField> data = new ArrayList<>();
        data.add(objectField);
        List<ParquetIOField> parquetIOFields = fieldConverter.convert(data);

        assertEquals(parquetIOFields.get(0).getSubFields().size(), 3);
        assertEquals(parquetIOFields.get(0).getSubFields().get(0).getType(), ParquetIODataType.STRING);
        assertEquals(parquetIOFields.get(0).getSubFields().get(0).getRepetitionType(), ParquetIORepetitionType.OPTIONAL);
    }

    @Test
    public void TestCreateMapField() throws ConnectorSDKException {
        List<SchemaField> subFields = new ArrayList<>();
        SchemaField keyField = new SchemaField("key", DataType.StringType, null);
        List<SchemaField> valueSubFields = new ArrayList<>();
        SchemaField valueSubField1 = new SchemaField("valueSubField1", DataType.StringType, null);
        SchemaField valueSubField2 = new SchemaField("valueSubField2", DataType.StringType, null);
        valueSubFields.add(valueSubField1);
        valueSubFields.add(valueSubField2);
        SchemaField valueField = new SchemaField("value", DataType.Field_ObjectType, valueSubFields);
        subFields.add(keyField);
        subFields.add(valueField);
        SchemaField mapField = new SchemaField("mapField", DataType.Field_MapType, subFields);

        List<SchemaField> data = new ArrayList<>();
        data.add(mapField);
        List<ParquetIOField> parquetIOFields = fieldConverter.convert(data);

        assertEquals(parquetIOFields.get(0).getSubFields().size(), 2);
        assertEquals(parquetIOFields.get(0).getSubFields().get(0).getName(), "key");
        assertEquals(parquetIOFields.get(0).getSubFields().get(1).getName(), "value");
        assertEquals(parquetIOFields.get(0).getSubFields().get(1).getSubFields().size(), 2);
        assertEquals(parquetIOFields.get(0).getSubFields().get(1).getSubFields().get(0).getName(), "valueSubField1");
        assertEquals(parquetIOFields.get(0).getSubFields().get(1).getSubFields().get(0).getType(), ParquetIODataType.STRING);
    }
}
