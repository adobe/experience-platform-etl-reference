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
package com.adobe.platform.ecosystem.examples.data.validation;

import com.adobe.platform.ecosystem.examples.catalog.model.DataType;
import com.adobe.platform.ecosystem.examples.catalog.model.SchemaField;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class SchemaKeysBasedDataValidatorTest {

    private JSONParser parser;
    private SchemaKeysBasedDataValidator validator;

    @Before
    public void setup() {
        parser = new JSONParser();
    }

    @Test
    public void testValidateJSONObjectOfPrimitives() throws ParseException, ConnectorSDKException {
        List<SchemaField> schemaFields = getPrimitiveFields();
        String dataString = "{\n" +
                "  \"primitiveField1\" : \"stringPrimitiveField1\",\n" +
                "  \"primitiveField2\" : true,\n" +
                "  \"primitiveField3\" : 24.97\n" +
                "}";
        validator.validateData(schemaFields, getDataTable(dataString));
    }

    @Test(expected = ConnectorSDKException.class)
    public void testValidateJSONObjectOfPrimitivesFailure() throws ParseException, ConnectorSDKException{
        List<SchemaField> schemaFields = getPrimitiveFields();
        String dataString = "{\n" +
                "  \"primitiveField1\" : \"stringPrimitiveField1\",\n" +
                "  \"primitiveField2\" : true,\n" +
                "  \"primitiveField3\" : 24.97\n" +
                "  \"incorrectField\" :  \"incorrectFieldValue\",\n" +
                "}";
        validator.validateData(schemaFields, getDataTable(dataString));
    }

    @Test
    public void testValidateJSONObjectOfObjects() throws ParseException, ConnectorSDKException {
        List<SchemaField> schemaFields = getObjectFields();
        String dataString = "{\n" +
                "  \"objectField1\" : {\n" +
                "    \"primitiveField1\" : \"stringPrimitiveField1\",\n" +
                "    \"primitiveField2\" : true,\n" +
                "    \"primitiveField3\" : 24.97\n" +
                "  }\n" +
                "}";
        validator.validateData(schemaFields, getDataTable(dataString));
    }

    @Test(expected = ConnectorSDKException.class)
    public void testValidateJSONObjectOfObjectsFailure() throws ParseException, ConnectorSDKException {
        List<SchemaField> schemaFields = getObjectFields();
        String dataString = "{\n" +
                "  \"objectFieldWrong\" : {\n" +
                "    \"primitiveField1\" : \"stringPrimitiveField1\",\n" +
                "    \"primitiveField2\" : true,\n" +
                "    \"primitiveField3\" : 24.97\n" +
                "  }\n" +
                "}";
        validator.validateData(schemaFields, getDataTable(dataString));
    }

    @Test
    public void testValidateJSONObjectOfJSONArrays() throws ParseException, ConnectorSDKException {
        List<SchemaField> schemaFields = getObjectOfArraysFields();
        String dataString = "{\n" +
                "  \"objectField1\": {\n" +
                "    \"arrayField1\": [\n" +
                "      \"string1\",\n" +
                "      \"string2\"\n" +
                "    ],\n" +
                "  }\n" +
                "}";
        validator.validateData(schemaFields, getDataTable(dataString));
    }

    @Test(expected = ConnectorSDKException.class)
    public void testValidateJSONObjectOfJSONArraysFailure() throws ParseException, ConnectorSDKException {
        List<SchemaField> schemaFields = getObjectOfArraysFields();
        String dataString = "{\n" +
                "  \"objectField1\": {\n" +
                "    \"arrayField1\": {\n" +
                "      \"primitiveField1\": \"stringPrimitiveField1\"\n" +
                "    }\n" +
                "  }\n" +
                "}";
        validator.validateData(schemaFields, getDataTable(dataString));
    }

    @Test
    public void testValidateJSONArrayOfPrimitives() throws ParseException, ConnectorSDKException {
        List<SchemaField> schemaFields = getArrayFields();
        String dataString = "{\n" +
                "  \"arrayField1\" : [\n" +
                "      \"string1\", \"string2\"\n" +
                "    ]\n" +
                "}";
        validator.validateData(schemaFields, getDataTable(dataString));
    }

    @Test
    public void testValidateJSONArrayOfJSONObjects() throws ParseException, ConnectorSDKException {
        List<SchemaField> schemaFields = getArrayOfObjectFields();
        String dataString = "{\n" +
                "  \"arrayField1\" : [\n" +
                "      {\n" +
                "        \"primitiveField1\" : \"stringPrimitiveField1\",\n" +
                "        \"primitiveField2\" : true,\n" +
                "        \"primitiveField3\" : 24.97\n" +
                "      }\n" +
                "    ]\n" +
                "}";
        validator.validateData(schemaFields, getDataTable(dataString));
    }

    @Test(expected = ConnectorSDKException.class)
    public void testValidateJSONArrayOfJSONObjectsFailureOfObject() throws ParseException, ConnectorSDKException {
        List<SchemaField> schemaFields = getArrayOfObjectFields();
        String dataString = "{\n" +
                "  \"arrayField1\" : [\n" +
                "      {\n" +
                "        \"primitiveField1\" : \"stringPrimitiveField1\",\n" +
                "        \"primitiveField2\" : true,\n" +
                "        \"primitiveField3\" : 24.97\n" +
                "        \"primitiveField4\" : \"shouldNotBePresent\",\n" +
                "      }\n" +
                "    ]\n" +
                "}";
        validator.validateData(schemaFields, getDataTable(dataString));
    }

    @Test(expected = ConnectorSDKException.class)
    public void testValidateJSONArrayOfJSONObjectsFailureDueToPrimitiveField() throws ParseException, ConnectorSDKException {
        List<SchemaField> schemaFields = getArrayOfObjectFields();
        String dataString = "{\n" +
                "  \"arrayField1\" : [\n" +
                "      \"primitiveField\"\n" +
                "    ]\n" +
                "}";
        validator.validateData(schemaFields, getDataTable(dataString));
    }

    //Not yet supported.
    @Test
    public void testValidateJSONArrayOfMap() {

    }

    //Not yet supported.
    @Test
    public void testValidateJSONArrayOfJSONArrays() {

    }

    @Test
    public void testMapOfJSONObjects() throws ParseException, ConnectorSDKException {
        List<SchemaField> schemaFields = getMapOfObjectFields();
        String dataString = "{\n" +
                "  \"mapString1\" : {\n" +
                "    \"string1\" : {\n" +
                "      \"primitiveField1\" : \"stringPrimitiveField1\",\n" +
                "      \"primitiveField2\" : true,\n" +
                "      \"primitiveField3\" : 24.97\n" +
                "    }\n" +
                "  }\n" +
                "}";
        validator.validateData(schemaFields, getDataTable(dataString));
    }

    @Test
    public void testMapOfJSONArrays() throws ParseException, ConnectorSDKException {
        List<SchemaField> schemaFields = getMapOfArrayFields();
        String dataString = "{\n" +
                "  \"mapString1\" : {\n" +
                "    \"string1\" : [\n" +
                "        \"string1\", \"string2\"\n" +
                "      ]\n" +
                "  }\n" +
                "}";
        validator.validateData(schemaFields, getDataTable(dataString));
    }

    @Test
    public void testMapOfMaps() throws ParseException, ConnectorSDKException {
        List<SchemaField> schemaFields = getMapOfMapFields();
        String dataString = "{\n" +
                "  \"mapString1\" : {\n" +
                "    \"mapString2\" : {\n" +
                "      \"stringToList\" : [\n" +
                "          {\n" +
                "            \"primitiveField1\" : \"stringPrimitiveField1\",\n" +
                "            \"primitiveField2\" : true,\n" +
                "            \"primitiveField3\" : 24.97\n" +
                "          }\n" +
                "        ]\n" +
                "    }\n" +
                "  }\n" +
                "}";
        validator.validateData(schemaFields, getDataTable(dataString));
    }

    @Test
    public void testMapOfPrimitives() throws ParseException, ConnectorSDKException {
        List<SchemaField> schemaFields = getMapFields();
        String dataString = "{\n" +
                "  \"mapString1\" : {\n" +
                "    \"string1\" : \"string1\",\n" +
                "    \"string2\" : \"string2\"\n" +
                "  }\n" +
                "}";
        validator.validateData(schemaFields, getDataTable(dataString));
    }

    private List<JSONObject> getDataTable(String dataString) throws ParseException {
        JSONObject dataObject = (JSONObject) parser.parse(dataString);
        return Arrays.asList(dataObject);
    }

    private List<SchemaField> getPrimitiveFields() {
        SchemaField primitiveField1 = new SchemaField("primitiveField1", DataType.StringType, null);
        SchemaField primitiveField2 = new SchemaField("primitiveField2", DataType.BooleanType, null);
        SchemaField primitiveField3 = new SchemaField("primitiveField3", DataType.FloatType, null);
        return Arrays.asList(primitiveField1, primitiveField2, primitiveField3);
    }

    private List<SchemaField> getObjectFields() {
        SchemaField root = new SchemaField("objectField1", DataType.Field_ObjectType, getPrimitiveFields());
        return Arrays.asList(root);
    }

    private List<SchemaField> getArrayFields() {
        SchemaField arrayField = new SchemaField("arrayField1", DataType.Field_ArrayType, null, DataType.StringType);
        return Arrays.asList(arrayField);
    }

    private List<SchemaField> getObjectOfArraysFields() {
        SchemaField root = new SchemaField("objectField1", DataType.Field_ObjectType, getArrayFields());
        return Arrays.asList(root);
    }

    private List<SchemaField> getMapFields() {
        SchemaField keyField = new SchemaField("key", DataType.StringType, null);
        SchemaField valueField = new SchemaField("value", DataType.StringType, null);
        SchemaField mapField1 = new SchemaField("mapString1", DataType.Field_MapType, Arrays.asList(keyField, valueField));
        return Arrays.asList(mapField1);
    }

    private List<SchemaField> getObjectOfMapFields() {
        SchemaField root = new SchemaField("objectField1", DataType.Field_ObjectType, getMapFields());
        return Arrays.asList(root);
    }

    private List<SchemaField> getArrayOfObjectFields() {
        SchemaField arrayField = new SchemaField("arrayField1", DataType.Field_ArrayType, getPrimitiveFields(), DataType.Field_ObjectType);
        return Arrays.asList(arrayField);
    }

    private List<SchemaField> getMapOfObjectFields() {
        SchemaField keyField = new SchemaField("key", DataType.StringType, null);
        SchemaField valueField = new SchemaField("value", DataType.Field_ObjectType, getPrimitiveFields());
        SchemaField mapField1 = new SchemaField("mapString1", DataType.Field_MapType, Arrays.asList(keyField, valueField));
        return Arrays.asList(mapField1);
    }

    private List<SchemaField> getMapOfArrayFields() {
        SchemaField keyField = new SchemaField("key", DataType.StringType, null);
        SchemaField valueField = new SchemaField("value", DataType.Field_ArrayType, null, DataType.StringType);
        SchemaField mapField1 = new SchemaField("mapString1", DataType.Field_MapType, Arrays.asList(keyField, valueField));
        return Arrays.asList(mapField1);
    }

    private List<SchemaField> getMapOfMapFields() {
        SchemaField keyField = new SchemaField("key", DataType.StringType, null);
        SchemaField valueField = new SchemaField("value", DataType.Field_ArrayType, getPrimitiveFields(), DataType.Field_ObjectType);
        SchemaField mapFields2 = new SchemaField("mapString2", DataType.Field_MapType, Arrays.asList(keyField, valueField));

        SchemaField valueFieldOuter = new SchemaField("value", DataType.Field_MapType, Arrays.asList(keyField, valueField));
        SchemaField mapField1 = new SchemaField("mapString1", DataType.Field_MapType, Arrays.asList(keyField, valueFieldOuter));
        return Arrays.asList(mapField1);
    }

}
