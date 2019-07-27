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
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Validator to validate data with schema.
 * Checks for data to be compliant with schema at all levels.
 * <p>
 * Validator currently has support for DataTypes Object, Array, Map and Primitives.
 * Does not support [Array of Array] and [Array of Map].
 *
 * @author shesriva
 */
public class SchemaKeysBasedDataValidator {

    private static Logger logger = Logger.getLogger(SchemaKeysBasedDataValidator.class.getName());

    public static void validateData(List<SchemaField> schemaFields,
                                    List<JSONObject> dataTable) throws ConnectorSDKException {
        for (JSONObject jsonObject : dataTable) {
            validatingFunction(jsonObject, schemaFields);
        }
    }

    public static void validatingFunction(JSONObject object, List<SchemaField> fields) throws ConnectorSDKException {
        final Iterator iterator = object.keySet().iterator();
        while (iterator.hasNext()) {
            final String key = (String) iterator.next();

            // Check if key is present.
            List<SchemaField> matchedFields = fields
                    .stream()
                    .filter(schemaField -> schemaField.getName().equals(key))
                    .collect(Collectors.toList());

            // If Key absent, throw error.
            if (matchedFields.size() <= 0) {
                logger.log(Level.SEVERE, "Key=%s provided in the data but not present in schema.", key);
                throw new ConnectorSDKException(String.format("Schema does not contain field=%s given in the data.", key));
            }

            // If Key present, get the SchemaField
            SchemaField matchingField = matchedFields.get(0);

            // Proceed only if we do not have primitive type => primitive types require no further checks.
            if (matchingField.getIsPrimitive()) {
                continue;
            }

            // Validating Schema Field type to that of actual 'value' present against 'key' inside Json object.
            final Object value = object.get(key);
            validateKeyTypeWithSchemaType(value, matchingField.getType(), matchingField.getName());

            // Object, Array or Map types.
            if (matchingField.getType() == DataType.Field_ObjectType) {
                validatingFunction((JSONObject) object.get(key), matchingField.getSubFields());
            } else if (matchingField.getType() == DataType.Field_ArrayType) {
                // If Sub type are of type primitive. Do nothing return.
                if (matchingField.getArraySubType() == null || DataType.isPrimitiveDataType(matchingField.getArraySubType())) {
                    return;
                } else {
                    invokeValidatingFunctionOnArray((JSONArray) value, matchingField);
                }
            } else if (matchingField.getType() == DataType.Field_MapType) {
                invokeValidatingFunctionOnMapType((JSONObject) value, matchingField);
            }
        }
    }

    private static void invokeValidatingFunctionOnMapType(JSONObject value, SchemaField mapField) throws ConnectorSDKException {
        // Check on types of 'mapField''s 2nd subField, ie at index [1].

        final SchemaField valueField = mapField.getSubFields().get(1);

        // End checking if value is primitive type.
        if (valueField.getIsPrimitive()) return;

        // Call required recursive function based on dataType of valueField.
        checkValidationOnChildrenKeys(value, valueField);
    }

    private static void checkValidationOnChildrenKeys(JSONObject value, SchemaField valueField) throws ConnectorSDKException {
        final Iterator iterator = value.keySet().iterator();
        DataType dataType = valueField.getType();

        while (iterator.hasNext()) {
            String key = (String) iterator.next();
            Object object = value.get(key);
            validateKeyTypeWithSchemaType(object, valueField.getType(), "");

            if (dataType.equals(DataType.Field_ObjectType)) {
                validatingFunction((JSONObject) object, valueField.getSubFields());
            } else if (dataType.equals(DataType.Field_MapType)) {
                invokeValidatingFunctionOnMapType((JSONObject) object, valueField);
            } else if (dataType.equals(DataType.Field_ArrayType)) {
                invokeValidatingFunctionOnArray((JSONArray) object, valueField);
            }
        }
    }

    private static void invokeValidatingFunctionOnArray(JSONArray arrayData, SchemaField arrayField) throws ConnectorSDKException {
        // TODO : To Add Array of Array and Array of Map types.
        if (arrayField.getArraySubType() == DataType.Field_ObjectType) {
            final int length = arrayData.size();
            for (int i = 0; i < length; i++) {
                // Validate each element to be of object type.
                validateKeyTypeWithSchemaType(arrayData.get(i), arrayField.getArraySubType(), arrayField.getName());

                // Invoke Recursion!
                validatingFunction((JSONObject) arrayData.get(i), arrayField.getSubFields());
            }
        }
    }

    /**
     * Function to validate incoming object to be in accordance with expected dataType.
     */
    private static void validateKeyTypeWithSchemaType(Object object, DataType dataType, String name) throws ConnectorSDKException {
        if (object instanceof JSONObject && dataType.equals(DataType.Field_ObjectType)) {
            return;
        } else if (object instanceof JSONArray && dataType.equals(DataType.Field_ArrayType)) {
            return;
        } else if (object instanceof JSONObject && dataType.equals(DataType.Field_MapType)) {
            return;
        } else {
            final String errMsg = String.format(
                    "Type mismatch found in Json value type for key and schema field name=[%s]",
                    name
            );
            logger.log(Level.SEVERE, errMsg);
            throw new ConnectorSDKException(errMsg);
        }
    }
}
