
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

import com.adobe.platform.ecosystem.examples.constants.SDKConstants;
import com.adobe.platform.ecosystem.examples.parquet.model.ParquetIODataType;
import com.adobe.platform.ecosystem.examples.parquet.model.ParquetIOField;
import com.adobe.platform.ecosystem.examples.parquet.model.ParquetIORepetitionType;
import com.adobe.platform.ecosystem.examples.catalog.model.DataSet;
import com.adobe.platform.ecosystem.examples.catalog.model.DataType;
import com.adobe.platform.ecosystem.examples.catalog.model.SchemaField;
import com.adobe.platform.ecosystem.examples.data.Pair;
import com.adobe.platform.ecosystem.examples.data.functions.FieldConverterFunction;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Concrete implementation to convert a
 * JSON object to Parquet fields.
 */
public class JSONParquetFieldConverter implements ParquetFieldConverter<JSONObject> {
    private final List<SchemaField> catalogSchemaFields;

    public JSONParquetFieldConverter(List<SchemaField> catalogSchemaFields) {
        this.catalogSchemaFields = catalogSchemaFields;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<ParquetIOField> convert(JSONObject data) throws ConnectorSDKException {
        List<ParquetIOField> fields = new ArrayList<>();
        for (Object key : data.keySet()) {
            LinkedList<String> fieldPath = new LinkedList<>();
            fieldPath.add(key.toString());
            fields.add(createFieldFromJSON(fieldPath, data.get(key)));
        }
        return fields;
    }

    /**
     * Function to create equivalent {@link ParquetIOField}
     * from a given field path <code>fieldPath</code>.
     * <code>fieldPath</code> represent the name of fields
     * to traverse in Catalog schema in order to determine
     * correct {@link ParquetIODataType} and {@link ParquetIORepetitionType}.
     *
     * @param fieldPath
     * @param value
     * @return
     */
    private ParquetIOField createFieldFromJSON(LinkedList<String> fieldPath, Object value) throws ConnectorSDKException {
        final Pair<ParquetIODataType, ParquetIORepetitionType> pair = getParquetTypeAndRepetition(fieldPath);
        final ParquetIODataType pDataType = pair.getFirst();

        // Order of Processing
        // 1. Check for Map types (Build only value recursively).
        // 2. Build for List types (for Complex Arrays only)
        // 3. Build for primitive Arrays.
        // 4. Build for primitive types (non-repeating)
        // 5. Build for Complex types excluding Complex Arrays (covered in 2).

        if (pDataType == ParquetIODataType.Map) {
            ParquetIOField mapKey =
                new ParquetIOField(
                    SDKConstants.CATALOG_MAP_KEY,
                    ParquetIODataType.STRING,
                    ParquetIORepetitionType.REQUIRED,
                    null
                );
            final JSONObject mapJson = (JSONObject) value;
            // Extract only first key as all values will resolve to same structure.
            final String firstKey = (String) mapJson.keySet().iterator().next();
            final Object firstValue = mapJson.get(firstKey);

            // Cloning the fieldPath and  adding 'value' as the new node
            // to the linked list, as Catalog Schema fields for
            // map sub type will have two fields. One named 'key'
            // and other 'value'
            final LinkedList<String> childLinkedList = (LinkedList<String>) fieldPath.clone();
            childLinkedList.addLast(SDKConstants.CATALOG_MAP_VALUE);
            ParquetIOField mapValue =
                createFieldFromJSON( // recurse!
                    childLinkedList,
                    firstValue
                );

            return new ParquetIOField(
                fieldPath.peekLast(),
                pair.getFirst(),
                pair.getSecond(),
                Arrays.asList(mapKey, mapValue)
            );
        } else if (pDataType == ParquetIODataType.GROUP && pair.getSecond() == ParquetIORepetitionType.REPEATED) { // Complex Arrays
            // Ideally 'LIST' in ParquetIODataType should have been used
            // But we are using Repeated Group to signal complex lists.
            JSONObject elementObj = getJsonObject(value, pair);


            List<ParquetIOField> subFields = new ArrayList<>();
            for (Object subKey : elementObj.keySet()) {
                LinkedList<String> childLinkedList = (LinkedList<String>) fieldPath.clone();
                childLinkedList.addLast(subKey.toString());
                ParquetIOField subField =
                    createFieldFromJSON(
                        childLinkedList,
                        elementObj.get(childLinkedList.peekLast())
                    );
                subFields.add(subField);
            }

            ParquetIOField element = new ParquetIOField(
                "element",
                ParquetIODataType.GROUP,
                ParquetIORepetitionType.OPTIONAL,
                subFields
            );

            return new ParquetIOField(
                fieldPath.peekLast(),
                ParquetIODataType.LIST,
                ParquetIORepetitionType.OPTIONAL,
                Arrays.asList(element)
            );
        } else if (pDataType != ParquetIODataType.GROUP && pair.getSecond() == ParquetIORepetitionType.REPEATED) { // Primitive Array
            ParquetIOField element = new ParquetIOField(
                "element",
                pair.getFirst(),
                ParquetIORepetitionType.OPTIONAL,
                null
            );

            ParquetIOField parquetIOField =
                new ParquetIOField(
                    fieldPath.peekLast(),
                    ParquetIODataType.LIST,
                    ParquetIORepetitionType.OPTIONAL,
                    Arrays.asList(element)
                );
            return parquetIOField;
        } else if (pDataType != ParquetIODataType.GROUP ) { // Primitive types
            ParquetIOField parquetIOField =
                new ParquetIOField(
                    fieldPath.peekLast(),
                    pair.getFirst(),
                    pair.getSecond(),
                    null
                );
            return parquetIOField;
        } else { // Complex types
            // Assumption, that even if pipeline does not have data, it will still have keys as reference.
            JSONObject jObj = getJsonObject(value, pair);
            List<ParquetIOField> subFields = new ArrayList<>();
            for (Object subKey : jObj.keySet()) {
                LinkedList<String> childLinkedList = (LinkedList<String>) fieldPath.clone();
                childLinkedList.addLast(subKey.toString());
                ParquetIOField subField =
                    createFieldFromJSON(
                        childLinkedList,
                        jObj.get(childLinkedList.peekLast())
                    );
                subFields.add(subField);
            }
            ParquetIOField parquetIOField =
                new ParquetIOField(
                    fieldPath.peekLast(),
                    pair.getFirst(),
                    pair.getSecond(),
                    subFields
                );
            return parquetIOField;
        }
    }

    /**
     * Inspects catalog data type
     * and returns the first element
     * for array group type.
     */
    private JSONObject getJsonObject(Object value, Pair<ParquetIODataType, ParquetIORepetitionType> pair) {
        if (pair.getFirst() == ParquetIODataType.GROUP && pair.getSecond() == ParquetIORepetitionType.REPEATED) {
            // Because of PLAT-14737, We will be receiving JsonObjects
            // as we are consuming only first element while reading both
            // primitive and complex array types.
            if (value instanceof JSONArray) {
                JSONArray array = (JSONArray) value;
                return (JSONObject) array.get(0);
            }
        }
        return (JSONObject) value;
    }

    /**
     * Function which traverses a given path which
     * contains the the keys (starting from root node).
     *
     * <pre>
     * <p>
     *     for eg:
     *     Let's say fieldPath = ['A', 'B'] and
     *     catalog schema is:
     *     fields: [
     *       {
     *         "name" : "A",
     *         "type" : "object",
     *         "subFields" : [
     *         {
     *             "name" : "B",
     *             "type" : "string",
     *         }]
     *       }
     *     ]
     * </pre>
     * <p>
     * For above case: return value will be
     * Pair<ParquetIODataType.String, ParquetIORepetitionType.OPTIONAL>
     *
     * </p>
     *
     * @param fieldPath
     * @return
     */
    private Pair<ParquetIODataType, ParquetIORepetitionType> getParquetTypeAndRepetition(LinkedList<String> fieldPath) throws ConnectorSDKException {
        SchemaField schemaField = null;
        List<SchemaField> searchableSchemaFields = this.catalogSchemaFields;
        Iterator pathIterator = fieldPath.iterator();
        while (pathIterator.hasNext()) {
            String currentKey = (String) pathIterator.next();
            schemaField = getCatalogSchemaFieldFromKey(currentKey, searchableSchemaFields);
            if (schemaField == null) {
                throw new ConnectorSDKException("Field not present in the dataset schema");
            }
            if (schemaField.getSubFields() != null && schemaField.getSubFields().size() > 0)
                searchableSchemaFields = schemaField.getSubFields();
        }
        Pair<ParquetIODataType, ParquetIORepetitionType> pair = new Pair<>();
        pair.setSecond(ParquetIORepetitionType.OPTIONAL); // Default value to start with
        if (schemaField.getType() == DataType.StringType
            || schemaField.getType() == DataType.BooleanType
            || schemaField.getType() == DataType.ByteType
            || schemaField.getType() == DataType.ShortType
            || schemaField.getType() == DataType.IntegerType
            || schemaField.getType() == DataType.LongType
            || schemaField.getType() == DataType.FloatType
            || schemaField.getType() == DataType.DoubleType
            || schemaField.getType() == DataType.DateType
            || schemaField.getType() == DataType.DateTimeType
            || schemaField.getType() == DataType.BinaryType
            || schemaField.getType() == DataType.Field_ObjectType
            || schemaField.getType() == DataType.Field_MapType) {
            pair.setFirst(FieldConverterFunction.catalogToParquetFieldFunction.apply(schemaField.getType()));
        } else if (schemaField.getType() == DataType.Field_ArrayType) {
            // We need to check the sub-array type for determining 'Group'
            // type for Parquet or not.
            pair.setSecond(ParquetIORepetitionType.REPEATED);
            if (schemaField.getArraySubType() == DataType.Field_ObjectType) {
                pair.setFirst(ParquetIODataType.GROUP);
            } else {
                pair.setFirst(FieldConverterFunction.catalogToParquetFieldFunction.apply(schemaField.getArraySubType()));
            }
        }
        return pair;
    }

    private SchemaField getCatalogSchemaFieldFromKey(String currentKey, List<SchemaField> catalogSchemaFields) {
        for (SchemaField schemaField : catalogSchemaFields) {
            if (schemaField.getName().equalsIgnoreCase(currentKey)) {
                return schemaField;
            }
        }
        return null; // Ideally we should never reach here.
    }
}