
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

import com.adobe.platform.ecosystem.examples.parquet.model.ParquetIODataType;
import com.adobe.platform.ecosystem.examples.parquet.model.ParquetIOField;
import com.adobe.platform.ecosystem.examples.parquet.model.ParquetIORepetitionType;
import com.adobe.platform.ecosystem.examples.catalog.model.DataSet;
import com.adobe.platform.ecosystem.examples.catalog.model.DataType;
import com.adobe.platform.ecosystem.examples.catalog.model.SchemaField;
import com.adobe.platform.ecosystem.examples.data.Pair;
import com.adobe.platform.ecosystem.examples.data.functions.FieldConverterFunction;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import io.jsonwebtoken.lang.Collections;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
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
    public List<ParquetIOField> convert(JSONObject data) {
        List<ParquetIOField> fields = new ArrayList<>();
        for (Object key : data.keySet()) {
            LinkedList<String> fieldPath = new LinkedList<>();
            fieldPath.add(key.toString());
            fields.add(createFieldFromJSON(fieldPath, data.get(key), null));
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
    @SuppressWarnings("unchecked")
    private ParquetIOField createFieldFromJSON(LinkedList<String> fieldPath, Object value, ParquetIORepetitionType repType) {
        Pair<ParquetIODataType, ParquetIORepetitionType> pair = getParquetTypeAndRepetition(fieldPath, repType);
        if (pair.getFirst() != ParquetIODataType.GROUP && pair.getFirst() != ParquetIODataType.Map && pair.getFirst() != ParquetIODataType.LIST) {
            ParquetIOField parquetIOField =
                    new ParquetIOField(
                            fieldPath.peekLast(),
                            pair.getFirst(),
                            pair.getSecond(),
                            null
                    );
            return parquetIOField;
        } else if (pair.getFirst() == ParquetIODataType.LIST) {
            JSONObject jObj = getJsonObject(value, pair);
            List<ParquetIOField> subFields = new ArrayList<>();
            if (jObj == null) {
                ParquetIOField subField =
                        new ParquetIOField(
                                "element",
                                ParquetIODataType.STRING,
                                ParquetIORepetitionType.OPTIONAL,
                                null
                        );
                subFields.add(subField);
            } else {
                for (Object subKey : jObj.keySet()) {
                    LinkedList<String> childLinkedList = (LinkedList<String>) fieldPath.clone();
                    childLinkedList.addLast(subKey.toString());
                    ParquetIOField subField =
                            createFieldFromJSON(
                                    childLinkedList,
                                    jObj.get(childLinkedList.peekLast()),
                                    null
                            );

                    subFields.add(subField);
                }
            }
            ParquetIOField parquetIOField =
                    new ParquetIOField(
                            fieldPath.peekLast(),
                            pair.getFirst(),
                            pair.getSecond(),
                            subFields
                    );
            return parquetIOField;
        } else {
            // Assumption, that even if pipeline does not have data, it will still have keys as reference.
            JSONObject jObj = getJsonObject(value, pair);
            List<ParquetIOField> subFields = new ArrayList<>();
            for (Object subKey : jObj.keySet()) {
                ParquetIORepetitionType repTypeSubField = null;
                if(pair.getFirst() == ParquetIODataType.Map && subKey.toString().equals("key")) {
                    repTypeSubField = ParquetIORepetitionType.REQUIRED;
                }
                LinkedList<String> childLinkedList = (LinkedList<String>) fieldPath.clone();
                childLinkedList.addLast(subKey.toString());
                ParquetIOField subField =
                        createFieldFromJSON(
                                childLinkedList,
                                jObj.get(childLinkedList.peekLast()),
                                repTypeSubField
                        );
                subFields.add(subField);
            }
            if (pair.getFirst() == ParquetIODataType.Map) {
                subFields.sort(Comparator.comparing(ParquetIOField::getName));
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

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private Object updateValueForList(LinkedList<String> fieldPath, Object value) {
        SchemaField schemaField = null;
        List<SchemaField> searchableSchemaFields = this.catalogSchemaFields;
        Iterator pathIterator = fieldPath.iterator();
        JSONObject listChildObject = new JSONObject();
        while (pathIterator.hasNext()) {
            String currentKey = (String) pathIterator.next();
            schemaField = getCatalogSchemaFieldFromKey(currentKey, searchableSchemaFields);
            // TODO: what if Schema field is NULL here?
            if (schemaField.getSubFields() != null && schemaField.getSubFields().size() > 0)
                searchableSchemaFields = schemaField.getSubFields();
        }
        if (schemaField.getArraySubType() == DataType.StringType) {
            listChildObject.put("element", value);
        } else if (schemaField.getArraySubType() == DataType.Field_ObjectType) {
            listChildObject.put("element", value);
        }
        return listChildObject;
    }

    /**
     * Inspects catalog data type
     * and returns the first element
     * for array group type.
     */
    @SuppressWarnings("unchecked")
    private JSONObject getJsonObject(Object value, Pair<ParquetIODataType, ParquetIORepetitionType> pair) {
        if(pair.getFirst() == ParquetIODataType.GROUP && pair.getSecond() == ParquetIORepetitionType.REPEATED) {
            // Because of PLAT-14737, We will be receiving JsonObjects
            // as we are consuming only first element while reading both
            // primitive and complex array types.
            if(value instanceof JSONArray) {
                JSONArray array = (JSONArray) value;
                return (JSONObject) array.get(0);
            }
        } else if(pair.getFirst() == ParquetIODataType.LIST && pair.getSecond() == ParquetIORepetitionType.REPEATED) {
            if(value instanceof JSONArray) {
                JSONArray array = (JSONArray) value;
                if (array.get(0) instanceof JSONObject) {
                    return (JSONObject) array.get(0);
                } else {
                    return null;
                }
            }
        } else if (pair.getFirst() == ParquetIODataType.Map) {
            if(value instanceof JSONObject) {
                JSONObject mapObject = (JSONObject) value;
                JSONObject firstPair = new JSONObject();
                for(Object key: mapObject.keySet()) {
                    firstPair.put("key", key);
                    firstPair.put("value", mapObject.get(key));
                    value = firstPair;
                    break;
                }
            }
        }
        return (JSONObject) value;
    }

    /**
     * Function which traverses a given path which
     * contains the the keys (starting from root node).
     *
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
     *
     * For above case: return value will be
     * Pair<ParquetIODataType.String, ParquetIORepetitionType.OPTIONAL>
     *
     * </p>
     *
     *
     * @param fieldPath
     * @return
     */
    private Pair<ParquetIODataType, ParquetIORepetitionType> getParquetTypeAndRepetition(LinkedList<String> fieldPath, ParquetIORepetitionType repType) {
        SchemaField schemaField = null;
        List<SchemaField> searchableSchemaFields = this.catalogSchemaFields;
        Iterator pathIterator = fieldPath.iterator();
        while (pathIterator.hasNext()) {
            String currentKey = (String) pathIterator.next();
            schemaField = getCatalogSchemaFieldFromKey(currentKey, searchableSchemaFields);
            // TODO: what if Schema field is NULL here?
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
                || schemaField.getType() == DataType.Field_ObjectType) {
            pair.setFirst(FieldConverterFunction.catalogToParquetFieldFunction.apply(schemaField.getType()));
        } else if (schemaField.getType() == DataType.Field_ArrayType || schemaField.getType() == DataType.Field_MapType) {
            // We need to check the sub-array type for determining 'Group'
            // type for Parquet or not.
            pair.setSecond(ParquetIORepetitionType.REPEATED);
            if (schemaField.getType() == DataType.Field_MapType) {
                pair.setFirst(ParquetIODataType.Map);
            } else if (schemaField.getType() == DataType.Field_ArrayType) {
                pair.setFirst(ParquetIODataType.LIST);
            } else if (schemaField.getArraySubType() == DataType.Field_ObjectType) {
                pair.setFirst(ParquetIODataType.GROUP);
            } else {
                pair.setFirst(FieldConverterFunction.catalogToParquetFieldFunction.apply(schemaField.getArraySubType()));
            }
        }
        if (repType != null) {
            pair.setSecond(repType);
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