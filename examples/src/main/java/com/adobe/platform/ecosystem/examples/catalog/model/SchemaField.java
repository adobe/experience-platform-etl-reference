
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.adobe.platform.ecosystem.examples.catalog.model.DataSet.FieldsFrom;
import com.adobe.platform.ecosystem.examples.constants.SDKConstants;
import com.adobe.platform.ecosystem.examples.util.JsonUtil;

public class SchemaField {
    private String name;

    private DataType type;

    private DataType arraySubType; // Will be set only when SchemaField.type is a DataType.Field_ArrayType.

    private List<SchemaField> subFields = null;

    private Dule dule;

    public SchemaField(JSONObject field, boolean useFlatNamesForLeafNodes) {
        this(null, field, "", useFlatNamesForLeafNodes, FieldsFrom.FIELDS);
    }

    public SchemaField(JSONObject subFieldJson, String currentHierarchy, boolean useFlatNamesForLeafNodes) {
        this(null, subFieldJson, currentHierarchy, useFlatNamesForLeafNodes, FieldsFrom.FIELDS);
    }

    public SchemaField(String name, JSONObject field, boolean useFlatNamesForLeafNodes, FieldsFrom fieldsFrom) {
        if(FieldsFrom.OBSERVABLE_SCHEMA == fieldsFrom) {
            getSchemaFieldFromObservableSchema(name, field, "", useFlatNamesForLeafNodes);
        } else {
            getSchemaField(field, "", useFlatNamesForLeafNodes);
        }
    }

    public SchemaField(String name, JSONObject subFieldJson, String currentHierarchy, boolean useFlatNamesForLeafNodes, FieldsFrom fieldsFrom) {
        if(FieldsFrom.OBSERVABLE_SCHEMA == fieldsFrom) {
            getSchemaFieldFromObservableSchema(name, subFieldJson, currentHierarchy, useFlatNamesForLeafNodes);
        } else {
            getSchemaField(subFieldJson, currentHierarchy, useFlatNamesForLeafNodes);
        }
    }

    private void getSchemaFieldFromObservableSchema(String name, JSONObject field, String parentHierarchy, boolean useFlatNameForLeafNodes) {
        String type = (String) field.get(SDKConstants.CATALOG_SCHEMA_TYPE);

        switch (type) {
        case "string":
            this.name = getNameForNonComplexField(parentHierarchy, field, useFlatNameForLeafNodes);
            this.type = DataType.StringType;
            if("date-time".equals(field.get(SDKConstants.CATALOG_SCHEMA_TYPE_FORMAT)))
                this.type = DataType.DateTimeType;
            else if("date".equals(field.get(SDKConstants.CATALOG_SCHEMA_TYPE_FORMAT)))
                this.type = DataType.DateType;
            break;
        case "integer":
            this.name = getNameForNonComplexField(name, parentHierarchy, field, useFlatNameForLeafNodes);
            this.type = DataType.IntegerType;
            break;
        case "date":
            // format, we will start parsing date in same format
            this.name = getNameForNonComplexField(parentHierarchy, field, useFlatNameForLeafNodes);
            this.type = DataType.DateType;
            break;
        case "date-time":
            this.name = getNameForNonComplexField(parentHierarchy, field, useFlatNameForLeafNodes);
            this.type = DataType.DateTimeType;
            break;
        case "number":
            this.name = getNameForNonComplexField(name, parentHierarchy, field, useFlatNameForLeafNodes);
            this.type = DataType.DoubleType;
            break;
        case "boolean":
            this.name = getNameForNonComplexField(name, parentHierarchy, field, useFlatNameForLeafNodes);
            this.type = DataType.BooleanType;
            break;
        case "object":
            this.name = name;
            this.type = DataType.Field_ObjectType;
            HashMap<?, ?> props = (HashMap<?, ?>)field.get("properties");
            List<SchemaField> schemaSubFields = new ArrayList<>();
            props.forEach((key, value) -> {
                JSONObject subFieldJson = (JSONObject) value;
                String currentHeirachy = getNewHeirarchy(this.name,
                        parentHierarchy);
                SchemaField subSchemaField = new SchemaField((String)key, subFieldJson,
                        currentHeirachy, useFlatNameForLeafNodes, FieldsFrom.OBSERVABLE_SCHEMA);
                schemaSubFields.add(subSchemaField);
            });
            this.subFields = schemaSubFields;
            break;
        case "array":
            this.type = DataType.Field_ArrayType;
            this.name = name;
            JSONObject items = (JSONObject) field
                    .get(SDKConstants.ITEMS);
            if (items != null) {
                props = (HashMap<?, ?>)items.get("properties");
                List<SchemaField> schemaSubFieldsArray = new ArrayList<>();
                if (props != null) {
                    this.arraySubType = DataType.Field_ObjectType;
                    props.forEach((key, value) -> {
                        JSONObject subFieldJson = (JSONObject) value;
                        String currentHeirachy = getNewHeirarchy(this.name,
                                parentHierarchy);
                        SchemaField subSchemaField = new SchemaField((String)key, subFieldJson,
                                currentHeirachy, useFlatNameForLeafNodes, FieldsFrom.OBSERVABLE_SCHEMA);
                        schemaSubFieldsArray.add(subSchemaField);
                    });
                    this.subFields = schemaSubFieldsArray;
                } else {
                    String typeOfSubType = (String) items.get(SDKConstants.TYPE);
                    this.arraySubType = DataType.StringType;
                    if(useFlatNameForLeafNodes)
                        this.name = getNameForNonComplexField(name, parentHierarchy, field, useFlatNameForLeafNodes);
                }
                break;
            }
        default:
            this.name = getNameForNonComplexField(name, parentHierarchy, field, useFlatNameForLeafNodes);
            this.type = DataType.StringType;
    }

    this.dule = new Dule(JsonUtil.getJsonObject(field,
            SDKConstants.CATALOG_DULE));
    }


    private void getSchemaField(JSONObject field, String parentHierarchy, boolean useFlatNameForLeafNodes) {
        String type = (String) field.get(SDKConstants.CATALOG_SCHEMA_TYPE);

        switch (type) {
            case "string":
                this.name = getNameForNonComplexField(parentHierarchy, field, useFlatNameForLeafNodes);
                this.type = DataType.StringType;
                break;
            case "long":
                this.name = getNameForNonComplexField(parentHierarchy, field, useFlatNameForLeafNodes);
                this.type = DataType.LongType;
                break;
            case "date":// TODO Once catalog start giving information for date
                // format, we will start parsing date in same format
                this.name = getNameForNonComplexField(parentHierarchy, field, useFlatNameForLeafNodes);
                this.type = DataType.StringType;
                break;
            case "integer":
                this.name = getNameForNonComplexField(parentHierarchy, field, useFlatNameForLeafNodes);
                this.type = DataType.IntegerType;
                break;
            case "float":
                this.name = getNameForNonComplexField(parentHierarchy, field, useFlatNameForLeafNodes);
                this.type = DataType.FloatType;
                break;
            case "double":
                this.name = getNameForNonComplexField(parentHierarchy, field, useFlatNameForLeafNodes);
                this.type = DataType.DoubleType;
                break;
            case "boolean":
                this.name = getNameForNonComplexField(parentHierarchy, field, useFlatNameForLeafNodes);
                this.type = DataType.BooleanType;
                break;
            case "object":
                this.name = (String) field.get(SDKConstants.CATALOG_NAME);
                this.type = DataType.Field_ObjectType;
                JSONArray subFields = (JSONArray) field
                        .get(SDKConstants.SUB_FIELDS);
                List<SchemaField> schemaSubFields = new ArrayList<>();
                for (int i = 0; i < subFields.size(); i++) {
                    JSONObject subFieldJson = (JSONObject) subFields.get(i);
                    String currentHeirachy = getNewHeirarchy(this.name,
                            parentHierarchy);
                    SchemaField subSchemaField = new SchemaField(subFieldJson,
                            currentHeirachy, useFlatNameForLeafNodes);
                    schemaSubFields.add(subSchemaField);
                }
                this.subFields = schemaSubFields;
                break;
            case "array":
                this.type = DataType.Field_ArrayType;
                this.name = (String) field.get(SDKConstants.CATALOG_NAME);
                JSONObject subType = (JSONObject) field
                        .get(SDKConstants.SUB_TYPE);
                if (subType != null) {
                    JSONArray subFieldsArray = (JSONArray) subType
                            .get(SDKConstants.SUB_FIELDS);
                    List<SchemaField> schemaSubFieldsArray = new ArrayList<>();
                    if (subFieldsArray != null) {
                        this.arraySubType = DataType.Field_ObjectType;
                        for (int i = 0; i < subFieldsArray.size(); i++) {
                            JSONObject subFieldJson = (JSONObject) subFieldsArray
                                    .get(i);
                            String currentHeirachy = getNewHeirarchy(this.name,
                                    parentHierarchy);
                            SchemaField subSchemaField = new SchemaField(
                                    subFieldJson, currentHeirachy, useFlatNameForLeafNodes);
                            schemaSubFieldsArray.add(subSchemaField);
                        }
                        this.subFields = schemaSubFieldsArray;
                    } else {// This condition arises when there is no tag
                        // *subFields* exists in array type objects OR
                        // the sub type is a primitive type.
                        String typeOfSubType = (String) subType.get(SDKConstants.TYPE);
                        // TODO: add if-else based on type of sub-type.
                        this.arraySubType = DataType.StringType;
                        if(useFlatNameForLeafNodes)
                            this.name = getNameForNonComplexField(parentHierarchy, field, useFlatNameForLeafNodes);
                    }
                    break;
                }
            default:
                this.name = getNameForNonComplexField(parentHierarchy, field, useFlatNameForLeafNodes);
                this.type = DataType.StringType;
        }

        this.dule = new Dule(JsonUtil.getJsonObject(field,
                SDKConstants.CATALOG_DULE));
    }

    private String getNewHeirarchy(String name, String parentHeirarchy) {
        if (parentHeirarchy.equals("")) {
            return name;
        } else {
            return parentHeirarchy + SDKConstants.FIELDS_DELIM + name;
        }
    }

    private String getNameForNonComplexField(String name, String parentHeirarchy,
                                             JSONObject field, boolean flattenNamesForPrimitiveTypes) {
        String fieldName = name;
        if(name == null || name.length() == 0) {
            fieldName = (String) field.get(SDKConstants.CATALOG_NAME);
        }
        if (fieldName != null) {
            if (parentHeirarchy != "" && flattenNamesForPrimitiveTypes) {
                fieldName = parentHeirarchy + SDKConstants.FIELDS_DELIM
                        + fieldName;
            }
        } else {
            fieldName = parentHeirarchy;// In case *name* tag doesn't exist,
            // we are calling field name with
            // the name of parent
        }
        return fieldName;
    }

    private String getNameForNonComplexField(String parentHeirarchy,
            JSONObject field, boolean flattenNamesForPrimitiveTypes) {
        return getNameForNonComplexField(null, parentHeirarchy, field, flattenNamesForPrimitiveTypes);
    }

    public String getName() {
        return name;
    }

    public DataType getType() {
        return type;
    }

    public List<SchemaField> getSubFields() {
        return subFields;
    }

    public Dule getDule() {
        return dule;
    }

    public boolean getIsPrimitive() {
       if(type != DataType.Field_ObjectType && type != DataType.Field_ArrayType) {
           return true;
       } else
           return false;
    }

    public DataType getArraySubType() {
        return arraySubType;
    }
}

