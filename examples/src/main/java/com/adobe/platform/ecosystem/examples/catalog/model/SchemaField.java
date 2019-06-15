
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import com.adobe.platform.ecosystem.examples.catalog.function.DataTypeFunction;
import com.adobe.platform.ecosystem.examples.data.validation.api.Rule;
import com.adobe.platform.ecosystem.examples.data.validation.impl.rules.SchemaValidationRule;
import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.adobe.platform.ecosystem.examples.catalog.model.DataSet.FieldsFrom;
import com.adobe.platform.ecosystem.examples.constants.SDKConstants;
import com.adobe.platform.ecosystem.examples.util.JsonUtil;

import static com.adobe.platform.ecosystem.examples.constants.SDKConstants.CATALOG_MAP_KEY;
import static com.adobe.platform.ecosystem.examples.constants.SDKConstants.CATALOG_MAP_VALUE;

/**
 * Single place for type-mapping
 * from XDM/(old Catalog) types
 * to Connector SDK types. These
 * types get later mapped to
 * parquet types.
 *
 * Please refer for type mapping
 * between XDM -> Parquet type.
 * <href>https://wiki.corp.adobe.com/pages/viewpage.action?spaceKey=DMSArchitecture&title=XDM+Architecture#XDMArchitecture-XDMDataTypes</href>
 *
 * @author vedhera on 7/20/2018.
 */
public class SchemaField {
    private String name;

    private DataType type;

    private DataType arraySubType; // Will be set only when SchemaField.type is a DataType.Field_ArrayType.

    private List<SchemaField> subFields = null;

    private Dule dule;

    private List<Rule<?>> rules;

    public SchemaField(JSONObject field, boolean useFlatNamesForLeafNodes) {
        this(field, "", useFlatNamesForLeafNodes);
    }

    public SchemaField(JSONObject subFieldJson, String currentHierarchy, boolean useFlatNamesForLeafNodes) {
        this(null, subFieldJson, currentHierarchy, useFlatNamesForLeafNodes, DataSet.FieldsFrom.FIELDS);
    }

    public SchemaField(String name, JSONObject field, boolean useFlatNamesForLeafNodes, DataSet.FieldsFrom fieldsFrom) {
        this(name, field, "", useFlatNamesForLeafNodes, fieldsFrom);
    }

    public SchemaField(String name, JSONObject subFieldJson, String currentHierarchy, boolean useFlatNamesForLeafNodes, DataSet.FieldsFrom fieldsFrom) {
        if(DataSet.FieldsFrom.OBSERVABLE_SCHEMA == fieldsFrom) {
            getSchemaFieldFromObservableSchema(name, subFieldJson, currentHierarchy, useFlatNamesForLeafNodes);
        } else {
            getSchemaField(subFieldJson, currentHierarchy, useFlatNamesForLeafNodes);
        }
    }

    public SchemaField(String name, DataType type, List<SchemaField> schemaFields) {
        this(name, type, schemaFields, null);
    }

    public SchemaField(String name, DataType type, List<SchemaField> subFields, DataType arraySubType) {
        this.name = name;
        this.type = type;
        this.arraySubType = arraySubType;
        this.subFields = subFields;
    }

    private void getSchemaFieldFromObservableSchema(String name, JSONObject field, String parentHierarchy, boolean useFlatNameForLeafNodes) {
        String type = (String) field.get(SDKConstants.CATALOG_SCHEMA_META_XDM_TYPE);

        if(StringUtils.isEmpty(type))
            type = (String) field.get(SDKConstants.CATALOG_SCHEMA_TYPE);


        switch (type) {
            case "string":
                this.name = getNameForNonComplexField(name, parentHierarchy, field, useFlatNameForLeafNodes);
                this.type = DataType.StringType;
                break;
            case "int":
            case "integer":
                this.name = getNameForNonComplexField(name, parentHierarchy, field, useFlatNameForLeafNodes);
                this.type = DataType.IntegerType;
                break;
            case "long":
                this.name = getNameForNonComplexField(name, parentHierarchy, field, useFlatNameForLeafNodes);
                this.type = DataType.LongType;
                break;
            case "number":
                this.name = getNameForNonComplexField(name, parentHierarchy, field, useFlatNameForLeafNodes);
                this.type = DataType.DoubleType;
                break;
            case "float":
                this.name = getNameForNonComplexField(name, parentHierarchy, field, useFlatNameForLeafNodes);
                this.type = DataType.FloatType;
                break;
            case "double":
                this.name = getNameForNonComplexField(name, parentHierarchy, field, useFlatNameForLeafNodes);
                this.type = DataType.DoubleType;
                break;
            case "boolean":
                this.name = getNameForNonComplexField(name, parentHierarchy, field, useFlatNameForLeafNodes);
                this.type = DataType.BooleanType;
                break;
            case "date":
                this.name = getNameForNonComplexField(name, parentHierarchy, field, useFlatNameForLeafNodes);
                this.type = DataType.DateType;
                break;
            case "date-time":
                this.name = getNameForNonComplexField(name, parentHierarchy, field, useFlatNameForLeafNodes);
                this.type = DataType.DateTimeType;
                break;
            case "short":
                this.name = getNameForNonComplexField(name, parentHierarchy, field, useFlatNameForLeafNodes);
                this.type = DataType.ShortType;
                break;
            case "byte":
                this.name = getNameForNonComplexField(name, parentHierarchy, field, useFlatNameForLeafNodes);
                this.type = DataType.ByteType;
                break;
            case "binary":
                this.name = getNameForNonComplexField(name, parentHierarchy, field, useFlatNameForLeafNodes);
                this.type = DataType.BinaryType;
                break;
            case "map":
            case "object":
                this.name = name;
                this.type = DataType.Field_ObjectType;
                populateForMapOrObjectType(this, field, parentHierarchy, useFlatNameForLeafNodes);
                break;
            case "array":
                this.type = DataType.Field_ArrayType;
                this.name = name;
                populateForArrayType(this, field, parentHierarchy, useFlatNameForLeafNodes);
                break;
            default:
                this.name = getNameForNonComplexField(name, parentHierarchy, field, useFlatNameForLeafNodes);
                this.type = DataType.StringType;
        }

        this.dule = new Dule(JsonUtil.getJsonObject(field,
                SDKConstants.CATALOG_DULE));

        // Setup schema rules.
        setupSchemaRules(type, field);
    }

    private void populateForArrayType(SchemaField schemaField, JSONObject field, String parentHierarchy, boolean useFlatNameForLeafNodes) {
        final JSONObject items = (JSONObject) field.get(SDKConstants.ITEMS);
        if (items != null) {
            // TODO: Add support for Array of Arrays. Peek on 'type' property in 'items'.
            // TODO: Add support for Array of Maps. Peek on 'type' property in 'items'.
            HashMap<?, ?> props = (HashMap<?, ?>) items.get("properties");
            List<SchemaField> schemaSubFieldsArray = new ArrayList<>();
            if (props != null) {
                schemaField.arraySubType = DataType.Field_ObjectType;
                props.forEach((key, value) -> {
                    JSONObject subFieldJson = (JSONObject) value;
                    String currentHeirachy = getNewHeirarchy(schemaField.name, parentHierarchy);
                    SchemaField subSchemaField = new SchemaField(
                        (String) key,
                        subFieldJson,
                        currentHeirachy,
                        useFlatNameForLeafNodes,
                        DataSet.FieldsFrom.OBSERVABLE_SCHEMA
                    );
                    schemaSubFieldsArray.add(subSchemaField);
                });
                schemaField.subFields = schemaSubFieldsArray;
            } else {
                String typeOfSubType = (String) items.get(SDKConstants.TYPE);
                schemaField.arraySubType = DataTypeFunction.primitiveFunction().apply(typeOfSubType);
                if (useFlatNameForLeafNodes) {
                    schemaField.name = getNameForNonComplexField(name, parentHierarchy, field, useFlatNameForLeafNodes);
                }
            }
        }
    }

    private void populateForMapOrObjectType(final SchemaField schemaField, JSONObject field, String parentHierarchy, boolean useFlatNameForLeafNodes) {
        if (field.get("additionalProperties") == null) {
            populateForObjectType(schemaField, field, parentHierarchy, useFlatNameForLeafNodes);
        } else {
            populateForMapType(schemaField, field, parentHierarchy, useFlatNameForLeafNodes);
        }
    }

    private void populateForObjectType(SchemaField schemaField, JSONObject field, String parentHierarchy, boolean useFlatNameForLeafNodes) {
        HashMap<?, ?> props = (HashMap<?, ?>) field.get("properties");
        List<SchemaField> schemaSubFields = new ArrayList<>();
        props.forEach((key, value) -> {
            JSONObject subFieldJson = (JSONObject) value;
            final String currentHierarchy = getNewHeirarchy(schemaField.name, parentHierarchy);
            SchemaField subSchemaField = new SchemaField(
                (String) key,
                subFieldJson,
                currentHierarchy,
                useFlatNameForLeafNodes,
                DataSet.FieldsFrom.OBSERVABLE_SCHEMA
            );
            schemaSubFields.add(subSchemaField);
        });
        schemaField.subFields = schemaSubFields;
    }

    private void populateForMapType(SchemaField schemaField, JSONObject field, String parentHierarchy, boolean useFlatNameForLeafNodes) {
        schemaField.type = DataType.Field_MapType;

        final SchemaField mapKey = new SchemaField(
            CATALOG_MAP_KEY,
            DataType.StringType,
            null
        );

        final JSONObject additionalProps = (JSONObject) field.get("additionalProperties");
        final String valueTypeStr = (String) additionalProps.get("type");

        SchemaField mapValue;

        // TODO: Check for hierarchy for primitive types with 'useFlatNameForLeafNodes' == true!

        if (valueTypeStr.equalsIgnoreCase("object")) { // Map<String, Object>
            mapValue = new SchemaField(
                CATALOG_MAP_VALUE,
                DataType.Field_ObjectType,
                null
            );

            // This will populate subFields for 'object' type values.
            populateForMapOrObjectType(
                mapValue,
                additionalProps,
                parentHierarchy,
                useFlatNameForLeafNodes
            );

        } else if (valueTypeStr.equalsIgnoreCase("array")) { // Map<String, Array<...>>
            mapValue = new SchemaField(
                CATALOG_MAP_VALUE,
                DataType.Field_ArrayType,
                null
            );

            // This will populate subFields for 'array' type values.
            populateForArrayType(
                mapValue,
                additionalProps,
                parentHierarchy,
                useFlatNameForLeafNodes
            );
        } else { // Map<String, Primitive>
            mapValue = new SchemaField(
                CATALOG_MAP_VALUE,
                DataTypeFunction.primitiveFunction().apply(valueTypeStr),
                null
            );
        }

        schemaField.subFields = Arrays.asList(mapKey, mapValue);
    }

    @SuppressWarnings("unchecked")
    private void setupSchemaRules(String type, JSONObject field) {
        if(type.equalsIgnoreCase("array") ||
            type.equalsIgnoreCase("object")) {
            return;
        }
        // Adding Validation rule for field.
        final Rule<?> rule = SchemaValidationRule
            .fromJsonBuilder()
            .with(field)
            .build();
        if(rule != null) {
            this.rules = new ArrayList<>();
            this.rules.add(rule);
        }
    }

    private void getSchemaField(JSONObject field, String parentHierarchy, boolean useFlatNameForLeafNodes) {
        String type = (String) field.get(SDKConstants.CATALOG_SCHEMA_META_XDM_TYPE);

        if(StringUtils.isEmpty(type))
            type = (String) field.get(SDKConstants.CATALOG_SCHEMA_TYPE);

        switch (type) {
            case "string":
                this.name = getNameForNonComplexField(parentHierarchy, field, useFlatNameForLeafNodes);
                this.type = DataType.StringType;
                break;
            case "long":
                this.name = getNameForNonComplexField(parentHierarchy, field, useFlatNameForLeafNodes);
                this.type = DataType.LongType;
                break;
            case "date":
                // format, we will start parsing date in same format
                this.name = getNameForNonComplexField(parentHierarchy, field, useFlatNameForLeafNodes);
                this.type = DataType.StringType;
                break;
            case "date-time":
                this.name = getNameForNonComplexField(parentHierarchy, field, useFlatNameForLeafNodes);
                this.type = DataType.DateTimeType;
                break;
            case "integer":
            case "int":
                this.name = getNameForNonComplexField(parentHierarchy, field, useFlatNameForLeafNodes);
                this.type = DataType.IntegerType;
                break;
            case "byte":
                this.name = getNameForNonComplexField(parentHierarchy, field, useFlatNameForLeafNodes);
                this.type = DataType.ByteType;
                break;
            case "short" :
                this.name = getNameForNonComplexField(parentHierarchy, field, useFlatNameForLeafNodes);
                this.type = DataType.ShortType;
                break;
            case "number":
                this.name = getNameForNonComplexField(parentHierarchy, field, useFlatNameForLeafNodes);
                this.type = DataType.DoubleType;
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
            case "binary":
                this.name = getNameForNonComplexField(parentHierarchy, field, useFlatNameForLeafNodes);
                this.type = DataType.BinaryType;
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

        // Setup schema rules.
        setupSchemaRules(type, field);
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
            // Below should NEVER happen!
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
        return type != DataType.Field_ObjectType
            && type != DataType.Field_ArrayType
            && type != DataType.Field_MapType;
    }

    public DataType getArraySubType() {
        return arraySubType;
    }

    public List<Rule<?>> getRules() {
        return rules;
    }
}

