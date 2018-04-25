
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

/**
 * Created by vedhera on 8/25/2017.
 */

import static com.adobe.platform.ecosystem.examples.constants.SDKConstants.CATALOG_FILE_DESCRIPTION_DELIMITERS_KEY;
import static com.adobe.platform.ecosystem.examples.constants.SDKConstants.CATALOG_FILE_DESCRIPTION_PERSISTED_KEY;
import static com.adobe.platform.ecosystem.examples.constants.SDKConstants.CATALOG_FORMAT;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.adobe.platform.ecosystem.examples.constants.SDKConstants;
import com.adobe.platform.ecosystem.examples.data.FileFormat;
import com.adobe.platform.ecosystem.examples.util.JsonUtil;

/**
 * Represent Dataset entity in Catalog.
 */
public class DataSet extends BaseModel {
    private static final Map<String, DataType> _keys;

    private String basePath;

    private String viewId;

    private String connectionId;

    private String description;

    private JSONObject dule;

    private JSONArray fields;

    private JSONObject fileDescription;

    private Dule duleObj = null;

    private List<SchemaField> fieldsList = null;

    private List<SchemaField> flatFieldsList = null;

    private static final Logger logger = Logger.getLogger(BaseModel.class
            .getName());

    static {
        _keys = new HashMap<String, DataType>() {
            {
                put(SDKConstants.CATALOG_BASEPATH, DataType.StringType);
                put(SDKConstants.CATALOG_DSV, DataType.StringType);
                put(SDKConstants.CATALOG_DESCRIPTION, DataType.StringType);
                put(SDKConstants.CONNECTION_ID, DataType.StringType);
                put(SDKConstants.CATALOG_FIELDS, DataType.JsonArrayType);
                put(SDKConstants.CATALOG_DULE, DataType.Field_ObjectType);
                put(SDKConstants.CATALOG_FILE_DESCRIPTION,
                        DataType.Field_ObjectType);
            }
        };
    }

    public DataSet() {
        super();
    }

    public DataSet(JSONObject jsonObject) {
        super(jsonObject);
        Field[] fields = DataSet.class.getDeclaredFields();
        super.populateFields(fields, this._keys, this, jsonObject);
    }

    public String getBasePath() {
        return basePath;
    }

    public String getViewId() {
        return viewId;
    }

    public String getConnectionId() {
        return connectionId;
    }

    public String getDescription() {
        return description;
    }

    public Dule getDule() {
        if (duleObj == null) {
            duleObj = new Dule(dule);
        }

        return duleObj;
    }

    /**
     * Public getter to return list of schema
     * fields as represented in catalog.
     * Parameter <code>useFlatNamesForLeafNodes</code>
     * determines to use fully qualified name
     * representation from root to leaf path in the
     * name of a primitive field.
     *
     * <p>
     *     For eg:
     *     {
     *         "name": "birthData",
     *         "type": "object",
     *         "subFields": [
     *             {
     *                 "name": "birthDay",
     *                 "type": "string"
     *             },
     *             {
     *                 "name": "birthMonth",
     *                 "type": "string"
     *             },
     *             {
     *                 "name": "birthYear",
     *                 "type": "string"
     *             }
     *         ]
     *     }
     *
     *     Using <code>getFields(true)</code>
     *     will give 'birthDate.birthYear' as the name for
     *     the primitive type birthYear.
     * </p>
     *
     * @param useFlatNamesForLeafNodes
     * @return List of {@link SchemaField}
     */
    public List<SchemaField> getFields(boolean useFlatNamesForLeafNodes) {
        fieldsList = new ArrayList<>();
        for (int i = 0; i < this.fields.size(); i++) {
            Object field = this.fields.get(i);
            if (field instanceof JSONObject) {
                fieldsList.add(new SchemaField((JSONObject) field, useFlatNamesForLeafNodes));
            }
        }
        return fieldsList;
    }

    public List<SchemaField> getFlattenedSchemaFields() {
        if (flatFieldsList != null) {
            return flatFieldsList;
        } else {
            flatFieldsList = recursivelyPopulateSchemaField(getFields(true));
             return flatFieldsList;
        }
    }

    public void matchFlattenedSchemaFields(Map<String, String> sdkFields) {
        List<SchemaField> sfs = getFlattenedSchemaFields();
        for(SchemaField sf : sfs) {
            for(Object sdkField : sdkFields.keySet().toArray()) {
                if(sf.getName().replace(".", "_").equals(sdkField.toString())) {
                    sdkFields.put(sdkField.toString(), sf.getName());
                    logger.log(Level.FINE, "Modified field match found " + sdkField.toString() + " " + sf.getName());
                    break;
                }
            }
        }
        logger.log(Level.FINE, "Modified fields populated ");
    }

    public Boolean getIsDuleEnabled() {
        return getDule().isEnabled();
    }

    public FileDescription getFileDescription() {
        return new FileDescription(fileDescription);
    }

    private List<SchemaField> recursivelyPopulateSchemaField(
            List<SchemaField> fields) {
        ArrayList<SchemaField> flatFields = new ArrayList<SchemaField>();
        for (SchemaField field : fields) {
            if ((field.type != DataType.Field_ObjectType    //Checking if not object and not array
                    && field.type != DataType.Field_ArrayType)
                || (field.type == DataType.Field_ArrayType    //Checking if array but without subfields, hence primitive.
                    && field.getSubFields() == null)) {
                flatFields.add(field);
            } else {
                for (SchemaField subField : field.getSubFields()) {
                    if (subField.getSubFields() != null) {
                        List<SchemaField> schemaFieldsFromSubFields = recursivelyPopulateSchemaField(subField
                                .getSubFields());
                        flatFields.addAll(schemaFieldsFromSubFields);
                    } else { // Sub fields will be null in case of a non-complex
                        // field.
                        flatFields.add(subField);
                    }

                }
            }
        }
        return flatFields;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataSet build(JSONObject obj) {
        return new DataSet(obj);
    }

    public class SchemaField {
        private String name;

        private DataType type;

        private DataType arraySubType; // Will be set only when SchemaField.type is a DataType.Field_ArrayType.

        private List<SchemaField> subFields = null;

        private Dule dule;

        public SchemaField(JSONObject field, boolean useFlatNamesForLeafNodes) {
            getSchemaField(field, "", useFlatNamesForLeafNodes);
        }

        public SchemaField(JSONObject subFieldJson, String currentHierarchy, boolean useFlatNamesForLeafNodes) {
            getSchemaField(subFieldJson, currentHierarchy, useFlatNamesForLeafNodes);
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

        private String getNameForNonComplexField(String parentHeirarchy,
                                                 JSONObject field, boolean flattenNamesForPrimitiveTypes) {
            String fieldName = (String) field.get(SDKConstants.CATALOG_NAME);
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

    public class FileDescription {
        private boolean persisted;

        private FileFormat format;

        private char delimiter;

        private static final char DEFAULT_DELIMITER = ',';

        public FileDescription(JSONObject fileDescription) {
            this.persisted = JsonUtil.getBoolean(fileDescription,
                    CATALOG_FILE_DESCRIPTION_PERSISTED_KEY);
            JSONArray delimArray = JsonUtil.getJsonArray(fileDescription,
                    CATALOG_FILE_DESCRIPTION_DELIMITERS_KEY);
            if (delimArray.size() > 0) {
                this.delimiter = ((String) delimArray.get(0)).charAt(0);
            } else {
                this.delimiter = DEFAULT_DELIMITER;
            }
            String fileFormat = JsonUtil.getString(fileDescription,
                    CATALOG_FORMAT);
            switch (fileFormat) {
                case "csv":
                    this.format = FileFormat.CSV;
                    break;
                case "parquet":
                    this.format = FileFormat.PARQUET;
                    break;
                case "json":
                    this.format = FileFormat.JSON;
                    break;
                default:
                    logger.severe("File format in Catalog metadata of type "
                            + fileFormat + " for dataSetId: "
                            + DataSet.this.getId());
                    break;
            }
        }

        public boolean isPersisted() {
            return persisted;
        }

        public FileFormat getFormat() {
            return format;
        }

        public char getDelimiter() {
            return delimiter;
        }
    }
}