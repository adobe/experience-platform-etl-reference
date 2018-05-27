
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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.adobe.platform.ecosystem.examples.catalog.api.CatalogService;
import com.adobe.platform.ecosystem.examples.catalog.impl.CatalogFactory;
import com.adobe.platform.ecosystem.examples.constants.SDKConstants;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKUtil;

/**
 * Represent Dataset entity in Catalog.
 */
public class DataSet extends BaseModel {
    private static final Map<String, DataType> _keys;

    private String basePath;

    private String viewId;

    private String connectionId;

    private String description;

    private String schema;

    private JSONObject dule;

    private JSONArray fields;

    private JSONObject observableSchema;

    private JSONObject fileDescription;

    private Dule duleObj = null;

    private List<SchemaField> fieldsList = null;

    private List<SchemaField> flatFieldsList = null;

    private static final Logger logger = Logger.getLogger(BaseModel.class
            .getName());

    public static enum FieldsFrom {
        FIELDS,
        OBSERVABLE_SCHEMA,
        SCHEMA
    }

    static {
        _keys = new HashMap<String, DataType>() {
            {
                put(SDKConstants.CATALOG_BASEPATH, DataType.StringType);
                put(SDKConstants.CATALOG_DSV, DataType.StringType);
                put(SDKConstants.CATALOG_DESCRIPTION, DataType.StringType);
                put(SDKConstants.CONNECTION_ID, DataType.StringType);
                put(SDKConstants.CATALOG_SCHEMA, DataType.StringType);
                put(SDKConstants.CATALOG_FIELDS, DataType.JsonArrayType);
                put(SDKConstants.CATALOG_OBSERVABLE_SCHEMA, DataType.Field_ObjectType);
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
        super.populateFields(fields, _keys, this, jsonObject);
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
        return getFields(useFlatNamesForLeafNodes,FieldsFrom.FIELDS);
    }

    public List<SchemaField> getFields(boolean useFlatNamesForLeafNodes, FieldsFrom fieldsFrom) {
        if(fieldsFrom == FieldsFrom.FIELDS) {
            fieldsList = new ArrayList<>();
            for (int i = 0; i < this.fields.size(); i++) {
                Object field = this.fields.get(i);
                if (field instanceof JSONObject) {
                    fieldsList.add(new SchemaField((JSONObject) field, useFlatNamesForLeafNodes));
                }
            }
        } else if(fieldsFrom == FieldsFrom.OBSERVABLE_SCHEMA) {
            if(this.observableSchema.get(SDKConstants.TYPE) != null) {
                Schema schema = new Schema(this.observableSchema);
                fieldsList = schema.getSchemaFields(useFlatNamesForLeafNodes);
            } else {
                logger.warning("Observable Schema not defined for dataset id " + this.getId());
            }
        } else if(fieldsFrom == FieldsFrom.SCHEMA) {
            if(this.schema != null && this.schema.length() > 0) {
                try {
                    CatalogService cs = CatalogFactory.getCatalogService();
                    fieldsList = cs.getSchemaFields(getImsOrg(), 
                            ConnectorSDKUtil.getInstance().getAccessToken(), 
                            this.schema.replace("@", ""), useFlatNamesForLeafNodes);
                } catch (ConnectorSDKException e) {
                    logger.severe("Failed in getting catalog service while listing fields for dataset id " + this.getId());
                }
            } else {
                logger.warning("Schema path not defined for " + this.getId());
            }
        }
        return fieldsList;
    }

    public List<SchemaField> getFlattenedSchemaFields() {
        return getFlattenedSchemaFields(FieldsFrom.FIELDS);
    }

    public List<SchemaField> getFlattenedSchemaFields(FieldsFrom fieldsFrom) {
        if (flatFieldsList != null) {
            return flatFieldsList;
        } else {
            flatFieldsList = recursivelyPopulateSchemaField(getFields(true, fieldsFrom));
             return flatFieldsList;
        }
    }

    public void matchFlattenedSchemaFields(Map<String, String> sdkFields) {
        matchFlattenedSchemaFields(sdkFields, FieldsFrom.FIELDS);
    }

    public void matchFlattenedSchemaFields(Map<String, String> sdkFields, FieldsFrom fieldsFrom) {
        List<SchemaField> sfs = getFlattenedSchemaFields(fieldsFrom);
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
        try {
            return new FileDescription(fileDescription);
        } catch (ConnectorSDKException csdke) {
            logger.severe("Error getting file description. Dataset id " + this.getId());
        }
        return null;
    }

    private List<SchemaField> recursivelyPopulateSchemaField(
            List<SchemaField> fields) {
        if(fields == null) {
            return null;
        }
        ArrayList<SchemaField> flatFields = new ArrayList<SchemaField>();
        for (SchemaField field : fields) {
            if ((field.getType() != DataType.Field_ObjectType    //Checking if not object and not array
                    && field.getType() != DataType.Field_ArrayType)
                || (field.getType() == DataType.Field_ArrayType    //Checking if array but without subfields, hence primitive.
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

}