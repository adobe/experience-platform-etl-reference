
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
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.simple.JSONObject;

import com.adobe.platform.ecosystem.examples.catalog.model.DataSet.FieldsFrom;
import com.adobe.platform.ecosystem.examples.constants.SDKConstants;

/**
 * Represent Schema entity in XDM Registry.
 *
 * @author vedhera on 7/20/2018.
 */
public class Schema extends BaseModel {

    private List<SchemaField> fieldsList = null;

    private static final Logger logger = Logger.getLogger(Schema.class
            .getName());

    private JSONObject rawSchema = null;

    public Schema() {
        super();
    }

    public Schema(JSONObject schemaObj) {
        super(schemaObj);
        this.rawSchema = schemaObj;
    }

    public List<SchemaField> getSchemaFields(boolean useFlatNamesForLeafNodes) {
        if(fieldsList == null && rawSchema != null) {
            HashMap<?, ?> props = (HashMap<?, ?>)rawSchema.get(SDKConstants.PROPERTIES);
            if(props == null) {
                logger.severe("Improper Schema Object " + rawSchema);
                return null;
            }
            fieldsList = new ArrayList<>();
            if(isAdhocSchema()) {
                logger.log(Level.SEVERE, "Found Adhoc Schema, fetching properties from inside namespace.");
                props = getPropertiesObjectAdhoc(props);
            }
            props.forEach((key, value) -> {
                fieldsList.add(new SchemaField((String)key, (JSONObject) value, useFlatNamesForLeafNodes, DataSet.FieldsFrom.OBSERVABLE_SCHEMA));
            });
        }
        return fieldsList;
    }

    private HashMap<?,?> getPropertiesObjectAdhoc(HashMap<?,?> props) {
        final String namespace = (String) rawSchema.get(SDKConstants.META_NAMESPACE);
        final JSONObject properties = (JSONObject) props.get(namespace);
        return (HashMap<?, ?>)properties.get(SDKConstants.PROPERTIES);
    }

    private boolean isAdhocSchema() {
        final List<String> metaExtendsList = (List<String>)rawSchema.get(SDKConstants.META_EXTENDS);
        return (metaExtendsList != null && !metaExtendsList.isEmpty())
                ? metaExtendsList.contains(SDKConstants.ADHOC_CLASS) : false;
    }

    @Override
    public BaseModel build(JSONObject schemaObj) {
        return new Schema(schemaObj);
    }
}
