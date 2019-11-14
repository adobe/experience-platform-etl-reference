
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

import com.adobe.platform.ecosystem.examples.constants.SDKConstants;
import com.adobe.platform.ecosystem.examples.util.JsonUtil;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;


/**
 * Base class to represent models
 * comprising of common attributes from
 * Catalog.
 */
abstract public class BaseModel implements EntityBuilder {
    private String id;

    private String name;

    private String imsOrg;

    private String sandboxName;

    private long created;

    protected static final Map<String, DataType> _keys;

    private static final Logger logger = Logger.getLogger(BaseModel.class.getName());

    static {
        _keys = new HashMap<String, DataType>() {{
            put(SDKConstants.CATALOG_ID, DataType.StringType);
            put(SDKConstants.CATALOG_NAME, DataType.StringType);
            put(SDKConstants.CATALOG_IMSORG, DataType.StringType);
            put(SDKConstants.CATALOG_SANDBOX_NAME, DataType.StringType);
            put(SDKConstants.CATALOG_CREATED_KEY, DataType.LongType);
        }};
    }

    protected BaseModel(JSONObject jsonObject){
        Field[] fields = BaseModel.class.getDeclaredFields();
        populateFields(fields,this._keys,this,jsonObject);
    }

    /**
     * Empty constructor
     * required by the generic
     * method to instantiate objects using
     * <code>class.newInstance()</code>
     */
    public BaseModel() {

    }

    public String getName() {
        return name;
    }

    public String getId() {
        return id;
    }

    public String getImsOrg() {return imsOrg;}

    public String getSandboxName() {
        return sandboxName;
    }

    public long getCreated() {
        return created;
    }

    protected void populateFields(Field[] fields,
                                  Map<String,DataType> keys,
                                  Object classObject,
                                  JSONObject jsonObject) {
        for(Field f: fields) {
            String fieldName = f.getName();
            f.setAccessible(true);
            if(!fieldName.equals(SDKConstants.KEYS) && keys.get(fieldName)!=null){
                // Set field value from JSON.
                try {
                    switch (keys.get(fieldName)){
                        case StringType:
                            String sValue = JsonUtil.getString(jsonObject,fieldName);
                            f.set(classObject,sValue );
                            break;
                        case IntegerType:
                            int iValue = JsonUtil.getInteger(jsonObject,fieldName);
                            f.set(classObject,iValue);
                            break;
                        case DateType:
                            String dValue = JsonUtil.getString(jsonObject,fieldName);
                            f.set(classObject,dValue);
                            break;
                        case LongType:
                            long lValue = JsonUtil.getLong(jsonObject,fieldName);
                            f.set(classObject,lValue);
                            break;
                        case FloatType:
                            String fValue = JsonUtil.getString(jsonObject,fieldName);
                            f.set(classObject,fValue);
                            break;
                        case JsonArrayType:
                            JSONArray jaValue = JsonUtil.getJsonArray(jsonObject,fieldName);
                            f.set(classObject,jaValue);
                            break;
                        case BooleanType:
                            Boolean bool = JsonUtil.getBoolean(jsonObject,fieldName);
                            f.set(classObject,bool);
                            break;
                        case Field_ObjectType:
                            JSONObject joValue = JsonUtil.getJsonObject(jsonObject, fieldName);
                            f.set(classObject, joValue);
                            break;

                    }
                } catch (IllegalAccessException e) {
                    //TODO: please check if the exception needs to be propagated above
                    logger.severe("Error while fetching populateFields : " + e.getMessage());
                }
            }
        }
    }
}