
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
 * Created by prigarg on 9/20/2017.
 */

import com.adobe.platform.ecosystem.examples.constants.SDKConstants;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class Dule extends BaseModel {

    private JSONArray contracts;
    private JSONArray identifiability;
    private JSONArray loginState;
    private JSONArray specialTypes;
    private String other;

    protected static final Map<String, DataType> _keys;

    static {
        _keys = new HashMap<String, DataType>() {{
            put(SDKConstants.DULE_CONTRACTS, DataType.JsonArrayType);
            put(SDKConstants.DULE_IDENTIFIABILITY, DataType.JsonArrayType);
            put(SDKConstants.DULE_LOGIN_STATE, DataType.JsonArrayType);
            put(SDKConstants.DULE_SPECIAL_TYPES, DataType.JsonArrayType);
            put(SDKConstants.DULE_OTHER, DataType.StringType);
        }};
    }

    public Dule() {
        super();
    }

    public Dule(JSONObject jsonObject) {
        super(jsonObject);
        Field[] fields = Dule.class.getDeclaredFields();
        super.populateFields(fields,this._keys,this,jsonObject);
    }

    public Boolean isEnabled() {
        return !contracts.isEmpty() || !identifiability.isEmpty() || !loginState.isEmpty() ||
                !specialTypes.isEmpty() || !other.isEmpty();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Dule build(JSONObject obj) {
        return new Dule(obj);
    }
}