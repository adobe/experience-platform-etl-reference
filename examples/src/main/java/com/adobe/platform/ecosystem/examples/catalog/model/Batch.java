
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
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class Batch extends BaseModel {
    private static final Map<String, DataType> _keys;

    private String status;
    private JSONArray relatedObjects;

    static {
        _keys = new HashMap<String, DataType>() {{
            put(SDKConstants.CATALOG_STATUS, DataType.StringType);
            put(SDKConstants.CATALOG_BATCH_RELATEDOBJECTS, DataType.JsonArrayType);
        }};
    }

    public Batch() {
        super();
    }

    public Batch(JSONObject jsonObject) {
        super(jsonObject);
        Field[] fields = Batch.class.getDeclaredFields();
        super.populateFields(fields,this._keys,this,jsonObject);
    }

    public String getStatus() {
        return status;
    }

    public JSONArray getRelatedObjects() {
        return relatedObjects;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Batch build(JSONObject obj) {
        return new Batch(obj);
    }
}