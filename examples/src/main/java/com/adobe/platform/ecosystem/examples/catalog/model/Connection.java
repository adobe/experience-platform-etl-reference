
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
 * Created by prigarg on 9/22/2017.
 */

import com.adobe.platform.ecosystem.examples.constants.SDKConstants;
import org.json.simple.JSONObject;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

public class Connection extends BaseModel {
    private static final Map<String, DataType> _keys;

    private JSONObject dule;

    private Dule duleObj;

    static {
        _keys = new HashMap<String, DataType>() {{
            put(SDKConstants.CATALOG_DULE, DataType.Field_ObjectType);
        }};
    }

    public Connection() {
        super();
    }

    public Connection(JSONObject jsonObject) {
        super(jsonObject);
        Field[] fields = Connection.class.getDeclaredFields();
        super.populateFields(fields,this._keys,this,jsonObject);
    }

    public Dule getDule() {
        if(duleObj == null){
            duleObj = new Dule(dule);
        }

        return duleObj;
    }

    public Boolean getIsDuleEnabled() { return getDule().isEnabled();}

    /**
     * {@inheritDoc}
     */
    @Override
    public Connection build(JSONObject obj) {
        return new Connection(obj);
    }
}