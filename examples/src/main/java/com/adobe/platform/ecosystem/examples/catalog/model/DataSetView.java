
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
import org.json.simple.JSONObject;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataSetView extends DataSet {
    private static final Map<String, DataType> _keys;

    private Boolean isLookup;

    static {
        _keys = new HashMap<String, DataType>() {{
            put(SDKConstants.CATALOG_ISLOOKUP, DataType.BooleanType);
        }};
    }

    public DataSetView() {
        super();
    }

    public DataSetView(JSONObject jsonObject) {
        super(jsonObject);
        Field[] fields = DataSetView.class.getDeclaredFields();
        super.populateFields(fields,this._keys,this,jsonObject);
    }

    public Boolean getIsLookup() {
        return isLookup;
    }

    public Boolean getIsDuleEnabled(){
        Boolean _isDuleEnabled = false;
        List<SchemaField> fields = getFields(false);

        for(int i=0; i< fields.size(); i++) {
            _isDuleEnabled = _isDuleEnabled || fields.get(i).getDule().isEnabled();
        }

        for(int i=0; i<fields.size(); i++) {
            _isDuleEnabled = _isDuleEnabled || getFields(false).get(i).getDule().isEnabled();
        }
        return _isDuleEnabled;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataSetView build(JSONObject obj) {
        return new DataSetView(obj);
    }
}