
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
package com.adobe.platform.ecosystem.examples.util;

/**
 * Created by vedhera on 8/25/2017.
 */

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.adobe.platform.ecosystem.examples.catalog.model.DataType;

/**
 * Util class to extract keys
 * from different json objects.
 */
public class SDKDataTypeJsonUtil {

    public static String getString(JSONObject jsonObject, String key) {
        if(jsonObject.containsKey(key)) {
            return (String) jsonObject.get(key);
        } else {
            return "";
        }
    }

    public static Boolean getBoolean(JSONObject jsonObject, String key) {
        if(jsonObject.containsKey(key)) {
            return Boolean.parseBoolean(jsonObject.get(key).toString());
        } else {
            return false;
        }
    }

    public static long getLong(JSONObject jsonObject, String key) {
        if(jsonObject.containsKey(key)) {
            return Long.parseLong(jsonObject.get(key).toString());
        } else {
            return 0;
        }
    }

    public static int getInteger(JSONObject jsonObject, String key) {
        if(jsonObject.containsKey(key)) {
            return Integer.parseInt(jsonObject.get(key).toString());
        } else {
            return -1;
        }
    }

    public static JSONArray getJsonArray(JSONObject jsonObject, String key) {
        if(jsonObject.containsKey(key)) {
            return (JSONArray) jsonObject.get(key);
        } else {
            return new JSONArray();
        }
    }

    public static  JSONObject getJsonObject(JSONObject jsonObject, String key) {
        if(jsonObject.containsKey(key)) {
            return (JSONObject) jsonObject.get(key);
        } else {
            return new JSONObject();
        }
    }

    public static Object getFloat(JSONObject jsonObject, String key) {
        if(jsonObject.containsKey(key)) {
            return Float.parseFloat(jsonObject.get(key).toString());
        } else {
            return -1;
        }
    }

    public static Object getDouble(JSONObject jsonObject, String key) {
        if(jsonObject.containsKey(key)) {
            return Double.parseDouble(jsonObject.get(key).toString());
        } else {
            return -1;
        }
    }

    public static Object getKeyValueFromJSONObject(JSONObject row, String key, DataType catalogDataType) {
        Object colValueObject = null;
        if(catalogDataType == DataType.StringType) {
             return colValueObject = getString(row, key);
        } else if(catalogDataType == DataType.BooleanType) {
            if(row.get(key) instanceof Integer){
                int colValue = getInteger(row, key);
                switch(colValue){
                case 1:
                    colValueObject = Boolean.TRUE;
                    break;
                default:
                    colValueObject = Boolean.FALSE;
                    break;
                }
            }
            else if(row.get(key) instanceof Boolean){
                colValueObject = getBoolean(row, key);
            }
            return colValueObject;
        } else if(catalogDataType == DataType.IntegerType) {
            return colValueObject = getInteger(row, key);
        } else if(catalogDataType == DataType.LongType) {
            return colValueObject = getLong(row, key);
        } else if(catalogDataType == DataType.FloatType) {
            return colValueObject = getFloat(row, key);
        } else if(catalogDataType == DataType.DoubleType) {
            return colValueObject = getDouble(row, key);
        } else {
            return colValueObject;
        }
    }
}