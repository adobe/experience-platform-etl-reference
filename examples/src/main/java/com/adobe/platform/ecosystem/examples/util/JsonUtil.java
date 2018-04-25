
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

/**
 * Util class to extract keys
 * from different json objects.
 */
public class JsonUtil {

    public static String getString(JSONObject jsonObject, String key) {
        if(jsonObject.containsKey(key)) {
            return (String) jsonObject.get(key);
        } else {
            return "";
        }
    }

    public static Boolean getBoolean(JSONObject jsonObject, String key) {
        if(jsonObject.containsKey(key)) {
            return (Boolean) jsonObject.get(key);
        } else {
            return false;
        }
    }

    public static long getLong(JSONObject jsonObject, String key) {
        if(jsonObject.containsKey(key)) {
            return (long) jsonObject.get(key);
        } else {
            return 0;
        }
    }

    public static int getInteger(JSONObject jsonObject, String key) {
        if(jsonObject.containsKey(key)) {
            return (int) jsonObject.get(key);
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
}