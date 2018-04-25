
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

import static org.junit.Assert.*;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.Before;
import org.junit.Test;

public class JsonUtilTest {

    JSONObject jsonObj = new JSONObject();
    JSONArray jsonarray = new JSONArray();

    @Before
    public void before() {
        jsonObj.put("intkey", 1);
        jsonObj.put("boolkey", true);

        jsonarray.add(50);
        jsonObj.put("arraykey", jsonarray);
    }

    @Test
    public void testConstructor() {
        JsonUtil jsonUtil = new JsonUtil();
        assertTrue(jsonUtil != null);
    }

    @Test
    public void testGetInteger() {
        assertTrue(JsonUtil.getInteger(jsonObj, "intkey") == 1);
        assertTrue(JsonUtil.getInteger(jsonObj, "intkey1") == -1);
    }

    @Test
    public void testGetBoolean() {
        assertTrue(JsonUtil.getBoolean(jsonObj, "boolkey"));
        assertTrue(!JsonUtil.getBoolean(jsonObj, "boolkey1"));
    }

    @Test
    public void testJsonArray() {
        assertTrue(JsonUtil.getJsonArray(jsonObj, "arraykey").size() > 0);
        assertTrue(JsonUtil.getJsonArray(jsonObj, "arraykey1").size() == 0);
    }
}