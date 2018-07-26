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
package com.adobe.platform.ecosystem.examples.data.write.mapper;

import com.adobe.platform.ecosystem.examples.catalog.model.SDKField;
import org.json.simple.JSONObject;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;

/**
 * @author vedhera on 7/19/2018.
 */
public class MapperUtilTest {


    @Test
    public void getJsonObjectTest() {

        List<SDKField> sdkFields = new ArrayList<SDKField>();
        SDKField field01 = new SDKField("a.b.c", "string");
        SDKField field02 = new SDKField("a.b.d", "string");
        SDKField field05 = new SDKField("a.e.c.d", "string");
        SDKField field03 = new SDKField("e.f", "string");
        SDKField field04 = new SDKField("e.g", "string");

        sdkFields.add(field01);
        sdkFields.add(field02);
        sdkFields.add(field05);
        sdkFields.add(field03);
        sdkFields.add(field04);

        List<List<Object>> dataTable = new ArrayList<List<Object>>();

        ArrayList<Object> record01 = new ArrayList<Object>();

        record01.add("val01");
        record01.add("val02");
        record01.add("val05");
        record01.add("val03");
        record01.add("val04");
        dataTable.add(record01);

        List<JSONObject> convertedData = MapperUtil.convert(sdkFields, dataTable);

        assertNotNull(convertedData);
        assertEquals(convertedData.size(), 1);
        assertEquals(convertedData.get(0).keySet().size(), 2);
        assertTrue(convertedData.get(0).get("a") instanceof JSONObject);
        assertTrue(convertedData.get(0).get("e") instanceof JSONObject);

        JSONObject hierarchy1 = (JSONObject) convertedData.get(0).get("a");
        JSONObject hierarchy2 = (JSONObject) convertedData.get(0).get("e");

        assertEquals(hierarchy1.keySet().size(), 2);
        assertEquals(hierarchy2.keySet().size(), 2);

        assertTrue(hierarchy1.get("b") instanceof JSONObject);
        assertTrue(hierarchy1.get("e") instanceof JSONObject);

        JSONObject hierarchy1_1 = (JSONObject) hierarchy1.get("b");
        JSONObject hierarchy1_2 = (JSONObject) hierarchy1.get("e");
        assertEquals(hierarchy1_1.keySet().size(), 2);
        assertTrue(hierarchy1_1.get("c") instanceof String);
        assertTrue(hierarchy1_1.get("d") instanceof String);
        assertEquals(hierarchy1_1.get("c"), "val01");
        assertEquals(hierarchy1_1.get("d"), "val02");

        assertEquals(hierarchy1_2.keySet().size(), 1);


        assertTrue(hierarchy2.get("f") instanceof String);
        assertEquals(hierarchy2.get("f"), "val03");
        assertTrue(hierarchy2.get("g") instanceof String);
        assertEquals(hierarchy2.get("g"), "val04");
    }
}
