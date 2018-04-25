
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

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.json.simple.parser.ParseException;
import org.junit.Before;
import org.junit.Test;

import com.adobe.platform.connector.ut.BaseTest;

public class DataSetTest extends BaseTest{

    DataSet ds = null;
    DataSet ds_Sample1 = null;

    String datasetInnerSample1 = "{\"name\":\"dsvName\","
            + "\"viewId\":\"dsvId\",\"connectionId\":\"conId\",\"dule\":"
            + "{\"loginState\":[\"Identified\"],\"specialTypes\":[\"S1\",\"S2\"]},"
            + "\"fields\":[{\"name\":\"id1\",\"type\":\"string\"},"
            + "{\"name\":\"createtime\",\"type\":\"long\"},"
            + "{\"name\":\"_date\",\"type\":\"date\"},"
            + "{\"name\":\"abc_xyz\",\"type\":\"string\"},"
            + "{\"name\":\"Name\",\"type\":\"object\","
            + "\"subFields\":["
            + "{\"name\":\"firstName\",\"type\":\"string\"},"
            + "{\"name\":\"_yahoo\",\"type\":\"string\"},"
            + "{\"name\":\"abc_\",\"type\":\"object\","
            + "\"subFields\":["
            + "{\"name\":\"superb\",\"type\":\"string\"}"
            + "]}"
            + "]}],"
            + "\"basePath\":\"adl://datalake/datalake\","
            + "\"fileDescription\":{\"persisted\":false,\"format\":\"csv\",\"delimiters\":[\",\"]}}";

    @Before
    public void setupDataSet() throws ParseException {
        ds = getDataSetFromString(datasetInnerSample);
        ds_Sample1 = getDataSetFromString(datasetInnerSample1);
    }

    @Test
    public void testGetConnectionId() throws ParseException {
        assertTrue(ds.getConnectionId().equalsIgnoreCase("conId"));
    }

    @Test
    public void testGetFields() throws ParseException {
        assertTrue(ds.getFields(false).size() == 4);
    }

    @Test
    public void testGetFlattenedSchemaFields() throws ParseException {
        assertTrue(ds.getFlattenedSchemaFields().size() == 4);
    }

    @Test
    public void testMatchFlattenedSchemaFields() throws ParseException {
        Map<String,String> sdkFields = new HashMap<>();
        sdkFields.put("Name_firstName", "");
        sdkFields.put("_date", "");
        sdkFields.put("Name_abc__superb", "");
        sdkFields.put("abc_xyz", "");
        ds_Sample1.matchFlattenedSchemaFields(sdkFields);
        assertTrue(sdkFields.get("_date").equals("_date"));
        assertTrue(sdkFields.get("Name_firstName").equals("Name.firstName"));
        assertTrue(sdkFields.get("Name_abc__superb").equals("Name.abc_.superb"));
        assertTrue(sdkFields.get("abc_xyz").equals("abc_xyz"));
    }

    @Test
    public void testGetDataSetViewId() throws ParseException {
        assertTrue("dsvId".equals(ds.getViewId()));
    }

    @Test
    public void testGetDataSetViewFieldName() throws ParseException {
        assertTrue("id1".equals(ds.getFlattenedSchemaFields().get(0).getName()));
    }

    @Test
    public void testGetDataSetViewFieldType() throws ParseException {
        assertTrue(ds.getFlattenedSchemaFields().get(0).getType() == DataType.StringType);
    }

    @Test
    public void testGetIsDuleEnabled() throws ParseException {
        assertTrue(ds.getIsDuleEnabled() == true);
    }

    @Test
    public void testGetFieldDescription() {
        assertNotNull(ds.getFileDescription());
        assertNotNull(ds.getFileDescription().getDelimiter());
        assertNotNull (ds.getFileDescription().getFormat());
        assertNotNull (ds.getFileDescription().isPersisted());
        assert (ds.getFileDescription().getDelimiter() == ',');
    }
}