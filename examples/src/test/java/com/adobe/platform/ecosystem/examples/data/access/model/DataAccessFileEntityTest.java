
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
package com.adobe.platform.ecosystem.examples.data.access.model;

import com.adobe.platform.connector.ut.BaseTest;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Created by vedhera on 01/09/2018.
 */
public class DataAccessFileEntityTest extends BaseTest {
    DataAccessFileEntity dataAccessFileEntity = null;

    @Before
    public void setupProcessingEntities() throws ParseException {
        dataAccessFileEntity = getDASFileEntityFromString(dataAccessServiceFileEntityResponse);
    }

    @Test
    public void testGetDataSetFileId() {
        assertTrue(dataAccessFileEntity.getDataSetFileId().equals("dataSetFileId1"));
    }

    @Test
    public void testGetDataSetViewId() {
        assertTrue(dataAccessFileEntity.getDataSetViewId().equals("dataSetViewId1"));
    }

    @Test
    public void testGetDASFileHref() {
        assertTrue(dataAccessFileEntity.getDataAccessServiceHref().equals("https://platform-int.adobe.io:443/data/foundation/export/files/dataSetFileId1"));
    }

    private DataAccessFileEntity getDASFileEntityFromString(String processingEntitiesJsonString) throws ParseException {
        JSONParser jsonParser = new JSONParser();
        JSONObject jObject = (JSONObject)(jsonParser.parse(processingEntitiesJsonString));
        JSONArray jArray = (JSONArray) jObject.get("data");
        return new DataAccessFileEntity((JSONObject) jArray.get(0));
    }
}