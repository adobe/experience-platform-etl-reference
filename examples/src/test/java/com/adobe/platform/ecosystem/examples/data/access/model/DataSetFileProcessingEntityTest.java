
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
 * Created by vedhera on 10/10/2017.
 */

public class DataSetFileProcessingEntityTest extends BaseTest {
    DataSetFileProcessingEntity dsfpe = null;

    @Before
    public void setupProcessingEntities() throws ParseException {
        dsfpe = getDataSetFileProcessingEntityFromString(dataAccessFilesResponse);
    }

    @Test
    public void testGetHref() throws ParseException {
        assertTrue(dsfpe.getHref().equals("href1"));
    }

    @Test
    public void testGetLength() throws ParseException {
        assertTrue(dsfpe.getLength() == 204);
    }

    @Test
    public void testGetName() throws ParseException {
        assertTrue(dsfpe.getName().equals("sql.csv"));
    }

    private DataSetFileProcessingEntity getDataSetFileProcessingEntityFromString(String processingEntitiesJsonString) throws ParseException {
        JSONParser jsonParser = new JSONParser();
        JSONObject jObject = (JSONObject)(jsonParser.parse(processingEntitiesJsonString));
        JSONArray jArray = (JSONArray) jObject.get("data");
        return new DataSetFileProcessingEntity((JSONObject) jArray.get(0));
    }
}