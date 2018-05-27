
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
package com.adobe.platform.ecosystem.examples.data.ingestion.impl;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.json.simple.JSONObject;
import org.junit.Before;
import org.junit.Test;

import com.adobe.platform.ecosystem.examples.data.FileFormat;

import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;
import com.adobe.platform.ecosystem.ut.BaseTest;

/**
 * Created by vardgupt on 10/10/2017.
 */

public class DataIngestionServiceTest extends BaseTest {

    private DataIngestionServiceImpl dis = new DataIngestionServiceImpl("testEndpoint",httpClient);

    private JSONObject payload;

    public DataIngestionServiceTest() throws ConnectorSDKException {
    }

    @Before
    public void setupDataWiring() throws Exception {
       setUp();
       payload = new JSONObject();
       payload.put("dataSetId",  "testDataSetId");
    }

    @Test
    public void testConstructorWithOneArguement() {
        DataIngestionServiceImpl ds = null;
        try {
            ds = new DataIngestionServiceImpl("testEndpoint");
        } catch (ConnectorSDKException e) {
            assertTrue(false);
        }
        assertTrue(ds != null);
    }

    @Test
    public void testConstructorWithTwoArguement() {
        assertTrue(dis != null);
    }

    @Test(expected = ConnectorSDKException.class)
    public void testGetBatchId() throws Exception {
        setupTestForHttpOutput(batchSample);
        dis.getBatchId("testIMSOrg","testAccessToken", payload);
    }

    @Test
    public void testWriteToBatch() throws ConnectorSDKException {
        String bufferString = "testData";
        try{
            setupTestForHttpOutput(batchSample);
            dis.writeToBatch("testBatchId", "testDataSetId", "testIMSOrg", "testAccessToken", FileFormat.PARQUET, bufferString.getBytes());
        }
        catch(Exception e){
            assertTrue(true);
        }
    }

    @Test
    public void testSignalBatchCompletion() throws ConnectorSDKException {
        try{
            setupTestForHttpOutput(batchSample);
            dis.signalBatchCompletion("testBatchId", "testIMSOrg", "testAccessToken");
        }
        catch(Exception e){
            assertFalse(true);
        }
    }

}