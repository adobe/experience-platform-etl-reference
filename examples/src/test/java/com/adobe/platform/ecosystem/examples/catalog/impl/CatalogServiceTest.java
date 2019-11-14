
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
package com.adobe.platform.ecosystem.examples.catalog.impl;

import com.adobe.platform.ecosystem.examples.catalog.model.*;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKUtil;
import com.adobe.platform.ecosystem.examples.util.ResourceName;
import com.adobe.platform.ecosystem.ut.BaseTest;
import org.apache.http.client.methods.HttpGet;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

public class CatalogServiceTest extends BaseTest{

    CatalogServiceImpl catService;

    @Test
    public void testConstructor() throws Exception{
        assertTrue(catService != null);

        CatalogServiceImpl catService = new CatalogServiceImpl(null);
        assertTrue(catService != null);


        catService = new CatalogServiceImpl(ConnectorSDKUtil.getInstance().getEndPoint(ResourceName.CATALOG));
        assertTrue(catService != null);
    }

    @Before
    public void before() throws Exception {
        setUp();
        catService = new CatalogServiceImpl(ConnectorSDKUtil.getInstance().getEndPoint(ResourceName.CATALOG), httpClient);
        setupTestForHttpOutput(datasetSample);
    }

    @Test
    public void testGetDataset() throws ConnectorSDKException, IOException {
        setupTestForHttpOutput(datasetSample);
        DataSet ds = catService.getDataSet("testOrg", "sandboxName", "testToken", "testDSId");
        assertTrue(ds.getBasePath().startsWith("adl://"));
    }

    @Test
    public void testGetDatasetException() throws ConnectorSDKException {
        try {
            catService.getDataSet(null, null, null, null);
            fail("Exception should have been thrown");
        } catch (ConnectorSDKException ce) {
            //Test passed here
        }
    }

    @Test
    public void testGetConnection() throws IOException, ConnectorSDKException {
        setupTestForHttpOutput(connectionSample);
        Connection connection = catService.getConnection("testOrg", "sandboxName", "testToken", "conId");
        assertTrue(connection!=null);
        assertTrue("conId".equals(connection.getId()));
    }

    @Test
    public void testGetSchema() throws IOException, ConnectorSDKException {
        setupTestForHttpOutput(schemaSample);
        List<SchemaField> schemaFields = catService.getSchemaFields("testOrg", "sandboxName", "testToken", "/testschema", true);
        assertTrue(schemaFields!=null);
        assertTrue(schemaFields.size() == 4);
    }

    @Test
    public void testGetConnectionException() throws ConnectorSDKException {
        try {
            catService.getConnection(null, null, null, null);
            fail("Exception should have been thrown");
        } catch (ConnectorSDKException ce) {
            //Test passed here
        }
    }

    @Test
    public void testGetDatasets() throws ConnectorSDKException, IOException {
        setupTestForHttpOutput(datasetSample);
        Map<String,String> params = new HashMap<>();
        List<DataSet> listDS = catService.getDataSets("testOrg", "sandboxName", "testToken",
                params, CatalogAPIStrategy.ONCE);
        assertTrue(listDS.size() > 0);
    }

    @Test
    public void testCreateBatch() throws ParseException, UnsupportedOperationException, IOException, ConnectorSDKException {
        JSONArray jsonArray = new JSONArray();
        jsonArray.add(0,batchResp);

        getStringAsHttpOutputStream(jsonArray.toJSONString());
        getStringAsHttpOutputStreamForGetBatchId(batchIdResp);

        try {
            catService.createBatch("testOrg", "sandboxName", "testToken", new JSONObject());
        }catch (Exception ex){
            assertTrue(ex instanceof ConnectorSDKException);
        }
    }

    @Test
    public void testCreateBatchException() throws ConnectorSDKException {
        try {
            catService.createBatch(null, null, null, null);
            fail("Exception should have been thrown");
        } catch (ConnectorSDKException ce) {
            //Test passed here
        }
    }

    @Test
    public void testGetBatchByBatchId() throws ParseException, UnsupportedOperationException, IOException, ConnectorSDKException {
        setupTestForHttpOutput(batchSample);
        getStringAsHttpOutputStream(batchSample);

        catService.getBatchByBatchId("testOrg", "sandboxName", "testToken","testBatch");
    }

    @Test
    public void testGetBatchByBatchIdException() throws ConnectorSDKException {
        try {
            catService.getBatchByBatchId(null, null, null, null);
            fail("Exception should have been thrown");
        } catch (ConnectorSDKException ce) {
            //Test passed here
        }
    }

    @Test
    public void testGetDataSetFiles() throws UnsupportedOperationException, IOException, ConnectorSDKException{
        setupTestForHttpOutput(credSample);
        getStringAsHttpOutputStream(credSample);
        Map<String,String> params = new HashMap<>();
        List<DataSetFile> listDSF = catService.getDataSetFiles("testOrg", "sandboxName", "testToken",
                params, CatalogAPIStrategy.ONCE);
        assertTrue(!listDSF.isEmpty());
    }

    @Test
    public void testGetDataSetView() throws UnsupportedOperationException, IOException, ConnectorSDKException{
        setupTestForHttpOutput(datasetSample);
        getStringAsHttpOutputStream(datasetSample);
        DataSetView dsv = catService.getDataSetView("testOrg", "sandboxName", "testToken",
                "testDSId");
        assertTrue(dsv!=null);
        assertTrue("testDSId".equals(dsv.getId()));
    }


    @Test
    public void testPollForBatchCompletionStatus() throws IOException, ConnectorSDKException {
        // Basic mocks.
        when(httpResponse.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(200);
        when(httpResponse.getEntity()).thenReturn(httpEntity);

        when(httpClient.execute(any())).thenAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            String path;
            if (args[0] instanceof HttpGet) {
                HttpGet request = (HttpGet) args[0];
                path = request.getURI().getPath();

                if (path.contains("/batch/testBatch")) {
                    InputStream stream = new ByteArrayInputStream(batchSample.getBytes(StandardCharsets.UTF_8.name()));
                    when(httpEntity.getContent()).thenReturn(stream);
                    return httpResponse;
                }
            }
            return null;
        });
        Batch batch = catService.pollForBatchProcessingCompletion("testOrg", "sandboxName",
                "testToken", "testBatch");
        assertEquals("success", batch.getStatus());
    }
}