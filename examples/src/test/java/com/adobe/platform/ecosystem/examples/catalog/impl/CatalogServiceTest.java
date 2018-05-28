
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
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
    public void testGetDataset() throws ConnectorSDKException {
        DataSet ds = catService.getDataSet("testOrg", "testToken", "testDSId");
        assertTrue(ds.getBasePath().startsWith("adl://"));
    }

    @Test
    public void testGetDatasetException() throws ConnectorSDKException {
        try {
            catService.getDataSet(null, null, null);
            fail("Exception should have been thrown");
        } catch (ConnectorSDKException ce) {
            //Test passed here
        }
    }

    @Test
    public void testGetConnection() throws IOException, ConnectorSDKException {
        setupTestForHttpOutput(connectionSample);
        Connection connection = catService.getConnection("testOrg", "testToken", "conId");
        assertTrue(connection!=null);
        assertTrue("conId".equals(connection.getId()));
    }

    @Test
    public void testGetSchema() throws IOException, ConnectorSDKException {
        setupTestForHttpOutput(schemaSample);
        List<SchemaField> schemaFields = catService.getSchemaFields("testOrg", "testToken", "/testschema", true);
        assertTrue(schemaFields!=null);
        assertTrue(schemaFields.size() == 4);
    }

    @Test
    public void testGetConnectionException() throws ConnectorSDKException {
        try {
            catService.getConnection(null, null, null);
            fail("Exception should have been thrown");
        } catch (ConnectorSDKException ce) {
            //Test passed here
        }
    }

    @Test
    public void testGetDatasets() throws ConnectorSDKException{
        Map<String,String> params = new HashMap<>();
        List<DataSet> listDS = catService.getDataSets("testOrg", "testToken",params,CatalogAPIStrategy.ONCE);
        assertTrue(listDS.size() > 0);
    }

    @Test
    public void testCreateBatch() throws ParseException, UnsupportedOperationException, IOException, ConnectorSDKException {
        JSONArray jsonArray = new JSONArray();
        jsonArray.add(0,batchResp);

        getStringAsHttpOutputStream(jsonArray.toJSONString());
        getStringAsHttpOutputStreamForGetBatchId(batchIdResp);

        try {
            catService.createBatch("testOrg", "testToken", new JSONObject());
        }catch (Exception ex){
            assertTrue(ex instanceof ConnectorSDKException);
        }
    }

    @Test
    public void testCreateBatchException() throws ConnectorSDKException {
        try {
            catService.createBatch(null, null, null);
            fail("Exception should have been thrown");
        } catch (ConnectorSDKException ce) {
            //Test passed here
        }
    }

    @Test
    public void testGetBatchByBatchId() throws ParseException, UnsupportedOperationException, IOException, ConnectorSDKException {
        getStringAsHttpOutputStream(batchSample);

        catService.getBatchByBatchId("testOrg", "testToken","testBatch");
    }

    @Test
    public void testGetBatchByBatchIdException() throws ConnectorSDKException {
        try {
            catService.getBatchByBatchId(null, null, null);
            fail("Exception should have been thrown");
        } catch (ConnectorSDKException ce) {
            //Test passed here
        }
    }

    @Test
    public void testGetDataSetFiles() throws UnsupportedOperationException, IOException, ConnectorSDKException{
        getStringAsHttpOutputStream(credSample);
        Map<String,String> params = new HashMap<>();
        List<DataSetFile> listDSF = catService.getDataSetFiles("testOrg", "testToken", params, CatalogAPIStrategy.ONCE);
        assertTrue(!listDSF.isEmpty());
    }

    @Test
    public void testGetDataSetView() throws UnsupportedOperationException, IOException, ConnectorSDKException{
        getStringAsHttpOutputStream(datasetSample);
        DataSetView dsv = catService.getDataSetView("testOrg", "testToken", "testDSId");
        assertTrue(dsv!=null);
        assertTrue("testDSId".equals(dsv.getId()));
    }
}