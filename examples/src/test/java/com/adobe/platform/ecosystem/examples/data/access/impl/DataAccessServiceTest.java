
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
package com.adobe.platform.ecosystem.examples.data.access.impl;

import com.adobe.platform.ecosystem.examples.data.access.model.DataAccessFileEntity;
import com.adobe.platform.ecosystem.examples.data.access.model.DataSetFileProcessingEntity;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;
import com.adobe.platform.ecosystem.ut.BaseTest;
import org.apache.http.client.methods.HttpGet;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * Created by vedhera on 10/10/2017.
 */
public class DataAccessServiceTest extends BaseTest {
    private DataAccessServiceImpl das = new DataAccessServiceImpl("testEndpoint",httpClient);

    public DataAccessServiceTest() throws ConnectorSDKException {
    }

    @Before
    public void before() throws Exception {
        super.setUp();
        Mockito.when(httpClient.execute(Mockito.any())).thenReturn(httpResponse);
        Mockito.when(httpResponse.getStatusLine()).thenReturn(statusLine);
        Mockito.when(statusLine.getStatusCode()).thenReturn(200);
        Mockito.when(httpResponse.getEntity()).thenReturn(httpEntity);
        setupHttpClientMocking();
    }

    private void setupHttpClientMocking() throws IOException {
        Mockito.when(httpClient.execute(Mockito.any())).thenAnswer(
                (invocation -> {
                    Object[] args = invocation.getArguments();
                    HttpGet request = (HttpGet) args[0];
                    String path = request.getURI().getPath();
                    if(path.contains("testDSFId")) {
                        InputStream stream = new ByteArrayInputStream(dataAccessFilesResponse.getBytes(StandardCharsets.UTF_8.name()));
                        Mockito.when(httpEntity.getContent()).thenReturn(stream);
                        return httpResponse;
                    } else if(path.contains("failDSFId")) {
                        InputStream stream = new ByteArrayInputStream(dataAccessFileCorruptResponse.getBytes(StandardCharsets.UTF_8.name()));
                        Mockito.when(httpEntity.getContent()).thenReturn(stream);
                        return httpResponse;
                    } else if(path.contains("batchId")) {
                        InputStream stream = new ByteArrayInputStream(dataAccessServiceFileEntityResponse.getBytes(StandardCharsets.UTF_8.name()));
                        Mockito.when(httpEntity.getContent()).thenReturn(stream);
                        return httpResponse;
                    }
                    return null;
                })
        );
    }

    @Test
    public void testConstructorWithOneArguement() {
        DataAccessServiceImpl ds = null;
        try {
            ds = new DataAccessServiceImpl("testEndpoint");
        } catch (ConnectorSDKException e) {
            assertTrue(false);
        }
        assertTrue(ds != null);
    }

    @Test
    public void testConstructorWithTwoArguement() {
        assertTrue(das != null);
    }

    @Test
    public void testDataAccessAPIForTwoFilesEntries() throws ConnectorSDKException {
        List<DataSetFileProcessingEntity> dataSetFileEntries = das.getDataSetFileEntries("testOrg", "testToken", "testDSFId");
        assertTrue(dataSetFileEntries.size() == 2);
    }

    @Test
    public void testDataAccessAPIForException() throws ConnectorSDKException {
        try {
            List<DataSetFileProcessingEntity> dataSetFileEntries = das.getDataSetFileEntries("testOrg", "testToken", "failDSFId");
        } catch (Exception ex) {
            assertTrue(ex instanceof ConnectorSDKException);
        }
    }

    @Test
    public void testGetDataSetFilesFromBatchId() throws ConnectorSDKException {
        List<DataAccessFileEntity> dataSetFileEntries = das.getDataSetFilesFromBatchId("testOrg", "testToken", "batchId");
        assertTrue(dataSetFileEntries.size() == 1);
    }
}