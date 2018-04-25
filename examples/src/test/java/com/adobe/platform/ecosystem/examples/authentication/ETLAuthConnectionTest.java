
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
package com.adobe.platform.ecosystem.examples.authentication;

import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;
import com.adobe.platform.connector.ut.BaseTest;
import org.apache.http.client.methods.HttpGet;
import org.json.simple.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertTrue;

/**
 * Created by nidhi on 25/9/17.
 */
public class ETLAuthConnectionTest extends BaseTest{

    @Before
    public void before() throws  Exception {
        setUp();
    }

    private void setupHttpForBatches() throws IOException {
        Mockito.when(httpClient.execute(Mockito.any())).thenAnswer((
                invocation -> {
                    Object[] args = invocation.getArguments();
                    String path;
                    HttpGet request = (HttpGet) args[0];
                    path = request.getURI().getPath();
                    if(path.contains("/batches")) {
                        String batchResponse = "{\"key\":\"value\"}";
                        InputStream stream = new ByteArrayInputStream(batchResponse.getBytes(StandardCharsets.UTF_8.name()));
                        Mockito.when(httpEntity.getContent()).thenReturn(stream);
                        return httpResponse;
                    }
                    return null;
                }
        ));

        Mockito.doReturn(statusLine).when(httpResponse).getStatusLine();
        Mockito.doReturn(200).when(statusLine).getStatusCode();
        Mockito.doReturn(httpEntity).when(httpResponse).getEntity();


    }

    @Test
    public void testConstructor() {
        ETLAuthConnection etlAuthConnection = new ETLAuthConnection();
        assertTrue(etlAuthConnection != null);
        ETLAuthConnection etlAuthConnection1 = new ETLAuthConnection();
        assertTrue(etlAuthConnection1 != null);
    }


    @Test
    public void testFailureValidConnection() throws Exception {
        ETLAuthConnection etlAuthConnection = new ETLAuthConnection();
        getStringAsHttpOutputStream("");
        try {
            etlAuthConnection.validateConnection( "accessToken", "imsOrg");
            assertTrue(false);
        }catch (Exception ex){
            assertTrue(ex instanceof ConnectorSDKException);
        }
    }

    @Test
    public void testEmptyDataset() throws Exception {
        ETLAuthConnection etlAuthConnection = new ETLAuthConnection();
        getStringAsHttpOutputStream(new JSONObject().toString());
        try {
            etlAuthConnection.validateConnection( "accessToken", "imsOrg");
            assertTrue(false);
        }catch (Exception ex){
            assertTrue(ex instanceof ConnectorSDKException);
        }
    }
}