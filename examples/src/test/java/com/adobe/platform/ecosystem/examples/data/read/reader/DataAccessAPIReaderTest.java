
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
package com.adobe.platform.ecosystem.examples.data.read.reader;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.ParseException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.message.HeaderGroup;
import org.json.simple.JSONArray;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.adobe.platform.ecosystem.examples.parquet.read.ParquetIOReader;
import com.adobe.platform.ecosystem.examples.parquet.wiring.impl.ParquetIOImpl;
import com.adobe.platform.ecosystem.examples.constants.SDKConstants;
import com.adobe.platform.ecosystem.examples.data.wiring.DataWiringParam;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;
import com.adobe.platform.ecosystem.ut.BaseTest;

/**
 * Created by vedhera on 10/11/2017.
 */

public class DataAccessAPIReaderTest extends BaseTest {
    private DataAccessAPIReader dataAccessAPIReader;

    @Before
    public void before() throws Exception {
        super.setUp();
        Mockito.when(httpClient.execute(Mockito.any())).thenReturn(httpResponse);
        Mockito.when(httpResponse.getStatusLine()).thenReturn(statusLine);
        Mockito.when(statusLine.getStatusCode()).thenReturn(200);
        Mockito.when(httpResponse.getEntity()).thenReturn(httpEntity);
        Mockito.when(catService.getDataSetFiles(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(getDataSetFiles());
        Mockito.when(dataset.getFileDescription()).thenReturn(fileDescription);
        Mockito.when(fileDescription.getDelimiter()).thenReturn(',');
        setupHttpClientMocking();
    }

    private void setupHttpClientMocking() throws Exception {
        Mockito.when(das.getDataSetFileEntries(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any())).thenAnswer(
                (invocation -> {
                    Object[] args = invocation.getArguments();
                    String imsOrg = (String) args[0];
                    if(imsOrg.equals("ParquetImsOrg")) {
                        return getParquetFileEntities();
                    } else if(imsOrg.equals("JSONImsOrg")) {
                        return getJSONFileEntities();
                    } else {
                        return getFileEntities();
                    }
                })
        );
        Mockito.when(httpClient.execute(Mockito.any())).thenAnswer(
                (invocation -> {
                    Object[] args = invocation.getArguments();
                    String path;
                    if(args[0] instanceof HttpGet) {
                        HttpGet request = (HttpGet) args[0];
                        path = request.getURI().getPath();
                    } else {
                        HttpPost request = (HttpPost) args[0];
                        path = request.getURI().getPath();
                    }

                    if(path.contains("href1")) {
                        InputStream stream = new ByteArrayInputStream(dataAccessFileEntityResponse.getBytes(StandardCharsets.UTF_8.name()));
                        Mockito.when(httpEntity.getContent()).thenReturn(stream);
                        Header[] headers = getHeaders();
                        Mockito.when(httpResponse.getHeaders("Content-Range")).thenReturn(headers);
                        return httpResponse;
                    } else if(path.contains("href3")) { // JSON file Access
                        InputStream stream = new ByteArrayInputStream(dataAccessJSONFileEntityResponse.getBytes(StandardCharsets.UTF_8.name()));
                        Mockito.when(httpEntity.getContent()).thenReturn(stream);
                        Header[] headers = getHeaders();
                        Mockito.when(httpResponse.getHeaders("Content-Range")).thenReturn(headers);
                        return httpResponse;
                    } else if(path.contains(SDKConstants.JWT_EXCHANGE_IMS_URI)) {
                        InputStream stream = new ByteArrayInputStream(jwtExchangeResponse.getBytes(StandardCharsets.UTF_8.name()));
                        Mockito.when(httpEntity.getContent()).thenReturn(stream);
                        return httpResponse;
                    }
                    return null;
                })
        );
    }

    private Header[] getHeaders() {
        Header header = new Header() {
            @Override
            public String getValue(){
                return "bytes 0-1000/800";
            }
            @Override
            public String getName(){
                return "Content-Range";
            }
            @Override
            public HeaderElement[] getElements() throws ParseException {
                return null;
            }
        };
        Header[] headers = {header};
        return headers;
    }

    @Test
    public void testConstructor() throws Exception {
        dataAccessAPIReader = new DataAccessAPIReader(catService,das,param,httpClient, new ParquetIOImpl().getParquetIOReader(true), null);
        assert(dataAccessAPIReader != null);
    }

    @Test
    public void testReadOnce() throws ConnectorSDKException {
        dataAccessAPIReader = new DataAccessAPIReader(catService,das,param,httpClient, new ParquetIOImpl().getParquetIOReader(true), null);
        JSONArray array = dataAccessAPIReader.read(4);
        assert(array.size() == 4);
    }

    @Test
    public void testReadMoreThanOnce() throws ConnectorSDKException {
        dataAccessAPIReader = new DataAccessAPIReader(catService,das,param,httpClient, new ParquetIOImpl().getParquetIOReader(true), null);
        JSONArray array = dataAccessAPIReader.read(5);
        JSONArray array1 = dataAccessAPIReader.read(5);
        assert(array.size() == 5);
        assert(array1.size() == 4);
    }

    @Test
    public void testCSVReaderProcessorHasMoreData() throws ConnectorSDKException {
        dataAccessAPIReader = new DataAccessAPIReader(catService,das,param,httpClient, new ParquetIOImpl().getParquetIOReader(true), null);
        assert(dataAccessAPIReader.hasMoreData() == true);
        dataAccessAPIReader.read(5);
        assert(dataAccessAPIReader.hasMoreData() == true);
        dataAccessAPIReader.read(5);
        assert(dataAccessAPIReader.hasMoreData() == false);
    }

    @Test
    public void testParquetProcessor() throws ConnectorSDKException {
        ParquetIOReader mockPIOReader = Mockito.mock(ParquetIOReader.class);
        DataWiringParam mockParam = new DataWiringParam("ParquetImsOrg", dataset);
        dataAccessAPIReader = new DataAccessAPIReader(catService,das,mockParam,httpClient, mockPIOReader,null);
        assert(dataAccessAPIReader.read(2).size()==0);
    }

    @Test
    public void testReadOnceJSON() throws ConnectorSDKException {
        DataWiringParam mockParam = new DataWiringParam("JSONImsOrg", "sandboxName", dataset);
        dataAccessAPIReader = new DataAccessAPIReader(catService,das,mockParam,httpClient, new ParquetIOImpl().getParquetIOReader(true), null);
        JSONArray array = dataAccessAPIReader.read(4);
        assert(array.size() == 4);
    }

    @Test
    public void testReadMoreThanOnceJSON() throws ConnectorSDKException {
        DataWiringParam mockParam = new DataWiringParam("JSONImsOrg", "sandboxName", dataset);
        dataAccessAPIReader = new DataAccessAPIReader(catService,das,mockParam,httpClient, new ParquetIOImpl().getParquetIOReader(true), null);
        JSONArray array = dataAccessAPIReader.read(4);
        assert(array.size() == 4);
        array = dataAccessAPIReader.read(1);
        assert(array.size() == 1);
    }
}