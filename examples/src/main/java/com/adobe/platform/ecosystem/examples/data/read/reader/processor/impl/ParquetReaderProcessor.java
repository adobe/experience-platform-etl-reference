
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
package com.adobe.platform.ecosystem.examples.data.read.reader.processor.impl;

import com.adobe.platform.ecosystem.examples.parquet.read.ParquetIOReader;
import com.adobe.platform.ecosystem.examples.parquet.utility.ParquetIOUtil;
import com.adobe.platform.ecosystem.examples.constants.SDKConstants;
import com.adobe.platform.ecosystem.examples.data.access.api.DataAccessService;
import com.adobe.platform.ecosystem.examples.data.access.model.DataSetFileProcessingEntity;
import com.adobe.platform.ecosystem.examples.data.read.reader.processor.api.ReaderProcessor;
import com.adobe.platform.ecosystem.examples.data.wiring.DataWiringParam;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKUtil;
import com.adobe.platform.ecosystem.examples.util.HttpClientUtil;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.json.simple.JSONObject;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by vedhera on 11/20s/2017.
 */
public class ParquetReaderProcessor extends ReaderProcessor {
    private final ParquetIOReader parquetIOReader;
    private final DataWiringParam param;
    private final HttpClient httpClient;
    private final HttpClientUtil httpClientUtil;
    private static final Logger logger = Logger.getLogger(ParquetReaderProcessor.class.getName());

    public ParquetReaderProcessor(ParquetIOReader parquetIOReader, DataAccessService das, HttpClient httpClient, DataWiringParam param, List<String> dataSetFileSet, List<DataSetFileProcessingEntity> dataSetFileProcessingSet) throws ConnectorSDKException {
        super(das, param, dataSetFileSet, dataSetFileProcessingSet);
        this.parquetIOReader = parquetIOReader;
        this.httpClient = httpClient == null ? HttpClientUtil.getHttpClient() : httpClient;
        httpClientUtil = new HttpClientUtil(httpClient);
        this.param = param;
    }

    @Override
    public List<JSONObject> processData(int rows) throws ConnectorSDKException {
        List<JSONObject> data = new ArrayList<>();
        while(data.size() < rows && hasMoreData()) {
            processData(data, rows-data.size());
        }
        return data;
    }

    private List<JSONObject> processData(List<JSONObject> data, int rows) throws ConnectorSDKException {
        try {
            if(parquetIOReader.hasBufferData()) {
                data.addAll(parquetIOReader.processData(rows));
            } else {
                if(!dataSetFileProcessingSet.isEmpty()) {
                    File localParquetFile = loadParquetFileFromProcessingEntity(dataSetFileProcessingSet.get(processingSetReadIndex));
                    parquetIOReader.initFileForRead(localParquetFile);
                    data.addAll(parquetIOReader.processData(rows));
                }
            }

            if(!parquetIOReader.hasBufferData()) {
                prepareAndCheckEntitiesIndex();
            }

        } catch (Exception ex) {
            logger.severe("Error while processing parquet file " + ex);
            throw new ConnectorSDKException("Error while processing parquet file", ex);
        }
        return data;
    }

    @Override
    public Boolean hasMoreData() throws ConnectorSDKException {
        if(parquetIOReader.hasBufferData() || dataSetFileReadIndex < dataSetFileSet.size()) {
            return true;
        } else {
            return false;
        }
    }

    private File loadParquetFileFromProcessingEntity(DataSetFileProcessingEntity dataSetFileProcessingEntity) throws URISyntaxException, IOException, ConnectorSDKException {
        URIBuilder builder = new URIBuilder(dataSetFileProcessingEntity.getHref());
        HttpGet request = new HttpGet(builder.build());
        request.setHeader("Authorization", "Bearer " + param.getAuthToken());
        request.setHeader(SDKConstants.CONNECTION_HEADER_IMS_ORG_KEY, param.getImsOrg());
        request.setHeader(SDKConstants.CONNECTION_HEADER_X_API_KEY,
                ConnectorSDKUtil.getInstance().getConnectionProperty(SDKConstants.CREDENTIAL_CLIENT_KEY));
        logger.log(Level.FINE,request.getRequestLine().getUri());
        HttpResponse response = httpClientUtil.executeRequest(request, false);
        checkErrorResponseCode(response);
        String entityName = dataSetFileProcessingEntity.getName();
        if(entityName.endsWith(".parquet")) {
            entityName = entityName.substring(0,entityName.indexOf(".parquet"));
        }
        File tempParquetFile = ParquetIOUtil.getLocalFilePath(entityName);
        FileOutputStream fos = new FileOutputStream(tempParquetFile.getAbsolutePath());
        InputStream is = response.getEntity().getContent();
        int read = 0;
        byte[] buffer = new byte[32768];
        while( (read = is.read(buffer)) > 0) {
            fos.write(buffer, 0, read);
        }

        return tempParquetFile;
    }
}