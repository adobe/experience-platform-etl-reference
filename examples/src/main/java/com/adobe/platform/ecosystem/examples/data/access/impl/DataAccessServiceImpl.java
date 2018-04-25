
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

/**
 * Created by vedhera on 10/09/2017.
 */

import com.adobe.platform.ecosystem.examples.constants.SDKConstants;
import com.adobe.platform.ecosystem.examples.data.access.api.DataAccessService;
import com.adobe.platform.ecosystem.examples.data.access.model.DataAccessFileEntity;
import com.adobe.platform.ecosystem.examples.data.access.model.DataSetFileProcessingEntity;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKUtil;
import com.adobe.platform.ecosystem.examples.util.HttpClientUtil;
import com.adobe.platform.ecosystem.examples.util.ResourceName;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Concrete implementation for reading
 * datasets for underlying datasource.
 */
public class DataAccessServiceImpl implements DataAccessService {
    private String _endpoint;

    private HttpClientUtil httpClientUtil;

    private static Logger logger = Logger.getLogger(DataAccessService.class.getName());

    public DataAccessServiceImpl(String endpoint) throws ConnectorSDKException {
        new DataAccessServiceImpl(endpoint,null);
    }

    public DataAccessServiceImpl(String endpoint, HttpClient httpClient) throws ConnectorSDKException {
        this._endpoint = endpoint;
        HttpClient hClient = httpClient == null ? HttpClientUtil.getHttpClient() : httpClient;
        this.httpClientUtil = new HttpClientUtil(hClient);
    }

    @Override
    public List<DataSetFileProcessingEntity> getDataSetFileEntries(
            String imsOrg,
            String accessToken,
            String dataSetFileId
    ) throws ConnectorSDKException {

        List<DataSetFileProcessingEntity> processingEntities = new ArrayList<>();
        try {
            ConnectorSDKUtil utilInstance = ConnectorSDKUtil.getInstance();
            String dataAccessURI = utilInstance.getEndPoint(ResourceName.DATA_ACCESS);
            URIBuilder builder = new URIBuilder(dataAccessURI);
            builder.setPath(builder.getPath() + "/files/" + dataSetFileId);
            HttpGet request = new HttpGet(builder.build());
            httpClientUtil.addHeader(request, accessToken, imsOrg, SDKConstants.CONNECTION_HEADER_JSON_CONTENT);
            String response = httpClientUtil.execute(request);

            JSONParser parser = new JSONParser();
            JSONObject jsonObject = (JSONObject) parser.parse(response);
            if(jsonObject.containsKey(SDKConstants.DATA_ACCESS_DATA_KEY)) {
                JSONArray jsonArray = (JSONArray) jsonObject.get(SDKConstants.DATA_ACCESS_DATA_KEY);
                for (int i=0; i<jsonArray.size(); i++) {
                    JSONObject jdata = (JSONObject) jsonArray.get(i);
                    processingEntities.add(new DataSetFileProcessingEntity(jdata));
                }
            } else {
                logger.log(Level.FINE, "Data is missing for dataSet file Id : " + dataSetFileId);
            }

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error while fetching DataSetFileProcessingEntity :" + e.getMessage());
            throw new ConnectorSDKException("Error while fetching DataSetFileProcessingEntity :" + e.getMessage(), e.getCause());
        }

        return processingEntities;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<DataAccessFileEntity> getDataSetFilesFromBatchId(String imsOrg, String accessToken, String batchId) throws ConnectorSDKException {
        List<DataAccessFileEntity> processingEntities = new ArrayList<>();
        try {
            ConnectorSDKUtil utilInstance = ConnectorSDKUtil.getInstance();
            String dataAccessURI = utilInstance.getEndPoint(ResourceName.DATA_ACCESS);
            URIBuilder builder = new URIBuilder(dataAccessURI);
            builder.setPath(builder.getPath() + "/batches/" + batchId + "/files");
            HttpGet request = new HttpGet(builder.build());
            httpClientUtil.addHeader(request, accessToken, imsOrg, SDKConstants.CONNECTION_HEADER_JSON_CONTENT);
            String response = httpClientUtil.execute(request);

            JSONParser parser = new JSONParser();
            JSONObject jsonObject = (JSONObject) parser.parse(response);
            if(jsonObject.containsKey(SDKConstants.DATA_ACCESS_DATA_KEY)) {
                JSONArray jsonArray = (JSONArray) jsonObject.get(SDKConstants.DATA_ACCESS_DATA_KEY);
                for (int i=0; i<jsonArray.size(); i++) {
                    JSONObject jdata = (JSONObject) jsonArray.get(i);
                    processingEntities.add(new DataAccessFileEntity(jdata));
                }
            }

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error while fetching DataAccessFileEntity :" + e.getMessage());
            throw new ConnectorSDKException("Error while fetching DataAccessFileEntity :" + e.getMessage(), e.getCause());
        }
        return processingEntities;
    }
}