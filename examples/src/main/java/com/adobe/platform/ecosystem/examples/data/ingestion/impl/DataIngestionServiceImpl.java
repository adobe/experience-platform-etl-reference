
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

/**
 * Created by vedhera on 10/09/2017.
 */

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.adobe.platform.ecosystem.examples.constants.SDKConstants;
import com.adobe.platform.ecosystem.examples.data.FileFormat;
import com.adobe.platform.ecosystem.examples.data.ingestion.api.DataIngestionService;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKUtil;
import com.adobe.platform.ecosystem.examples.util.HttpClientUtil;
import com.adobe.platform.ecosystem.examples.util.ResourceName;

/**
 * Concrete implementation for writing
 * datasets for underlying datasource.
 */

public class DataIngestionServiceImpl implements DataIngestionService {
    private String _endpoint;

    private HttpClientUtil httpClientUtil;

    private static Logger logger = Logger.getLogger(DataIngestionService.class.getName());
    private final String DIS_BATCH_COMPLETION_SIGNAL_KEYWORD = "COMPLETE";

    public DataIngestionServiceImpl(String endpoint) throws ConnectorSDKException {
        new DataIngestionServiceImpl(endpoint,null);
    }

    public DataIngestionServiceImpl(String endpoint, HttpClient httpClient) throws ConnectorSDKException {
        this._endpoint = endpoint;
        HttpClient hClient = httpClient == null ? HttpClientUtil.getHttpClient(true) : httpClient;
        this.httpClientUtil = new HttpClientUtil(hClient);
    }

    /* (non-Javadoc)
     * @see com.adobe.platform.ecosystem.examples.data.ingestion.api.DataIngestionService#getBatch(java.lang.String, java.lang.String)
     */
    @Override
    public String getBatchId(String imsOrg, String accessToken, JSONObject payload) throws ConnectorSDKException {

        logger.log(Level.INFO,"creating batch under imsOrg: "+imsOrg);

        String batchId = "";
        String response = "";
        try {
            ConnectorSDKUtil utilInstance = ConnectorSDKUtil.getInstance();
            String dataIngestionURI = utilInstance.getEndPoint(ResourceName.DATA_INGESTION);
            URIBuilder builder = new URIBuilder(dataIngestionURI);
            builder.setPath(builder.getPath() + "/batches/");
            HttpPost request = new HttpPost(builder.build());
            StringEntity requestEntity = new StringEntity(payload.toString(),
                    ContentType.APPLICATION_JSON);
            request.setEntity(requestEntity);
            httpClientUtil.addHeader(request, accessToken, imsOrg, SDKConstants.CONNECTION_HEADER_JSON_CONTENT);
            response = httpClientUtil.execute(request);
        } catch (Exception e) {
            logger.severe("Printing response: " + response);
            logger.severe("Error while fetching batches : " + e.getMessage());
            throw new ConnectorSDKException(e.getMessage(), e);
        }

        JSONParser parser = new JSONParser();
        JSONObject obj;
        try {
            obj = (JSONObject) parser.parse(response);
            if(obj.get("id")!=null)
                batchId = obj.get("id").toString();
            else
                throw new ConnectorSDKException("id key not found in batch posting response");
        } catch (ParseException e) {
            logger.severe("Error in parsing response: "+e.getMessage());
            throw new ConnectorSDKException("Error while getting batch :" + e.getMessage(), e.getCause());
        }

        return batchId;
    }

    /* (non-Javadoc)
     * @see com.adobe.platform.ecosystem.examples.data.ingestion.api.DataIngestionService#writeToBatch(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, byte[])
     */
    @Override
    public int writeToBatch(String batchId, String dataSetId, String imsOrg, String accessToken, FileFormat fileFormat, byte[] buffer) throws ConnectorSDKException {
        int outputResponse = -1;
        InputStream stream;
        logger.log(Level.INFO,"Going to write for batchId with imsOrg:"+imsOrg);
        try {
            String fileName = System.currentTimeMillis()+"."+fileFormat.getExtension().toLowerCase();
            stream = new ByteArrayInputStream(buffer);
            ConnectorSDKUtil utilInstance = ConnectorSDKUtil.getInstance();
            String dataIngestionURI = utilInstance.getEndPoint(ResourceName.DATA_INGESTION);
            URIBuilder builder = new URIBuilder(dataIngestionURI);
            builder.setPath(builder.getPath() + "/batches/"+batchId+"/datasets/"+dataSetId+"/files/"+fileName);
            HttpPut request = new HttpPut(builder.build());
            request.setEntity(new ByteArrayEntity(buffer));
            request.setHeader("Content-Type", ContentType.APPLICATION_OCTET_STREAM.toString());
            request.setHeader("Authorization", "Bearer " + accessToken);
            request.setHeader(SDKConstants.CONNECTION_HEADER_IMS_ORG_KEY, imsOrg);
            request.setHeader(SDKConstants.CONNECTION_HEADER_X_API_KEY,
                    utilInstance.getConnectionProperty(SDKConstants.CREDENTIAL_CLIENT_KEY));
            HttpResponse response = httpClientUtil.executeRequest(request,false);
            if(response!=null && response.getStatusLine().getStatusCode()==200)
                outputResponse = 0;
        } catch (Exception e) {
            logger.severe("Error in writeToBatch : "+e.getMessage());
            throw new ConnectorSDKException("Error while writing to batch :" + e.getMessage(), e.getCause());
        }
        return outputResponse;

    }

    /* (non-Javadoc)
     * @see com.adobe.platform.connector.sdk.data.ingestion.api.DataIngestionService#writeLargeFileToBatch(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, byte[])
     */
    @Override
    public int writeLargeFileToBatch(String batchId, String dataSetId, String imsOrg, String accessToken, FileFormat fileFormat, byte[] buffer) throws ConnectorSDKException {
        int outputResponse = -1;
        logger.log(Level.INFO,"Going to write for batchId with imsOrg:"+imsOrg);
        try {
            int patchSize = Integer.parseInt(System.getProperty(SDKConstants.SIMPLE_FILE_UPLOAD_LIMIT, "500000000"));
            int noOfPatches = 1;
            if (buffer.length > patchSize) {
                noOfPatches = (buffer.length%(patchSize) != 0) ?
                        buffer.length/patchSize + 1 : buffer.length/patchSize;
            }

            String fileName = System.currentTimeMillis()+"."+fileFormat.getExtension().toLowerCase();
            InputStream stream = new ByteArrayInputStream(buffer);
            outputResponse = initializeFileUpload(batchId, dataSetId, imsOrg, accessToken, fileName);
            if (outputResponse == 0) {
                int rangeEnd = -1;
                for (int iter = 0; iter < noOfPatches; iter++) {
                    int rangeStart = rangeEnd + 1;
                    rangeEnd = rangeStart + patchSize - 1;
                    if (rangeEnd > buffer.length)
                        rangeEnd = buffer.length - 1;
                    outputResponse = uploadPatch(rangeStart, rangeEnd, buffer.length, batchId, dataSetId, imsOrg, accessToken, fileName, stream);
                    if (outputResponse != 0) {
                        break;
                    }
                }
            }
            if (outputResponse == 0)
                outputResponse = completeUpload(batchId, dataSetId, imsOrg, accessToken, fileName);
        } catch (Exception e) {
            logger.severe("Error in writeLargeFileToBatch : "+e.getMessage());
            throw new ConnectorSDKException("Error while writing to batch :" + e.getMessage(), e.getCause());
        }
        return outputResponse;
    }

    private int completeUpload(String batchId, String dataSetId, String imsOrg, String accessToken, String fileName) throws ConnectorSDKException {
        int outputResponse = -1;
        try {
            ConnectorSDKUtil utilInstance;
            utilInstance = ConnectorSDKUtil.getInstance();
            String dataIngestionURI = utilInstance.getEndPoint(ResourceName.DATA_INGESTION);
            URIBuilder builder = new URIBuilder(dataIngestionURI);
            builder.setPath(builder.getPath() + "/batches/"+batchId+"/datasets/"+dataSetId+"/files/"+fileName);
            builder.setParameter("action", "complete");
            HttpPost request = new HttpPost(builder.build());
            request.setHeader("Authorization", "Bearer " + accessToken);
            request.setHeader(SDKConstants.CONNECTION_HEADER_IMS_ORG_KEY, imsOrg);
            request.setHeader(SDKConstants.CONNECTION_HEADER_X_API_KEY,
                    utilInstance.getConnectionProperty(SDKConstants.CREDENTIAL_CLIENT_KEY));
            HttpResponse response = httpClientUtil.executeRequest(request,false);
            if(response!=null && response.getStatusLine().getStatusCode()==200)
                outputResponse = 0;
            } catch (Exception e) {
                logger.severe("Error in initializeFileUpload : "+e.getMessage());
                throw new ConnectorSDKException("Error while writing to batch :" + e.getMessage(), e.getCause());
            }
        return outputResponse;
    }

    private int uploadPatch(int rangeStart, int rangeEnd, int contentLength, String batchId, String dataSetId, String imsOrg, String accessToken, String fileName, InputStream stream) throws ConnectorSDKException {
        int outputResponse = -1;
        try {
            ConnectorSDKUtil utilInstance = ConnectorSDKUtil.getInstance();
            String dataIngestionURI = utilInstance.getEndPoint(ResourceName.DATA_INGESTION);
            URIBuilder builder = new URIBuilder(dataIngestionURI);
            builder.setPath(builder.getPath() + "/batches/"+batchId+"/datasets/"+dataSetId+"/files/"+fileName);
            HttpPatch request = new HttpPatch(builder.build());
            byte[] buf = new byte[rangeEnd - rangeStart + 1];
            int res = stream.read(buf, 0, rangeEnd - rangeStart + 1);
            request.setEntity(new ByteArrayEntity(buf));
            request.setHeader("Content-Type", ContentType.APPLICATION_OCTET_STREAM.toString());
            String rangeHeader = String.format("bytes %d-%d/%d", rangeStart, rangeEnd, contentLength);
            request.setHeader("Content-Range", rangeHeader);
            request.setHeader("Authorization", "Bearer " + accessToken);
            request.setHeader(SDKConstants.CONNECTION_HEADER_IMS_ORG_KEY, imsOrg);
            request.setHeader(SDKConstants.CONNECTION_HEADER_X_API_KEY,
                    utilInstance.getConnectionProperty(SDKConstants.CREDENTIAL_CLIENT_KEY));
            HttpResponse response = httpClientUtil.executeRequest(request,false);
            if(response!=null && response.getStatusLine().getStatusCode()==200)
                outputResponse = 0;
        } catch (Exception e) {
            logger.severe("Error in uploadPatch : "+e.getMessage());
            throw new ConnectorSDKException("Error while writing to batch :" + e.getMessage(), e.getCause());
        }
        return outputResponse;
    }

    private int initializeFileUpload(String batchId, String dataSetId, String imsOrg, String accessToken, String fileName) throws ConnectorSDKException {
        int outputResponse = -1;
        try {
        ConnectorSDKUtil utilInstance;
        utilInstance = ConnectorSDKUtil.getInstance();
        String dataIngestionURI = utilInstance.getEndPoint(ResourceName.DATA_INGESTION);
        URIBuilder builder = new URIBuilder(dataIngestionURI);
        builder.setPath(builder.getPath() + "/batches/"+batchId+"/datasets/"+dataSetId+"/files/"+fileName);
        builder.setParameter("action", "initialize");
        HttpPost request = new HttpPost(builder.build());
        request.setHeader("Authorization", "Bearer " + accessToken);
        request.setHeader(SDKConstants.CONNECTION_HEADER_IMS_ORG_KEY, imsOrg);
        request.setHeader(SDKConstants.CONNECTION_HEADER_X_API_KEY,
                utilInstance.getConnectionProperty(SDKConstants.CREDENTIAL_CLIENT_KEY));
        HttpResponse response = httpClientUtil.executeRequest(request,false);
        if(response!=null && response.getStatusLine().getStatusCode()==201)
            outputResponse = 0;
        } catch (Exception e) {
            logger.severe("Error in initializeFileUpload : "+e.getMessage());
            throw new ConnectorSDKException("Error while writing to batch :" + e.getMessage(), e.getCause());
        }
        return outputResponse;
    }

    /* (non-Javadoc)
     * @see com.adobe.platform.ecosystem.examples.data.ingestion.api.DataIngestionService#signalBatchCompletion(java.lang.String, java.lang.String, java.lang.String)
     */
    @Override
    public int signalBatchCompletion(String batchId, String imsOrg, String accessToken) throws ConnectorSDKException {
        int outputResponse = -1;
        logger.log(Level.FINER,"Inside signalBatchCompletion");
        try {
            ConnectorSDKUtil utilInstance = ConnectorSDKUtil.getInstance();
            String dataIngestionURI = utilInstance.getEndPoint(ResourceName.DATA_INGESTION);
            URIBuilder builder = new URIBuilder(dataIngestionURI);
            builder.setPath(builder.getPath() + "/batches/"+batchId);
            builder.setParameter("action", DIS_BATCH_COMPLETION_SIGNAL_KEYWORD);
            HttpPost request = new HttpPost(builder.build());
            httpClientUtil.addHeader(request, accessToken, imsOrg, SDKConstants.CONNECTION_HEADER_JSON_CONTENT);
            String response = httpClientUtil.execute(request);
            logger.log(Level.INFO,"Batch Completion signalled and got response "+response);
            outputResponse = 0;
        } catch (Exception e) {
            logger.severe("Error in signalBatchCompletion : "+e.getMessage());
            throw new ConnectorSDKException("Error while signalling batch completion :" + e.getMessage(), e.getCause());

        }
        return outputResponse;
    }
}