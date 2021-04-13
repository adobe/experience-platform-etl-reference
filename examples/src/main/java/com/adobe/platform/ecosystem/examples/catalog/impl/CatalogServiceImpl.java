
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

import com.adobe.platform.ecosystem.examples.catalog.api.CatalogService;
import com.adobe.platform.ecosystem.examples.catalog.model.*;
import com.adobe.platform.ecosystem.examples.constants.SDKConstants;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKUtil;
import com.adobe.platform.ecosystem.examples.util.HttpClientUtil;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.awaitility.core.ConditionFactory;
import org.awaitility.core.ConditionTimeoutException;
import org.hamcrest.Matchers;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.awaitility.Awaitility.to;
import static org.awaitility.Awaitility.with;

/**
 * Created by vedhera on 8/25/2017.
 */
public class CatalogServiceImpl implements CatalogService {

    private static final Integer SLEEP_TIMEOUT = 30000; //milliseconds
    private static final Integer API_CALL_TIMEOUT = 60; //minutes

    private String _endpoint;

    private HttpClientUtil httpClientUtil;

    private static Logger logger = Logger.getLogger(CatalogServiceImpl.class.getName());

    protected CatalogServiceImpl(String endpoint, HttpClient httpClient) throws ConnectorSDKException {
        this._endpoint = endpoint;
        HttpClient hClient = httpClient == null ? HttpClientUtil.getHttpClient() : httpClient;
        httpClientUtil = new HttpClientUtil(hClient);
    }

    protected CatalogServiceImpl(String endpoint) throws ConnectorSDKException {
        this(endpoint, HttpClientUtil.getHttpClient());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<DataSet> getDataSets(String imsOrg, String sandboxName, String authToken, Map<String, String> params,
                                     CatalogAPIStrategy strategy) throws ConnectorSDKException {
        List<DataSet> dataSets;
        try {
            String catalogURI = this._endpoint + "/dataSets";
            dataSets = getEntities(catalogURI, imsOrg, sandboxName, authToken, params, strategy, false, DataSet.class);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error while fetching datasets :" + e.getMessage());
            throw new ConnectorSDKException("Error while fetching datasets :" + e.getMessage(), e.getCause());
        }

        return dataSets;
    }

    @Override
    public List<DataSet> getDataSets(String imsOrg, String authToken, Map<String, String> params, CatalogAPIStrategy strategy) throws ConnectorSDKException {
        return getDataSets(imsOrg, null, authToken, params, strategy);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Connection getConnection(String imsOrg, String sandboxName, String authToken, String connectionId)
            throws ConnectorSDKException {
        Connection connection;
        try {
            String catalogURI = this._endpoint + "/connections/" + connectionId;
            connection = getEntity(catalogURI, imsOrg, sandboxName, authToken, connectionId, false, Connection.class);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error while fetching connection for connectionId :" + e.getMessage());
            throw new ConnectorSDKException(e.getMessage(), e.getCause());
        }
        return connection;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataSet getDataSet(String imsOrg, String sandboxName, String authToken, String dataSetId) throws ConnectorSDKException {
        DataSet dataSet;
        try {
            String catalogURI = this._endpoint + "/dataSets/" + dataSetId + "?properties=" + getDataSetProperties();
            dataSet = getEntity(catalogURI, imsOrg, sandboxName, authToken, dataSetId, false, DataSet.class);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error while fetching dataSet for dataSetId :" + e.getMessage());
            throw new ConnectorSDKException(e.getMessage(), e.getCause());
        }
        return dataSet;
    }

    @Override
    public DataSet getDataSet(String imsOrg, String authToken, String dataSetId) throws ConnectorSDKException {
        return getDataSet(imsOrg, null, authToken, dataSetId);
    }

    private String getDataSetProperties() {
        final String value = ConnectorSDKUtil.getSystemProperty(SDKConstants.DATASET_API_SYSTEM_PROPERTY);
        if (value == null || value.isEmpty()) {
            return SDKConstants.CATALOG_DATASET_DEFAULT_PROPERTY;
        }
        return value;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public Batch createBatch(String imsOrg, String sandboxName, String authToken, JSONObject payload) throws ConnectorSDKException {
        Batch batch;
        try {
            StringEntity requestEntity = new StringEntity(payload.toString(),
                    ContentType.APPLICATION_JSON);
            URIBuilder builder = new URIBuilder(this._endpoint);
            builder.setPath(builder.getPath() + "/batch");
            HttpPost request = new HttpPost(builder.build());
            request.setEntity(requestEntity);
            httpClientUtil.addHeader(request, authToken, imsOrg, sandboxName, SDKConstants.CONNECTION_HEADER_JSON_CONTENT);
            String response = httpClientUtil.execute(request);
            JSONParser parser = new JSONParser();
            JSONArray jsonArray = (JSONArray) parser.parse(response);
            String batchCreationresponse = (String) jsonArray.get(0);
            String batchId = batchCreationresponse
                    .substring(batchCreationresponse.lastIndexOf("/") + 1);
            batch = getBatchByBatchId(imsOrg, sandboxName, authToken, batchId);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error in createBatch :" + e.getMessage());
            throw new ConnectorSDKException(e.getMessage(), e.getCause());
        }
        return batch;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Batch getBatchByBatchId(String imsOrg, String sandboxName, String authToken,
                                   String batchId) throws ConnectorSDKException {
        Batch batch;
        try {
            String catalogURI = this._endpoint + "/batch/" + batchId;
            batch = getEntity(catalogURI, imsOrg, sandboxName, authToken, batchId, false, Batch.class);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error in getBatchByBatchId :" + e.getMessage());
            throw new ConnectorSDKException(e.getMessage(), e.getCause());
        }
        return batch;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<DataSetFile> getDataSetFiles(String imsOrg, String sandboxName, String authToken, Map<String, String> params,
                                             CatalogAPIStrategy strategy) throws ConnectorSDKException {
        List<DataSetFile> dataSetFiles;
        try {
            String catalogURI = this._endpoint + "/dataSetFiles";
            dataSetFiles = getEntities(catalogURI, imsOrg, sandboxName, authToken, params, strategy, false, DataSetFile.class);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error in getCredentials :" + e.getMessage());
            throw new ConnectorSDKException(e.getMessage(), e.getCause());
        }
        return dataSetFiles;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataSetView getDataSetView(String imsOrg, String sandboxName, String authToken, String viewId)
            throws ConnectorSDKException {
        DataSetView dataSetView;
        try {
            String catalogURI = this._endpoint + "/dataSetView/" + viewId;
            dataSetView = getEntity(catalogURI, imsOrg, sandboxName, authToken, viewId, false, DataSetView.class);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error in getCredentials :" + e.getMessage());
            throw new ConnectorSDKException(e.getMessage(), e.getCause());
        }
        return dataSetView;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Batch> getBatches(String imsOrg, String sandboxName, String authToken, Map<String, String> params,
                                  CatalogAPIStrategy strategy) throws ConnectorSDKException {
        List<Batch> batches;
        try {
            String catalogURI = this._endpoint + "/batches";
            batches = getEntities(catalogURI, imsOrg, sandboxName, authToken, params, strategy, false, Batch.class);
        } catch (Exception e) {
            logger.log(Level.INFO, "Error in getBatches :" + e.getMessage());
            throw new ConnectorSDKException(e.getMessage(), e.getCause());
        }
        return batches;
    }

    /**
     * Method to query Catalog for a
     * specific entity having <code>entityId</code>.
     */
    private <T extends BaseModel> T getEntity(String entityEndpoint,
                                              String imsOrg,
                                              String sandboxName,
                                              String authToken,
                                              String entityId,
                                              boolean isFlat,
                                              Class<T> clazz) throws ConnectorSDKException, ParseException, URISyntaxException {
        T entity = getEntities(entityEndpoint,
                imsOrg,
                sandboxName,
                authToken,
                new HashMap<>(),
                CatalogAPIStrategy.ONCE,
                isFlat,
                clazz).get(0);
        if (!isFlat && !entity.getId().equals(entityId)) {
            throw new ConnectorSDKException("Entity id fetched from Catalog does not equal " + entityId + " for class: " + clazz);
        }
        return entity;
    }

    /**
     * Method which queries Catalog
     * for defined generic of type <code>T</code>.
     * It queries based on {@link CatalogAPIStrategy}.
     *
     * @return List of entities of type <code>T</code>.
     * @throws ConnectorSDKException
     */
    private <T extends BaseModel> List<T> getEntities(String entityEndpoint,
                                                      String imsOrg,
                                                      String sandboxName,
                                                      String authToken,
                                                      Map<String, String> params,
                                                      CatalogAPIStrategy strategy,
                                                      boolean isFlat,
                                                      Class<T> clazz) throws URISyntaxException, ConnectorSDKException, ParseException {
        List<T> entities = new ArrayList<>();
        URIBuilder builder = new URIBuilder(entityEndpoint);
        addParam(builder, params);
        HttpGet request = new HttpGet(builder.build());
        httpClientUtil.addHeader(request, authToken, imsOrg, sandboxName, SDKConstants.CONNECTION_HEADER_JSON_CONTENT);
        String response = httpClientUtil.execute(request);
        JSONParser parser = new JSONParser();
        JSONObject jsonObject = (JSONObject) parser.parse(response);
        if (!jsonObject.isEmpty()) {
            if(isFlat) {
                entities.add(getNewInstance(clazz, jsonObject));
            } else {
                for (Object key : jsonObject.keySet()) {
                    String entityId = (String) key;
                    JSONObject jdata = (JSONObject) jsonObject.get(entityId);
                    jdata.put(SDKConstants.CATALOG_ID, entityId);
                    jdata.put(SDKConstants.CATALOG_SANDBOX_NAME, sandboxName);
                    entities.add(getNewInstance(clazz, jdata));
                }
            }
        }
        if (checkForRecursiveAPICall(strategy, entities)) {
            updateOffsetsForNextAPICall(params);
            entities.addAll(getEntities(entityEndpoint, imsOrg, sandboxName, authToken, params, strategy, false, clazz));
        }
        return entities;
    }

    private <T extends BaseModel> T getNewInstance(Class<T> clazz, JSONObject jObj) throws ConnectorSDKException {
        try {
            return (T) clazz.newInstance().build(jObj);
        } catch (InstantiationException e) {
            logger.log(Level.SEVERE, "Instantiation exception for new object of type " + clazz + " :: " + e.getMessage());
            throw new ConnectorSDKException(e.getMessage(), e.getCause());
        } catch (IllegalAccessException e) {
            logger.log(Level.SEVERE, "Illegal access exception for new object of type " + clazz + " :: " + e.getMessage());
            throw new ConnectorSDKException(e.getMessage(), e.getCause());
        }
    }

    private void updateOffsetsForNextAPICall(Map<String, String> params) {
        if (params != null) {
            int currentOffset = Integer.parseInt(params.getOrDefault(SDKConstants.CATALOG_QUERY_PARAM_OFFSET, "0"));
            int newOffSet = currentOffset + SDKConstants.CATALOG_MAX_LIMIT_PER_API_CALL;
            params.put(SDKConstants.CATALOG_QUERY_PARAM_OFFSET, String.valueOf(newOffSet));
        }
    }

    private void addParam(URIBuilder builder, Map<String, String> params) {
        if (params != null) {
            Iterator<String> itr = params.keySet().iterator();
            while (itr.hasNext()) {
                String key = itr.next();
                builder.setParameter(key, params.get(key));
            }
        }
    }

    private boolean checkForRecursiveAPICall(CatalogAPIStrategy strategy, List<? extends BaseModel> objects) {
        if (strategy.equals(CatalogAPIStrategy.REPEATED)) {
            // Check if current size is less than max objects in 1 API call.
            return objects.size() == SDKConstants.CATALOG_MAX_LIMIT_PER_API_CALL ? true : false;
        } else {
            return false;
        }
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public List<SchemaField> getSchemaFields(String imsOrg, String sandboxName, String authToken, String schemaPath
            , boolean useFlatNamesForLeafNodes) throws ConnectorSDKException {
        List<SchemaField> schemaFields = new ArrayList<SchemaField>();
        try {
            String catalogURI = this._endpoint + schemaPath + "?expansion=xdm";
            schemaFields = getEntity(catalogURI, imsOrg, sandboxName, authToken, schemaPath, true, Schema.class)
                    .getSchemaFields(useFlatNamesForLeafNodes);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error while fetching schema for schema Id :" + e.getMessage());
            throw new ConnectorSDKException(e.getMessage(), e.getCause());
        }
        return schemaFields;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Batch pollForBatchProcessingCompletion(String imsOrg, String sandboxName, String accessToken, String batchId)
            throws ConnectorSDKException {
        ConditionFactory conditionFactory = with()
                .pollInterval(SLEEP_TIMEOUT, MILLISECONDS)
                .conditionEvaluationListener(condition -> logger.log(Level.SEVERE, "[{0}] (elapsed time [{1}]ms, remaining time [{2}]ms)\n",
                        new Object[]{"Check Batch status",
                                condition.getElapsedTimeInMS(),
                                condition.getRemainingTimeInMS()})
                )
                .given()
                .await()
                .atMost(API_CALL_TIMEOUT, TimeUnit.MINUTES);

        Batch batch;
        try {
            batch = conditionFactory.untilCall(
                    to(this).getBatchByBatchId(imsOrg, sandboxName, accessToken, batchId),
                    Matchers.hasProperty("status",
                            Matchers.anyOf(
                                    Matchers.equalTo(SDKConstants.CATALOG_BATCH_STATUS_ACTIVE),
                                    Matchers.equalTo(SDKConstants.CATALOG_BATCH_STATUS_FAILED),
                                    Matchers.equalTo(SDKConstants.CATALOG_BATCH_STATUS_SUCCESS),
                                    Matchers.equalTo(SDKConstants.CATALOG_BATCH_STATUS_FAILURE)
                            )
                    ));
        } catch (ConditionTimeoutException e) {
            logger.log(Level.SEVERE, "Batch {} does not have status success/failure after {} minutes", new Object[]{batchId, API_CALL_TIMEOUT});
            throw new ConnectorSDKException(
                    String.format("Batch %s does not have status success/failure after %d minutes", batchId, API_CALL_TIMEOUT),
                    e);
        }
        return batch;
    }
}