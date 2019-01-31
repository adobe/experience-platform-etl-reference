/*
 * ADOBE CONFIDENTIAL
 * __________________
 * Copyright 2019 Adobe Systems Incorporated
 * All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Adobe Systems Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to Adobe Systems Incorporated and its
 * suppliers and are protected by all applicable intellectual property laws,
 * including trade secret and copyright laws.
 *
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Adobe Systems Incorporated.
 *
 *
 */
package com.adobe.platform.ecosystem.examples.schemaregistry;

import com.adobe.platform.ecosystem.examples.catalog.impl.CatalogAPIStrategy;
import com.adobe.platform.ecosystem.examples.catalog.impl.CatalogServiceImpl;
import com.adobe.platform.ecosystem.examples.catalog.model.BaseModel;
import com.adobe.platform.ecosystem.examples.catalog.model.Schema;
import com.adobe.platform.ecosystem.examples.catalog.model.SchemaField;
import com.adobe.platform.ecosystem.examples.catalog.model.SchemaRef;
import com.adobe.platform.ecosystem.examples.constants.SDKConstants;
import com.adobe.platform.ecosystem.examples.schemaregistry.api.SchemaRegistryService;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;
import com.adobe.platform.ecosystem.examples.util.HttpClientUtil;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * SchemaRegistry service for calling
 * SchemaRegistry APIs.
 *
 * @author shesriva on 23/01/2019.
 * */
public class SchemaRegistryServiceImpl implements SchemaRegistryService {

    private String _endpoint;

    private HttpClientUtil httpClientUtil;

    private static Logger logger = Logger.getLogger(CatalogServiceImpl.class.getName());

    protected SchemaRegistryServiceImpl(String endpoint, HttpClient httpClient) throws ConnectorSDKException {
        this._endpoint = endpoint;
        HttpClient hClient = httpClient == null ? HttpClientUtil.getHttpClient() : httpClient;
        httpClientUtil = new HttpClientUtil(hClient);
    }

    protected SchemaRegistryServiceImpl(String endpoint) throws ConnectorSDKException {
        this(endpoint, HttpClientUtil.getHttpClient());
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

    private <T extends BaseModel> T getNewInstance(Class<T> clazz, JSONObject jObj) throws ConnectorSDKException {
        try {
            return (T) clazz.newInstance().build(jObj);
        } catch (InstantiationException e) {
            logger.log(Level.SEVERE, "Instantiation exception for new object of type " + clazz + " :: " + e.getMessage());
            throw new ConnectorSDKException(e.getMessage(), e);
        } catch (IllegalAccessException e) {
            logger.log(Level.SEVERE, "Illegal access exception for new object of type " + clazz + " :: " + e.getMessage());
            throw new ConnectorSDKException(e.getMessage(), e);
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

    private <T extends BaseModel> T getEntity(String entityEndpoint,
                                              String imsOrg,
                                              String authToken,
                                              String metaAltId,
                                              String contentType,
                                              boolean isFlat,
                                              Class<T> clazz) throws ConnectorSDKException, ParseException, URISyntaxException {
        T entity = getEntities(entityEndpoint,
                imsOrg,
                authToken,
                contentType,
                new HashMap<>(),
                CatalogAPIStrategy.ONCE,
                isFlat,
                clazz).get(0);
        if (!isFlat && !getMetaAltId(entity.getId()).equals(metaAltId)) {
            throw new ConnectorSDKException("Meta Alt id fetched from SchemaRegistry does not equal " + metaAltId + " for class: " + clazz);
        }
        return entity;
    }

    private <T extends BaseModel> List<T> getEntities(String entityEndpoint,
                                                      String imsOrg,
                                                      String authToken,
                                                      String contentType,
                                                      Map<String, String> params,
                                                      CatalogAPIStrategy strategy,
                                                      boolean isFlat,
                                                      Class<T> clazz) throws URISyntaxException, ConnectorSDKException, ParseException {
        List<T> entities = new ArrayList<>();
        URIBuilder builder = new URIBuilder(entityEndpoint);
        addParam(builder, params);
        HttpGet request = new HttpGet(builder.build());
        httpClientUtil.addHeader(request, authToken, imsOrg, contentType);
        String response = httpClientUtil.execute(request);
        JSONParser parser = new JSONParser();
        JSONObject jsonObject = (JSONObject) parser.parse(response);
        if (!jsonObject.isEmpty()) {
            if (isFlat) {
                entities.add(getNewInstance(clazz, jsonObject));
            } else {
                for (Object key : jsonObject.keySet()) {
                    String entityId = (String) key;
                    JSONObject jdata = (JSONObject) jsonObject.get(entityId);
                    jdata.put(SDKConstants.CATALOG_ID, entityId);
                    entities.add(getNewInstance(clazz, jdata));
                }
            }

        }
        if (checkForRecursiveAPICall(strategy, entities)) {
            //updateOffsetsForNextAPICall(params);
            entities.addAll(getEntities(entityEndpoint, imsOrg, authToken, contentType, params, strategy, false, clazz));
        }
        return entities;
    }

    /*
    "$id": "https://ns.adobe.com/workshop/schemas/26e80bdbc444f2198f57ed614427229a",
    "meta:altId": "_workshop.schemas.26e80bdbc444f2198f57ed614427229a"
    */
    private String getMetaAltId(String schemaRefId)
        throws ConnectorSDKException {
        try {
            return "_" + schemaRefId.split("ns.adobe.com/")[1].replace("/", ".");
        } catch(Exception e) {
            logger.log(Level.SEVERE, "Error while constructing metaAltId for schema Id :" + e.getMessage());
            throw new ConnectorSDKException(e.getMessage(), e.getCause());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<SchemaField> getSchemaFields(String imsOrg, String authToken, SchemaRef schemaRef, boolean useFlatNamesForLeafNodes)
    throws ConnectorSDKException {
        List<SchemaField> schemaFields = new ArrayList<>();
        try {
            String metaAltId = getMetaAltId(schemaRef.getId());
            String schemaRegistryURI = this._endpoint + "tenant/schemas/" + metaAltId;

            schemaFields = getEntity(
                    schemaRegistryURI,
                    imsOrg,
                    authToken,
                    metaAltId,
                    getContentType(schemaRef.getContentType()),
                    true,
                    Schema.class
            ).getSchemaFields(useFlatNamesForLeafNodes);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error while fetching schema for schema Id :" + e.getMessage());
            throw new ConnectorSDKException(e.getMessage(), e.getCause());
        }
        return schemaFields;
    }

    /*
    rawContentType - application/vnd.adobe.xed+json; version=1
    contentType - application/vnd.adobe.xed-full+json; version=1
    */
    private String getContentType(String rawContentType) {
        return String.format("%sxed-full%s", rawContentType.split("xed")[0], rawContentType.split("xed")[1]);
    }
}
