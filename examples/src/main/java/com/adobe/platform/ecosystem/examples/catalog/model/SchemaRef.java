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
package com.adobe.platform.ecosystem.examples.catalog.model;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;
import com.adobe.platform.ecosystem.examples.util.JsonUtil;
import org.json.simple.JSONObject;

import static com.adobe.platform.ecosystem.examples.constants.SDKConstants.CATALOG_SCHEMA_REF_CONTENT_TYPE;
import static com.adobe.platform.ecosystem.examples.constants.SDKConstants.CATALOG_SCHEMA_REF_ID;

/**
 * Pojo for storing schemaRef details in dataset
 *
 * @author shesriva on 01/14/2019.
 * */
public class SchemaRef {
    private String id;

    private String contentType;

    public SchemaRef(JSONObject schemaRef) {
        this.id = JsonUtil.getString(schemaRef, CATALOG_SCHEMA_REF_ID);
        this.contentType = JsonUtil.getString(schemaRef, CATALOG_SCHEMA_REF_CONTENT_TYPE);
    }

    public SchemaRef(String id, String contentType){
        this.id = id;
        this.contentType = contentType;
    }

    public String getId() {
        return id;
    }

    public SchemaRef setId(String id) {
        this.id = id;
        return this;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    @Override
    public String toString() {
        return "SchemaRef{" +
                "id='" + id + '\'' +
                ", contentType='" + contentType + '\'' +
                '}';
    }
}