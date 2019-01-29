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

import com.adobe.platform.ecosystem.examples.schemaregistry.api.SchemaRegistryService;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKUtil;
import com.adobe.platform.ecosystem.examples.util.ResourceName;
import org.apache.http.client.HttpClient;

/**
 * Factory for obtaining SchemaRegistryService
 *
 * @author shesriva on 23/01/2019.
 * */
public class SchemaRegistryFactory {
    public static SchemaRegistryService getSchemaRegistryService(HttpClient httpClient) throws ConnectorSDKException {
        ConnectorSDKUtil adobeResourceUtil = ConnectorSDKUtil.getInstance();
        String schemaRegistryEndPoint = adobeResourceUtil.getEndPoint(ResourceName.SCHEMA_REGISTRY);
        return new SchemaRegistryServiceImpl(schemaRegistryEndPoint, httpClient);
    }

    public static SchemaRegistryService getSchemaRegistryService() throws ConnectorSDKException{
        return getSchemaRegistryService(null);
    }
}
