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
package com.adobe.platform.ecosystem.examples.schemaregistry.api;

import com.adobe.platform.ecosystem.examples.catalog.model.SchemaField;
import com.adobe.platform.ecosystem.examples.catalog.model.SchemaRef;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;

import java.util.List;

/**
 * XDMRegistryService interface.
 *
 * @author shesriva on 23/01/2019.
 * */
public interface SchemaRegistryService {

    /**
     * Fetch Schema from XDM Registry
     *
     * @param imsOrg                   imsOrg in context.
     * @param authToken                authToken
     * @param schemaRef                schemaRef
     * @param useFlatNamesForLeafNodes flag for flattening names at leaf nodes.
     * @return List of SchemaFields.
     * @throws ConnectorSDKException
     */
    List<SchemaField> getSchemaFields(String imsOrg, String authToken, SchemaRef schemaRef, boolean useFlatNamesForLeafNodes) throws ConnectorSDKException;
}
