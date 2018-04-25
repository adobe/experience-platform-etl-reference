
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

import com.adobe.platform.ecosystem.examples.util.ConnectorSDKUtil;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;
import com.adobe.platform.ecosystem.examples.util.ResourceName;
import org.apache.http.client.HttpClient;

import com.adobe.platform.ecosystem.examples.catalog.api.CatalogService;

/**
 * Created by vedhera on 8/25/2017.
 */

/**
 * Factory to provide Catalog
 * service implementation.
 */
public class CatalogFactory {

    public static CatalogService getCatalogService(HttpClient httpClient) throws ConnectorSDKException {
        ConnectorSDKUtil adobeResourceUtil = ConnectorSDKUtil.getInstance();
        String catalogEndpoint = adobeResourceUtil.getEndPoint(ResourceName.CATALOG);
        return new CatalogServiceImpl(catalogEndpoint, httpClient);
    }


    public static CatalogService getCatalogService() throws ConnectorSDKException{
        return getCatalogService(null);
    }
}