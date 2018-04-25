
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
package com.adobe.platform.ecosystem.examples.authentication;

import com.adobe.platform.ecosystem.examples.catalog.api.CatalogService;
import com.adobe.platform.ecosystem.examples.catalog.impl.CatalogAPIStrategy;
import com.adobe.platform.ecosystem.examples.catalog.impl.CatalogFactory;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;

import java.util.HashMap;

/**
 * Created by nidhi on 22/9/17.
 */
public class ETLAuthConnection {

    /**
     * This API checks of the connection is successful with the connection parameters
     * provided by the client during ETL connection initialization
     * @param accessToken
     * @param imsOrg
     * @throws ConnectorSDKException
     */
    public void validateConnection(String accessToken, String imsOrg)
            throws ConnectorSDKException{
        //fetch batches from Catalog to test the validitity of connection credentials
        try {
            CatalogService catalogService = CatalogFactory.getCatalogService();
            catalogService.getBatches(imsOrg, accessToken, new HashMap<>(), CatalogAPIStrategy.ONCE);
        }catch (Exception ex){
            throw new ConnectorSDKException("Please check the connection parameters imsOrg, catalog end point, access token : " + ex.getMessage(), ex);
        }
    }


}