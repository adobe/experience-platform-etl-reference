
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
package com.adobe.platform.ecosystem.examples.data.ingestion.factory;

/**
 * Created by vardgupt on 10/09/2017.
 */

import com.adobe.platform.ecosystem.examples.util.ConnectorSDKUtil;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;
import com.adobe.platform.ecosystem.examples.util.ResourceName;
import org.apache.http.client.HttpClient;

import com.adobe.platform.ecosystem.examples.data.ingestion.api.DataIngestionService;
import com.adobe.platform.ecosystem.examples.data.ingestion.impl.DataIngestionServiceImpl;

/**
 * Factory to provide DataIngestionService
 * impl.
 */
public class DataIngestionServiceFactory {

    public static DataIngestionService getDataIngestionService(HttpClient httpClient) throws ConnectorSDKException{
        String endpoint = ConnectorSDKUtil.getInstance().getEndPoint(ResourceName.DATA_INGESTION);
        return new DataIngestionServiceImpl(endpoint, httpClient);
    }
}