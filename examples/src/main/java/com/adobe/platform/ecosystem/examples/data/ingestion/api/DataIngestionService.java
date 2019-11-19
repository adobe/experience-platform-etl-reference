
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
package com.adobe.platform.ecosystem.examples.data.ingestion.api;

import com.adobe.platform.ecosystem.examples.data.FileFormat;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;
import org.json.simple.JSONObject;


/**
 * Created by vardgupt on 10/09/2017.
 */


/**
 * Interface to talk to data ingestion API's
 */
public interface DataIngestionService {
    /**
     * This method is used to get batchID which is
     * required as param for uploading nay file in
     * Data Ingestion Service API.
     *
     * @param imsOrg IMS Org in context.
     * @param sandboxName x-sandbox-name in context.
     * @param accessToken accessToken
     * @param payload payload for object.
     * @return returns the created batch Id.
     */

    String getBatchId(String imsOrg, String sandboxName, String accessToken, JSONObject payload) throws ConnectorSDKException;

    /**
     * This method is used to upload file via
     * Data Ingestion Service API.
     *
     * @param batchId
     * @param dataSetId
     * @param imsOrg
     * @param sandboxName
     * @param accessToken
     * @param fileFormat
     * @param buffer
     * @return
     */

    int writeToBatch(String batchId, String dataSetId, String imsOrg, String sandboxName, String accessToken, FileFormat fileFormat, byte[] buffer) throws ConnectorSDKException;

    /**
     * This method is used to signal completion
     * of batch uploads.
     * @param batchId
     * @param imsOrg
     * @param sandboxName
     * @param accessToken
     * @return
     */
    int signalBatchCompletion(String batchId, String imsOrg, String sandboxName, String accessToken) throws ConnectorSDKException;
}