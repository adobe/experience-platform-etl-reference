
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
package com.adobe.platform.ecosystem.examples.data.access.api;

/**
 * Created by vedhera on 10/09/2017.
 */

import com.adobe.platform.ecosystem.examples.data.access.model.DataAccessFileEntity;
import com.adobe.platform.ecosystem.examples.data.access.model.DataSetFileProcessingEntity;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;

import java.util.List;

/**
 * Interface to talk to data
 * access API's.
 */
public interface DataAccessService {
    /**
     * Lists all file entities present
     * in a datasetfile path(folderName).
     * @param imsOrg ImsOrg
     * @param accessToken Access Token
     * @param dataSetFileId DataSetFile id in Catalog metastore.
     * @return List<{@link DataSetFileProcessingEntity}> List of files present under one <code>dataSetFileId</code>.
     * @throws ConnectorSDKException
     */
    List<DataSetFileProcessingEntity> getDataSetFileEntries(String imsOrg, String accessToken, String dataSetFileId) throws ConnectorSDKException;

    /**
     * Lists all dataSetFiles referenced through catalog batch - <code>batchId</code>
     * @param imsOrg ImsOrg
     * @param accessToken Access Token
     * @param batchId batchId id in Catalog metastore.
     * * @return List<{@link DataAccessFileEntity}> List of files linked with <code>batchId</code>
     * @throws ConnectorSDKException
     */
    List<DataAccessFileEntity> getDataSetFilesFromBatchId(String imsOrg, String accessToken, String batchId) throws ConnectorSDKException;
}