
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
package com.adobe.platform.ecosystem.examples.catalog.api;

/**
 * Created by vedhera on 8/30/2017.
 */

import com.adobe.platform.ecosystem.examples.catalog.impl.CatalogAPIStrategy;
import com.adobe.platform.ecosystem.examples.catalog.model.*;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;
import org.json.simple.JSONObject;

import java.util.List;
import java.util.Map;

/**
 * Interface for defining API's to interact with Catalog and fetch various
 * entities.
 */
public interface CatalogService {

    /**
     * Fetch data sets from catalog
     * @param imsOrg imsOrg in context.
     * @param authToken authToken
     * @param params params
     * @param strategy strategy
     * @return List of Datasets.
     * @throws ConnectorSDKException
     */
    List<DataSet> getDataSets(String imsOrg, String authToken, Map<String, String> params, CatalogAPIStrategy strategy) throws ConnectorSDKException;

    /**
     * Get data set corresponding to the given connection ID
     * @param imsOrg
     * @param authToken
     * @param connectionId
     * @return
     * */
    Connection getConnection(String imsOrg, String authToken, String connectionId) throws ConnectorSDKException ;

    /**
     * Get data set corresponding to the given <code>dataSetId</code>
     * @param imsOrg
     * @param authToken
     * @param dataSetId
     * @return
     * @throws ConnectorSDKException
     */
    DataSet getDataSet(String imsOrg, String authToken, String dataSetId) throws ConnectorSDKException;

    /**
     * Create batch in catalog for the payload
     * @param imsOrg
     * @param authToken
     * @param payload
     * @return
     */
    Batch createBatch(String imsOrg, String authToken, JSONObject payload) throws ConnectorSDKException;

    /**
     * Get batch from the catalog for the given batchId
     * @param imsOrg
     * @param authToken
     * @param batchId
     * @return
     */
    Batch getBatchByBatchId(String imsOrg, String authToken,
            String batchId) throws ConnectorSDKException;

    /**
     * Gets latest {@code count} number of DataSetfiles
     * from Catalog.
     * @param imsOrg
     * @param authToken
     * @param params
     * @param strategy
     * @return
     * @throws ConnectorSDKException
     */
    List<DataSetFile> getDataSetFiles(String imsOrg, String authToken, Map<String, String> params, CatalogAPIStrategy strategy) throws ConnectorSDKException;

    /**
     * Gets DataSetView metadata from Catalog
     * for {@code viewId} datasetview id.
     * @param imsOrg
     * @param authToken
     * @param viewId
     * @return
     * @throws ConnectorSDKException
     */
    DataSetView getDataSetView(String imsOrg, String authToken, String viewId) throws ConnectorSDKException;

    /**
     * Get's list of {@link Batch} from Catalog given
     * time range parameters.
     * @param imsOrg
     * @param authToken
     * @param params
     * @param strategy
     * @return
     * @throws ConnectorSDKException
     */
    List<Batch> getBatches(String imsOrg, String authToken, Map<String, String> params, CatalogAPIStrategy strategy) throws ConnectorSDKException;
}