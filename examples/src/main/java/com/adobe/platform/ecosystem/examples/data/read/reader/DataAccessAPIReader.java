
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
package com.adobe.platform.ecosystem.examples.data.read.reader;

/**
 * Created by vedhera on 10/09/2017.
 */

import com.adobe.platform.ecosystem.examples.parquet.read.ParquetIOReader;
import com.adobe.platform.ecosystem.examples.catalog.api.CatalogService;
import com.adobe.platform.ecosystem.examples.catalog.impl.CatalogAPIStrategy;
import com.adobe.platform.ecosystem.examples.catalog.model.Batch;
import com.adobe.platform.ecosystem.examples.catalog.model.DataSetFile;
import com.adobe.platform.ecosystem.examples.constants.SDKConstants;
import com.adobe.platform.ecosystem.examples.data.access.api.DataAccessService;
import com.adobe.platform.ecosystem.examples.data.access.model.DataAccessFileEntity;
import com.adobe.platform.ecosystem.examples.data.access.model.DataSetFileProcessingEntity;
import com.adobe.platform.ecosystem.examples.data.read.ReadAttributes;
import com.adobe.platform.ecosystem.examples.data.read.Reader;
import com.adobe.platform.ecosystem.examples.data.read.reader.processor.api.ReaderProcessor;
import com.adobe.platform.ecosystem.examples.data.read.reader.processor.impl.CSVReaderProcessor;
import com.adobe.platform.ecosystem.examples.data.read.reader.processor.impl.JSONReaderProcessor;
import com.adobe.platform.ecosystem.examples.data.read.reader.processor.impl.ParquetReaderProcessor;
import com.adobe.platform.ecosystem.examples.data.wiring.DataWiringParam;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKUtil;
import com.adobe.platform.ecosystem.examples.util.HttpClientUtil;
import org.apache.http.client.HttpClient;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Reader which queries Data Access APIs
 * for reading data for any underlying
 * data source.
 */
public class DataAccessAPIReader implements Reader {
    private ReaderProcessor processor;

    private static final Logger logger = Logger.getLogger(DataAccessAPIReader.class.getName());
    private static final String DEFAULT_DATASETFILE_COUNT_KEY = "defaultDataSetFileReadCount";

    public DataAccessAPIReader(
            CatalogService cs,
            DataAccessService das,
            DataWiringParam param,
            HttpClient httpClient,
            ParquetIOReader parquetIOReader,
            ReadAttributes readAttr
    ) throws ConnectorSDKException {
        if(httpClient == null) {
            httpClient = HttpClientUtil.getHttpClient();
        }
        initReader(cs,das,param,httpClient,parquetIOReader,readAttr);
    }

    private void initReader(
            CatalogService cs,
            DataAccessService das,
            DataWiringParam param,
            HttpClient httpClient,
            ParquetIOReader parquetIOReader,
            ReadAttributes readAttr
    ) throws ConnectorSDKException {
        int dataSetFileReadIndex = 0;
        List<String> dataSetFileSet = initDataSetFileSet(cs,das,param,readAttr);

        List<DataSetFileProcessingEntity> dataSetFileProcessingSet;
        if(dataSetFileSet == null || dataSetFileSet.isEmpty() ) {
            dataSetFileProcessingSet = new ArrayList<>();

        } else {
            dataSetFileProcessingSet = initDataSetFileProcessingSet(das,param,dataSetFileSet.get(dataSetFileReadIndex));
        }

        // TODO: @vedhera Logic to be updated to read format from Catalog.
        if(!dataSetFileProcessingSet.isEmpty()) {
            String fileName = dataSetFileProcessingSet.get(0).getName();
            if (fileName.endsWith(".csv") || fileName.endsWith(".txt")) {
                processor = new CSVReaderProcessor(das, httpClient, param, dataSetFileSet, dataSetFileProcessingSet);
            } else if (fileName.endsWith(".parquet")) {
                processor = new ParquetReaderProcessor(parquetIOReader, das, httpClient, param, dataSetFileSet, dataSetFileProcessingSet);
            } else if (fileName.endsWith(".json")) {
                processor = new JSONReaderProcessor(das, httpClient, param, dataSetFileSet, dataSetFileProcessingSet);
            } else {
                throw new ConnectorSDKException("datasetFile processing entity format is unsupported for type: " + fileName);
            }
        } else {
            processor = new CSVReaderProcessor(das, httpClient, param, dataSetFileSet, dataSetFileProcessingSet);
        }
    }

    @Override
    public JSONArray read(int rows) throws ConnectorSDKException {
        List<JSONObject> data = processData(rows);
        JSONArray res = new JSONArray();
        res.addAll(data);
        return res;
    }

    @Override
    public Integer getErrorRowCount() {
        return processor.getErrorRecordCount();
    }

    @Override
    public Boolean hasMoreData() throws ConnectorSDKException {
        return processor.hasMoreData();
    }

    private List<JSONObject> processData(int rows) throws ConnectorSDKException {
        return processor.processData(rows);
    }

    private List<DataSetFileProcessingEntity> initDataSetFileProcessingSet(DataAccessService das,DataWiringParam param,String dataSetFileId) throws ConnectorSDKException {
        return das.getDataSetFileEntries(param.getImsOrg(),param.getSandboxName(),param.getAuthToken(),dataSetFileId);
    }

    private List<String> initDataSetFileSet(CatalogService cs, DataAccessService das, DataWiringParam param,ReadAttributes readAttributes) throws ConnectorSDKException {
        List<String> dataSetFileSet = new ArrayList<>();

        if(readAttributes != null) {
            Map<String, String> params = new HashMap<>();
            if(readAttributes.getEpochStartDate() != null && !readAttributes.getEpochStartDate().isEmpty()
                    && readAttributes.getDuationInMillis() != null && !readAttributes.getDuationInMillis().isEmpty()) {
                long epochTime = Long.parseLong(readAttributes.getEpochStartDate());
                long durationInInt = Long.parseLong(readAttributes.getDuationInMillis());
                long createdBefore = epochTime + durationInInt;
                params.put(SDKConstants.CATALOG_CREATED_AFTER_KEY,readAttributes.getEpochStartDate());
                params.put(SDKConstants.CATALOG_CREATED_BEFORE_KEY, String.valueOf(createdBefore));
                params.put(SDKConstants.CATALOG_QUERY_PARAM_LIMIT, String.valueOf(SDKConstants.CATALOG_MAX_LIMIT_PER_API_CALL));
                params.put(SDKConstants.CATALOG_QUERY_PARAM_OFFSET, "0");
                params.put(SDKConstants.CATALOG_QUERY_PARAM_SORT, SDKConstants.CATALOG_QUERY_PARAM_DESC_CREATED);
            }

            for(Batch batch : cs.getBatches(param.getImsOrg(),param.getSandboxName(),param.getAuthToken(),params,
                    CatalogAPIStrategy.REPEATED)){
                for(DataAccessFileEntity fileEntity : das.getDataSetFilesFromBatchId(param.getImsOrg(),param.getSandboxName(),
                        param.getAuthToken() ,batch.getId())) {
                    // This check is because the batch information in Catalog
                    // does not have dataSet/dataSetView information. So we might
                    // be fetching batches from different dataSet id with a given time and epoch range.
                    if(fileEntity.getDataSetViewId().equals(param.getDataSet().getViewId())) {
                        dataSetFileSet.add(fileEntity.getDataSetFileId());
                    }
                }
            }
        } else {
            int fileCount = Integer.parseInt(ConnectorSDKUtil.getInstance().getOrDefaultConfigValue(DEFAULT_DATASETFILE_COUNT_KEY));
            for(DataSetFile dsf : cs.getDataSetFiles(param.getImsOrg(), param.getSandboxName(), param.getAuthToken(),
                    getQueryParam(param.getDataSet().getViewId(), fileCount), CatalogAPIStrategy.ONCE)) {
                dataSetFileSet.add(dsf.getId());
            }
        }
        return dataSetFileSet;
    }

    /**
     * Creates query parameter map for
     * querying dataSetFiles from Catalog.
     * @param viewId
     * @param fileCount
     * @return
     */
    private Map<String,String> getQueryParam(String viewId, int fileCount) {
        Map<String,String> params = new HashMap<>();
        params.put(SDKConstants.CATALOG_QUERY_PARAM_SORT, SDKConstants.CATALOG_QUERY_PARAM_DESC_CREATED);
        params.put(SDKConstants.CATALOG_QUERY_PARAM_LIMIT, String.valueOf(fileCount));
        params.put("dataSetViewId", viewId);
        return params;
    }
}