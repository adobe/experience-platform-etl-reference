
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
package com.adobe.platform.ecosystem.examples.data.read.reader.processor.api;

/**
 * Created by vedhera on 11/18/2017.
 */

import com.adobe.platform.ecosystem.examples.constants.SDKConstants;
import com.adobe.platform.ecosystem.examples.data.access.api.DataAccessService;
import com.adobe.platform.ecosystem.examples.data.access.model.DataSetFileProcessingEntity;
import com.adobe.platform.ecosystem.examples.data.read.reader.processor.impl.CSVReaderProcessor;
import com.adobe.platform.ecosystem.examples.data.wiring.DataWiringParam;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;
import org.apache.http.HttpResponse;
import org.json.simple.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Abstract class to provide interfaces which needs
 * to be implemented for different type of processors
 * required for different data types.
 *
 * For eg: {@link com.adobe.platform.ecosystem.examples.data.read.reader.processor.impl.CSVReaderProcessor}
 */
abstract public class ReaderProcessor {
    private final DataAccessService das;
    private final DataWiringParam param;
    protected final List<String> dataSetFileSet;
    protected List<DataSetFileProcessingEntity> dataSetFileProcessingSet;
    protected int dataSetFileReadIndex = -1;
    protected int processingSetReadIndex = -1;

    private static final int DEFAULT_ERROR_COUNT = 0;
    private static final Logger logger = Logger.getLogger(ReaderProcessor.class.getName());

    protected ReaderProcessor(DataAccessService das, DataWiringParam param, List<String> dataSetFileSet, List<DataSetFileProcessingEntity> dataSetFileProcessingSet) {
        this.das = das;
        this.param = param;
        this.dataSetFileSet = dataSetFileSet;
        this.dataSetFileProcessingSet = dataSetFileProcessingSet;
        initProcessor();
    }

    private void initProcessor() {
        dataSetFileReadIndex = 0;
        processingSetReadIndex = 0;
    }

    /**
     * Abstract method needs to be implemented to
     * read {@code rows} number of rows from source.
     *
     * @param rows Number of rows required by client.
     * @return List of json objects, each object corresponding to 1 row of CSV record.
     * @throws ConnectorSDKException
     */
    abstract public List<JSONObject> processData(int rows) throws ConnectorSDKException;

    /**
     * Abstract method needs to be implemented to
     * provide information if underlying source
     * as more records.
     *
     * @return returns {@code true} or {@code false}
     * @throws ConnectorSDKException
     */
    abstract public Boolean hasMoreData() throws ConnectorSDKException;

    /**
     * Gives default inplementation for error row count
     * while reading from source.
     * Currently extended by {@link CSVReaderProcessor#getErrorRecordCount()}
     *
     * @return returns default error count
     */
    public Integer getErrorRecordCount(){
        return DEFAULT_ERROR_COUNT;
    }

    protected void prepareAndCheckEntitiesIndex() throws ConnectorSDKException {
        // Try to increase the entity counter.
        processingSetReadIndex++;

        // Increment to read entities of next datasetfile if
        // all entities of current DSF have exhausted.
        if(dataSetFileProcessingSet.size() == 0 || processingSetReadIndex == dataSetFileProcessingSet.size()) {
            processingSetReadIndex = 0;
            dataSetFileReadIndex++;
        }

        if(dataSetFileReadIndex < dataSetFileSet.size()){
            initDataSetFileProcessingSet(dataSetFileSet.get(dataSetFileReadIndex));
        }
    }

    private void initDataSetFileProcessingSet(String dataSetFileId) throws ConnectorSDKException {
        dataSetFileProcessingSet = das.getDataSetFileEntries(param.getImsOrg(),param.getAuthToken(),dataSetFileId);
    }

    protected void checkErrorResponseCode(HttpResponse response) throws ConnectorSDKException {
        StringBuffer result = new StringBuffer();
        int responseCode = response.getStatusLine().getStatusCode();
        logger.log(Level.FINE, "Response : " + responseCode);
        // Handling Non-Success codes
        try {
            if (responseCode != 200 && responseCode != 201) {
                if(response.getEntity() != null && response.getEntity().getContent() != null){
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(
                            response.getEntity().getContent(), SDKConstants.ENCODING_UTF8));
                    String line = "";
                    while ((line = bufferedReader.readLine()) != null) {
                        result.append(line);
                    }
                }
                throw new ConnectorSDKException("Error code : "
                        + response.getStatusLine().getStatusCode() + ", response : " + result);
            }
        } catch (IOException ex) {
            String msg = "Error while reading entity and error code from response: ";
            logger.severe(msg + ex);
            throw new ConnectorSDKException(msg,ex);
        }
    }
}