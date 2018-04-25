
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
package com.adobe.platform.ecosystem.examples.data.read.reader.processor.impl;

import com.adobe.platform.ecosystem.examples.constants.SDKConstants;
import com.adobe.platform.ecosystem.examples.data.access.api.DataAccessService;
import com.adobe.platform.ecosystem.examples.data.access.model.DataSetFileProcessingEntity;
import com.adobe.platform.ecosystem.examples.data.read.reader.processor.api.ReaderProcessor;
import com.adobe.platform.ecosystem.examples.data.wiring.DataWiringParam;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;
import com.adobe.platform.ecosystem.examples.util.HttpClientUtil;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.message.BasicHeader;
import org.json.simple.JSONObject;

import java.io.*;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Processor required for reading CSV files.
 * Implements {@link ReaderProcessor}
 */
public class CSVReaderProcessor extends ReaderProcessor {
    private final DataWiringParam param;
    private final HttpClientUtil httpClientUtil;

    private static final Logger logger = Logger.getLogger(CSVReaderProcessor.class.getName());

    // Data members to hold reading state.
    private long offset = 0;
    private long byteLength = 8 * 1024 * 1024; // Starting with 8 MB byte range.
    private long maxByteLength = -1; //This will be initialised to -1 when starting with a fresh processing entity.
    private String partialRecordBuffer = "";
    private List<JSONObject> recordsUnusedFromPreviousIteration;
    private int unusedRecordsPointer = -1;
    private CSVRecord headerFields;
    private int errorCount;

    public CSVReaderProcessor(DataAccessService das, HttpClient httpClient, DataWiringParam param, List<String> dataSetFileSet, List<DataSetFileProcessingEntity> dataSetFileProcessingSet) throws ConnectorSDKException {
        super(das,param,dataSetFileSet,dataSetFileProcessingSet);
        this.param = param;
        HttpClient hClient = httpClient == null ? HttpClientUtil.getHttpClient() : httpClient;
        this.httpClientUtil = new HttpClientUtil(hClient);
    }

    /**
     * {@inheritDoc}
     */
    public List<JSONObject> processData(int rows) throws ConnectorSDKException {
        List<JSONObject> data = new ArrayList<>();
        while(data.size() < rows && hasMoreData()) {
            processData(data, rows-data.size());
        }
        return data;
    }

    private List<JSONObject> processData(List<JSONObject> data, int rows) throws ConnectorSDKException {
        // Consume records from previous buffer.
        if(recordsUnusedFromPreviousIteration != null
                && unusedRecordsPointer != -1
                && unusedRecordsPointer < recordsUnusedFromPreviousIteration.size())
        {
            data.addAll(getRecordsFromUnusedRecords(rows));
        } else {
            if(!dataSetFileProcessingSet.isEmpty()) {
                data.addAll(readFromProcessingEntity(dataSetFileProcessingSet.get(processingSetReadIndex), rows));
            }
            // If current entity is exhausted and set to null then,
            // initialize with next processingEntity or next DSF.
            if(!checkIfNewOffSetIsPermissible()) {
                prepareAndCheckEntitiesIndex();
            }
        }
        return data;
    }

    private boolean checkIfNewOffSetIsPermissible() {
        if(maxByteLength == -1) {
            //This case should not be hit as we will always be setting maxByteLength after the first API call
            //Or if we have no dataset file entity
            return false;
        }
        offset = offset + byteLength + 1;
        if(offset >= maxByteLength) { // Offset can only equal to 1 less than the byte range as API supports 0th based indexing.
            offset = 0;
            maxByteLength = -1;
            return false;
        } else {
            return true;
        }
    }

    private List<JSONObject> getRecordsFromUnusedRecords(int rows) {
        List<JSONObject> records = new ArrayList<>();
        int count =0;
        for(int i=unusedRecordsPointer; i<recordsUnusedFromPreviousIteration.size() && count < rows; i++) {
            records.add(recordsUnusedFromPreviousIteration.get(i));
            count++;
        }

        unusedRecordsPointer = unusedRecordsPointer + rows;
        if(unusedRecordsPointer >= recordsUnusedFromPreviousIteration.size()) {
            unusedRecordsPointer = -1; // Resetting values as stored unused records are exhausted.
            recordsUnusedFromPreviousIteration = null;
        }

        return records;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Boolean hasMoreData() {
        if( recordsUnusedFromPreviousIteration != null
                && unusedRecordsPointer != -1
                && unusedRecordsPointer < recordsUnusedFromPreviousIteration.size()) {
            return true;
        } else if (maxByteLength != -1 && offset < maxByteLength) {
            return true;
        } else if(maxByteLength == -1 && dataSetFileReadIndex < dataSetFileSet.size()) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Overriding default implementation from parent class.
     *
     * @return number of rows that failed while reading from source.
     * This is a cumulative number for all the {@link CSVReaderProcessor#processData(int)}
     * call till now.
     */
    @Override
    public Integer getErrorRecordCount() {
        return errorCount;
    }

    private List<JSONObject> readFromProcessingEntity(DataSetFileProcessingEntity entity, int rows) throws ConnectorSDKException {
        HttpResponse response = null;
        try {
            URIBuilder builder = new URIBuilder(entity.getHref());
            HttpGet request = new HttpGet(builder.build());
            httpClientUtil.addHeader(request,param.getAuthToken(),param.getImsOrg(),SDKConstants.CONNECTION_HEADER_JSON_CONTENT);
            httpClientUtil.setHeader(request, getHttpHeaders());

            logger.log(Level.FINE,request.getRequestLine().getUri());
            response = httpClientUtil.executeRequest(request, false);

            String responseString = new BasicResponseHandler().handleResponse(response);

            // Set Max Byte Range limit if this the first API call
            if(maxByteLength == -1) {
                Header contentRangeHeader[] = response.getHeaders("Content-Range");
                if(contentRangeHeader!=null && contentRangeHeader.length>0) {
                    String value = contentRangeHeader[0].getValue();
                    maxByteLength = Long.parseLong(value.substring(value.lastIndexOf("/") + 1));
                }
            }

            if(offset == 0) {
                headerFields = null; // Initialising if First API call in series for a DataSetFile entry.
            }
            String newTotalRecordBuffer = partialRecordBuffer + responseString;
            partialRecordBuffer = "";
            return getRecords(newTotalRecordBuffer,rows);

        } catch(URISyntaxException uriEx) {
            logger.severe("Error in method readFromProcessingEntity while parsing URL: " + uriEx.getMessage());
            throw new ConnectorSDKException(uriEx.getMessage(), uriEx);
        } catch (IOException ioEx) {
            logger.severe("Error in method readFromProcessingEntity during IO operation: " + ioEx.getMessage());
            throw new ConnectorSDKException(ioEx.getMessage(), ioEx);
        } catch(Exception ex) {
            logger.severe("Generic error in method readFromProcessingEntity: " + ex.getMessage());
            throw new ConnectorSDKException(ex.getMessage(), ex);
        } finally {
            try {
                //closing the http connection
                if (response != null && response instanceof CloseableHttpResponse) {
                    ((CloseableHttpResponse) response).close();
                }
            }catch (IOException ex){
                logger.severe("Error while closing connection " + ex.getMessage());
                throw new ConnectorSDKException(ex.getMessage(), ex);
            }
        }
    }

    private List<JSONObject> getRecords(String recordsAsString, int rows) throws IOException {
        List<JSONObject> data = new ArrayList<>();
        String completeRecordString = getTotalRecordsFromCurrentString(recordsAsString);
        if(completeRecordString != null && !completeRecordString.isEmpty()) {
            StringReader reader = new StringReader(completeRecordString);

            CSVParser parser = getCSVParser(reader);
            int recordCount = 0;
            Iterator<CSVRecord> iteratorCSVRecords = parser.iterator();
            if(hasMoreData(iteratorCSVRecords) && headerFields == null){
                CSVRecord headerRecord = iteratorCSVRecords.next();
                if(headerRecord!=null)
                    headerFields = headerRecord;
            }

            while(hasMoreData(iteratorCSVRecords) && recordCount < rows){
                CSVRecord csvRecord = iteratorCSVRecords.next();
                if(csvRecord!=null){
                    JSONObject record = getJsonObjectFromCSVRecord(csvRecord);
                    if(record != null) {
                        data.add(record);
                        recordCount++;
                    }
                }
            }

            // Store records up till the last record as last record might be partial data.
            if(hasMoreData(iteratorCSVRecords)) {
                initUnusedBufferList();

                while(hasMoreData(iteratorCSVRecords)) {
                    CSVRecord csvRecord = iteratorCSVRecords.next();
                    if(csvRecord!=null)
                        addNotNullJsonRecordFromCSVRecord(csvRecord);
                }
            }
        }
        return data;
    }

    /**
     * Method that checks the following for extracting complete records.
     * Checks the last index of '\r' or '\n'
     *  - If there exists such index then:
     *      - If there is data after last index and there CAN be further API iterations,
     *        use data till this index and store buffer for prepending it to next response ELSE
     *        use entire string as complete records.
     *      - If there is no data after last index then <code>recordsAsString</code> is all
     *        records.
     *  - If there no such index and:
     *      - If there can be more API calls (means offset is valid for next call(s)) store this buffer ELSE
     *      - Treat this as record and return for consumption.
     * @param recordsAsString
     * @return String which contains only complete records.
     */
    private String getTotalRecordsFromCurrentString(String recordsAsString) {
        //Special handling for last record.
        String completeRecords = "";
        int index = recordsAsString.lastIndexOf("\n");
        if(index == -1) {
            index = recordsAsString.lastIndexOf("\r");
        }
        if(index != -1) {
            partialRecordBuffer = recordsAsString.substring(index + 1).trim();
            if(partialRecordBuffer.isEmpty()) { //Entire string received consists of complete records.
                completeRecords = recordsAsString;
            }
            completeRecords = recordsAsString.substring(0,index+1);
        } else { // No \n or \r\n found in data. Everything is buffer to be prepend from next API response if possible..
            partialRecordBuffer = recordsAsString;
        }

        // One last check to see if the next offset
        // will be a valid offset or not.
        // If next incremented offset is not valid
        // this means we are at last iteration of processing entity
        // and need to encompass everything we have.
        if(!partialRecordBuffer.isEmpty() && !seekIfNextOffsetIsValid()) {
            completeRecords = completeRecords + partialRecordBuffer;
            partialRecordBuffer = "";
        }

        return completeRecords;
    }

    private boolean seekIfNextOffsetIsValid() {
        long seekNextOffset = offset + byteLength + 1;
        return seekNextOffset >= maxByteLength ? false : true;
    }

    private CSVParser getCSVParser(StringReader reader) throws IOException {
        char delimFromCatalog = param.getDataSet().getFileDescription().getDelimiter();
        CSVParser parser = CSVFormat.DEFAULT.withDelimiter(delimFromCatalog).withEscape('\\').parse(reader);
        return parser;
    }

    private void addNotNullJsonRecordFromCSVRecord(CSVRecord csvRecord) {
        JSONObject jsonRecord = getJsonObjectFromCSVRecord(csvRecord);
        if(jsonRecord != null) {
            initUnusedBufferList();
            recordsUnusedFromPreviousIteration.add(jsonRecord);
        }
    }

    private void initUnusedBufferList() {
        if(recordsUnusedFromPreviousIteration == null && unusedRecordsPointer == -1) {
            recordsUnusedFromPreviousIteration = new ArrayList<>();
            unusedRecordsPointer = 0;
        }
    }

    private JSONObject getJsonObjectFromCSVRecord(CSVRecord csvRecord) {
        JSONObject jsonRecord;
        if(csvRecord.size() != headerFields.size()) {
            errorCount++;
            return null;
        }
        jsonRecord = new JSONObject();
        for(int i=0; i<headerFields.size(); i++) {
            jsonRecord.put(headerFields.get(i),csvRecord.get(i));
        }
        return jsonRecord;
    }

    private List<Header> getHttpHeaders() {
        List<Header> headers = new ArrayList<>();
        headers.add(getByteRangeHeader());
        return headers;
    }

    private Header getByteRangeHeader() {
        return new BasicHeader("Range","bytes="+offset+"-"+getUpperByteRange(offset));
    }

    private long getUpperByteRange(long offset) {
        long calculatedUpperMaxByte = offset + byteLength;
        // Intent of doing maxByteLength - 1 is because the byte-Ranges are 0th
        // Index based. So if API returns 0-100/200 then this means last valuable
        // byte is at 199. Next range query should be 101-199.
        long upperByteLimit = maxByteLength == -1 ? calculatedUpperMaxByte : ( calculatedUpperMaxByte > maxByteLength ? (maxByteLength-1) : calculatedUpperMaxByte);
        return upperByteLimit;
    }

    private Boolean hasMoreData(Iterator<CSVRecord> iteratorCSVRecords){
        try{
            return iteratorCSVRecords.hasNext();
        }
        catch(Exception e){
            errorCount++;
            logger.log(Level.FINE,"Error in checking hasMoreData");
        }
        return false;
    }
}