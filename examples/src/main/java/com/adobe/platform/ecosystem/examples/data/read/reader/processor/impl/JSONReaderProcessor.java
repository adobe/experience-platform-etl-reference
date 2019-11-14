
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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.logging.Logger;

import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.adobe.platform.ecosystem.examples.constants.SDKConstants;
import com.adobe.platform.ecosystem.examples.data.access.api.DataAccessService;
import com.adobe.platform.ecosystem.examples.data.access.model.DataSetFileProcessingEntity;
import com.adobe.platform.ecosystem.examples.data.read.reader.processor.api.ReaderProcessor;
import com.adobe.platform.ecosystem.examples.data.wiring.DataWiringParam;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;
import com.adobe.platform.ecosystem.examples.util.HttpClientUtil;


/**
 * @author vardgupt
 *
 */
public class JSONReaderProcessor extends ReaderProcessor{
    private final DataWiringParam param;
    private final HttpClient httpClient;
    private final HttpClientUtil httpClientUtil;

    private static final Logger logger = Logger.getLogger(JSONReaderProcessor.class.getName());
    private Boolean isEndOfFile = false;
    private int noOfRecordsFetched = 0;
    private static final int range = 8*1024*1024; // Setting response content length to 1 MB
    private int offset = 0;
    private int limit = range;
    private String previousBuffer = "";
    private List<JSONObject> bufferedRecords;
    private int bufferPointer = 0;
    private int requestedRows = 0;

    public JSONReaderProcessor(DataAccessService das, HttpClient httpClient, DataWiringParam param, List<String> dataSetFileSet, List<DataSetFileProcessingEntity> dataSetFileProcessingSet) throws ConnectorSDKException {
        super(das,param,dataSetFileSet,dataSetFileProcessingSet);
        this.param = param;
        this.httpClient = httpClient == null ? HttpClientUtil.getHttpClient() : httpClient;
        httpClientUtil = new HttpClientUtil(httpClient);
    }

    @Override
    public List<JSONObject> processData(int rows) throws ConnectorSDKException {
        List<JSONObject> data = new ArrayList<>();
        while(data.size() < rows && hasMoreData()) {
            processData(data, rows-data.size());
        }
        return data;
    }

    private List<JSONObject> processData(List<JSONObject> data, int rows) throws ConnectorSDKException {
        if(requestedRows == 0)
            requestedRows = rows;
        if(bufferedRecords!=null && bufferPointer < bufferedRecords.size()) { //Buffer exists
            data.addAll(readFromExistingBuffer(rows));
        } else {
            if(!dataSetFileProcessingSet.isEmpty()) {
                data.addAll(readFromProcessingEntity(dataSetFileProcessingSet.get(processingSetReadIndex), rows));
            }
        }

        // If buffered reader is exhausted and set to null then,
        // initialize with next processingEntity or next DSF.
        if(bufferedRecords==null && isEndOfFile) {
            prepareAndCheckEntitiesIndex();
            offset = 0;
            limit = range;
        }

        // Will go into recursion if there are still DSF's to be read
        if(!(data.size() < rows && dataSetFileReadIndex < dataSetFileSet.size())) {
            noOfRecordsFetched = 0; //resetting for next iteration
            requestedRows = 0; //resetting for next iteration
        }
        return data;
    }

    @Override
    public Boolean hasMoreData() throws ConnectorSDKException {
        return (bufferedRecords!=null && bufferedRecords.size() > 0) || dataSetFileReadIndex < dataSetFileSet.size();
    }

    private List<JSONObject> readFromExistingBuffer(int rows) throws ConnectorSDKException {
        List<JSONObject> records = new ArrayList<JSONObject>();
        try {
            while (records.size() < rows && bufferPointer < bufferedRecords.size()) {
                records.add(bufferedRecords.get(bufferPointer++));
            }
        } catch (Exception e) {
            logger.severe("Error in readFromExistingBuffer : " + e.getMessage());
            throw new ConnectorSDKException(e.getMessage(), e);
        }
        clearBuffer(bufferPointer);
        return records;
    }

    private List<JSONObject> readFromProcessingEntity(DataSetFileProcessingEntity dataSetFileProcessingEntity,int rows) throws ConnectorSDKException {
        try {
            String expression = "";
            String jsonObjectExpression = "";
            String jsonObjectString = "";
            isEndOfFile = false;
            List<JSONObject> records = new ArrayList<JSONObject>();
            while(noOfRecordsFetched < requestedRows && !isEndOfFile){
                String href = dataSetFileProcessingEntity.getHref();
                expression = previousBuffer + readEntity(href);
                if(expression.startsWith("["))
                    jsonObjectExpression = expression.substring(1);
                else
                    jsonObjectExpression = expression;
                int validJSONObjectClosingIndex = getValidJsonEndIndexFromExpression(jsonObjectExpression);
                offset = limit+1;
                limit = limit+range;
                jsonObjectString = jsonObjectExpression.substring(0,validJSONObjectClosingIndex+1);
                jsonObjectString = "["+jsonObjectString+"]";
                JSONParser parser = new JSONParser();

                JSONArray jsonArray = (JSONArray) parser.parse(jsonObjectString);

                if(jsonArray!=null && jsonArray.size()>0){
                    for(int k = 0; k<jsonArray.size(); k++){
                        if(records.size()<rows)
                            records.add((JSONObject) jsonArray.get(k));
                        else{// Putting extra fetched records to leftOverRecords variable, which will be used in subsequent calls, if any.
                            if(bufferedRecords == null)
                                bufferedRecords = new ArrayList<JSONObject>();
                            bufferedRecords.add((JSONObject) jsonArray.get(k));
                        }
                    }
                }
            }
            return records;
        } catch(Exception e) {
            logger.severe("Error in readFromProcessingEntity : " + e.getMessage());
            throw new ConnectorSDKException(e.getMessage(), e);
        }
    }

    private void clearBuffer(int currentPointer) {
        if(bufferedRecords.size() == currentPointer){
            bufferedRecords = null;
            bufferPointer = 0;
        }
    }

    public static boolean isOpenParenthesis(char c)
    {
        if ( c=='(' || c=='[' || c=='{' )
            return true;
        else
            return false;
    }

    public static boolean isClosedParenthesis(char c)
    {
        if ( c==')' || c==']' || c=='}' )
            return true;
        else
            return false;
    }

    private static boolean isParenthesesMatched(char open,char closed)
    {
        if ( open=='(' && closed==')' )
            return true;
        else if ( open=='[' && closed==']' )
            return true;
        else if ( open=='{' && closed=='}' )
            return true;
        else
            return false;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public int getValidJsonEndIndexFromExpression(String exp){
        int validJSONObjectClosingIndex = 0;
        previousBuffer = "";
        Stack s = new Stack();
        int i;
        char current_char;
        Character c;
        char c1;

        for ( i=0; i < exp.length(); i++ )
        {
            current_char=exp.charAt( i );
            if (isOpenParenthesis(current_char ))
            {
                c=new Character( current_char );
                s.push( c );
            }
            else if ( isClosedParenthesis(current_char))
            {
                if ( s.isEmpty() )
                {
                    break;
                }
                else
                {
                    c=(Character)s.pop();
                    if(s.size() == 0){
                        noOfRecordsFetched++;
                        validJSONObjectClosingIndex = i;
                    }
                    c1=c.charValue();
                    if ( !isParenthesesMatched( c1, current_char ) )
                    {
                        break;
                    }
                }
            }
        }

        if(validJSONObjectClosingIndex+1<exp.length()){
            previousBuffer = exp.substring(validJSONObjectClosingIndex+2);
        }
        return validJSONObjectClosingIndex;
    }

    private String readEntity(String href) throws ConnectorSDKException {
        String responseBody = "";
        try{
            URIBuilder builder = new URIBuilder(href);

            HttpClientUtil httpClientUtil = new HttpClientUtil(httpClient);
            HttpGet request = new HttpGet(builder.build());
            request.setHeader("Range","bytes="+offset+"-"+limit);
            httpClientUtil.addHeader(request, param.getAuthToken(), param.getImsOrg(), param.getSandboxName(),
                    SDKConstants.CONNECTION_HEADER_JSON_CONTENT);
            HttpResponse response = httpClientUtil.executeRequest(request, false);
            //TODO Check for error code in response
            StringBuffer result = new StringBuffer();
            BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));

            String line = "";
            while ((line = rd.readLine()) != null) {
                result.append(line);
            }
            responseBody = result.toString();
            Header contentRangeHeader[] = response.getHeaders("Content-Range");
            if(contentRangeHeader!=null && contentRangeHeader.length>0)
                isEndOfFile = checkEndOfFile(contentRangeHeader[0].getValue().toString());
        }catch(Exception e){
            logger.severe("Error in readEntity: " + e.getMessage());
            throw new ConnectorSDKException(e.getMessage(), e);
        }
        return responseBody;
    }

    private Boolean checkEndOfFile(String contentRange) throws ConnectorSDKException{
        //Expected argument value is something like this - "bytes 10241-20480/14156"
        Boolean isEndOfFileFlag = true;
        try{
            String rangeString = contentRange.substring(6);
            String rangeStringArray[] = rangeString.split("/");
            String rangeArray[] = rangeStringArray[0].split("-");
            int startRange = Integer.parseInt(rangeArray[0]);
            int endRange = Integer.parseInt(rangeArray[1]);
            int totalBytesAvailableInFile = Integer.parseInt(rangeStringArray[1]);
            if(totalBytesAvailableInFile<startRange)
                isEndOfFileFlag =  true;
            else if(endRange<totalBytesAvailableInFile)
                isEndOfFileFlag = false;
            else if(endRange>=totalBytesAvailableInFile)
                isEndOfFileFlag = true;
        }
        catch(Exception e){
            logger.severe("Error in checkEndOfFile: " + e.getMessage());
            throw new ConnectorSDKException(e.getMessage(), e);
        }
        return isEndOfFileFlag;
    }
}