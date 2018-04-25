
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
package com.adobe.platform.ecosystem.examples.data.write.writer.formatter;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.json.simple.JSONObject;

import com.adobe.platform.ecosystem.examples.catalog.model.DataSet.SchemaField;
import com.adobe.platform.ecosystem.examples.catalog.model.DataType;
import com.adobe.platform.ecosystem.examples.catalog.model.SDKField;
import com.adobe.platform.ecosystem.examples.data.wiring.DataWiringParam;
import com.adobe.platform.ecosystem.examples.data.write.Formatter;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;
import com.adobe.platform.ecosystem.examples.util.SDKDataTypeJsonUtil;

/**
 * Created by vardgupt on 10/24/2017.
 */

public class CSVDataFormatter implements Formatter{


    private final DataWiringParam param;
    private static Logger logger = Logger.getLogger(CSVDataFormatter.class.getName());
    private static final Character DOUBLE_QUOTE_CHAR = Character.valueOf('"');
    private static final String DOUBLE_QUOTE_STRING = Character.valueOf('"').toString();
    private static final char[] csvSpecialCharacters = {'\n','\r'};
    private static final char DEFAULT_DELIMITER = ',';

    public CSVDataFormatter(DataWiringParam param) {
        this.param = param;
    }

    private Boolean checkSpecialCharacterExistsInColValue(String colValue){
        for(int i = 0; i<csvSpecialCharacters.length; i++){
            if(colValue.contains(csvSpecialCharacters[i]+""))
                return true;
        }
        return false;
    }

    @Override
    public byte[] getBuffer(List<SDKField> sdkFields,List<List<Object>> dataTable) throws ConnectorSDKException{
        List<JSONObject> dataRecords = convertPlatformDataToJSONObjects(sdkFields, dataTable);
        return getBuffer(dataRecords);
    }

    @Override
    public byte[] getBuffer(List<JSONObject> dataTable) throws ConnectorSDKException {
        byte[] buffer;
        char delimFromCatalog;
        if(param.getDataSet().getFileDescription()==null)
            delimFromCatalog = DEFAULT_DELIMITER;
        else
            delimFromCatalog = param.getDataSet().getFileDescription().getDelimiter();
        StringBuffer records = new StringBuffer();
        List<SchemaField> fieldList = param.getDataSet().getFields(true);
        StringBuffer headerRow = getHeader(fieldList, dataTable.get(0), delimFromCatalog);
        headerRow = new StringBuffer(headerRow.substring(0, headerRow.length()-1));
        records.append(headerRow).append('\n');
        //records.add(headerRow);
        StringBuffer record;
        for(JSONObject row: dataTable) {
            record = new StringBuffer();
            for(SchemaField field: fieldList){
                String fieldName = field.getName();
                Object colValueObject = "";
                if(row != null && row.get(fieldName)!= null) {
                    DataType catalogDataType = field.getType();
                    colValueObject = SDKDataTypeJsonUtil.getKeyValueFromJSONObject(row,fieldName, catalogDataType);
                    if(checkWhetherEnclosingIsRequired(colValueObject.toString(),delimFromCatalog)){
                        colValueObject = DOUBLE_QUOTE_CHAR.toString()+colValueObject+DOUBLE_QUOTE_CHAR;
                    }
                    record.append(colValueObject).append(delimFromCatalog);
                }
            }
            record = new StringBuffer(record.substring(0, record.length() - 1));
            records.append(record).append('\n');
        }
        records = new StringBuffer(records.substring(0, records.length()));
        logger.fine(records.toString());
        try {
            buffer = records.toString().getBytes(StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            logger.severe("Exception in getting Buffer "+e.getMessage());
            throw new ConnectorSDKException("Error while getting Buffer :" + e.getMessage(), e.getCause());
        }
        return buffer;
    }

    private StringBuffer getHeader(List<SchemaField> fieldList, JSONObject jsonObject, char delimFromCatalog) {
        StringBuffer headerRow = new StringBuffer();
        for(SchemaField field: fieldList){
            String fieldName = field.getName();
            if(jsonObject!=null && jsonObject.containsKey(fieldName)){
                if(checkWhetherEnclosingIsRequired(fieldName,delimFromCatalog))
                    fieldName = DOUBLE_QUOTE_CHAR+fieldName+DOUBLE_QUOTE_CHAR;
                headerRow.append(fieldName).append(delimFromCatalog);
            }
        }
        return headerRow;
    }

    @SuppressWarnings("unchecked")
    private List<JSONObject> convertPlatformDataToJSONObjects(List<SDKField> sdkFields, List<List<Object>> dataTable) {
        List<JSONObject> jsonObjects = new ArrayList<JSONObject>();
        int ordinalPosition = 0;
        for(List<Object> platformData: dataTable){
            if(sdkFields.size()==platformData.size()){
                ordinalPosition = 0;
                JSONObject row = new JSONObject();
                for(SDKField sdkField: sdkFields){
                        row.put(sdkField.getName(), platformData.get(ordinalPosition));
                    ordinalPosition++;
                }
                jsonObjects.add(row);
            }
        }
        return jsonObjects;
    }


    private Boolean checkWhetherEnclosingIsRequired(String colValue, char delimFromCatalog){
        return !colValue.startsWith(DOUBLE_QUOTE_STRING) && !colValue.endsWith(DOUBLE_QUOTE_STRING) && (colValue.contains(delimFromCatalog+"") || checkSpecialCharacterExistsInColValue(colValue));
    }

}