
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.adobe.platform.ecosystem.examples.hierarchy.util.HierarchicalBuilderUtil;
import com.adobe.platform.ecosystem.examples.parquet.entity.Node;
import com.adobe.platform.ecosystem.examples.catalog.model.SDKField;
import com.adobe.platform.ecosystem.examples.data.write.Formatter;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;

/**
 * Created by vardgupt on 10/17/2017.
 */

public class JSONDataFormatter implements Formatter{

    private static Logger logger = Logger.getLogger(JSONDataFormatter.class.getName());
    private int currentColIndex = 0;

    /* (non-Javadoc)
     * @see com.adobe.platform.ecosystem.examples.data.write.Formatter#getBuffer(java.util.ArrayList, java.util.ArrayList)
     */
    @Override
    public byte[] getBuffer(List<SDKField> sdkFields,List<List<Object>> dataTable) throws ConnectorSDKException {
        logger.log(Level.FINE,"Inside getBuffer for JSONFormmater");
        JSONArray records = getRecords(sdkFields, dataTable);
        byte[] buffer = null;
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(records);
        try {
            buffer = strBuilder.toString().getBytes(StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            throw new ConnectorSDKException("Error while executing getDataBuffer :" + e.getMessage(), e.getCause());
        }
        return buffer;
    }

    @Override
    public byte[] getBuffer(List<JSONObject> dataTable) throws ConnectorSDKException {
        return new byte[0];
    }

    /**
     * @param sdkFields
     * @param dataTable
     * @return
     */
    @SuppressWarnings("unchecked")
    private JSONArray getRecords(List<SDKField> sdkFields, List<List<Object>> dataTable) {
        JSONArray records = new JSONArray();
        LinkedHashMap<String,String> schemaMap = new LinkedHashMap<String,String>();
        if(sdkFields!=null && sdkFields.size()>0){
            for (SDKField field: sdkFields) {
                String name = field.getName();
                String type = field.getType();
                schemaMap.put(name, type);
            }
            Node node = HierarchicalBuilderUtil.buildHierarchy(schemaMap,delimiter);
            logger.log(Level.FINE,"Hierarchy has been built, going to add records now");
            if(dataTable!=null && dataTable.size()>0){
                for(List<Object> row: dataTable){
                    JSONObject record = getRecord(node, row);
                    records.add(record);
                }
                logger.log(Level.INFO,"No. of records found:"+records.size());
            }
        }
        return records;
    }

    /**
     * @param node
     * @param row
     * @return
     */
    private JSONObject getRecord(Node node, List<Object> row){
        currentColIndex = 0;
        JSONObject record = new JSONObject();
        record = getJSONBody(node, record, row);
        return record;
    }

    /**
     * @param parentNode
     * @param jsonObject
     * @param row
     * @return
     */
    @SuppressWarnings("unchecked")
    private JSONObject getJSONBody(Node parentNode, JSONObject jsonObject, List<Object> row) {
            LinkedHashMap<Integer, ArrayList<Node>> children = parentNode.getChildren();
            int childIndex=0;
            for(int index: children.keySet()){
                childIndex = index;
                break;
            }
            if(children.get(childIndex)!=null){
                ArrayList<Node> childrenAtSameLevel = children.get(childIndex);
                JSONObject jsonBody = new JSONObject();
                for(int i=0; i<childrenAtSameLevel.size(); i++){
                    Node currentNode = childrenAtSameLevel.get(i);
                    String keyName = currentNode.getKey();
                    if(currentNode.getChildren()==null){
                        jsonBody.put(keyName, row.get(currentColIndex++));
                        if(currentNode.getDepth()>1)//this will avoid putting level 1 nodes to root node tag, i.e. two columns value will be inserted like {"A":"val1", "B":"val2"} instead {"root":{"A":"val1", "B":"val2"}}
                            jsonObject.put(parentNode.getKey(), jsonBody);
                        else
                            jsonObject = jsonBody;
                    }
                    else{
                        jsonObject = getJSONBody(currentNode,jsonObject, row);
                    }
                }
            }
            return jsonObject;
        }
}