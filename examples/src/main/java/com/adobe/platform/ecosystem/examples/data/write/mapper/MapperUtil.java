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
package com.adobe.platform.ecosystem.examples.data.write.mapper;

import com.adobe.platform.ecosystem.examples.catalog.model.SDKField;
import org.json.simple.JSONObject;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Util to convert data from Flat connector
 * to that of Procedural connector. This is
 * done as the Procedural connector flow
 * generates Parquet schema by reconciling data
 * types of columns with Catalog.
 *
 * @author vedhera on 7/19/2018.
 */
public class MapperUtil {

    private static final String DELIM = ".";

    public static List<JSONObject> convert(List<SDKField> fields, List<List<Object>> data) {
        return data.stream()
                .map(dataRow -> convertRow(fields, dataRow))
                .collect(Collectors.toList());
    }

    /**
     * Fields can be of type
     * a.b.c
     * a.b.d
     * e.f
     * e.g
     * h
     */
    public static JSONObject convertRow(List<SDKField> fields, List<Object> dataRow) {
        JSONObject rootObject = new JSONObject();
        int dataIndexCounter = 0;

        // For each field create the hierarchy in the root JSON object
        // if not present and add the value at leaf node.
        for (SDKField field : fields) {
            addColumnDataToJsonObject(
                    rootObject,
                    field.getName().split("["+DELIM+"]"),
                    0,
                    field.getName().split("["+DELIM+"]").length,
                    dataRow.get(dataIndexCounter++)

            );
        }
        return rootObject;
    }

    private static void addColumnDataToJsonObject(JSONObject currentRootJsonObject,
                                                  String[] colunmNames, int columnCounter,
                                                  int maxColumnCounter, Object columnData) {
        String currentColumnName = colunmNames[columnCounter];
        if (columnCounter == maxColumnCounter - 1) { // As indices are '0' based
            final String columnName = colunmNames[columnCounter];
            currentRootJsonObject.put(currentColumnName, columnData);
            return;
        }

        // Traverse if the key is present
        // If not create the key in json object hierarchy.
        if(!currentRootJsonObject.containsKey(currentColumnName)) {
            JSONObject other = new JSONObject();
            currentRootJsonObject.put(currentColumnName, other);
        }

        JSONObject other = (JSONObject) currentRootJsonObject.get(currentColumnName);

        // Mind = Blown! recursion
        addColumnDataToJsonObject(
                other,
                colunmNames,
                columnCounter+1,
                maxColumnCounter,
                columnData
        );
    }
}

