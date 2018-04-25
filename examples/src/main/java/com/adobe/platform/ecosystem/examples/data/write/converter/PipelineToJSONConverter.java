
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
package com.adobe.platform.ecosystem.examples.data.write.converter;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Responsible for transforming data
 * coming from pipeline to the standardised
 * SDKModel.
 * Going forward this can be abstracted through
 * a factory which can work for several pipeline
 * input types. Currently it supports a Json object as
 * string and converts it to JSON object..
 *
 * @author vedhera
 */
public class PipelineToJSONConverter {
    private static final JSONParser parser = new org.json.simple.parser.JSONParser();

    private static final Logger logger = Logger.getLogger(PipelineToJSONConverter.class.getName());

    public static List<JSONObject> getFields(List<Object> pipelineData, String rootId) {
        List<JSONObject> records = new ArrayList<>();
        if(pipelineData == null) {
            logger.log(Level.INFO,"Input data is empty for transformation.");
            return records;
        }
        for (Object data : pipelineData) {
            try {
                String stringData = (String) data;
                records.add((JSONObject) ((JSONObject) parser.parse(stringData)).get(rootId));
            } catch (Exception ex) {
                logger.severe("Error while parsing data: " + data + " to JSON object.");
            }
        }
        return records;
    }
}