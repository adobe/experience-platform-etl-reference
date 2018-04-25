
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
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @author vedhera on 2/20/2018
 */
public class PipelineToJSONConverterTest {
    private List<Object> pipelineData;

    private final String ROOT_ID = "rootId";

    @Before
    public void setup() {
        JSONObject row1 = new JSONObject();
        JSONObject innerData1 = new JSONObject();
        innerData1.put("key1_1", "value1_1");
        innerData1.put("key1_2", "value1_2");
        row1.put(ROOT_ID, innerData1);

        JSONObject row2 = new JSONObject();
        JSONObject innerData2 = new JSONObject();
        innerData2.put("key2_1", "value2_1");
        innerData2.put("key2_2", "value2_2");
        row2.put(ROOT_ID, innerData2);

        pipelineData = new ArrayList<>();
        pipelineData.add(row1.toString());
        pipelineData.add(row2.toString());
    }

    @Test
    public void TestConversionForEmptyPipelineData() {
        List<JSONObject> jsonData = PipelineToJSONConverter.getFields(null, ROOT_ID);
        assert (jsonData.size() == 0);
    }

    @Test
    public void TestConversionForCorruptPipelineData() {
        List<Object> badData = new ArrayList<>();
        badData.add("{ bad JSon String");
        List<JSONObject> jsonData = PipelineToJSONConverter.getFields(badData, ROOT_ID);
        assert (jsonData.size() == 0);
    }

    @Test
    public void TestPipelineDataToJsonConversion() {
        List<JSONObject> jsonData = PipelineToJSONConverter.getFields(pipelineData, ROOT_ID);
        assert (jsonData.size() == 2);
        JSONObject row1 = jsonData.get(0);
        Set<String> firstKeySet = row1.keySet();
        assert (firstKeySet.contains("key1_1"));
        assert (firstKeySet.contains("key1_2"));

        JSONObject row2 = jsonData.get(1);
        Set<String> secondKeySet = row2.keySet();
        assert (secondKeySet.contains("key2_1"));
        assert (secondKeySet.contains("key2_2"));
    }
}