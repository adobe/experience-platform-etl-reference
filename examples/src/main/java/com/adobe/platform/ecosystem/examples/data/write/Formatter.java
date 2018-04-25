
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
package com.adobe.platform.ecosystem.examples.data.write;

import java.util.List;

import com.adobe.platform.ecosystem.examples.catalog.model.SDKField;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;
import org.json.simple.JSONObject;

/**
 * Created by vardgupt on 10/24/2017.
 */


public interface Formatter {
    public static final String delimiter = "_";

    /**
     * Returns buffer for input data
     * which could be written to downstream
     * solutions.
     * @param sdkFields
     * @param dataTable
     * @return
     * @throws ConnectorSDKException
     */
    byte[] getBuffer(List<SDKField> sdkFields, List<List<Object>> dataTable) throws ConnectorSDKException;

    /**
     * Returns buffer for input data
     * which could be written to downstream
     * solutions.
     * @param dataTable rows of input data
     * @return buffer to be written.
     * @throws ConnectorSDKException
     */
    byte[] getBuffer(List<JSONObject> dataTable) throws ConnectorSDKException;
}