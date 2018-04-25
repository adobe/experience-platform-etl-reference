
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
package com.adobe.platform.ecosystem.examples.data.access.model;

/**
 * Created by vedhera on 10/09/2017.
 */

import org.json.simple.JSONObject;

/**
 * Java POJO to encapsulate
 * processing links of files per DataSetFile
 * entry.
 */
public class DataSetFileProcessingEntity {
    private String name;

    private long length;

    private String href;

    public DataSetFileProcessingEntity(JSONObject jobj) {
        if(jobj.containsKey("name")) {
            this.name = (String) jobj.get("name");
        }

        if(jobj.containsKey("length")) {
            this.length = Long.parseLong((String) jobj.get("length"));
        }

        if(jobj.containsKey("_links")
                && ((JSONObject)jobj.get("_links")).containsKey("self")
                && ((JSONObject)((JSONObject)jobj.get("_links")).get("self")).containsKey("href")) {
            this.href = (String) ((JSONObject)((JSONObject)jobj.get("_links")).get("self")).get("href");
        }
    }

    public String getName() {
        return name;
    }

    public long getLength() {
        return length;
    }

    public String getHref() {
        return href;
    }
}