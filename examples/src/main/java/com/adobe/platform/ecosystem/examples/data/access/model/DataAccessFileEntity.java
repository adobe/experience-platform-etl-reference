
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

import com.adobe.platform.ecosystem.examples.constants.SDKConstants;
import org.json.simple.JSONObject;

/**
 * POJO to capture metadata provided
 * by DataAccessService while querying for
 * datasetfiles through Batch API.
 *
 * @author vedhera
 */
public class DataAccessFileEntity {
    private String dataSetFileId;

    private String dataSetViewId;

    private String dataAccessServiceHref;

    public DataAccessFileEntity(JSONObject jobj) {
        if(jobj.containsKey(SDKConstants.DATA_ACCESS_DATASETFILEID_KEY)) {
            this.dataSetFileId = (String) jobj.get(SDKConstants.DATA_ACCESS_DATASETFILEID_KEY);
        }

        if(jobj.containsKey(SDKConstants.DATA_ACCESS_DATASETVIEWID_KEY)) {
            this.dataSetViewId = (String) jobj.get(SDKConstants.DATA_ACCESS_DATASETVIEWID_KEY);
        }

        if(jobj.containsKey(SDKConstants.DATA_ACCESS_LINKS_KEY)
                && ((JSONObject)jobj.get(SDKConstants.DATA_ACCESS_LINKS_KEY)).containsKey(SDKConstants.DATA_ACCESS_SELF_KEY)
                && ((JSONObject)((JSONObject)jobj.get(SDKConstants.DATA_ACCESS_LINKS_KEY)).get(SDKConstants.DATA_ACCESS_SELF_KEY)).containsKey(SDKConstants.DATA_ACCESS_HREF_KEY)) {
            this.dataAccessServiceHref = (String) ((JSONObject)((JSONObject)jobj
                    .get(SDKConstants.DATA_ACCESS_LINKS_KEY))
                    .get(SDKConstants.DATA_ACCESS_SELF_KEY))
                    .get(SDKConstants.DATA_ACCESS_HREF_KEY);
        }
    }

    public String getDataSetFileId() {
        return dataSetFileId;
    }

    public String getDataSetViewId() {
        return dataSetViewId;
    }

    public String getDataAccessServiceHref() {
        return dataAccessServiceHref;
    }
}