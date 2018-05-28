
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
package com.adobe.platform.ecosystem.examples.catalog.model;


import static com.adobe.platform.ecosystem.examples.constants.SDKConstants.CATALOG_FILE_DESCRIPTION_DELIMITERS_KEY;
import static com.adobe.platform.ecosystem.examples.constants.SDKConstants.CATALOG_FILE_DESCRIPTION_PERSISTED_KEY;
import static com.adobe.platform.ecosystem.examples.constants.SDKConstants.CATALOG_FORMAT;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.adobe.platform.ecosystem.examples.catalog.model.DataSet.FieldsFrom;
import com.adobe.platform.ecosystem.examples.constants.SDKConstants;
import com.adobe.platform.ecosystem.examples.data.FileFormat;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;
import com.adobe.platform.ecosystem.examples.util.JsonUtil;

public class FileDescription {
    private boolean persisted;

    private FileFormat format;

    private char delimiter;

    private static final char DEFAULT_DELIMITER = ',';

    public FileDescription(JSONObject fileDescription) throws ConnectorSDKException {
        this.persisted = JsonUtil.getBoolean(fileDescription,
                CATALOG_FILE_DESCRIPTION_PERSISTED_KEY);
        JSONArray delimArray = JsonUtil.getJsonArray(fileDescription,
                CATALOG_FILE_DESCRIPTION_DELIMITERS_KEY);
        if (delimArray.size() > 0) {
            this.delimiter = ((String) delimArray.get(0)).charAt(0);
        } else {
            this.delimiter = DEFAULT_DELIMITER;
        }
        String fileFormat = JsonUtil.getString(fileDescription,
                CATALOG_FORMAT);
        switch (fileFormat) {
            case "csv":
                this.format = FileFormat.CSV;
                break;
            case "parquet":
                this.format = FileFormat.PARQUET;
                break;
            case "json":
                this.format = FileFormat.JSON;
                break;
            default:
                throw new ConnectorSDKException("File format of type " + fileFormat + " is not supported!.");
        }
    }

    public boolean isPersisted() {
        return persisted;
    }

    public FileFormat getFormat() {
        return format;
    }

    public char getDelimiter() {
        return delimiter;
    }
}