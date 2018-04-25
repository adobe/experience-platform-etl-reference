
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
package com.adobe.platform.ecosystem.examples.data.read;
/**
 * Created by vedhera on 8/25/2017.
 */

import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;
import org.json.simple.JSONArray;

/**
 * Interface for providing basic
 * operations for data source read.
 */
public interface Reader {
    /**
     * API returning if
     * platform has more data
     * for given dataSource.
     * @return Boolean
     */
    Boolean hasMoreData() throws ConnectorSDKException;

    /**
     * Reads given number of rows
     * from datasource.
     * @param rows
     * @return
     * @throws ConnectorSDKException
     */
    JSONArray read(int rows) throws ConnectorSDKException;

    /**
     * This API returns the count of
     * error rows which were not in
     * a valid format
     * @return
     */
    Integer getErrorRowCount();
}