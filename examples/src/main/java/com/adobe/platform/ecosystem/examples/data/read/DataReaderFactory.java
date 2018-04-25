
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

import java.util.Map;

/**
 * Factory to be implemented to provide
 * concrete implementations for data reader.
 */
public interface DataReaderFactory {
    /**
     * Inteface to provider reader
     * which will read data from multiple
     * data sources.
     * @return Concrete reader implementing {@code Reader}
     * @throws ConnectorSDKException
     */
    Reader getReader() throws ConnectorSDKException;

    /**
     * Inteface to provider reader
     * which will read data from multiple
     * data sources.
     * @param readerAttributes Read attributes.
     * @return Concrete reader implementing {@code Reader}
     * @throws ConnectorSDKException
     */
    Reader getReader(Map<String,String> readerAttributes) throws ConnectorSDKException;
}