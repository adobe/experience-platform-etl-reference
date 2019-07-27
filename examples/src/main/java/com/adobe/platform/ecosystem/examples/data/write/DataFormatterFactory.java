
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

import com.adobe.platform.ecosystem.examples.data.FileFormat;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;

/**
 * Created by vardgupt on 10/24/2017.
 */

/**
 * Interface for defining formatters
 * capable of writing different formats
 * to target dataSets.
 */
public interface DataFormatterFactory {
    /**
     * Interface to return formatter
     * based on file type.
     * @param fileFormat
     * @param writeAttributes
     * @return Concrete implementation of {@link Formatter} based on
     * <code>fileFormat.</code>
     * @throws ConnectorSDKException
     */
    Formatter getFormatter(FileFormat fileFormat, WriteAttributes writeAttributes) throws ConnectorSDKException;
}