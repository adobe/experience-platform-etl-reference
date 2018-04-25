
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
package com.adobe.platform.ecosystem.examples.parquet.wiring.api;

import com.adobe.platform.ecosystem.examples.parquet.read.ParquetIOReader;
import com.adobe.platform.ecosystem.examples.parquet.write.ParquetIOWriter;

/**
 * Created by vardgupt on 10/10/2017.
 */

/**
 * Interfaces for parquet
 * reader/writers.
 */
public interface ParquetIO {
    /**
     * Interface to provide reader for
     * performing parquet read operations.
     * @param doFlatten to flatten the data after reading.
     * @return {@code ParquetIOReader} instance
     */
    ParquetIOReader getParquetIOReader(boolean doFlatten);

    /**
     * Interface to provide writer for
     * performing parquet writer operations.
     * @return
     */
    ParquetIOWriter getParquetIOWriter();
}