
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
package com.adobe.platform.ecosystem.examples.parquet.wiring.impl;

import com.adobe.platform.ecosystem.examples.parquet.read.ParquetIOReader;
import com.adobe.platform.ecosystem.examples.parquet.read.ParquetIOReaderImpl;
import com.adobe.platform.ecosystem.examples.parquet.wiring.api.ParquetIO;
import com.adobe.platform.ecosystem.examples.parquet.write.ParquetIOWriter;
import com.adobe.platform.ecosystem.examples.parquet.write.ParquetIOWriterImpl;
import org.apache.hadoop.conf.Configuration;


/**
 * Created by vedhera on 10/10/2017.
 */
public class ParquetIOImpl implements ParquetIO {

    @Override
    public ParquetIOReader getParquetIOReader(boolean doFlatten) {
        Configuration configuration = new Configuration();
        return new ParquetIOReaderImpl.ParquetIOReaderBuilder()
                .with(readerBuilder -> {
                    readerBuilder.conf = configuration;
                    readerBuilder.doFlatten = doFlatten;
                })
                .build();
    }

    @Override
    public ParquetIOWriter getParquetIOWriter() {
        return new ParquetIOWriterImpl();
    }
}