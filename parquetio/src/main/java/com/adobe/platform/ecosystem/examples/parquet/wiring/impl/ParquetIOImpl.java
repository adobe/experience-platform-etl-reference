
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

import com.adobe.platform.ecosystem.examples.parquet.exception.ParquetIOErrorCode;
import com.adobe.platform.ecosystem.examples.parquet.exception.ParquetIOException;
import com.adobe.platform.ecosystem.examples.parquet.read.ParquetIOReader;
import com.adobe.platform.ecosystem.examples.parquet.read.ParquetIOReaderImpl;
import com.adobe.platform.ecosystem.examples.parquet.read.configuration.ParquetReaderConfiguration;
import com.adobe.platform.ecosystem.examples.parquet.wiring.api.ParquetIO;
import com.adobe.platform.ecosystem.examples.parquet.write.ParquetIOWriter;
import com.adobe.platform.ecosystem.examples.parquet.write.ParquetIOWriterImpl;
import org.apache.hadoop.conf.Configuration;


/**
 * Created by vedhera on 10/10/2017.
 */
public class ParquetIOImpl implements ParquetIO {

    /**
     * {@inheritDoc}
     */
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

    /**
     * {@inheritDoc}
     */
    @Override
    public ParquetIOReader getParquetIOReader(ParquetReaderConfiguration readerConfiguration) throws ParquetIOException {
        if(readerConfiguration == null) {
            throw new ParquetIOException(ParquetIOErrorCode.PARQUETIO_INPUT_CONFIGURATION_NULL);
        }

        ParquetIOReader reader = new ParquetIOReaderImpl.ParquetIOReaderBuilder()
                .with(readerBuilder -> {
                    readerBuilder.conf = readerConfiguration.getConfiguration();
                    readerBuilder.doFlatten = readerConfiguration.getShouldFlattenData();
                    readerBuilder.path = readerConfiguration.getPath();
                })
                .build();
        reader.initFileForRead();
        return reader;
    }

    /**
     * {@inheritDoc}
     */
    @Override
	public ParquetIOWriter getParquetIOWriter() {
		return new ParquetIOWriterImpl();
	}
}