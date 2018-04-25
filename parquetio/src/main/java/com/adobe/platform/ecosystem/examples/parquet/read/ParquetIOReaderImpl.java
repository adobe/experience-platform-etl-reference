
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
package com.adobe.platform.ecosystem.examples.parquet.read;

/**
 * Created by vedhera on 24/10/2017.
 */

import com.adobe.platform.ecosystem.examples.parquet.exception.ParquetIOErrorCode;
import com.adobe.platform.ecosystem.examples.parquet.exception.ParquetIOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.json.simple.JSONObject;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

/**
 * Concrete implementation to
 * read records from a parquet file.
 */
public class ParquetIOReaderImpl implements ParquetIOReader {
    private Group readingGroup;
    private ParquetReader<Group> reader;
    private final Configuration configuration;
    private final boolean doFlatten;

    ParquetIOReaderImpl(Configuration configuration, boolean doFlatten) {
        this.configuration = configuration;
        this.doFlatten = doFlatten;
    }

    public List<JSONObject> processData(int rows) throws ParquetIOException {
        return getDataFromExistingBuffer(rows);

    }

    @Override
    public boolean hasBufferData() {
        if(readingGroup != null) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void initFileForRead(File file) throws ParquetIOException {
        try {
            if(reader != null) {
                reader.close(); // Close previous file reader.
            }
            reader = ParquetReader.builder(new GroupReadSupport(), new Path(file.getAbsolutePath()))
                    .withConf(configuration)
                    .build();
        } catch (IOException ioex) {
            throw new ParquetIOException(ParquetIOErrorCode.PARQUETIO_IO_EXCEPTION,ioex);
        }
    }

    @Override
    public Iterator getIterator() throws ParquetIOException {
        if(reader == null) {
            throw new ParquetIOException(ParquetIOErrorCode.PAQUETIO_READER_NOT_INITIALISED);
        }
        return new ParquetIOReaderIterator<Group>(reader,doFlatten).iterator();
    }

    private List<JSONObject> getDataFromExistingBuffer(int rows) throws ParquetIOException {
        List<JSONObject> records = new ArrayList<>();
        try {
            if(readingGroup == null) {
                readingGroup = reader.read();
            }

            while(readingGroup != null && records.size() < rows) {
                records.add(ReaderUtil.getJSONDataFromGroup(readingGroup, doFlatten));
                readingGroup = reader.read();
            }
        } catch (IOException ioex) {
            throw new ParquetIOException(ParquetIOErrorCode.PARQUETIO_IO_EXCEPTION,ioex);
        }
        return records;
    }

    public static class ParquetIOReaderBuilder {
        public Configuration conf;
        public boolean doFlatten;
        // TODO: More fields to be added based on compressions style etc..

        public ParquetIOReaderBuilder with(Consumer<ParquetIOReaderBuilder> builderFunction) {
            builderFunction.accept(this);
            return this;
        }

        public ParquetIOReaderImpl build() {
            return new ParquetIOReaderImpl(conf,doFlatten);
        }
    }

    public ParquetReader<Group> getReader() {
        return reader;
    }

    public Configuration getConfiguration() {
        return configuration;
    }
}