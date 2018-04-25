
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

import com.adobe.platform.ecosystem.examples.parquet.exception.ParquetIOErrorCode;
import com.adobe.platform.ecosystem.examples.parquet.exception.ParquetIOException;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.logging.Logger;

/**
 * Created by vedhera on 30/11/2017.
 */

/**
 * Iterable class to read records iteratively from
 * Parquet record reader.
 * @param <T>
 */
public class ParquetIOReaderIterator<T extends Group> implements Iterable<JSONObject> {
    private T readingGroup;
    private final ParquetReader<T> reader;
    private final boolean doFlatten;
    private final static Logger LOGGER = Logger.getLogger(ParquetIOReaderIterator.class.getName());

    ParquetIOReaderIterator(ParquetReader<T> reader, boolean doFlatten) throws ParquetIOException {
        if(reader == null) {
            throw new ParquetIOException(ParquetIOErrorCode.PAQUETIO_READER_NOT_INITIALISED);
        }
        this.reader = reader;
        this.doFlatten = doFlatten;
        try {
            this.readingGroup = reader.read();
        } catch (IOException ex) {
            throw new ParquetIOException(ParquetIOErrorCode.PARQUETIO_IO_EXCEPTION);
        }
    }

    @Override
    public Iterator<JSONObject> iterator() {
        return new Iterator<JSONObject>() {
            @Override
            public boolean hasNext() {
                return readingGroup != null;
            }

            @Override
            public JSONObject next() {
                T tempGroup;
                JSONObject value;
                try {
                    tempGroup = readingGroup;
                    readingGroup = reader.read();
                    value = ReaderUtil.getJSONDataFromGroup(tempGroup,doFlatten);
                } catch (IOException e) {
                    LOGGER.severe("Error occurred while reading next record from iterator: " + e);
                    throw new NoSuchElementException("Error occurred while reading next record from iterator: " + e);
                }
                return value;
            }
        };
    }
}