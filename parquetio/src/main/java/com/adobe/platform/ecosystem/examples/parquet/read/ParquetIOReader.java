
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

import com.adobe.platform.ecosystem.examples.parquet.exception.ParquetIOException;
import com.adobe.platform.ecosystem.examples.parquet.model.ParquetIOField;
import org.apache.hadoop.fs.Path;
import org.json.simple.JSONObject;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Created by vedhera on 24/10/2017.
 */

/**
 * Interface to expose API's
 * to read records from parquet file.
 */
public interface ParquetIOReader {
    /**
     * abstract method to process data and
     * return list of JsonObject with {@code rows}
     * or less records.
     * @param rows
     * @return
     */
    List<JSONObject> processData(int rows) throws ParquetIOException;

    /**
     * abstract method to check
     * whether records can be read
     * from an exisiting parquet file.
     * @return boolean
     */
    boolean hasBufferData();

    /**
     * abstract method to init
     * parquet reader for a file
     * on disk.
     * @param file
     */
    void initFileForRead(File file) throws ParquetIOException;

    @Deprecated
    /**
     * abstract method to init
     * parquet reader for a
     * {@link org.apache.hadoop.fs.Path}.
     *
     * @param path path pointing to the file.
     *             Can be both a remote or a
     *             local path.
     */
    ParquetIOReader initFileForRead(Path path) throws ParquetIOException;

    /**
     * abstract method to init
     * parquet reader for a
     * {@link org.apache.hadoop.fs.Path}.
     *
     */
    ParquetIOReader initFileForRead() throws ParquetIOException;

    /**
     * abstract method to provide iterator
     * for reading records.
     * @return Iterator
     */
    Iterator getIterator() throws ParquetIOException;

    /**
     * API to close the reader
     * which in turn closes on
     * the hadoop parquet reader
     */
    void readerClose() throws ParquetIOException;

    /**
     * API to extract schema from the
     * parquet file.
     *
     * @return equivalent schema
     * for parquet file.
     * @throws ParquetIOException
     */
    List<ParquetIOField> getSchema() throws ParquetIOException;
}