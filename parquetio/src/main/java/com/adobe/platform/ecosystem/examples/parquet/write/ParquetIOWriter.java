
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
package com.adobe.platform.ecosystem.examples.parquet.write;


/**
 * Created by vardgupt on 10/10/2017.
 */

import com.adobe.platform.ecosystem.examples.parquet.exception.ParquetIOException;
import com.adobe.platform.ecosystem.examples.parquet.model.ParquetIOField;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.MessageType;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Interface to expose API for writing
 * to parquet format.
 */
public interface ParquetIOWriter {
    /**
     * Interface to construct parquet schema from
     * parquet-IO fields.
     * @param fields
     * @return Parquet's message type.
     */
    MessageType getSchema(List<ParquetIOField> fields);

    /**
     *
     * @param columnToTypeMap
     * @param delimiter
     * @return
     */
    MessageType getSchema(Map<String,String> columnToTypeMap, String delimiter);


    /**
     * @param schema
     * @param fileName
     * @param noOfRecords
     * @return
     * @throws IOException
     */
    File writeSampleParquetFile(MessageType schema, String fileName, int noOfRecords) throws ParquetIOException;


    /**
     * @param schema
     * @param fileName
     * @return
     * @throws IOException
     */
    File writeParquetFile(MessageType schema, String fileName,List<SimpleGroup> records) throws ParquetIOException;
}