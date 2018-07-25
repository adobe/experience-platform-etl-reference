
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
package com.adobe.platform.ecosystem.examples.parquet.exception;

/**
 * Error codes specific to ParquetIO library.
 *
 * @author vedhera 18/01/2018
 */
public enum ParquetIOErrorCode implements ErrorCode {
    PAQUETIO_READER_NOT_INITIALISED(101,"Reader not initialized for read. Initialize first."),
    PARQUETIO_IO_EXCEPTION(102,"IO Exception while reading group."),
    PARQUETIO_FILE_DELLETION_EXCEPTION(103,"Exception while deleting file on local system."),
    PARQUETIO_INPUT_CONFIGURATION_NULL(104,"Input configuration is null. Kindly initialize first."),
    PARQUETIO_READER_CLOSE_EXCEPTION(105,"Error while closing hadoop parquet reader."),
    PARQUETIO_READER_METADATA_NULL_EXCEPTION(106,"Metadata read from parquet file is null. Kindly check input file.");

    private final int number;

    private final String description;

    ParquetIOErrorCode(int number, String description) {
        this.number = number;
        this.description = description;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getErrorMessage() {
        return "[Code: "
                + this.number
                + " message: "
                + this.description
                + "]";
    }
}