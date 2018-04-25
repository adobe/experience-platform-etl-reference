
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
package com.adobe.platform.ecosystem.examples.parquet.model;

import org.apache.parquet.schema.PrimitiveType;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;

/**
 * Enums to represent different
 * parquet-mr's data type for consumers
 * of Parquet-IO SDK.
 * @author vedhera on 2/9/2018.
 */
public enum ParquetIODataType {
    INTEGER("int32",PrimitiveType.PrimitiveTypeName.INT32),
    LONG("int64",PrimitiveType.PrimitiveTypeName.INT64),
    STRING("binary",PrimitiveType.PrimitiveTypeName.BINARY),
    FLOAT("float", PrimitiveType.PrimitiveTypeName.FLOAT),
    DOUBLE("double", PrimitiveType.PrimitiveTypeName.DOUBLE),
    BOOLEAN("boolean", PrimitiveType.PrimitiveTypeName.BOOLEAN),
    GROUP("group");
    // TODO: Create more enums to support further data types like BYTE_ARRAY or BINARY.

    private final String parquetSchemaName;

    private PrimitiveType.PrimitiveTypeName parquetPrimitiveType = null;

    ParquetIODataType(String parquetSchemaName, PrimitiveType.PrimitiveTypeName parquetPrimitiveType) {
        this.parquetSchemaName = parquetSchemaName;
        this.parquetPrimitiveType = parquetPrimitiveType;
    }

    ParquetIODataType(String parquetSchemaName) {
        this.parquetSchemaName = parquetSchemaName;
    }

    public String getParquetSchemaName() {
        return parquetSchemaName;
    }

    public PrimitiveType.PrimitiveTypeName getParquetPrimitiveType() {
        return parquetPrimitiveType;
    }
}