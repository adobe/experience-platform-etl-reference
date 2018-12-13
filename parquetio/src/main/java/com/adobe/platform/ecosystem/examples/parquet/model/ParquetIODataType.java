
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

import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

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
    BYTE("byte", PrimitiveType.PrimitiveTypeName.INT32),
    SHORT("short", PrimitiveType.PrimitiveTypeName.INT32),
    INTEGER("int32", PrimitiveType.PrimitiveTypeName.INT32),
    LONG("int64", PrimitiveType.PrimitiveTypeName.INT64),
    STRING("string", PrimitiveType.PrimitiveTypeName.BINARY),
    FLOAT("float", PrimitiveType.PrimitiveTypeName.FLOAT),
    DOUBLE("double", PrimitiveType.PrimitiveTypeName.DOUBLE),
    BOOLEAN("boolean", PrimitiveType.PrimitiveTypeName.BOOLEAN),
    DATE("date", PrimitiveType.PrimitiveTypeName.INT32),
    TIMESTAMP("timestamp", PrimitiveType.PrimitiveTypeName.INT64),
    BINARY("binary", PrimitiveType.PrimitiveTypeName.BINARY),
    GROUP("group"),
    LIST("list"),
    Map("map");
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

    public static ParquetIODataType fromType(Type type) {
        for (ParquetIODataType dataType : ParquetIODataType.values()) {
            PrimitiveType.PrimitiveTypeName pName = type.asPrimitiveType().getPrimitiveTypeName();
            if (dataType.getParquetPrimitiveType() == pName) {
                ParquetIODataType returnDataType = dataType;
                if (pName == PrimitiveType.PrimitiveTypeName.BINARY) {
                    returnDataType = ParquetIODataType.BINARY;
                    if (type.asPrimitiveType().getOriginalType() == OriginalType.UTF8) {
                        returnDataType = ParquetIODataType.STRING;
                    }
                } else if (pName == PrimitiveType.PrimitiveTypeName.INT64) {
                    returnDataType = ParquetIODataType.LONG;
                    if (type.asPrimitiveType().getOriginalType() == OriginalType.TIMESTAMP_MILLIS) {
                        returnDataType = ParquetIODataType.TIMESTAMP;
                    }
                } else if (pName == PrimitiveType.PrimitiveTypeName.INT32) {
                    returnDataType = ParquetIODataType.INTEGER;
                    if (type.asPrimitiveType().getOriginalType() == OriginalType.DATE) {
                        returnDataType = ParquetIODataType.DATE;
                    } else if(type.asPrimitiveType().getOriginalType() == OriginalType.INT_16) {
                        returnDataType = ParquetIODataType.SHORT;
                    } else if(type.asPrimitiveType().getOriginalType() == OriginalType.INT_8) {
                        returnDataType = ParquetIODataType.BYTE;
                    }
                }
                return returnDataType;
            }
        }
        return null;
    }

    public String getParquetSchemaName() {
        return parquetSchemaName;
    }

    public PrimitiveType.PrimitiveTypeName getParquetPrimitiveType() {
        return parquetPrimitiveType;
    }
}