
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
package com.adobe.platform.ecosystem.examples.data.functions;

import com.adobe.platform.ecosystem.examples.parquet.model.ParquetIODataType;
import com.adobe.platform.ecosystem.examples.catalog.model.DataType;

import java.util.function.Function;

/**
 * Util class to provide function for
 * various conversion operations.
 *
 * @author vedhera on 2/9/2018
 */
public class FieldConverterFunction {

    public static Function<DataType,ParquetIODataType> catalogToParquetFieldFunction = ( catalogDataType -> {
        if(catalogDataType == DataType.StringType) {
           return ParquetIODataType.STRING;
        } else if(catalogDataType == DataType.BooleanType) {
            return ParquetIODataType.BOOLEAN;
        } else if(catalogDataType == DataType.IntegerType) {
            return ParquetIODataType.INTEGER;
        } else if(catalogDataType == DataType.LongType) {
            return ParquetIODataType.LONG;
        } else if(catalogDataType == DataType.FloatType) {
            return ParquetIODataType.FLOAT;
        } else if(catalogDataType == DataType.DoubleType) {
            return ParquetIODataType.DOUBLE;
        } else if(catalogDataType == DataType.DateType) {
            return ParquetIODataType.DATE;
        } else if(catalogDataType == DataType.DateTimeType) {
            return ParquetIODataType.TIMESTAMP;
        } else if(catalogDataType == DataType.Field_ObjectType) {
            return ParquetIODataType.GROUP;
        }
        return null;
    });
}