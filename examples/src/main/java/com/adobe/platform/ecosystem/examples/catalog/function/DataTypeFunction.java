/*
 *  Copyright 2018-2019 Adobe.
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
package com.adobe.platform.ecosystem.examples.catalog.function;

import com.adobe.platform.ecosystem.examples.catalog.model.DataType;

import java.util.function.Function;

/**
 * Util class to provide function
 * for mapping Catalog {@code String}
 * types to SDK's {@code DataType}.
 *
 * @author vedhera 01/07/2019.
 */
public class DataTypeFunction {

    public static Function<String, DataType> primitiveFunction() {
        return (type -> {
            DataType dataType = null;
            switch (type) {
                case "string":
                    dataType = DataType.StringType;
                    break;
                case "long":
                    dataType = DataType.LongType;
                    break;
                case "date":
                    // format, we will start parsing date in same format
                    dataType = DataType.StringType;
                    break;
                case "date-time":
                    dataType = DataType.DateTimeType;
                    break;
                case "integer":
                case "int":
                    dataType = DataType.IntegerType;
                    break;
                case "byte":
                    dataType = DataType.ByteType;
                    break;
                case "short":
                    dataType = DataType.ShortType;
                    break;
                case "number":
                    dataType = DataType.DoubleType;
                    break;
                case "float":
                    dataType = DataType.FloatType;
                    break;
                case "double":
                    dataType = DataType.DoubleType;
                    break;
                case "boolean":
                    dataType = DataType.BooleanType;
                    break;
                case "binary":
                    dataType = DataType.BinaryType;
            }
            return dataType;
        });
    }
}

