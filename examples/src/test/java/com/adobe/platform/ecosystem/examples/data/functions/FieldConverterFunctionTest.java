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
import org.junit.Test;

import java.util.function.Function;

/**
 * @author vedhera om 2/20/2018
 */
public class FieldConverterFunctionTest {
    private final Function<DataType, ParquetIODataType> function = FieldConverterFunction.catalogToParquetFieldFunction;

    @Test
    public void testStringConversion() {
        assert (function.apply(DataType.StringType) == ParquetIODataType.STRING);
    }

    @Test
    public void testBooleanConversion() {
        assert (function.apply(DataType.BooleanType) == ParquetIODataType.BOOLEAN);
    }

    @Test
    public void testByteConversion() {
        assert (function.apply(DataType.ByteType) == ParquetIODataType.BYTE);
    }
    @Test
    public void testShortConversion() {
        assert (function.apply(DataType.ShortType) == ParquetIODataType.SHORT);
    }

    @Test
    public void testIntegerConversion() {
        assert (function.apply(DataType.IntegerType) == ParquetIODataType.INTEGER);
    }

    @Test
    public void testLongConversion() {
        assert (function.apply(DataType.LongType) == ParquetIODataType.LONG);
    }

    @Test
    public void testFloatConversion() {
        assert (function.apply(DataType.FloatType) == ParquetIODataType.FLOAT);
    }

    @Test
    public void testDateConversion() {
        assert (function.apply(DataType.DateType) == ParquetIODataType.DATE);
    }

    @Test
    public void testDateTimeConversion() {
        assert (function.apply(DataType.DateTimeType) == ParquetIODataType.TIMESTAMP);
    }

    @Test
    public void testDoubleConversion() {
        assert (function.apply(DataType.DoubleType) == ParquetIODataType.DOUBLE);
    }

    @Test
    public void testBinaryConversion() {
        assert (function.apply(DataType.BinaryType) == ParquetIODataType.BINARY);
    }

    @Test
    public void testObjectConversion() {
        assert (function.apply(DataType.Field_ObjectType) == ParquetIODataType.GROUP);
    }

    @Test
    public void testInvalidConversion() {
        assert (function.apply(DataType.Field_ArrayType) == null);
    }
}