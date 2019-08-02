
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
package com.adobe.platform.ecosystem.examples.catalog.model;

/**
 * Created by vedhera on 8/25/2017.
 */
public enum DataType {
    StringType,
    ByteType,
    ShortType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    DateType,
    DateTimeType,
    BooleanType,
    BinaryType,
    JsonArrayType,
    Field_ObjectType,
    Field_ArrayType,
    Field_MapType;

    public static boolean isPrimitiveDataType(DataType type) {
        return  type != null &&
                !type.equals(Field_MapType) &&
                !type.equals(Field_ArrayType) &&
                !type.equals(Field_ObjectType);
    }
}