
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

import java.util.List;

/**
 * POJO to represent metadata which
 * can be later transformed to parquet
 * {@link org.apache.parquet.schema.MessageType} schema.
 *
 * @author vedhera on 2/8/2018
 */
public class ParquetIOField {
    private String name;

    private ParquetIODataType type;

    private ParquetIORepetitionType repetitionType;

    private List<ParquetIOField> subFields;

    public ParquetIOField(String name, ParquetIODataType type,  ParquetIORepetitionType repetitionType, List<ParquetIOField> subFields) {
        this.name = name;
        this.type = type;
        this.repetitionType = repetitionType;
        this.subFields = subFields;
    }

    public String getName() {
        return name;
    }

    public ParquetIODataType getType() {
        return type;
    }

    public boolean isPrimitive() {
        return this.type != ParquetIODataType.GROUP;
    }

    public boolean isRepetetive() {
        return this.repetitionType == ParquetIORepetitionType.REPEATED;
    }

    public ParquetIORepetitionType getRepetitionType() {
        return repetitionType;
    }

    public List<ParquetIOField> getSubFields() {
        return subFields;
    }
}