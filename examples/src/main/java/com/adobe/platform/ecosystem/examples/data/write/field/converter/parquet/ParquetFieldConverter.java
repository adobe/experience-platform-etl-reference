
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
package com.adobe.platform.ecosystem.examples.data.write.field.converter.parquet;

import com.adobe.platform.ecosystem.examples.parquet.model.ParquetIOField;

import java.util.List;

/**
 * Interface to convert a given data type to
 * {@link ParquetIOField} to be able to
 * communicate correctly with parquet sdk.
 * @param <T>
 * @author vedhera 2/8/2018.
 */
public interface ParquetFieldConverter<T> {
    /**
     * API to convert a given reference
     * object to List of {@link ParquetIOField}.
     *
     * @param data Data from which {@link ParquetIOField} will be constructed.
     * @return List of parquet-IO fields.
     */
    List<ParquetIOField> convert(T data);
}