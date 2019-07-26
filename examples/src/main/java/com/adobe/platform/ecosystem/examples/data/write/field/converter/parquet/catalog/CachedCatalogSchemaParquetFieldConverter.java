/*
 *  Copyright 2019-2020 Adobe.
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
package com.adobe.platform.ecosystem.examples.data.write.field.converter.parquet.catalog;


import com.adobe.platform.ecosystem.examples.catalog.model.SchemaField;
import com.adobe.platform.ecosystem.examples.data.write.field.converter.parquet.ParquetFieldConverter;
import com.adobe.platform.ecosystem.examples.parquet.model.ParquetIOField;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;

import java.util.List;
import java.util.Optional;

/**
 * Cached implementation to convert
 * SchemaFields to ParquetIOFields.
 *
 * @author vedhera.
 */
public class CachedCatalogSchemaParquetFieldConverter implements ParquetFieldConverter<List<SchemaField>> {
    private final ParquetFieldConverter<List<SchemaField>> converter;
    private Optional<List<ParquetIOField>> parquetFields = Optional.empty();


    public CachedCatalogSchemaParquetFieldConverter(ParquetFieldConverter<List<SchemaField>> converter) {
        this.converter = converter;
    }

    @Override
    public List<ParquetIOField> convert(List<SchemaField> data) throws ConnectorSDKException {
        if (!parquetFields.isPresent()) {
            parquetFields = Optional.of(converter.convert(data));
        }
        return parquetFields.get();
    }

   CachedCatalogSchemaParquetFieldConverter setParquetIOFields(List<ParquetIOField> fields) {
        this.parquetFields = Optional.of(fields);
        return  this;
    }
}
