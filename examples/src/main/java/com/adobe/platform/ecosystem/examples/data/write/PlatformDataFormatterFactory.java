
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
package com.adobe.platform.ecosystem.examples.data.write;

import com.adobe.platform.ecosystem.examples.catalog.model.DataSet;
import com.adobe.platform.ecosystem.examples.catalog.model.DataType;
import com.adobe.platform.ecosystem.examples.catalog.model.SchemaField;
import com.adobe.platform.ecosystem.examples.data.validation.api.ValidationRegistry;
import com.adobe.platform.ecosystem.examples.data.validation.api.ValidationRegistryFactory;
import com.adobe.platform.ecosystem.examples.parquet.write.ParquetIOWriter;
import com.adobe.platform.ecosystem.examples.data.FileFormat;
import com.adobe.platform.ecosystem.examples.data.wiring.DataWiringParam;
import com.adobe.platform.ecosystem.examples.data.write.field.converter.parquet.JSONParquetFieldConverter;
import com.adobe.platform.ecosystem.examples.data.write.field.converter.parquet.ParquetFieldConverter;
import com.adobe.platform.ecosystem.examples.data.write.writer.extractor.Extractor;
import com.adobe.platform.ecosystem.examples.data.write.writer.extractor.JsonObjectsExtractor;
import com.adobe.platform.ecosystem.examples.data.write.writer.formatter.CSVDataFormatter;
import com.adobe.platform.ecosystem.examples.data.write.writer.formatter.JSONDataFormatter;
import com.adobe.platform.ecosystem.examples.data.write.writer.formatter.ParquetDataFormatter;
import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONObject;

import java.util.List;

/**
 * Created by vedhera on 8/25/2017.
 */

/**
 * Factory returning concrete implementation
 * for returning correct formatter based on
 * target type.
 */
public class PlatformDataFormatterFactory implements DataFormatterFactory {

    private ParquetIOWriter writer;

    private DataWiringParam param;

    private final ValidationRegistryFactory<Object> registryFactory;

    public PlatformDataFormatterFactory(ParquetIOWriter writer, DataWiringParam param, ValidationRegistryFactory<Object> registryFactory) {
        this.writer = writer;
        this.param = param;
        this.registryFactory = registryFactory;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Formatter getFormatter(FileFormat fileFormat) {
        Formatter formatter = null;
        switch (fileFormat) {
            case CSV:
                formatter = new CSVDataFormatter(param);
                break;
            case PARQUET:
                final List<SchemaField> fields = getFieldsFromDataSet();
                ParquetFieldConverter converter = new JSONParquetFieldConverter(fields);
                Extractor<JSONObject> extractor = new JsonObjectsExtractor();
                formatter = new ParquetDataFormatter(
                    this.writer,
                    this.param,
                    converter,
                    extractor,
                    getValidationRegistry(fields)
                );
                break;
            case JSON:
                formatter = new JSONDataFormatter();
                break;
            // Do we need to add a default type here?
        }
        return formatter;
    }

    private List<SchemaField> getFieldsFromDataSet() {
        if (!StringUtils.isEmpty(param.getDataSet().getSchema())) {
            return param.getDataSet().getFields(false, DataSet.FieldsFrom.SCHEMA);
        } else {
            return param.getDataSet().getFields(false, DataSet.FieldsFrom.FIELDS);
        }
    }

    private ValidationRegistry<Object> getValidationRegistry(List<SchemaField> fields) {
        final SchemaField rootField = new SchemaField(
            "root",
            DataType.Field_ObjectType,
            fields
        );
        return registryFactory.get(rootField);
    }
}