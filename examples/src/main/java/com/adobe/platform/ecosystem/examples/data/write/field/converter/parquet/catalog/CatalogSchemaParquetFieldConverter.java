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

import com.adobe.platform.ecosystem.examples.catalog.model.DataType;
import com.adobe.platform.ecosystem.examples.catalog.model.SchemaField;
import com.adobe.platform.ecosystem.examples.constants.SDKConstants;
import com.adobe.platform.ecosystem.examples.data.functions.FieldConverterFunction;
import com.adobe.platform.ecosystem.examples.data.write.field.converter.parquet.ParquetFieldConverter;
import com.adobe.platform.ecosystem.examples.parquet.model.ParquetIODataType;
import com.adobe.platform.ecosystem.examples.parquet.model.ParquetIOField;
import com.adobe.platform.ecosystem.examples.parquet.model.ParquetIORepetitionType;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Concrete implementation to convert
 * SchemaFields to ParquetIOFields.
 */
public class CatalogSchemaParquetFieldConverter implements ParquetFieldConverter<List<SchemaField>> {

    @Override
    public List<ParquetIOField> convert(List<SchemaField> data) throws ConnectorSDKException {
        if (data == null) {
            throw new ConnectorSDKException("Schema Fields are null. Please provide schema.");
        }
        List<ParquetIOField> fields = new ArrayList<>();
        for (SchemaField schemaField : data) {
            fields.add(getParquetIOField(schemaField));
        }
        return fields;
    }

    private ParquetIOField getParquetIOField(SchemaField schemaField) throws ConnectorSDKException {
        switch (schemaField.getType()) {
            case Field_MapType:
                return createMapField(schemaField);
            case Field_ArrayType:
                return createArrayField(schemaField);
            case Field_ObjectType:
                return createStructField(schemaField);
            default:
                // Primitive type.
                return new ParquetIOField(
                    schemaField.getName(),
                    FieldConverterFunction.catalogToParquetFieldFunction.apply(schemaField.getType()),
                    ParquetIORepetitionType.OPTIONAL,
                    null
                );
        }
    }

    private ParquetIOField createStructField(SchemaField schemaField) throws ConnectorSDKException {
        return new ParquetIOField(
            schemaField.getName(),
            ParquetIODataType.GROUP,
            ParquetIORepetitionType.OPTIONAL,
            convert(schemaField.getSubFields())
        );
    }

    private ParquetIOField createArrayField(SchemaField schemaField) throws ConnectorSDKException {
        ParquetIOField element = new ParquetIOField(
            "element",
            FieldConverterFunction.catalogToParquetFieldFunction.apply(schemaField.getArraySubType()),
            ParquetIORepetitionType.OPTIONAL,
            schemaField.getArraySubType() == DataType.Field_ObjectType ||
                schemaField.getArraySubType() == DataType.Field_ArrayType ||
                schemaField.getArraySubType() == DataType.Field_MapType ? convert(schemaField.getSubFields()) : null
        );

        ParquetIOField parquetIOField =
            new ParquetIOField(
                schemaField.getName(),
                ParquetIODataType.LIST,
                ParquetIORepetitionType.OPTIONAL,
                Arrays.asList(element)
            );
        return parquetIOField;
    }

    private ParquetIOField createMapField(SchemaField schemaField) throws ConnectorSDKException {
        // Manually creating key as we need to mark it as 'Required'
        final ParquetIOField key = new ParquetIOField(
            SDKConstants.CATALOG_MAP_KEY,
            FieldConverterFunction.catalogToParquetFieldFunction.apply(schemaField.getSubFields().get(0).getType()),
            ParquetIORepetitionType.REQUIRED,
            null
        );

        return new ParquetIOField(
            schemaField.getName(),
            ParquetIODataType.Map,
            ParquetIORepetitionType.OPTIONAL,
            Arrays.asList(
                key, // schemaField will have only 2 sub fields. One for `key`
                getParquetIOField(schemaField.getSubFields().get(1))  // and other `value`.
            )
        );
    }
}
