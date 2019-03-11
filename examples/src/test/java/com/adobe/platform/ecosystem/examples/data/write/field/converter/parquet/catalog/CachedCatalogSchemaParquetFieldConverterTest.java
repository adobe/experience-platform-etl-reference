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
import com.adobe.platform.ecosystem.examples.data.write.field.converter.parquet.ParquetFieldConverter;
import com.adobe.platform.ecosystem.examples.parquet.model.ParquetIODataType;
import com.adobe.platform.ecosystem.examples.parquet.model.ParquetIOField;
import com.adobe.platform.ecosystem.examples.parquet.model.ParquetIORepetitionType;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Matchers.anyListOf;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

/**
 * @author vedhera on 3/11/2019.
 */
public class CachedCatalogSchemaParquetFieldConverterTest {

    @Mock
    ParquetFieldConverter<List<SchemaField>> fieldConverter;

    private CachedCatalogSchemaParquetFieldConverter cachedConverter;

    @Before
    public void setup() throws ConnectorSDKException {
        initMocks(this);

        when(fieldConverter.convert(anyListOf(SchemaField.class))).thenReturn(getParquetIOFields());

        cachedConverter = new CachedCatalogSchemaParquetFieldConverter(fieldConverter);
    }

    private List<ParquetIOField> getParquetIOFields() {
        final List<ParquetIOField> parquetIOFields = new ArrayList<>();
        parquetIOFields.add(new ParquetIOField(
            "field1",
            ParquetIODataType.STRING,
            ParquetIORepetitionType.OPTIONAL,
            null
        ));
        return parquetIOFields;
    }

    private List<SchemaField> getMockSchemaFields() {
        final List<SchemaField> schemaFields = new ArrayList<>();
        schemaFields.add(new SchemaField(
            "field1",
            DataType.StringType,
            null
        ));
        return schemaFields;
    }

    @Test
    public void TestNonCachedSchemaFieldConversion() throws ConnectorSDKException {
        List<ParquetIOField> parquetFields = cachedConverter.convert(getMockSchemaFields());
        assert (parquetFields.size() == 1);
        verify(fieldConverter, times(1)).convert(anyListOf(SchemaField.class));
        cachedConverter.convert(getMockSchemaFields());
    }

    @Test
    public void TestCachedSchemaFieldConversion() throws ConnectorSDKException {
        cachedConverter.setParquetIOFields(getParquetIOFields());
        List<ParquetIOField> parquetFields = cachedConverter.convert(getMockSchemaFields());
        assert (parquetFields.size() == 1);
        verifyZeroInteractions(fieldConverter);
    }
}
